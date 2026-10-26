#!/usr/bin/env python3
"""REST re-pull producer for O7 G-b condition 3 (P3-S3a HANDOFF fix1-6 + fix2-L1; stdlib only).

Reads a recording directory (schema v1 bulk records, `resp_text` bodies), collects every
accepted absent minute G (an entry of a response's `absent` map), and for each run of
consecutive absent minutes of one symbol (split at UTC hour boundaries) sends ONE REST request
whose rows should have real rows on both sides:
    GET /fapi/v1/klines?symbol=S&interval=1m&startTime=G1-2m&endTime=Gk+2m&limit=k+4
If the reply lacks a row before G1 or after Gk, it re-requests once with +-30 minutes (still
limit <= 99, weight 1). Conditions enforced here (the comparator enforces them again):
  * run only after the recording, and only for G whose UTC day has ended: t_send_ms >= end of
    G's UTC day (minutes whose day has not ended are listed and skipped);
  * endTime >= G + 1m + 300 ms for every G in the request (Gk + 2m satisfies it).
Output: <out>/repull_<YYYYMMDDHH>.jsonl.gz (hour of the proved minutes), one line per request:
  {"kind":"rest_repull","schema":1,"symbol":S,"params":{"startTime","endTime","limit"},
   "t_send_ms":..,"http_status":..,"for_minutes":[..],"rows":[..]}  (or "error" instead of rows)
Same-IP budget: --weight-cap per rolling minute (default 120) and the start-up assertion
kp + other + cap <= --ip-budget (<= 1800), as the recorder. Requests go through --rest-base
(default the VPS passthrough, tagged client "repull", never injected).
"""
import argparse
import glob
import gzip
import json
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

DAY = 86_400_000


def day_end(ms):
    return ms - ms % DAY + DAY


def absent_minutes(recording):
    """{symbol: sorted absent minutes} from every bulk record with a 2xx resp_text."""
    out = defaultdict(set)
    files = sorted(glob.glob(os.path.join(recording, "*.jsonl.gz")) + glob.glob(os.path.join(recording, "*.jsonl")))
    for fn in files:
        opener = gzip.open if fn.endswith(".gz") else open
        with opener(fn, "rt") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                if rec.get("kind") != "bulk" or "resp_text" not in rec:
                    continue
                resp = json.loads(rec["resp_text"])
                for sym, mins in resp.get("absent", {}).items():
                    out[sym].update(mins)
    return {s: sorted(v) for s, v in out.items() if v}


def runs(minutes):
    """Consecutive runs, split at UTC hour boundaries (one output file per proved hour)."""
    out, cur = [], []
    for g in minutes:
        if cur and (g != cur[-1] + L.MINUTE or g // 3_600_000 != cur[0] // 3_600_000 or len(cur) >= 35):  # pad 30 keeps limit <= 99
            out.append(cur)
            cur = []
        cur.append(g)
    if cur:
        out.append(cur)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--recording", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rest-base", default="http://127.0.0.1:18980",
                    help="passthrough REST listener (or https://fapi.binance.com)")
    ap.add_argument("--weight-cap", type=int, default=120)
    ap.add_argument("--ip-budget", type=int, default=1800)
    ap.add_argument("--kp-weight-per-minute", type=int, default=1200, help="0 if kp-1m is stopped")
    ap.add_argument("--other-weight", type=int, default=0)
    ap.add_argument("--local-format-test", action="store_true",
                    help="LOCAL MOCK ONLY: also send for minutes whose UTC day has not ended (format check; "
                         "the comparator treats such records as no evidence)")
    a = ap.parse_args()
    total = a.kp_weight_per_minute + a.other_weight + a.weight_cap
    assert total <= a.ip_budget <= 1800, "kp %d + other %d + repull cap %d = %d > budget %d" % (
        a.kp_weight_per_minute, a.other_weight, a.weight_cap, total, a.ip_budget)
    if a.local_format_test:
        assert a.rest_base.startswith("http://127.0.0.1:"), "--local-format-test is for the loopback mock only"
    budget = L.RollingBudget(a.weight_cap)
    os.makedirs(a.out, exist_ok=True)
    files = {}
    targets = absent_minutes(a.recording)
    sent = skipped = 0
    for sym in sorted(targets):
        for run in runs(targets[sym]):
            if L.now_ms() < day_end(run[-1]) and not a.local_format_test:
                print("skip %s %s..%s: UTC day not over" % (sym, L.iso(run[0]), L.iso(run[-1])), flush=True)
                skipped += len(run)
                continue
            for pad in (2, 30):
                start, end = run[0] - pad * L.MINUTE, run[-1] + pad * L.MINUTE
                limit = len(run) + 2 * pad
                assert limit <= 99 and all(end >= g + L.MINUTE + 300 for g in run)
                while not budget.try_spend(1):
                    time.sleep(0.5)
                qs = "/fapi/v1/klines?symbol=%s&interval=1m&startTime=%d&endTime=%d&limit=%d" % (sym, start, end, limit)
                t_send = L.now_ms()
                rec = {"kind": "rest_repull", "schema": 1, "symbol": sym,
                       "params": {"startTime": start, "endTime": end, "limit": limit},
                       "t_send_ms": t_send, "for_minutes": run}
                try:
                    status, parsed, raw, _, _ = L.http_json(a.rest_base + qs, timeout=20, headers={
                        "X-Rehearsal-Client": "repull", "X-Rehearsal-Bypass-Inject": "1"})
                    rec["http_status"] = status
                    if status == 200 and isinstance(parsed, list):
                        rec["rows"] = parsed
                    else:
                        rec["error"] = "HTTP %s: %s" % (status, raw[:300].decode("utf-8", "replace"))
                except Exception as e:
                    rec["error"] = repr(e)
                hour = time.strftime("%Y%m%d%H", time.gmtime(run[0] / 1000))
                if hour not in files:
                    files[hour] = gzip.open(os.path.join(a.out, "repull_%s.jsonl.gz" % hour), "at")
                files[hour].write(json.dumps(rec, separators=(",", ":")) + "\n")
                sent += 1
                opens = [r[0] for r in rec.get("rows", [])]
                if any(o < run[0] for o in opens) and any(o > run[-1] for o in opens):
                    break  # both sides have real rows
    for fh in files.values():
        fh.close()
    print("repull: %d symbols, %d requests, %d minutes skipped (day not over), files %s" % (
        len(targets), sent, skipped, sorted(files)), flush=True)


if __name__ == "__main__":
    main()
