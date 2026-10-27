#!/usr/bin/env python3
"""O7 recorder for kp-1m evidence mode, writing P3-S3a recording schema v1 (stdlib only).

Authority for the format: scratch/clones/p3-s3a/HANDOFF-P3-S3a.md (fix2 > fix1 > section 2);
consumer: `kline-oracle kp-evidence-compare`. Every line carries "schema": 1, "kind" and "T"
(minute boundary, epoch ms on the 1m grid). Per UTC minute T:
  * kind "bulk" (2.1) at T+{--deltas} s: POST /fapi/v1/klines/bulk?with_evidence=true&
    closed_only=true&limit=<n>, body {"interval":"1m","limit":n,"closed_only":true,"symbols":[..]};
    fields T, delta, with_evidence=true, limit, req_symbols (exact non-empty list sent), n_req,
    t_send_ms (wall clock just before the POST), elapsed_s, http_status, and exactly one of
    resp_text (raw HTTP body text, 2xx) / error (non-2xx, transport error, unfinished thread).
    Each delta has its own thread, so a held request cannot delay the next delta.
  * kind "rest_probe" (2.2) at T+{--probe-deltas} s: for each probe coin the three forms sent
    concurrently through the passthrough (tag X-Rehearsal-Client: recorder; never injected):
    end_m (limit=3, endTime=now), start_time (startTime=M-(st_back+1)m, limit=st_back+3,
    endTime=now), cold_page (limit=241, endTime=now, no startTime); params, t_send_ms,
    elapsed_s, http_status, used_weight_1m (X-MBX-USED-WEIGHT-1M, IP-wide), rows | error.
  * kind "proxy_stats" (2.3) for minute T, written after T+60 s: kp-1m /health/ready is sampled
    every --stats-period s; fallback_backlog = max fallback_queued over the minute's samples;
    fallback_delay.{in_flight, rate_limited, concurrency_blocked} = sums over the samples of the
    per-pass gauges fallback_in_flight_blocked / fallback_budget_blocked /
    fallback_concurrency_blocked (unit: coin x sample, an increment for this minute only);
    rest_used_weight_1m = max X-MBX-USED-WEIGHT-1M the passthrough saw in the minute
    (GET <proxy-control>/stats), null if unavailable.
Output: <out>/YYYYMMDDHH.jsonl.gz keyed by T (only the three kinds above; the comparator reads
this level only). Bounded by --hours. Same-IP REST budget: startup asserts the planned probe
weight fits --recorder-weight-cap and kp + other + recorder caps fit --ip-budget (<= 1800/min,
design v8 section 6); a rolling 60 s counter refuses probes beyond the cap, and an upstream
X-MBX-USED-WEIGHT-1M above --ip-budget stops probing for that minute.
"""
import argparse
import hashlib
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

SCHEMA = 1
FORMS = ("end_m", "start_time", "cold_page")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--hours", type=float, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--base", default="http://127.0.0.1:1891", help="kp-1m HTTP listener")
    ap.add_argument("--rest-proxy", default="http://127.0.0.1:18980", help="passthrough REST listener")
    ap.add_argument("--proxy-control", default="http://127.0.0.1:18982", help="passthrough control listener")
    ap.add_argument("--deltas", default="0.5,1.5,5")
    ap.add_argument("--limit", type=int, default=3, help="evidence window minutes per bulk (1..100)")
    ap.add_argument("--symbols-file", default=None, help="fixed universe (one symbol per line); default "
                    "TRADING USDT PERPETUAL from exchangeInfo, refreshed every 600 s")
    ap.add_argument("--probe-symbols", default="auto:4", help="comma list or auto:N")
    ap.add_argument("--probe-deltas", default=None, help="default: same as --deltas")
    ap.add_argument("--st-back", type=int, default=2, help="start_time form covers E=M-st_back")
    ap.add_argument("--stats-period", type=float, default=1.0, help="/health sampling period for proxy_stats")
    ap.add_argument("--ip-budget", type=int, default=1800)
    ap.add_argument("--kp-weight-per-minute", type=int, default=1200, help="kp-1m rest.weight_per_minute")
    ap.add_argument("--other-weight", type=int, default=0, help="other processes on the same IP")
    ap.add_argument("--recorder-weight-cap", type=int, default=120)
    ap.add_argument("--timeout", type=float, default=20.0)
    return ap.parse_args(argv)


def planned_weight(n_probe, n_probe_deltas, st_back):
    per = L.kline_weight(3) + L.kline_weight(st_back + 3) + L.kline_weight(241)
    return n_probe * n_probe_deltas * per + 1  # +1: exchangeInfo, at most once per minute


def check_budget(a, n_probe, n_probe_deltas):
    """Hard assertions on the same-IP budget (design v8 section 6). Returns the planned weight."""
    plan = planned_weight(n_probe, n_probe_deltas, a.st_back)
    assert 1 <= a.limit <= 100, "evidence limit must be 1..100"
    assert plan <= a.recorder_weight_cap, "planned probe weight %d/min > recorder cap %d" % (plan, a.recorder_weight_cap)
    total = a.kp_weight_per_minute + a.other_weight + a.recorder_weight_cap
    assert total <= a.ip_budget, "kp %d + other %d + recorder cap %d = %d > same-IP budget %d" % (
        a.kp_weight_per_minute, a.other_weight, a.recorder_weight_cap, total, a.ip_budget)
    assert a.ip_budget <= 1800, "same-IP budget above the design ceiling 1800/min"
    return plan


def bulk_record(T, delta, limit, syms, t_send, elapsed, status, body, error):
    """Schema v1 section 2.1 record; exactly one of resp_text / error (fix1-5, fix2-M3)."""
    assert syms, "req_symbols must not be empty (fix2-M1)"
    rec = {"kind": "bulk", "schema": SCHEMA, "T": T, "delta": delta, "with_evidence": True, "limit": limit,
           "req_symbols": list(syms), "n_req": len(syms)}
    if t_send is not None:
        rec["t_send_ms"] = t_send
    if elapsed is not None:
        rec["elapsed_s"] = round(elapsed, 3)
    if status is not None:
        rec["http_status"] = status
    if error is None and status is not None and 200 <= status < 300:
        rec["resp_text"] = body.decode("utf-8")
    else:
        rec["error"] = error or ("HTTP %s: %s" % (status, body[:300].decode("utf-8", "replace")))
    return rec


class Recorder:
    def __init__(self, a):
        self.a = a
        self.deltas = tuple(float(x) for x in a.deltas.split(","))
        self.pdeltas = tuple(float(x) for x in (a.probe_deltas or a.deltas).split(","))
        for d in (self.deltas, self.pdeltas):
            assert list(d) == sorted(d) and d[-1] < 40, d
        self.out = L.HourlyGzip(a.out)
        self.budget = L.RollingBudget(a.recorder_weight_cap)
        self.ip_over_minute = None
        self.syms, self.syms_at, self.probes = None, 0.0, []
        self.health_samples = {}  # minute T -> list of /health/ready detail dicts
        self.lock = threading.Lock()

    def rest(self, path_qs, weight, bypass=False):
        if not self.budget.try_spend(weight):
            return {"error": "skipped: recorder REST budget"}
        hdr = {"X-Rehearsal-Client": "recorder"}
        if bypass:
            hdr["X-Rehearsal-Bypass-Inject"] = "1"
        t_send = L.now_ms()
        try:
            status, parsed, raw, rh, el = L.http_json(self.a.rest_proxy + path_qs, timeout=self.a.timeout, headers=hdr)
        except Exception as e:
            return {"t_send_ms": t_send, "error": repr(e)}
        w = rh.get("X-MBX-USED-WEIGHT-1M") or rh.get("x-mbx-used-weight-1m")
        w = int(w) if w is not None and str(w).isdigit() else None
        if w is not None and w > self.a.ip_budget:
            self.ip_over_minute = L.minute_floor(L.now_ms())
        out = {"t_send_ms": t_send, "elapsed_s": round(el, 3), "http_status": status, "used_weight_1m": w}
        if status == 200 and parsed is not None:
            out["parsed"] = parsed
        else:
            out["error"] = "HTTP %s: %s" % (status, raw[:300].decode("utf-8", "replace"))
        return out

    def universe(self):
        if self.a.symbols_file:
            with open(self.a.symbols_file) as f:
                syms = sorted({ln.strip() for ln in f if ln.strip() and not ln.startswith("#")})
        else:
            r = self.rest("/fapi/v1/exchangeInfo", 1, bypass=True)
            info = r.get("parsed")
            if not isinstance(info, dict):
                raise IOError("exchangeInfo via proxy failed: %s" % r.get("error"))
            syms = sorted(s["symbol"] for s in info.get("symbols", [])
                          if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
                          and s["symbol"].endswith("USDT") and s.get("pair") == s["symbol"])
        if not syms:
            raise IOError("empty universe; refusing to write bulk records with empty req_symbols")
        return syms

    def pick_probes(self, syms):
        spec = self.a.probe_symbols
        if not spec.startswith("auto:"):
            return [s for s in spec.split(",") if s]
        n = int(spec[5:])
        fixed = [s for s in ("BTCUSDT", "ETHUSDT") if s in syms][:n]
        day = time.strftime("%Y%m%d", time.gmtime())
        rest = sorted((s for s in syms if s not in fixed), key=lambda s: hashlib.sha1((day + s).encode()).hexdigest())
        return fixed + rest[: n - len(fixed)]

    # --- bulk (2.1)
    def bulk_sample(self, T, d, syms, out, k):
        L.sleep_until_ms(T + int(d * 1000))
        t_send = L.now_ms()
        try:
            status, _, raw, _, el = L.evidence_post(self.a.base, self.a.limit, syms, timeout=self.a.timeout)
            out[k] = bulk_record(T, d, self.a.limit, syms, t_send, el, status, raw, None)
        except Exception as e:
            out[k] = bulk_record(T, d, self.a.limit, syms, t_send, None, None, b"", repr(e))

    # --- rest_probe (2.2)
    def probe(self, T, d, sym):
        m = T - L.MINUTE
        if self.ip_over_minute == T:
            for form in FORMS:
                self.out.write({"kind": "rest_probe", "schema": SCHEMA, "T": T, "delta": d, "symbol": sym,
                                "form": form, "params": {}, "error": "skipped: IP weight over budget"}, T)
            return
        now = L.now_ms()
        params = {
            "end_m": {"endTime": now, "limit": 3},
            "start_time": {"startTime": m - (self.a.st_back + 1) * L.MINUTE, "endTime": now, "limit": self.a.st_back + 3},
            "cold_page": {"endTime": now, "limit": 241},
        }
        results = {}

        def one(form):
            p = params[form]
            qs = "/fapi/v1/klines?symbol=%s&interval=1m" % sym + "".join("&%s=%d" % kv for kv in sorted(p.items()))
            results[form] = self.rest(qs, L.kline_weight(p["limit"]))

        ths = [threading.Thread(target=one, args=(f,), daemon=True) for f in FORMS]
        for t in ths:
            t.start()
        for t in ths:
            t.join(self.a.timeout + 5)
        for form in FORMS:
            r = results.get(form, {"error": "probe thread did not finish"})
            rec = {"kind": "rest_probe", "schema": SCHEMA, "T": T, "delta": d, "symbol": sym, "form": form,
                   "params": params[form]}
            for k in ("t_send_ms", "elapsed_s", "http_status", "used_weight_1m"):
                if k in r:
                    rec[k] = r[k]
            if "error" in r or not isinstance(r.get("parsed"), list):
                rec["error"] = r.get("error", "response is not a row array")
            else:
                rec["rows"] = r["parsed"]
            self.out.write(rec, T)

    def probe_round(self, T, d, probes):
        L.sleep_until_ms(T + int(d * 1000))
        ths = [threading.Thread(target=self.probe, args=(T, d, s), daemon=True) for s in probes]
        for t in ths:
            t.start()
        for t in ths:
            t.join(self.a.timeout + 10)

    # --- proxy_stats (2.3)
    def health_sampler(self, deadline):
        while time.time() < deadline + 130:  # the last T can be up to 60 s after the deadline
            t = L.now_ms()
            try:
                status, d, _, _, _ = L.http_json(self.a.base + "/health/ready", timeout=2)
            except Exception:
                d = None
            if isinstance(d, dict):
                with self.lock:
                    self.health_samples.setdefault(L.minute_floor(t), []).append(d)
            time.sleep(max(0.05, self.a.stats_period - (L.now_ms() - t) / 1000.0))

    def proxy_stats(self, T):
        """Summarize minute [T, T+60 s) once it is over."""
        L.sleep_until_ms(T + L.MINUTE + 500)
        with self.lock:
            samples = self.health_samples.pop(T, [])
        weight = None
        try:
            s, st, _, _, _ = L.http_json(self.a.proxy_control + "/stats?from_ms=%d&to_ms=%d" % (T, T + L.MINUTE), timeout=5)
            if s == 200 and isinstance(st, dict) and st.get("max_weight_1m") is not None:
                weight = int(st["max_weight_1m"])
        except Exception:
            pass

        def total(key):
            return int(sum(x.get(key, 0) or 0 for x in samples))
        self.out.write({"kind": "proxy_stats", "schema": SCHEMA, "T": T, "t_ms": L.now_ms(),
                        "fallback_backlog": max([int(x.get("fallback_queued", 0) or 0) for x in samples] or [0]),
                        "rest_used_weight_1m": weight,
                        "fallback_delay": {"in_flight": total("fallback_in_flight_blocked"),
                                           "rate_limited": total("fallback_budget_blocked"),
                                           "concurrency_blocked": total("fallback_concurrency_blocked")}}, T)
        if not samples:
            print("proxy_stats T=%s: no /health samples (counts are 0 by absence)" % L.iso(T), flush=True)

    def run(self):
        deadline = time.time() + self.a.hours * 3600
        print("start deltas=%s probe_deltas=%s base=%s out=%s deadline=%s" % (
            self.deltas, self.pdeltas, self.a.base, self.a.out,
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(deadline))), flush=True)
        threading.Thread(target=self.health_sampler, args=(deadline,), daemon=True).start()
        stats = []
        while time.time() < deadline:
            now = time.time()
            T = (int(now) // 60 + 1) * 60 * 1000
            if self.syms is None or now - self.syms_at > 600:
                try:
                    self.syms, self.syms_at = self.universe(), now
                    self.probes = self.pick_probes(self.syms)
                    check_budget(self.a, len(self.probes), len(self.pdeltas))
                    print("universe %d symbols; probes %s" % (len(self.syms), self.probes), flush=True)
                except AssertionError:
                    raise
                except Exception as e:
                    print("universe error", e, flush=True)
                    time.sleep(5)
                    continue
            recs = [None] * len(self.deltas)
            syms = list(self.syms)
            ths = [threading.Thread(target=self.bulk_sample, args=(T, d, syms, recs, k), daemon=True)
                   for k, d in enumerate(self.deltas)]
            ths += [threading.Thread(target=self.probe_round, args=(T, d, list(self.probes)), daemon=True)
                    for d in self.pdeltas]
            st = threading.Thread(target=self.proxy_stats, args=(T,), daemon=True)
            st.start()
            stats.append(st)
            for t in ths:
                t.start()
            for t in ths:
                t.join(timeout=max(0.0, T / 1000.0 + self.deltas[-1] + 25 - time.time()))
            for k, d in enumerate(self.deltas):
                rec = recs[k] or bulk_record(T, d, self.a.limit, syms, None, None, None, b"", "sample thread did not finish")
                self.out.write(rec, T)
        last_T = T if stats else 0
        for st in stats:  # the last minute's proxy_stats is written at last_T + 60.5 s
            st.join(timeout=max(0.0, last_T / 1000.0 + 75 - time.time()))
        self.out.close()
        print("done", flush=True)


def main():
    a = parse_args()
    os.makedirs(a.out, exist_ok=True)
    n_probe = len(a.probe_symbols.split(",")) if not a.probe_symbols.startswith("auto:") else int(a.probe_symbols[5:])
    plan = check_budget(a, n_probe, len((a.probe_deltas or a.deltas).split(",")))
    print("budget ok: recorder planned %d/min <= cap %d; kp %d + other %d + cap %d <= %d" % (
        plan, a.recorder_weight_cap, a.kp_weight_per_minute, a.other_weight, a.recorder_weight_cap, a.ip_budget),
        flush=True)
    Recorder(a).run()


if __name__ == "__main__":
    main()
