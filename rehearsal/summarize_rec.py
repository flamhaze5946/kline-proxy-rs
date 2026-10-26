#!/usr/bin/env python3
"""Quick-look summary of a recorder_evidence.py output directory (sanity check, not the O7
comparator; the gate is P3-S3a `kline-oracle kp-evidence-compare`).

Prints per delta: samples, HTTP status counts, invariant violations, ws_final_trusted/
revised_total range, latency, and the share of trading coins whose latest minute was accepted
(final/absent); per REST form: samples, status, max X-MBX-USED-WEIGHT-1M, how often M matured
(successor row and endTime >= B+300 ms), and whether the three forms agree on M's values.
"""
import glob
import gzip
import json
import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402


def lines(pattern):
    for fn in sorted(glob.glob(pattern)):
        with gzip.open(fn, "rt") as f:
            for ln in f:
                yield json.loads(ln)


def main(d):
    per = defaultdict(lambda: {"n": 0, "status": defaultdict(int), "viol": 0, "acc": [], "lat": [],
                               "trusted": set(), "revised": set()})
    for r in lines(os.path.join(d, "*.jsonl.gz")):
        p = per[r["delta"]]
        p["n"] += 1
        p["status"][str(r.get("http_status", r.get("error", "?")))[:40]] += 1
        if r.get("invariant"):
            p["viol"] += 1
        resp = r.get("resp")
        if r.get("http_status") == 200 and isinstance(resp, dict):
            m = resp["window"]["last_open"]
            tr = [k for k, v in resp["status"].items() if v["state"] == "trading"]
            if tr:
                p["acc"].append(sum(L.minute_state(resp, k, m) in ("final", "absent") for k in tr) / len(tr))
            p["lat"].append(r["elapsed_s"])
            p["trusted"].add(resp["ws_final_trusted"])
            p["revised"].add(resp["revised_total"])
    print("bulk with_evidence samples")
    for delta in sorted(per):
        p = per[delta]
        print("  delta=%-4s n=%-3d status=%s invariant_violations=%d trusted=%s revised_total=%s "
              "accept_latest[min/p50]=%s/%s latency_s[p50/max]=%s/%s" % (
                  delta, p["n"], dict(p["status"]), p["viol"], sorted(p["trusted"]), sorted(p["revised"]),
                  round(min(p["acc"]), 3) if p["acc"] else None, round(statistics.median(p["acc"]), 3) if p["acc"] else None,
                  round(statistics.median(p["lat"]), 3) if p["lat"] else None, max(p["lat"]) if p["lat"] else None))
    forms = defaultdict(lambda: {"n": 0, "status": defaultdict(int), "w": 0, "mature": 0})
    values = defaultdict(dict)
    for r in lines(os.path.join(d, "rest", "*.jsonl.gz")):
        f = forms[r["form"]]
        f["n"] += 1
        f["status"][str(r.get("http_status", r.get("skipped") or r.get("error", "?")))] += 1
        if r.get("weight_1m"):
            f["w"] = max(f["w"], int(r["weight_1m"]))
        rows = r.get("resp") if isinstance(r.get("resp"), list) else []
        m = r["T"] - L.MINUTE
        opens = [x[0] for x in rows]
        if m in opens and any(o > m for o in opens) and r.get("end_time", 0) >= r["T"] + 300:
            f["mature"] += 1
            values[(r["T"], r["delta"], r["symbol"])][r["form"]] = rows[opens.index(m)][1:]
    print("REST probe forms")
    for name in ("em", "st", "cold"):
        f = forms[name]
        print("  %-4s n=%-3d status=%s max_weight_1m=%d M_mature=%d" % (name, f["n"], dict(f["status"]), f["w"], f["mature"]))
    full = [v for v in values.values() if len(v) == 3]
    agree = sum(1 for v in full if v["em"] == v["st"] == v["cold"])
    print("  same-moment triples with M mature in all three forms: %d, values identical: %d" % (len(full), agree))


if __name__ == "__main__":
    main(sys.argv[1])
