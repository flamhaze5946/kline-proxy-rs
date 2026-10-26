#!/usr/bin/env python3
"""O7 recorder for kp-1m evidence mode (extends design/recorder_o7pre.py; stdlib only).

Every UTC minute T:
  * bulk: at T+{--deltas} s one with_evidence POST bulk (closed_only, --limit minutes) for the
    universe, each delta on its own thread (a held request cannot delay the next delta);
  * REST probes: at T+{--probe-deltas} s, for each of a few --probe-symbols, the three request
    forms sent at the same moment through the passthrough proxy (tagged X-Rehearsal-Client:
    recorder, never injected): em (limit=3, endTime=now), st (startTime=M-(st_back+1)m,
    limit=st_back+3, endTime=now), cold (limit=241, endTime=now, no startTime);
  * health: /health/ready once at T+30 s.
Output, gzip per UTC hour under --out:
  YYYYMMDDHH.jsonl.gz           bulk records (recorder_o7pre fields + kind/with_evidence/limit/
                                 http_status/invariant)  -- one line per (T, delta)
  rest/YYYYMMDDHH.jsonl.gz      REST probe records       -- one line per (T, delta, symbol, form)
  health/YYYYMMDDHH.jsonl.gz    /health/ready snapshots
Bounded by --hours. Same-IP REST budget: startup asserts the planned probe weight fits
--recorder-weight-cap and that kp + other + recorder caps fit --ip-budget (default 1800/min,
design v8 section 6); at run time a rolling 60 s counter refuses any probe that would exceed the
cap, and an upstream X-MBX-USED-WEIGHT-1M above --ip-budget stops probing for that minute.
Field reference: RUNBOOK.md "Recording format".
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

FORM_WEIGHT = None  # filled after argument parsing


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--hours", type=float, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--base", default="http://127.0.0.1:1891", help="kp-1m HTTP listener")
    ap.add_argument("--rest-proxy", default="http://127.0.0.1:18980", help="passthrough REST listener")
    ap.add_argument("--deltas", default="0.5,1.5,5")
    ap.add_argument("--limit", type=int, default=3, help="evidence window minutes per bulk (1..100)")
    ap.add_argument("--symbols-file", default=None, help="fixed universe (one symbol per line); default "
                    "TRADING USDT PERPETUAL from exchangeInfo, refreshed every 600 s")
    ap.add_argument("--probe-symbols", default="auto:4", help="comma list or auto:N")
    ap.add_argument("--probe-deltas", default=None, help="default: same as --deltas")
    ap.add_argument("--st-back", type=int, default=2, help="startTime form covers E=M-st_back")
    ap.add_argument("--ip-budget", type=int, default=1800)
    ap.add_argument("--kp-weight-per-minute", type=int, default=1200, help="kp-1m rest.weight_per_minute")
    ap.add_argument("--other-weight", type=int, default=0, help="other processes on the same IP")
    ap.add_argument("--recorder-weight-cap", type=int, default=120)
    ap.add_argument("--health-offset", type=float, default=30.0)
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


class Recorder:
    def __init__(self, a):
        self.a = a
        self.deltas = tuple(float(x) for x in a.deltas.split(","))
        self.pdeltas = tuple(float(x) for x in (a.probe_deltas or a.deltas).split(","))
        for d in (self.deltas, self.pdeltas):
            assert list(d) == sorted(d) and d[-1] < 40, d
        self.bulk_out = L.HourlyGzip(a.out)
        self.rest_out = L.HourlyGzip(os.path.join(a.out, "rest"))
        self.health_out = L.HourlyGzip(os.path.join(a.out, "health"))
        self.budget = L.RollingBudget(a.recorder_weight_cap)
        self.ip_over_minute = None
        self.syms, self.syms_at, self.probes = None, 0.0, []

    def rest(self, path_qs, weight, bypass=False):
        if not self.budget.try_spend(weight):
            return {"budget_skip": True}
        hdr = {"X-Rehearsal-Client": "recorder"}
        if bypass:
            hdr["X-Rehearsal-Bypass-Inject"] = "1"
        t_send = L.now_ms()
        status, parsed, raw, rh, el = L.http_json(self.a.rest_proxy + path_qs, timeout=self.a.timeout, headers=hdr)
        w = rh.get("X-MBX-USED-WEIGHT-1M") or rh.get("x-mbx-used-weight-1m")
        if w is not None and int(w) > self.a.ip_budget:
            self.ip_over_minute = L.minute_floor(L.now_ms())
        return {"t_send_ms": t_send, "t_recv_ms": L.now_ms(), "http_status": status, "weight_1m": w,
                "resp": parsed if parsed is not None else raw[:500].decode("latin-1")}

    def universe(self):
        if self.a.symbols_file:
            with open(self.a.symbols_file) as f:
                return sorted({ln.strip() for ln in f if ln.strip() and not ln.startswith("#")})
        r = self.rest("/fapi/v1/exchangeInfo", 1, bypass=True)
        info = r.get("resp")
        if r.get("http_status") != 200 or not isinstance(info, dict):
            raise IOError("exchangeInfo via proxy failed: %r" % {k: r.get(k) for k in ("http_status", "budget_skip")})
        return sorted(s["symbol"] for s in info.get("symbols", [])
                      if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
                      and s["symbol"].endswith("USDT") and s.get("pair") == s["symbol"])

    def pick_probes(self, syms):
        spec = self.a.probe_symbols
        if not spec.startswith("auto:"):
            return [s for s in spec.split(",") if s]
        n = int(spec[5:])
        fixed = [s for s in ("BTCUSDT", "ETHUSDT") if s in syms][:n]
        day = time.strftime("%Y%m%d", time.gmtime())
        rest = sorted((s for s in syms if s not in fixed), key=lambda s: hashlib.sha1((day + s).encode()).hexdigest())
        return fixed + rest[: n - len(fixed)]

    def bulk_sample(self, T, d, syms, out, k):
        L.sleep_until_ms(int((T + d) * 1000))
        rec = {"kind": "bulk", "T": T * 1000, "delta": d, "n_req": len(syms), "with_evidence": True,
               "limit": self.a.limit}
        try:
            rec["t_send_ms"] = L.now_ms()
            status, parsed, raw, _, el = L.evidence_post(self.a.base, self.a.limit, syms, timeout=self.a.timeout)
            rec["elapsed_s"] = round(el, 3)
            rec["http_status"] = status
            rec["resp"] = parsed if parsed is not None else raw[:500].decode("latin-1")
            if status == 200:
                rec["invariant"] = L.window_violations(parsed, requested=syms, limit=self.a.limit)[:20]
        except Exception as e:
            rec["error"] = repr(e)
        out[k] = rec

    def probe(self, T, d, sym):
        if self.ip_over_minute == T * 1000:
            for form in ("em", "st", "cold"):
                self.rest_out.write({"kind": "rest", "T": T * 1000, "delta": d, "symbol": sym, "form": form,
                                     "skipped": "ip_weight_over_budget"}, T * 1000)
            return
        now = L.now_ms()
        m = T * 1000 - L.MINUTE
        forms = {
            "em": ("/fapi/v1/klines?symbol=%s&interval=1m&endTime=%d&limit=3" % (sym, now), 3),
            "st": ("/fapi/v1/klines?symbol=%s&interval=1m&endTime=%d&limit=%d&startTime=%d"
                   % (sym, now, self.a.st_back + 3, m - (self.a.st_back + 1) * L.MINUTE), self.a.st_back + 3),
            "cold": ("/fapi/v1/klines?symbol=%s&interval=1m&endTime=%d&limit=241" % (sym, now), 241),
        }
        results = {}

        def one(form):
            path, limit = forms[form]
            results[form] = dict(self.rest(path, L.kline_weight(limit)), path=path, end_time=now)

        ths = [threading.Thread(target=one, args=(f,), daemon=True) for f in forms]
        for t in ths:
            t.start()
        for t in ths:
            t.join(self.a.timeout + 5)
        for form in forms:
            rec = {"kind": "rest", "T": T * 1000, "delta": d, "symbol": sym, "form": form}
            rec.update(results.get(form, {"error": "probe thread did not finish"}))
            self.rest_out.write(rec, T * 1000)

    def probe_round(self, T, d, probes):
        L.sleep_until_ms(int((T + d) * 1000))
        ths = [threading.Thread(target=self.probe, args=(T, d, s), daemon=True) for s in probes]
        for t in ths:
            t.start()
        for t in ths:
            t.join(self.a.timeout + 10)

    def health(self, T):
        L.sleep_until_ms(int((T + self.a.health_offset) * 1000))
        status, parsed, raw, _, el = L.http_json(self.a.base + "/health/ready", timeout=10)
        self.health_out.write({"kind": "health", "T": T * 1000, "t_ms": L.now_ms(), "http_status": status,
                               "resp": parsed}, T * 1000)

    def run(self):
        deadline = time.time() + self.a.hours * 3600
        print("start deltas=%s probe_deltas=%s base=%s out=%s deadline=%s" % (
            self.deltas, self.pdeltas, self.a.base, self.a.out,
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(deadline))), flush=True)
        while time.time() < deadline:
            now = time.time()
            T = (int(now) // 60 + 1) * 60
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
            ths = [threading.Thread(target=self.bulk_sample, args=(T, d, list(self.syms), recs, k), daemon=True)
                   for k, d in enumerate(self.deltas)]
            ths += [threading.Thread(target=self.probe_round, args=(T, d, list(self.probes)), daemon=True)
                    for d in self.pdeltas]
            ths.append(threading.Thread(target=self.health, args=(T,), daemon=True))
            for t in ths:
                t.start()
            for t in ths:
                t.join(timeout=max(0.0, T + max(self.deltas[-1], self.a.health_offset) + 25 - time.time()))
            for k, d in enumerate(self.deltas):
                rec = recs[k] or {"kind": "bulk", "T": T * 1000, "delta": d, "n_req": len(self.syms),
                                  "error": "sample thread did not finish"}
                self.bulk_out.write(rec, T * 1000)
        for o in (self.bulk_out, self.rest_out, self.health_out):
            o.close()
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
