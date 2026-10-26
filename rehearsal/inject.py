#!/usr/bin/env python3
"""O7 injection orchestrator for kp-1m (design v8 section 7, injections 1-13; stdlib only).

  python3 inject.py --scenario 1,5,6,11 --config /etc/kp-1m/kp-1m.json --backend systemd \\
      --unit kp-1m-evidence --binary /opt/kp-1m/evidence/current/kline-proxy --out /var/lib/kp-1m-o7/inject
  python3 inject.py --scenario all --local            # local stack from local_stack.py
  python3 inject.py --list

Each scenario = prepare -> act -> assert -> recover. Asserts read /health/ready, evidence bulk
responses (with_evidence=true) and the passthrough event log (REST exchanges, WS lifecycle and
commands); state-file asserts read <persistence.directory>/{breaker,retained}.json. Every
scenario also checks the evidence window invariant on each sampled response. A scenario result is
PASS, FAIL or INCONCLUSIVE (the precondition the assert needs never happened, e.g. no request
landed in [B, B+300 ms)). Results: <out>/inject-<ts>.json (+ one line per scenario on stdout).
Stop/start uses --backend systemd (systemctl) or process (local_stack.py, local only).
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

NAMES = {
    1: "ws disconnect", 2: "ws resubscribe", 3: "block >=5 active topics before the minute starts",
    4: "ws TCP stall without disconnect", 5: "delay REST visibility of M+1", 6: "exchangeInfo status -> non-TRADING -> back",
    7: "restart with 0.4.24-format snapshot", 8: "cold load, WS first, startup sync released later",
    9: "breaker trip -> restart", 10: "directory change -> history repair -> restart within retention",
    11: "REST request in [B, B+300 ms) keeps M pending", 12: "TRADING restored while stopped",
    13: "retention expires while stopped",
}


class Service:
    def __init__(self, a, cfg_path):
        self.a, self.cfg_path = a, cfg_path

    def stop(self):
        if self.a.backend == "systemd":
            subprocess.check_call(["systemctl", "stop", self.a.unit])
        else:
            import local_stack
            local_stack.stop(self.a.run, "kp-1m")

    def start(self):
        if self.a.backend == "systemd":
            subprocess.check_call(["systemctl", "start", self.a.unit])
        else:
            import local_stack
            local_stack.kp_start(self.a.run)

    def reset_breaker(self):
        argv = [self.a.binary, "--reset-breaker", self.cfg_path]
        if self.a.backend == "systemd" and self.a.service_user:
            argv = ["runuser", "-u", self.a.service_user, "--"] + argv
        p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        return p.returncode, p.stdout[-2000:]


class Ctx:
    def __init__(self, a):
        self.a = a
        with open(a.config) as f:
            self.cfg = json.load(f)
        self.base = "http://" + self.cfg["listen"]
        self.state_dir = self.cfg["persistence"]["directory"]
        if not os.path.isabs(self.state_dir):
            self.state_dir = os.path.join(os.path.dirname(os.path.abspath(a.config)), self.state_dir)
        self.svc = Service(a, a.config)
        self.refresh_s = 0.8 * self.cfg.get("market_api", {}).get("metadata_refresh_seconds", 300)
        self.retention_min = self.cfg["evidence"].get("not_trading_retention_min", 241)
        self.stale_s = self.cfg.get("websocket", {}).get("topic_stale_seconds", 120)
        self.checks, self.violations = [], []

    # --- io
    def ctl(self, path, body=None):
        s, d, raw, _, _ = L.http_json(self.a.control + path, body, timeout=10)
        if s != 200:
            raise RuntimeError("control %s -> %s %r" % (path, s, raw[:200]))
        return d

    def inject(self, **rule):
        r = self.ctl("/inject", rule)
        self.note("inject", rule={k: r[k] for k in r if k in ("id", "kind", "symbols", "from_ms", "until_ms")})
        return r

    def clear(self, rid=None):
        self.ctl("/clear", {} if rid is None else {"id": rid})

    def seq(self):
        return self.ctl("/state")["last_seq"]

    def events(self, since, kinds=None):
        q = "/events?since=%d" % since + ("&kind=" + ",".join(kinds) if kinds else "")
        return self.ctl(q)

    def kp_rest(self, since, sym=None):
        return [e for e in self.events(since, ["rest"]) if e.get("client") == "kp-1m"
                and e.get("path", "").endswith("/klines") and (sym is None or e.get("symbol") == sym)]

    def health(self):
        try:
            s, d, _, _, _ = L.http_json(self.base + "/health/ready", timeout=10)
        except OSError:  # not listening (stopped / starting)
            return None, {}
        return s, d or {}

    def ev(self, symbols=None, limit=5, check=True):
        s, d, raw, _, el = L.http_json(L.evidence_url(self.base, limit, symbols), timeout=30)
        if s == 200 and check:
            req = sorted(set(symbols)) if symbols else None
            v = L.window_violations(d, requested=req, limit=limit)
            if v:
                self.violations.append({"t_ms": L.now_ms(), "violations": v[:5]})
        return s, d

    def trading_symbols(self):
        s, d = self.ev(None, 1)
        if s != 200:
            raise RuntimeError("evidence unavailable: %s" % s)
        return sorted(k for k, v in d["status"].items() if v["state"] == "trading")

    def retained(self):
        try:
            with open(os.path.join(self.state_dir, "retained.json")) as f:
                return {r["symbol"]: r for r in json.load(f)}
        except FileNotFoundError:
            return {}

    def breaker(self):
        with open(os.path.join(self.state_dir, "breaker.json")) as f:
            return json.load(f)

    def snapshot_path(self, sym):
        ident = "future\x001m\x00%s" % sym
        return os.path.join(self.state_dir, hashlib.sha256(ident.encode()).hexdigest() + ".kpr")

    # --- checks
    def note(self, step, **kw):
        kw.update(step=step, t=L.iso(L.now_ms()))
        print("   ", json.dumps(kw, default=str)[:400], flush=True)

    def check(self, name, ok, **detail):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print("    [%s] %s %s" % ("ok" if ok else "FAIL", name, json.dumps(detail, default=str)[:300]), flush=True)
        return ok

    def inconclusive(self, name, **detail):
        """The precondition this assert needs did not occur; neither PASS nor FAIL."""
        self.checks.append({"name": name, "ok": None, "detail": detail})
        print("    [inconclusive] %s %s" % (name, json.dumps(detail, default=str)[:300]), flush=True)

    def wait(self, pred, timeout, step=1.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            v = pred()
            if v:
                return v
            time.sleep(step)
        return None

    def wait_ready(self, timeout=180):
        return self.wait(lambda: self.health()[0] == 200 or None, timeout)

    def wait_evidence(self, timeout=120):
        def ok():
            try:
                return self.ev(None, 1, check=False)[0] == 200 or None
            except Exception:
                return None
        return self.wait(ok, timeout, 0.5)

    def next_boundary(self, offset_s=0.0, min_lead_s=2.0):
        b = L.minute_floor(L.now_ms()) + L.MINUTE
        if b + offset_s * 1000 - L.now_ms() < min_lead_s * 1000:
            b += L.MINUTE
        return b

    def pick(self, n, avoid=("QUIETUSDT", "GAPUSDT", "XAUUSDT")):
        if self.a.symbols:
            return self.a.symbols.split(",")[:n]
        try:
            top = self.ctl("/resolve?spec=top:%d" % (n + len(avoid)))["symbols"]
        except Exception:
            top = []
        top = [s for s in top if s not in avoid]
        if len(top) >= n:
            return top[:n]
        return [s for s in self.trading_symbols() if s not in avoid][:n]

    def rest_backed(self, events, sym, m, before_ms):
        """A completed kp-1m REST response that can mature minute m (successor row in the same
        response and endTime >= close(m)+1+300 ms) exists for sym before `before_ms`."""
        for e in events:
            if e.get("symbol") != sym or e.get("status") != 200 or e.get("t_done_ms", 1 << 62) > before_ms:
                continue
            rows = e.get("rows") or {}
            end = int(e["query"].get("endTime", 0))
            if end < m + L.MINUTE + 300 or not rows.get("n"):
                continue
            opens = rows.get("opens")
            if opens is not None:
                if m in opens and any(o > m for o in opens):
                    return True
            elif rows["first_open"] <= m < rows["last_open"]:
                return True
        return False

    def baseline(self):
        s, h = self.health()
        return h.get("revised_total"), h.get("ws_final_trusted")


def window_states(d, sym, b, minutes):
    return {(b - k * L.MINUTE): L.minute_state(d, sym, b - k * L.MINUTE) for k in range(1, minutes + 1)}


# ------------------------------------------------------------------------------- scenarios
def s1(c):
    rt0, tr0 = c.baseline()
    conns = c.ctl("/state")["conns"]
    seq0 = c.seq()
    b = c.next_boundary(20)
    L.sleep_until_ms(b + 20_000)
    c.inject(kind="ws_disconnect", conn="all")
    t_disc = L.now_ms()
    reopened = c.wait(lambda: len([e for e in c.events(seq0, ["ws_open"])]) >= len(conns) or None, 60)
    c.check("all WS connections re-established within 60 s", reopened, before=len(conns),
            reopened=len(c.events(seq0, ["ws_open"])))
    b1 = L.minute_floor(t_disc) + L.MINUTE  # the minute containing the disconnect closes at b1
    L.sleep_until_ms(b1 + 8_000)
    syms = c.trading_symbols()
    s, d = c.ev(syms, 5)
    bad = {x: window_states(d, x, b1, 2) for x in syms
           if any(v not in ("final", "absent") for v in window_states(d, x, b1, 2).values())}
    c.check("minutes around the disconnect are final/absent at B1+8s", not bad, b1=L.iso(b1), not_final=dict(list(bad.items())[:5]))
    rt, tr = c.baseline()
    c.check("no conflict, trust kept", rt == rt0 and tr, revised_total=(rt0, rt), trusted=tr)


def s2(c):
    rt0, _ = c.baseline()
    syms = c.pick(3)
    seq0 = c.seq()
    conns0 = len(c.events(0, ["ws_open"]))
    t0 = L.now_ms()
    r = c.inject(kind="ws_resubscribe", symbols=syms, until_ms=t0 + (c.stale_s + 90) * 1000)
    sat = c.wait(lambda: c.events(seq0, ["resubscribe_satisfied"]) or None, c.stale_s + 90, 2)
    cmds = [e for e in c.events(seq0, ["ws_cmd"]) if set(syms) & set(e.get("symbols", []))]
    c.check("kp-1m sent UNSUBSCRIBE/SUBSCRIBE for the silent topics", sat and any(e["method"] == "SUBSCRIBE" for e in cmds),
            commands=[(e["method"], e["symbols"][:5]) for e in cmds][:4],
            after_s=round(((sat[0]["t_ms"] if sat else L.now_ms()) - t0) / 1000, 1))
    c.check("no reconnect while resubscribing", len(c.events(seq0, ["ws_open"])) == 0, new_opens=len(c.events(seq0, ["ws_open"])))
    c.clear(r["id"])
    b = c.next_boundary(10)
    L.sleep_until_ms(b + 10_000)
    s, d = c.ev(syms, 5)
    ev = c.kp_rest(seq0)
    unbacked = []
    for x in syms:
        for m, st in window_states(d, x, b, 5).items():
            if m + L.MINUTE <= (sat[0]["t_ms"] if sat else L.now_ms()) and m >= t0 and st == "final" \
                    and not c.rest_backed(ev, x, m, L.now_ms()):
                unbacked.append((x, L.iso(m)))
    c.check("minutes fully inside the block are final only with REST backing", not unbacked, unbacked=unbacked[:5])
    rt, tr = c.baseline()
    c.check("no conflict, trust kept", rt == rt0 and tr, revised_total=(rt0, rt))


def blocked_minute_check(c, syms, b, seq0, label):
    s, d = c.ev(syms, 3)
    m = b - L.MINUTE
    ev = c.kp_rest(seq0)
    states = {x: L.minute_state(d, x, m) for x in syms}
    unbacked = [x for x, st in states.items() if st == "final" and not c.rest_backed(ev, x, m, L.now_ms())]
    c.check("%s: blocked M is final (REST-backed) or pending" % label,
            all(st in ("final", "pending", "missing") for st in states.values()) and not unbacked,
            states=states, unbacked=unbacked)
    return states, ev


def s3(c):
    rt0, _ = c.baseline()
    syms = c.pick(5)
    b = c.next_boundary(-5, 8)
    seq0 = c.seq()
    c.inject(kind="ws_block", symbols=syms, from_ms=b - 5_000, until_ms=b + 10_000)
    L.sleep_until_ms(b + 8_000)
    states, ev = blocked_minute_check(c, syms, b, seq0, "B+8s")
    counts = {x: len([e for e in ev if e["symbol"] == x and b <= e["t_recv_ms"] < b + 6_500]) for x in syms}
    c.check("<=3 designated fallback attempts per coin in [B, B+6.5s)", all(n <= 3 for n in counts.values()), counts=counts)
    L.sleep_until_ms(b + 70_000)
    s, d = c.ev(syms, 3)
    c.check("blocked M final by B+70s", all(L.minute_state(d, x, b - L.MINUTE) in ("final", "absent") for x in syms),
            states={x: L.minute_state(d, x, b - L.MINUTE) for x in syms})
    rt, tr = c.baseline()
    c.check("no conflict, trust kept", rt == rt0 and tr, revised_total=(rt0, rt))


def s4(c):
    rt0, _ = c.baseline()
    seq0 = c.seq()
    b = c.next_boundary(20)
    L.sleep_until_ms(b + 20_000)
    t0 = L.now_ms()
    r = c.inject(kind="ws_stall", conn="all", duration_ms=200_000)
    syms = c.pick(5)
    for k in (1, 2):
        L.sleep_until_ms(b + k * L.MINUTE + 8_000)
        blocked_minute_check(c, syms, b + k * L.MINUTE, seq0, "stall B+%dm+8s" % k)
    closed = c.wait(lambda: [e for e in c.events(seq0, ["ws_close"])] or None, 240, 2)
    c.check("kp-1m dropped the stalled connection (inbound deadline 180 s)", closed,
            after_s=round(((closed[0]["t_ms"] if closed else L.now_ms()) - t0) / 1000, 1),
            reason=closed[0].get("reason") if closed else None)
    c.clear(r["id"])
    reopened = c.wait(lambda: c.events(seq0, ["ws_open"]) or None, 60)
    c.check("reconnected", reopened)
    bn = c.next_boundary(10)
    L.sleep_until_ms(bn + 10_000)
    s, d = c.ev(syms, 5)
    c.check("latest minutes final after recovery", all(L.minute_state(d, x, bn - L.MINUTE) in ("final", "absent") for x in syms))
    rt, tr = c.baseline()
    c.check("no conflict, trust kept", rt == rt0 and tr, revised_total=(rt0, rt))


def probe_each(c, syms, m, timeout=15):
    """One evidence request per coin, concurrently; {sym: [{t_send_ms, t_return_ms, state}]}."""
    out = {x: [] for x in syms}

    def one(x):
        t0 = L.now_ms()
        try:
            s, d = c.ev([x], 2)
        except Exception:
            return
        if s == 200:
            out[x].append({"t_send_ms": t0, "t_return_ms": L.now_ms(), "state": L.minute_state(d, x, m)})
    ths = [threading.Thread(target=one, args=(x,), daemon=True) for x in syms]
    for t in ths:
        t.start()
    for t in ths:
        t.join(timeout)
    return out


def s5(c):
    rt0, _ = c.baseline()
    syms = c.pick(3)
    b = c.next_boundary(-5, 8)
    m = b - L.MINUTE
    seq0 = c.seq()
    c.inject(kind="ws_block", symbols=syms, only_final=True, minute_open=m, from_ms=b - 5_000, until_ms=b + 15_000)
    c.inject(kind="rest_delay_successor", symbols=syms, delay_ms=4_000, from_ms=b - 5_000, until_ms=b + 15_000)
    L.sleep_until_ms(b + 4_500)
    # A bulk sent before B+final_wait_ms is held until M is acceptable (design v8 section 4), so
    # "pending" cannot be read mid-way; the property is that no response carries a final M before
    # the REST response that matured it has completed. One request per coin (the wait is per set).
    held = probe_each(c, syms, m)
    L.sleep_until_ms(b + 7_500)
    s, d = c.ev(syms, 3)
    end = {x: L.minute_state(d, x, m) for x in syms}
    ev = c.kp_rest(seq0)
    per = {}
    for x in syms:
        rs = [e for e in ev if e["symbol"] == x and b <= e["t_recv_ms"] < b + 7_000]
        per[x] = [(round((e["t_recv_ms"] - b) / 1000, 2), bool(e["applied"])) for e in rs]
    c.check("attempts at ~T+1.5/3/6 s (<=3, the first two hidden)", all(
        len(v) == 3 and 1.5 <= v[0][0] < 2.3 and 3.0 <= v[1][0] < 3.8 and 6.0 <= v[2][0] < 6.8
        and v[0][1] and v[1][1] and not v[2][1] for v in per.values()), attempts=per)
    c.check("M final (R) after the third attempt", all(v == "final" for v in end.values())
            and all(c.rest_backed(ev, x, m, L.now_ms()) for x in syms), states=end)
    early = {x: [h for h in held[x] if h["state"] == "final" and not c.rest_backed(ev, x, m, h["t_return_ms"])]
             for x in syms}
    c.check("request sent at B+4.5s returned final only after the maturing REST response", held
            and all(held[x] for x in syms) and not any(early.values()),
            returned_s={x: [(round((h["t_send_ms"] - b) / 1000, 2), round((h["t_return_ms"] - b) / 1000, 2), h["state"])
                            for h in held[x]] for x in syms}, early=early)
    rt, tr = c.baseline()
    c.check("no conflict, trust kept", rt == rt0 and tr, revised_total=(rt0, rt))


def to_not_trading(c, sym):
    t0 = L.now_ms()
    r = c.inject(kind="rest_status", symbols=[sym], status="SETTLING")

    def nt():
        s, d = c.ev([sym], 5)
        return d if s == 200 and d["status"][sym]["state"] == "not_trading" else None
    d = c.wait(nt, 2 * c.refresh_s + 60, 2)
    return r, t0, d


def back_to_trading(c, sym, rid):
    c.clear(rid)

    def tr():
        s, d = c.ev([sym], 5)
        return d if s == 200 and d["status"][sym]["state"] == "trading" else None
    return c.wait(tr, 2 * c.refresh_s + 60, 2)


def s6(c):
    sym = c.pick(1)[0] if not c.a.symbols else c.a.symbols.split(",")[0]
    seq0 = c.seq()
    r, t0, d = to_not_trading(c, sym)
    if not c.check("coin reported not_trading{since}", d is not None):
        c.clear(r["id"])
        return
    since = d["status"][sym]["since"]
    rec = c.retained().get(sym)
    c.check("since within [inject, observed] and retained record written", t0 - 1000 <= since <= L.now_ms() and rec
            and rec["since"] == since and rec["expires_ms"] == since + c.retention_min * L.MINUTE, since=L.iso(since), record=rec)
    c.check("history retained", d["covered_from"][sym] is not None and len(d["klines"][sym]) > 0,
            covered_from=d["covered_from"][sym], rows=len(d["klines"][sym]))
    unsub = c.wait(lambda: [e for e in c.events(seq0, ["ws_open"]) if sym not in e["symbols"]] or None, 30)
    c.check("WS resubscribed without the coin", unsub)
    time.sleep(40)
    s, d = c.ev([sym], 10)
    before = {L.iso(t): st for t, st in window_states(d, sym, L.minute_floor(since) + L.MINUTE, 10).items()
              if t + L.MINUTE <= since}
    c.check("minutes before since repaired (final/absent)", all(v in ("final", "absent", "uncovered") for v in before.values()),
            states=before)
    seq1 = c.seq()
    d2 = back_to_trading(c, sym, r["id"])
    c.check("restored to trading", d2 is not None)
    c.check("old retained record revoked", sym not in c.retained())
    resub = c.wait(lambda: [e for e in c.events(seq1, ["ws_open"]) if sym in e["symbols"]] or None, 30)
    c.check("subscription restored", resub)
    expiry = rec["expires_ms"] if rec else None
    if expiry and (expiry - L.now_ms()) / 1000 <= c.a.max_wait_s:
        # covered_from slides with the 241-minute window, so it cannot show a cleanup. A cleanup
        # (catalog removal + re-add) would show as a new startup-form sync (limit=241, no startTime)
        # and a subscription change for the coin around the original expiry.
        seq_w5 = c.seq()
        L.sleep_until_ms(expiry + int(c.refresh_s * 1000) + 10_000)
        s, d3 = c.ev([sym], 10)
        restarts = [e["query"] for e in c.kp_rest(seq_w5, sym) if e["query"].get("limit") == "241"
                    and "startTime" not in e["query"]]
        resubs = [e["conn"] for e in c.events(seq_w5, ["ws_open", "ws_close"])]
        c.check("W5: no cleanup at the original expiry", d3["status"][sym]["state"] == "trading"
                and not restarts and not resubs and os.path.exists(c.snapshot_path(sym)) and sym not in c.retained(),
                startup_syncs=len(restarts), ws_changes=len(resubs), snapshot=os.path.exists(c.snapshot_path(sym)))
    else:
        c.inconclusive("W5 original-expiry check deferred (expiry beyond --max-wait-s)", expiry=L.iso(expiry or 0),
                hint="re-run with --max-wait-s >= retention or check retained.json/covered_from at that time")


def wait_snapshots(c):
    return [f for f in os.listdir(c.state_dir) if f.endswith(".kpr")]


def bridge_time(c, t_start, timeout=240):
    def done():
        try:
            s, d = c.ev(None, 100, check=False)
        except Exception:
            return None
        if s != 200:
            return None
        b = d["window"]["last_open"]
        syms = [k for k, v in d["status"].items() if v["state"] == "trading"]
        bad = [x for x in syms if not L.bridge_complete(d, x, d["window"]["first_open"], b)]
        return (L.now_ms(), d, syms) if syms and not bad else None
    return c.wait(done, timeout, 1.0)


def startup_shapes(c, seq0):
    """Per coin, the first successful request with limit >= 100 after start. Fallback (E<M) and
    routine requests are <= 99 rows, so only a startup sync or a cold page can be that large; W1
    requires it to be limit=241 without startTime. Returns ({sym: query} whole-window, {sym: query}
    narrowed). A narrowed startup with limit <= 99 is not distinguishable from a fallback here; the
    P3-S1 unit test startup_is_whole_window_even_when_ws_arrives_first_* covers that shape."""
    full, bad = {}, {}
    for e in c.kp_rest(seq0):
        q, x = e["query"], e["symbol"]
        if e.get("status") != 200 or int(q.get("limit", 0)) < 100 or x in full or x in bad:
            continue
        (full if q.get("limit") == "241" and "startTime" not in q else bad)[x] = q
    return full, bad


def s7(c):
    c.note("snapshots before stop", n=len(wait_snapshots(c)))
    c.svc.stop()
    seq0 = c.seq()
    # The restart loads what the shutdown dump wrote (0.4.24 format, design v8 section 5).
    c.check("0.4.24-format snapshots present for the restart (written on shutdown)", wait_snapshots(c),
            n=len(wait_snapshots(c)))
    t0 = L.now_ms()
    c.svc.start()
    got = bridge_time(c, t0, 240)
    c.check("100-minute bridge complete within 120 s of start", got and got[0] - t0 <= 120_000,
            seconds=round((got[0] - t0) / 1000, 1) if got else None, coins=len(got[2]) if got else 0)
    first, bad = startup_shapes(c, seq0)
    trading = c.trading_symbols()
    c.check("every trading coin's startup sync is the whole window (limit=241, no startTime)",
            first and not bad and set(trading) <= set(first), coins=len(first), trading=len(trading),
            bad=dict(list(bad.items())[:3]), without=sorted(set(trading) - set(first))[:5])


def s8(c):
    c.svc.stop()
    seq0 = c.seq()
    backup = os.path.join(c.state_dir, "cold-backup-%d" % L.now_ms())
    os.makedirs(backup)
    for f in wait_snapshots(c):
        os.rename(os.path.join(c.state_dir, f), os.path.join(backup, f))
    c.note("cold", moved_to=backup)
    t0 = L.now_ms()
    r = c.inject(kind="rest_error", path="/fapi/v1/klines", code=503, from_ms=t0, until_ms=t0 + 20_000)
    c.svc.start()
    got = bridge_time(c, t0 + 20_000, 260)
    opens = [e for e in c.events(seq0, ["ws_open"])]
    ok200 = [e for e in c.kp_rest(seq0) if e.get("status") == 200]
    c.check("WS delivered before the first startup sync was released", opens and ok200
            and opens[0]["t_ms"] < ok200[0]["t_recv_ms"],
            ws_open=L.iso(opens[0]["t_ms"]) if opens else None, first_200=L.iso(ok200[0]["t_recv_ms"]) if ok200 else None)
    first, bad = startup_shapes(c, seq0)
    trading = c.trading_symbols()
    c.check("startup sync after WS is still the whole window", first and not bad and set(trading) <= set(first),
            coins=len(first), trading=len(trading), bad=dict(list(bad.items())[:3]))
    c.check("bridge complete within 120 s after release", got and got[0] - (t0 + 20_000) <= 120_000,
            seconds=round((got[0] - t0 - 20_000) / 1000, 1) if got else None)
    c.clear(r["id"])


def s9(c):
    rt0, tr0 = c.baseline()
    sym = c.pick(1)[0]
    b = c.next_boundary(-5, 8)
    rm = c.inject(kind="rest_mutate", symbols=[sym], minutes_back=2, delta=1, from_ms=b - 5_000, until_ms=b + 20_000)
    rb = c.inject(kind="ws_block", symbols=[sym], only_final=True, minute_open=b - L.MINUTE, from_ms=b - 5_000, until_ms=b + 10_000)
    tripped = c.wait(lambda: c.health()[1].get("ws_final_trusted") is False or None, 90, 1)
    s, h = c.health()
    c.check("conflict on a W minute trips the breaker", tripped and h.get("revised_total", 0) > (rt0 or 0),
            revised_total=(rt0, h.get("revised_total")))
    s, d = c.ev([sym], 3)
    c.check("evidence responses carry ws_final_trusted=false", s == 200 and d["ws_final_trusted"] is False)
    c.clear(rm["id"])
    c.clear(rb["id"])
    br = c.wait(lambda: c.breaker().get("last_conflict_ms", 0) > 0 and c.breaker() or None, 30)
    c.check("breaker.json persisted last_conflict_ms", br, breaker=br)
    c.svc.stop()
    c.svc.start()
    c.wait_evidence(120)
    c.wait_ready(180)
    s, h = c.health()
    c.check("still tripped after restart", h.get("ws_final_trusted") is False, health_trusted=h.get("ws_final_trusted"))
    if c.a.keep_tripped:
        return
    total = h.get("revised_total")
    c.svc.stop()
    rc, out = c.svc.reset_breaker()
    c.check("offline --reset-breaker succeeded", rc == 0, output=out.strip()[-200:])
    c.svc.start()
    c.wait_ready(180)
    s, h = c.health()
    c.check("trusted again, revised_total preserved", h.get("ws_final_trusted") is True and h.get("revised_total") == total,
            revised_total=(total, h.get("revised_total")))


def s10(c):
    sym = c.pick(1)[0]
    r, t0, d = to_not_trading(c, sym)
    if not c.check("coin reported not_trading", d is not None):
        c.clear(r["id"])
        return
    since, rec = d["status"][sym]["since"], c.retained().get(sym)
    time.sleep(30)
    c.svc.stop()
    c.svc.start()
    c.wait_evidence(120)
    s, d2 = c.ev([sym], 5)
    rec2 = c.retained().get(sym)
    c.check("after restart: still not_trading, since and expiry unchanged",
            s == 200 and d2["status"][sym].get("state") == "not_trading" and d2["status"][sym].get("since") == since
            and rec2 == rec, status=d2["status"].get(sym) if s == 200 else s, record=(rec, rec2))
    back_to_trading(c, sym, r["id"])


def s11(c):
    """Startup sync timed across a boundary: exchangeInfo is held until shortly before B (< 5 s hold),
    so the first startup requests are built in [B, B+300 ms); M's WS x=true frame is dropped for every
    coin. Self-calibrating: up to --s11-attempts restarts, shifting the release by the observed
    startup latency (local 33 coins: ~320 ms from release to the first startup request)."""
    offset = c.a.s11_release_offset_ms
    for attempt in range(c.a.s11_attempts):
        ends = s11_attempt(c, offset)
        if ends is None:
            return
        c.note("s11 calibrate", attempt=attempt, release_offset_ms=offset, first_ends_ms_after_B=ends[:5])
        if not ends:
            break
        offset -= ends[0] - 120
    c.inconclusive("a request landed in [B, B+300 ms) with M+1 present", last_release_offset_ms=offset)


def s11_attempt(c, release_offset_ms):
    c.svc.stop()
    b = c.next_boundary(-8, 10)
    m = b - L.MINUTE
    seq0 = c.seq()
    c.inject(kind="ws_block", symbols="all", only_final=True, minute_open=m, from_ms=b - 8_000, until_ms=b + 3_000)
    c.inject(kind="rest_hold", path="/fapi/v1/exchangeInfo", release_ms=b + release_offset_ms, from_ms=b - 8_000, until_ms=b + 1_000)
    # kp-1m waits at most 5 s for exchangeInfo on a cold start (metadata.rs QUERY_BUDGET) and exits
    # otherwise, so the hold must stay well below that: start 2 s before B, release near B.
    L.sleep_until_ms(b - 2_000)
    c.svc.start()
    held, launched = {}, set()

    def watch():
        # As soon as a [B, B+300 ms) response with M+1 lands for a coin, send that coin its own
        # evidence request; record when it returns and with which state for M.
        while L.now_ms() < b + 1_400:
            for e in c.kp_rest(seq0):
                end = int(e["query"].get("endTime", 0))
                rows = e.get("rows") or {}
                x = e["symbol"]
                if (x not in launched and e.get("status") == 200 and b <= end < b + 300
                        and rows.get("n") and rows["last_open"] >= b):
                    launched.add(x)
                    threading.Thread(target=lambda x=x: held.update(probe_each(c, [x], m)), daemon=True).start()
            time.sleep(0.03)
    t = threading.Thread(target=watch, daemon=True)
    t.start()
    t.join(15)
    time.sleep(12)
    ev = c.kp_rest(seq0)
    if not launched:
        c.clear()
        c.wait_ready(180)
        return sorted(int(e["query"]["endTime"]) - b for e in ev if "endTime" in e["query"])
    viol, detail = [], {}
    for x in sorted(launched):
        for h in held.get(x, []):
            q = [e for e in ev if e["symbol"] == x and e.get("status") == 200 and int(e["query"].get("endTime", 0)) >= b + 300
                 and e["t_done_ms"] <= h["t_return_ms"]]
            detail[x] = (round((h["t_send_ms"] - b) / 1000, 3), round((h["t_return_ms"] - b) / 1000, 3), h["state"], len(q))
            if h["state"] == "final" and not c.rest_backed(ev, x, m, h["t_return_ms"]):
                viol.append(x)
    c.check("M not final until a qualifying (endTime >= B+300 ms) REST response", held and not viol,
            early_coins=len(launched), probes=detail, violations=viol)
    c.clear()
    c.wait_ready(180)
    return None


def s12(c):
    sym = c.pick(1)[0]
    r, t0, d = to_not_trading(c, sym)
    if not c.check("coin reported not_trading", d is not None):
        c.clear(r["id"])
        return
    c.svc.stop()
    c.clear(r["id"])
    seq0 = c.seq()
    c.svc.start()
    c.wait_evidence(120)
    s, d2 = c.ev([sym], 5)
    c.check("trading after restart, record revoked", s == 200 and d2["status"][sym]["state"] == "trading"
            and sym not in c.retained(), status=d2["status"].get(sym) if s == 200 else s)
    sub = c.wait(lambda: [e for e in c.events(seq0, ["ws_open"]) if sym in e["symbols"]] or None, 60)
    c.check("subscription restored", sub)


def s13(c):
    sym = c.pick(1)[0]
    r, t0, d = to_not_trading(c, sym)
    if not c.check("coin reported not_trading", d is not None):
        c.clear(r["id"])
        return
    rec = c.retained().get(sym)
    wait_s = (rec["expires_ms"] - L.now_ms()) / 1000 + 5 if rec else 0
    if not rec or wait_s > c.a.max_wait_s:
        c.inconclusive("expiry reachable within --max-wait-s", record=rec, wait_s=round(wait_s))
        back_to_trading(c, sym, r["id"])
        return
    snap_before = os.path.exists(c.snapshot_path(sym))
    c.svc.stop()
    L.sleep_until_ms(rec["expires_ms"] + 5_000)
    c.svc.start()
    c.wait_evidence(120)
    s, d2 = c.ev([sym], 5)
    c.check("unknown after expiry during downtime", s == 200 and d2["status"][sym]["state"] == "unknown",
            status=d2["status"].get(sym) if s == 200 else s)
    c.check("snapshot, retained record cleared", not os.path.exists(c.snapshot_path(sym)) and sym not in c.retained(),
            snapshot_before=snap_before, snapshot_after=os.path.exists(c.snapshot_path(sym)))
    time.sleep(c.refresh_s + 10)
    s, d3 = c.ev([sym], 5)
    c.check("retention not restarted after another directory refresh", d3["status"][sym]["state"] == "unknown"
            and sym not in c.retained())
    back_to_trading(c, sym, r["id"])


SCENARIOS = {1: s1, 2: s2, 3: s3, 4: s4, 5: s5, 6: s6, 7: s7, 8: s8, 9: s9, 10: s10, 11: s11, 12: s12, 13: s13}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--scenario", default=None, help="comma list of 1..13 or 'all'")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--local", action="store_true", help="local_stack.py defaults (process backend, loopback ports)")
    ap.add_argument("--config", default=None, help="kp-1m config JSON (listen, persistence.directory, ...)")
    ap.add_argument("--control", default="http://127.0.0.1:18982", help="passthrough control listener")
    ap.add_argument("--backend", choices=["systemd", "process"], default="systemd")
    ap.add_argument("--unit", default="kp-1m-evidence")
    ap.add_argument("--binary", default="/opt/kp-1m/evidence/current/kline-proxy")
    ap.add_argument("--service-user", default="kp1m")
    ap.add_argument("--run", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "local-run"))
    ap.add_argument("--symbols", default=None, help="override the coin pick (comma list)")
    ap.add_argument("--max-wait-s", type=float, default=900, help="longest wait for retention expiry asserts")
    ap.add_argument("--s11-release-offset-ms", type=int, default=-250,
                    help="11: exchangeInfo release relative to B (calibrated per attempt)")
    ap.add_argument("--s11-attempts", type=int, default=3)
    ap.add_argument("--keep-tripped", action="store_true", help="9: leave the breaker tripped (no offline reset)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if a.list:
        for k, v in NAMES.items():
            print("%2d  %s" % (k, v))
        return 0
    if a.local:
        import local_stack
        a.backend = "process"
        a.config = a.config or os.path.join(a.run, "kp-1m-local.json")
        a.binary = local_stack.BIN
        a.service_user = None
    assert a.config and a.scenario, "--config and --scenario are required"
    ids = list(SCENARIOS) if a.scenario == "all" else [int(x) for x in a.scenario.split(",")]
    out = a.out or os.path.join(os.path.dirname(os.path.abspath(a.config)), "inject")
    os.makedirs(out, exist_ok=True)
    results = []
    for n in ids:
        c = Ctx(a)
        print("== scenario %d: %s (%s)" % (n, NAMES[n], L.iso(L.now_ms())), flush=True)
        c.wait_ready(180)
        t0 = L.now_ms()
        try:
            SCENARIOS[n](c)
            err = None
        except Exception as e:  # an exception is a FAIL with the reason recorded
            err = repr(e)
            print("    exception:", err, flush=True)
        finally:
            try:
                c.clear()
            except Exception:
                pass
        if c.violations:
            c.check("evidence window invariant on every sample", False, violations=c.violations[:3])
        oks = [x["ok"] for x in c.checks]
        verdict = "FAIL" if err or False in oks or not oks else ("INCONCLUSIVE" if None in oks else "PASS")
        results.append({"scenario": n, "name": NAMES[n], "verdict": verdict, "error": err, "t_start": L.iso(t0),
                        "t_end": L.iso(L.now_ms()), "checks": c.checks})
        print("== scenario %d %s" % (n, verdict), flush=True)
        c.wait_ready(180)
    path = os.path.join(out, "inject-%s.json" % time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    with open(path, "w") as f:
        json.dump(results, f, indent=1, default=str)
    print("results:", path)
    return 1 if any(r["verdict"] == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
