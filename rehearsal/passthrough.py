#!/usr/bin/env python3
"""kp-1m rehearsal passthrough proxy: REST + WS relay with injection hooks (stdlib only).

kp-1m points `rest.future_url` at --rest-listen and `websocket.future_url` at
ws://<--ws-listen>/market/stream; this process relays to --rest-upstream / --ws-upstream
(Binance on the VPS, mock_upstream.py locally). kp-1m code is not changed.

With no injection active it is a transparent relay that only logs: every REST exchange
(path, query, client tag, status, X-MBX-USED-WEIGHT-1M, row summary, timing) and WS connection
lifecycle, client SUBSCRIBE/UNSUBSCRIBE commands and per-minute frame counts.

Injections (POST /inject on --control-listen, JSON {"kind": ..., ...}; see INJECTIONS below)
are time-bounded (from_ms/until_ms or count) and recorded as events. GET /events?since=SEQ
returns the in-memory event ring (REST entries included) for inject.py asserts.

Requests carrying `X-Rehearsal-Client: <name>` are tagged (the header is not forwarded); REST
injections apply only to client "kp-1m" unless a rule names other clients.
`X-Rehearsal-Bypass-Inject: 1` forwards untouched (the recorder's exchangeInfo uses it).
"""
import argparse
import asyncio
import http.client
import itertools
import json
import os
import socket
import ssl
import sys
import threading
import time
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

INJECTIONS = {
    # REST, applied before forwarding
    "rest_error": "respond `code` (default 503) with a Binance-style body; optional count",
    "rest_timeout": "sleep `sleep_ms` (default 20000, > kp-1m transport timeout 15 s) then forward",
    "rest_stall": "hold the connection open without a response for `hold_ms`, then close it",
    "rest_hold": "hold matching requests until `release_ms` (wall clock), then forward",
    # REST, applied to the upstream response
    "rest_delay_successor": "hide rows with open_time >= B for the first `delay_ms` of each minute",
    "rest_status": "rewrite exchangeInfo `status` of `symbols` (or drop them with remove=true)",
    "rest_mutate": "add `delta` cents to close of the row `minutes_back` before B (conflict probe)",
    # WS, upstream -> kp-1m direction
    "ws_block": "drop frames of `symbols` topics in [from_ms, until_ms); only_final/minute_open filters",
    "ws_resubscribe": "drop frames of `symbols` until kp-1m sends SUBSCRIBE for them (or until_ms)",
    "ws_stall": "stop relaying upstream->kp-1m on matching connections for `duration_ms` (TCP stays open)",
    "ws_disconnect": "abort matching connections (both sides) now",
}

LOCK = threading.RLock()
RULES = []
RULE_ID = itertools.count(1)
EVENTS = deque(maxlen=50_000)  # ~hours of kp-1m REST; files keep everything
SEQ = itertools.count(1)
ACTIVITY = defaultdict(deque)  # symbol -> deque[(minute, quote_volume)]
CONNS = {}
CONN_ID = itertools.count(1)
LOOP = None
ARGS = None
EVENT_FH = None
REST_LOG = None
COUNTS = defaultdict(int)


def emit(kind, **fields):
    rec = {"seq": next(SEQ), "t_ms": L.now_ms(), "kind": kind}
    rec.update(fields)
    with LOCK:
        EVENTS.append(rec)
    if kind == "rest":
        REST_LOG.write(rec, rec["t_ms"])
    elif EVENT_FH:
        with LOCK:
            EVENT_FH.write(json.dumps(rec, separators=(",", ":")) + "\n")
            EVENT_FH.flush()
    return rec


def active(kind, now, symbol=None, client=None, path=None):
    out = []
    with LOCK:
        for r in RULES:
            if r["kind"] != kind or r.get("done"):
                continue
            if now < r.get("from_ms", 0) or now >= r.get("until_ms", 1 << 62):
                continue
            if symbol is not None and r.get("symbols") not in (None, "all") and symbol not in r["symbols"]:
                continue
            if client is not None and client not in r.get("clients", ["kp-1m"]):
                continue
            if path is not None and r.get("path", "/fapi/v1/klines") not in path:
                continue
            out.append(r)
    return out


def resolve_symbols(spec):
    if spec in (None, "all") or isinstance(spec, list):
        return spec
    kind, _, n = spec.partition(":")
    n = int(n)
    with LOCK:
        if kind == "top":
            horizon = L.minute_floor(L.now_ms()) - 60 * L.MINUTE
            score = {s: sum(q for m, q in d if m >= horizon) for s, d in ACTIVITY.items()}
            return [s for s, _ in sorted(score.items(), key=lambda kv: -kv[1])[:n]]
        if kind == "first":
            seen = sorted({L.topic_symbol(t) for c in CONNS.values() for t in c["topics"]})
            return seen[:n]
    raise ValueError("bad symbol spec %r" % spec)


def add_rule(rule):
    if rule.get("kind") not in INJECTIONS:
        raise ValueError("unknown kind %r" % rule.get("kind"))
    rule = dict(rule)
    rule["symbols"] = resolve_symbols(rule.get("symbols"))
    rule["id"] = next(RULE_ID)
    rule["created_ms"] = L.now_ms()
    if "duration_ms" in rule and rule["kind"] != "ws_stall" and "until_ms" not in rule:
        rule["until_ms"] = rule.get("from_ms", rule["created_ms"]) + rule["duration_ms"]
    with LOCK:
        RULES.append(rule)
    emit("inject", rule=rule)
    if rule["kind"] in ("ws_disconnect", "ws_stall"):
        LOOP.call_soon_threadsafe(ws_act, rule)
        rule["done"] = rule["kind"] == "ws_disconnect"
    return rule


# ------------------------------------------------------------------------------------- REST
_TLS = threading.local()


def upstream_conn(fresh=False):
    u = urlparse(ARGS.rest_upstream)
    c = getattr(_TLS, "conn", None)
    if c is None or fresh:
        if c is not None:
            c.close()
        if u.scheme == "https":
            c = http.client.HTTPSConnection(u.hostname, u.port or 443, timeout=30,
                                            context=ssl.create_default_context())
        else:
            c = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=30)
        _TLS.conn = c
    return c


def forward(path_qs, headers):
    for attempt in (0, 1):
        c = upstream_conn(fresh=attempt == 1)
        try:
            t0 = L.now_ms()
            c.request("GET", path_qs, headers=headers)
            r = c.getresponse()
            body = r.read()
            return r.status, r.getheaders(), body, t0, L.now_ms()
        except (http.client.HTTPException, OSError):
            if attempt == 1:
                raise


def rows_summary(rows):
    if not isinstance(rows, list):
        return None
    opens = [r[0] for r in rows if isinstance(r, list) and r]
    s = {"n": len(opens)}
    if opens:
        s.update(first_open=opens[0], last_open=opens[-1])
    if len(opens) <= 12:
        s["opens"] = opens
    return s


class Rest(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, code, body, headers=()):
        self.send_response(code)
        sent = set()
        for k, v in headers:
            if k.lower() in ("content-length", "transfer-encoding", "connection", "content-encoding"):
                continue
            self.send_header(k, v)
            sent.add(k.lower())
        if "content-type" not in sent:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        t_recv = L.now_ms()
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        client = self.headers.get("X-Rehearsal-Client", "kp-1m")
        bypass = self.headers.get("X-Rehearsal-Bypass-Inject") == "1"
        sym = q.get("symbol")
        rec = {"client": client, "path": u.path, "query": q, "symbol": sym, "t_recv_ms": t_recv, "applied": []}
        hit = (lambda kind: []) if bypass else (lambda kind: active(kind, L.now_ms(), sym, client, u.path))
        for r in hit("rest_error"):
            with LOCK:
                if "count" in r:
                    r["count"] -= 1
                    r["done"] = r["count"] <= 0
            code = int(r.get("code", 503))
            rec.update(status=code, applied=["rest_error#%d" % r["id"]], t_done_ms=L.now_ms())
            emit("rest", **rec)
            return self.reply(code, json.dumps({"code": -1001, "msg": "rehearsal injected %d" % code}).encode())
        for r in hit("rest_stall"):
            rec["applied"].append("rest_stall#%d" % r["id"])
            time.sleep(r.get("hold_ms", 60_000) / 1000.0)
            rec.update(status=None, t_done_ms=L.now_ms(), closed_without_response=True)
            emit("rest", **rec)
            self.close_connection = True
            return
        for r in hit("rest_hold"):
            rec["applied"].append("rest_hold#%d" % r["id"])
            L.sleep_until_ms(r["release_ms"])
        for r in hit("rest_timeout"):
            rec["applied"].append("rest_timeout#%d" % r["id"])
            time.sleep(r.get("sleep_ms", 20_000) / 1000.0)
        fwd = {"Accept-Encoding": "identity", "User-Agent": self.headers.get("User-Agent", "kp-rehearsal"),
               "Accept": self.headers.get("Accept", "*/*")}
        try:
            status, headers, body, t_up, t_up_done = forward(self.path, fwd)
        except Exception as e:  # upstream failure is reported as 502 to the caller
            rec.update(status=502, error=repr(e), t_done_ms=L.now_ms())
            emit("rest", **rec)
            return self.reply(502, json.dumps({"code": -1001, "msg": "passthrough upstream error"}).encode())
        hmap = {k.lower(): v for k, v in headers}
        rec.update(t_up_send_ms=t_up, t_up_done_ms=t_up_done, status=status,
                   weight_1m=hmap.get("x-mbx-used-weight-1m"))
        if status == 200 and u.path.endswith("/exchangeInfo"):
            rules = [] if bypass else active("rest_status", L.now_ms(), None, client, None)
            if rules:
                info = json.loads(body)
                for r in rules:
                    keep = []
                    for s in info.get("symbols", []):
                        if r["symbols"] in (None, "all") or s.get("symbol") in r["symbols"]:
                            if r.get("remove"):
                                continue
                            s["status"] = r.get("status", "SETTLING")
                        keep.append(s)
                    info["symbols"] = keep
                    rec["applied"].append("rest_status#%d" % r["id"])
                body = json.dumps(info, separators=(",", ":")).encode()
        if status == 200 and u.path.endswith("/klines"):
            rows = json.loads(body)
            changed = False
            now = L.now_ms()
            b = L.minute_floor(now)
            for r in hit("rest_delay_successor"):
                if now < b + r.get("delay_ms", 4000):
                    kept = [x for x in rows if x[0] < b]
                    if len(kept) != len(rows):
                        rec["applied"].append("rest_delay_successor#%d hid %d" % (r["id"], len(rows) - len(kept)))
                        rows, changed = kept, True
            for r in hit("rest_mutate"):
                target = b - r.get("minutes_back", 2) * L.MINUTE
                for x in rows:
                    if x[0] == target:
                        cents = int(round(float(x[4]) * 100)) + int(r.get("delta", 1))
                        decimals = len(x[4].split(".")[1]) if "." in x[4] else 0
                        x[4] = ("%." + str(decimals) + "f") % (cents / 100.0)
                        x[2] = max(x[2], x[4], key=float)
                        x[3] = min(x[3], x[4], key=float)
                        rec["applied"].append("rest_mutate#%d open=%d" % (r["id"], target))
                        changed = True
            rec["rows"] = rows_summary(rows)
            if changed:
                body = json.dumps(rows, separators=(",", ":")).encode()
        rec["t_done_ms"] = L.now_ms()
        emit("rest", **rec)
        self.reply(status, body, [(k, v) for k, v in headers if k.lower().startswith(("x-mbx", "content-type", "date"))])


# --------------------------------------------------------------------------------------- WS
def ws_act(rule):
    targets = [c for c in CONNS.values() if rule.get("conn", "all") in ("all", c["id"])
               or (rule.get("symbols") and any(L.topic_symbol(t) in rule["symbols"] for t in c["topics"]))]
    for c in targets:
        if rule["kind"] == "ws_disconnect":
            for w in (c["cw"], c["uw"]):
                try:
                    w.transport.abort()
                except Exception:
                    pass
        else:
            c["stall_until"] = L.now_ms() + rule.get("duration_ms", 200_000)
    emit("ws_act", rule_id=rule["id"], action=rule["kind"], conns=[c["id"] for c in targets])


def drop_frame(conn, topic, payload, now):
    sym = L.topic_symbol(topic)
    final = b'"x":true' in payload
    for r in active("ws_block", now, sym):
        if r.get("only_final") and not final:
            continue
        mo = r.get("minute_open")
        if mo is not None and ('"t":%d,' % mo).encode() not in payload:
            continue
        return r
    for r in active("ws_resubscribe", now, sym):
        return r
    return None


async def up_to_client(conn, ur, cw):
    pending = []
    while True:
        while conn["stall_until"] > L.now_ms():
            await asyncio.sleep(0.05)
        fin, op, payload, raw = await L.ws_read_frame(ur)
        if not fin or (op == L.OP_CONT):
            pending.append((op, payload, raw))
            if not fin:
                continue
            payload = b"".join(p for _, p, _ in pending)
            raw = b"".join(r for _, _, r in pending)
            op = pending[0][0]
            pending = []
        now = L.now_ms()
        if op == L.OP_TEXT:
            topic = L.frame_topic(payload)
            if topic:
                conn["topic_last"][topic] = now
                if b'"x":true' in payload:
                    try:
                        k = json.loads(payload)["data"]["k"]
                        with LOCK:
                            d = ACTIVITY[L.topic_symbol(topic)]
                            d.append((k["t"], float(k["q"])))
                            while len(d) > 90:
                                d.popleft()
                    except (ValueError, KeyError):
                        pass
                r = drop_frame(conn, topic, payload, now)
                if r is not None:
                    conn["dropped"] += 1
                    COUNTS["ws_dropped"] += 1
                    continue
        conn["forwarded"] += 1
        COUNTS["ws_forwarded"] += 1
        cw.write(raw)
        await cw.drain()


async def client_to_up(conn, cr, uw):
    while True:
        fin, op, payload, raw = await L.ws_read_frame(cr)
        if op == L.OP_TEXT:
            try:
                msg = json.loads(payload)
            except ValueError:
                msg = {}
            method = msg.get("method")
            if method in ("SUBSCRIBE", "UNSUBSCRIBE"):
                params = msg.get("params", [])
                emit("ws_cmd", conn=conn["id"], method=method, n=len(params),
                     symbols=sorted({L.topic_symbol(t) for t in params}))
                if method == "SUBSCRIBE":
                    syms = {L.topic_symbol(t) for t in params}
                    with LOCK:
                        for r in RULES:
                            if r["kind"] == "ws_resubscribe" and not r.get("done") and r.get("symbols"):
                                r.setdefault("seen", set()).update(syms & set(r["symbols"]))
                                if r["seen"] >= set(r["symbols"]):
                                    r["done"], r["satisfied_ms"] = True, L.now_ms()
                                    emit("resubscribe_satisfied", rule_id=r["id"])
        uw.write(raw)
        await uw.drain()


async def ws_handle(cr, cw):
    try:
        head = await asyncio.wait_for(cr.readuntil(b"\r\n\r\n"), 10)
    except Exception:
        cw.close()
        return
    lines = head.decode("latin-1").split("\r\n")
    target = lines[0].split(" ")[1]
    hdr = {x.split(":", 1)[0].strip().lower(): x.split(":", 1)[1].strip() for x in lines[1:] if ":" in x}
    up = urlparse(ARGS.ws_upstream)
    tls = up.scheme == "wss"
    try:
        ur, uw = await asyncio.wait_for(asyncio.open_connection(
            up.hostname, up.port or (443 if tls else 80), ssl=ssl.create_default_context() if tls else None,
            server_hostname=up.hostname if tls else None), 10)
        key = L.base64.b64encode(os.urandom(16)).decode()
        host = up.hostname if not up.port else "%s:%d" % (up.hostname, up.port)
        uw.write(("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nUser-Agent: kp-rehearsal\r\n\r\n"
                  % (target, host, key)).encode())
        await uw.drain()
        uhead = await asyncio.wait_for(ur.readuntil(b"\r\n\r\n"), 10)
        if b" 101 " not in uhead.split(b"\r\n", 1)[0]:
            raise IOError("upstream refused upgrade: %r" % uhead[:80])
    except Exception as e:
        emit("ws_upstream_error", target=target[:120], error=repr(e))
        cw.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        cw.close()
        return
    cw.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
              "Sec-WebSocket-Accept: %s\r\n\r\n" % L.ws_accept(hdr.get("sec-websocket-key", ""))).encode())
    await cw.drain()
    q = parse_qs(urlparse(target).query)
    topics = [t for t in q.get("streams", [""])[0].split("/") if t]
    conn = {"id": next(CONN_ID), "topics": topics, "cw": cw, "uw": uw, "stall_until": 0, "forwarded": 0,
            "dropped": 0, "opened_ms": L.now_ms(), "topic_last": {}}
    CONNS[conn["id"]] = conn
    emit("ws_open", conn=conn["id"], n_topics=len(topics), symbols=sorted({L.topic_symbol(t) for t in topics}))
    tasks = [asyncio.ensure_future(up_to_client(conn, ur, cw)), asyncio.ensure_future(client_to_up(conn, cr, uw))]
    done, rest = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    reason = "?"
    for t in done:
        exc = t.exception()
        reason = ("upstream" if t is tasks[0] else "client") + ":" + (type(exc).__name__ if exc else "eof")
    for t in rest:
        t.cancel()
    for w in (cw, uw):
        try:
            w.transport.abort()
        except Exception:
            pass
    CONNS.pop(conn["id"], None)
    emit("ws_close", conn=conn["id"], reason=reason, forwarded=conn["forwarded"], dropped=conn["dropped"],
         lifetime_s=round((L.now_ms() - conn["opened_ms"]) / 1000, 1))


async def minute_summary():
    while True:
        await asyncio.sleep(60 - (time.time() % 60) + 30)
        emit("ws_minute", conns=len(CONNS), forwarded=COUNTS["ws_forwarded"], dropped=COUNTS["ws_dropped"])
        COUNTS.clear()


# ---------------------------------------------------------------------------------- control
class Control(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, code, obj):
        body = json.dumps(obj, default=lambda o: sorted(o) if isinstance(o, set) else str(o)).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/events":
            since, kinds = int(q.get("since", 0)), q.get("kind")
            kinds = set(kinds.split(",")) if kinds else None
            with LOCK:
                out = [e for e in EVENTS if e["seq"] > since and (kinds is None or e["kind"] in kinds)]
            return self.send(200, out[: int(q.get("limit", 100000))])
        if u.path == "/state":
            with LOCK:
                rules = [dict(r) for r in RULES]
            conns = [{"id": c["id"], "n_topics": len(c["topics"]), "forwarded": c["forwarded"],
                      "dropped": c["dropped"], "stalled": c["stall_until"] > L.now_ms()} for c in CONNS.values()]
            return self.send(200, {"now_ms": L.now_ms(), "rules": rules, "conns": conns,
                                   "last_seq": EVENTS[-1]["seq"] if EVENTS else 0, "kinds": INJECTIONS})
        if u.path == "/resolve":
            return self.send(200, {"symbols": resolve_symbols(q.get("spec", "top:5"))})
        self.send(404, {"error": "not found"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        try:
            if self.path == "/inject":
                return self.send(200, add_rule(body))
            if self.path == "/clear":
                with LOCK:
                    for r in RULES:
                        if body.get("id") in (None, r["id"]):
                            r["done"] = True
                    for c in CONNS.values():
                        if body.get("id") is None:
                            c["stall_until"] = 0
                emit("clear", target=body.get("id", "all"))
                return self.send(200, {"ok": True})
        except ValueError as e:
            return self.send(400, {"error": str(e)})
        self.send(404, {"error": "not found"})


def serve_thread(listen, handler):
    host, port = listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), handler)
    srv.daemon_threads = True
    srv.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


def main():
    global ARGS, LOOP, EVENT_FH, REST_LOG
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rest-listen", default="127.0.0.1:18980")
    ap.add_argument("--ws-listen", default="127.0.0.1:18981")
    ap.add_argument("--control-listen", default="127.0.0.1:18982")
    ap.add_argument("--rest-upstream", default="https://fapi.binance.com")
    ap.add_argument("--ws-upstream", default="wss://fstream.binance.com")
    ap.add_argument("--log-dir", required=True)
    ap.add_argument("--hours", type=float, default=None, help="exit after this long (default: run until stopped)")
    ARGS = ap.parse_args()
    for hp in (ARGS.rest_listen, ARGS.ws_listen, ARGS.control_listen):
        assert hp.startswith("127.0.0.1:"), "listeners are loopback only: %s" % hp
    os.makedirs(ARGS.log_dir, exist_ok=True)
    EVENT_FH = open(os.path.join(ARGS.log_dir, "events.jsonl"), "a")
    REST_LOG = L.HourlyGzip(os.path.join(ARGS.log_dir, "rest"))
    serve_thread(ARGS.rest_listen, Rest)
    serve_thread(ARGS.control_listen, Control)
    LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(LOOP)
    host, port = ARGS.ws_listen.rsplit(":", 1)
    LOOP.run_until_complete(asyncio.start_server(ws_handle, host, int(port)))
    asyncio.ensure_future(minute_summary())
    emit("start", rest_upstream=ARGS.rest_upstream, ws_upstream=ARGS.ws_upstream, pid=os.getpid())
    print("passthrough rest=%s ws=%s control=%s -> %s / %s" % (ARGS.rest_listen, ARGS.ws_listen,
          ARGS.control_listen, ARGS.rest_upstream, ARGS.ws_upstream), flush=True)
    if ARGS.hours:
        LOOP.call_later(ARGS.hours * 3600, LOOP.stop)
    try:
        LOOP.run_forever()
    finally:
        REST_LOG.close()
        emit("stop")


if __name__ == "__main__":
    main()
