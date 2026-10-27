#!/usr/bin/env python3
"""LOCAL ONLY minimal Binance USD-M mock (REST + continuousKline WS) for the rehearsal kit.

Never point this at, or run it on, the VPS. Bars are a deterministic function of (symbol, minute),
so WS x=true frames, forming frames and REST rows always agree (no conflicts in healthy runs).
Coins:
  * N regular PERPETUAL coins (BTCUSDT, ETHUSDT, ... then C000USDT ...), frames every 250 ms;
  * QUIETUSDT: every third minute has zero trades -> REST returns a zero-volume row, WS sends
    nothing for that minute (exercises the fallback path, design E1/E2);
  * GAPUSDT: minutes with k % 10 == 5 have no REST row and no WS frames (a real exchange gap;
    exercises absent with both anchors);
  * XAUUSDT as TRADIFI_PERPETUAL and BADUSDT with pair != symbol (directory exclusions).
REST:  /fapi/v1/time, /fapi/v1/exchangeInfo, /fapi/v1/klines (symbol, interval=1m, endTime,
       limit, optional startTime; forming row included when its open <= endTime), with an
       X-MBX-USED-WEIGHT-1M header from a rolling 60 s counter.
WS:    /market/stream?streams=a/b/... combined frames; SUBSCRIBE/UNSUBSCRIBE answered.
"""
import argparse
import asyncio
import json
import os
import sys
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

MAJORS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT"]


def universe(n):
    names = MAJORS[:n] + ["C%03dUSDT" % i for i in range(max(0, n - len(MAJORS)))]
    return names + ["QUIETUSDT", "GAPUSDT"]


def seed(sym):
    return zlib.crc32(sym.encode()) % 900 + 50


def traded(sym, k):
    if sym == "QUIETUSDT" and k % 3 == 0:
        return False
    return True


def exists(sym, k):
    return not (sym == "GAPUSDT" and k % 10 == 5)


def p(cents):
    return "%d.%02d" % (cents // 100, cents % 100)


def bar(sym, open_ms, now):
    """REST row for one minute (forming when not yet closed) or None for an exchange gap."""
    k = open_ms // L.MINUTE
    if not exists(sym, k):
        return None
    s = seed(sym)
    o = 10_000 + s * 100 + (k % 17) * 25
    closed = open_ms + L.MINUTE - 1 < now
    if not traded(sym, k):
        prev_c = o
        return [open_ms, p(prev_c), p(prev_c), p(prev_c), p(prev_c), "0.000", open_ms + L.MINUTE - 1,
                "0.00000", 0, "0.000", "0.00000", "0"]
    if closed:
        c, n = o + ((k % 5) - 2) * 10, 5 + k % 11
        h, l = max(o, c) + 30, min(o, c) - 20
    else:
        e = max(0, min(3, (now - open_ms) * 4 // L.MINUTE))
        c, n = o + (e - 1) * 5, 1 + e
        h, l = max(o, c) + 10, min(o, c) - 5
    v = 1000 + n * 100
    q = v * o // 100
    return [open_ms, p(o), p(h), p(l), p(c), "%d.000" % v, open_ms + L.MINUTE - 1, "%d.00000" % q, n,
            "%d.000" % (v // 2), "%d.00000" % (q // 2), "0"]


class State:
    def __init__(self, n):
        self.symbols = universe(n)
        self.weight = L.RollingBudget(10 ** 9)


ST = None


class Rest(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, code, obj, weight):
        body = json.dumps(obj, separators=(",", ":")).encode()
        ST.weight.try_spend(weight)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-MBX-USED-WEIGHT-1M", str(ST.weight.used()))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        now = L.now_ms()
        if u.path == "/fapi/v1/time":
            return self.send(200, {"serverTime": now}, 1)
        if u.path == "/fapi/v1/exchangeInfo":
            rows = [{"symbol": s, "pair": s, "contractType": "PERPETUAL", "status": "TRADING",
                     "deliveryDate": 4133404800000, "onboardDate": 1569398400000, "quoteAsset": "USDT",
                     "marginAsset": "USDT", "baseAsset": s[:-4]} for s in ST.symbols]
            rows.append({"symbol": "XAUUSDT", "pair": "XAUUSDT", "contractType": "TRADIFI_PERPETUAL",
                         "status": "TRADING", "deliveryDate": 4133404800000, "onboardDate": 0,
                         "quoteAsset": "USDT", "marginAsset": "USDT", "baseAsset": "XAU"})
            rows.append({"symbol": "BADUSDT", "pair": "BADXUSDT", "contractType": "PERPETUAL",
                         "status": "TRADING", "deliveryDate": 4133404800000, "onboardDate": 0,
                         "quoteAsset": "USDT", "marginAsset": "USDT", "baseAsset": "BAD"})
            return self.send(200, {"timezone": "UTC", "serverTime": now, "rateLimits": [],
                                   "exchangeFilters": [], "assets": [], "symbols": rows}, 1)
        if u.path == "/fapi/v1/klines":
            sym, limit = q.get("symbol", ""), int(q.get("limit", "500"))
            end = int(q.get("endTime", now))
            if sym not in ST.symbols and sym != "XAUUSDT":
                return self.send(400, {"code": -1121, "msg": "Invalid symbol."}, 1)
            top = L.minute_floor(min(end, now))
            if "startTime" in q:
                start = int(q["startTime"])
                first = start + (-start) % L.MINUTE
                opens = list(range(first, top + 1, L.MINUTE))
            else:
                opens = list(range(top - (limit + 15) * L.MINUTE, top + 1, L.MINUTE))
            rows = [r for r in (bar(sym, t, now) for t in opens) if r is not None]
            rows = rows[:limit] if "startTime" in q else rows[-limit:]
            return self.send(200, rows, L.kline_weight(limit))
        self.send(404, {"code": -1, "msg": "not found"}, 1)


async def ws_client(reader, writer):
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except Exception:
        writer.close()
        return
    lines = head.decode("latin-1").split("\r\n")
    target = lines[0].split(" ")[1]
    hdr = {ln.split(":", 1)[0].strip().lower(): ln.split(":", 1)[1].strip() for ln in lines[1:] if ":" in ln}
    q = parse_qs(urlparse(target).query)
    topics = set(t for t in q.get("streams", [""])[0].split("/") if t)
    writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  "Sec-WebSocket-Accept: %s\r\n\r\n" % L.ws_accept(hdr.get("sec-websocket-key", ""))).encode())
    await writer.drain()
    alive = {"on": True}

    async def pump_in():
        try:
            while True:
                fin, op, payload, _ = await L.ws_read_frame(reader)
                if op == L.OP_CLOSE:
                    break
                if op == L.OP_PING:
                    writer.write(L.ws_encode(L.OP_PONG, payload))
                elif op == L.OP_TEXT:
                    msg = json.loads(payload)
                    for t in msg.get("params", []):
                        (topics.add if msg.get("method") == "SUBSCRIBE" else topics.discard)(t)
                    writer.write(L.ws_encode(L.OP_TEXT, json.dumps({"result": None, "id": msg.get("id")}).encode()))
                await writer.drain()
        except Exception:
            pass
        alive["on"] = False

    asyncio.ensure_future(pump_in())
    last_final = L.minute_floor(L.now_ms())
    try:
        while alive["on"]:
            now = L.now_ms()
            cur = L.minute_floor(now)
            frames = []
            if cur > last_final and now - cur >= 100:
                for t in sorted(topics):
                    sym = L.topic_symbol(t)
                    r = bar(sym, cur - L.MINUTE, now)
                    if r is not None and traded(sym, (cur - L.MINUTE) // L.MINUTE):
                        frames.append((t, r, True))
                last_final = cur
            for t in sorted(topics):
                sym = L.topic_symbol(t)
                r = bar(sym, cur, now)
                if r is not None and traded(sym, cur // L.MINUTE) and now - cur >= 200:
                    frames.append((t, r, False))
            for t, r, x in frames:
                sym = L.topic_symbol(t)
                data = {"e": "continuous_kline", "E": now, "ps": sym, "ct": "TRADIFI_PERPETUAL" if sym == "XAUUSDT" else "PERPETUAL",
                        "k": {"t": r[0], "T": r[6], "i": "1m", "f": 1, "L": 2, "o": r[1], "c": r[4], "h": r[2],
                              "l": r[3], "v": r[5], "n": r[8], "x": x, "q": r[7], "V": r[9], "Q": r[10], "B": "0"}}
                writer.write(L.ws_encode(L.OP_TEXT, json.dumps({"stream": t, "data": data},
                                                               separators=(",", ":")).encode()))
            await writer.drain()
            await asyncio.sleep(0.25 - (L.now_ms() % 250) / 1000.0 + 0.01)
    except Exception:
        pass
    writer.close()


def main():
    global ST
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rest", default="127.0.0.1:18970")
    ap.add_argument("--ws", default="127.0.0.1:18971")
    ap.add_argument("--coins", type=int, default=30)
    a = ap.parse_args()
    for hp in (a.rest, a.ws):
        assert hp.startswith("127.0.0.1:"), "the mock binds loopback only"
    ST = State(a.coins)
    host, port = a.rest.split(":")
    srv = ThreadingHTTPServer((host, int(port)), Rest)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    host, port = a.ws.split(":")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.start_server(ws_client, host, int(port)))
    print("mock upstream rest=%s ws=%s coins=%d" % (a.rest, a.ws, len(ST.symbols)), flush=True)
    loop.run_forever()


if __name__ == "__main__":
    main()
