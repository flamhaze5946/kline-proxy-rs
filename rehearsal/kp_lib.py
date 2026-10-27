"""Shared helpers for the kp-1m rehearsal kit (stdlib only, Python >= 3.9).

Contents: wall-clock/minute helpers, a tiny JSON HTTP client, the kp-evidence-v1 server-side
window invariant (design v8 section 4) used by the recorder and the injection asserts, a rolling
REST weight budget, Binance futures kline weights, and an RFC 6455 frame codec shared by the
passthrough proxy and the local mock upstream.
"""
import base64
import gzip
import hashlib
import json
import os
import struct
import threading
import time
import urllib.error
import urllib.request
from collections import deque

MINUTE = 60_000
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def now_ms():
    return time.time_ns() // 1_000_000


def minute_floor(ms):
    return ms - ms % MINUTE


def iso(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + ".%03dZ" % (ms % 1000)


def sleep_until_ms(target_ms):
    while True:
        wait = target_ms - now_ms()
        if wait <= 0:
            return
        time.sleep(min(wait / 1000.0, 0.5))


def kline_weight(limit):
    """Binance USD-M /fapi/v1/klines request weight by limit (same table as transport.rs)."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def http_json(url, body=None, timeout=30.0, headers=None):
    """Return (status, parsed_json_or_None, raw_bytes, response_headers, elapsed_s)."""
    data = None if body is None else json.dumps(body).encode()
    hdrs = {"Content-Type": "application/json"} if body is not None else {}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST" if body is not None else "GET")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            status, rh = r.status, dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read()
        status, rh = e.code, dict(e.headers or {})
    elapsed = time.time() - t0
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return status, parsed, raw, rh, elapsed


def evidence_url(base, limit, symbols=None):
    q = "interval=1m&closed_only=true&with_evidence=true&limit=%d" % limit
    if symbols:
        q += "&symbols=" + ",".join(symbols)
    return base.rstrip("/") + "/fapi/v1/klines/bulk?" + q


def evidence_post(base, limit, symbols, timeout=30.0):
    """POST form of the evidence bulk, as the P3-S2 client sends it (P3-S3a schema v1 section 2.1):
    POST /fapi/v1/klines/bulk?with_evidence=true&closed_only=true&limit=<n>, 0.4.24 JSON body."""
    url = base.rstrip("/") + "/fapi/v1/klines/bulk?with_evidence=true&closed_only=true&limit=%d" % limit
    body = {"interval": "1m", "limit": limit, "closed_only": True}
    if symbols is not None:
        body["symbols"] = list(symbols)
    return http_json(url, body, timeout=timeout)


EVIDENCE_KEYS = ("interval", "ts_ms", "contract", "number_type", "limit_effective", "window",
                 "ws_final_trusted", "revised_total", "status", "klines", "evidence", "absent",
                 "missing", "synthetic", "covered_from")


def window_violations(resp, requested=None, limit=None):
    """Design v8 section 4 server invariant and contract echo, as a list of strings (empty = ok).

    For every trading/not_trading symbol: klines, absent and missing open times do not overlap
    and exactly tile [max(window.first_open, covered_from), window.last_open] on the 1m grid;
    evidence is dense, aligned with klines and only final|pending; synthetic rows are pending.
    `requested` (the normalized symbol set) must equal the status key set when given.
    """
    out = []
    if not isinstance(resp, dict):
        return ["response is not an object"]
    for k in EVIDENCE_KEYS:
        if k not in resp:
            out.append("missing top-level key %s" % k)
    if out:
        return out
    if resp["contract"] != "kp-evidence-v1" or resp["number_type"] != "string":
        out.append("contract/number_type echo mismatch")
    if limit is not None and resp["limit_effective"] != limit:
        out.append("limit_effective %r != %r" % (resp["limit_effective"], limit))
    w = resp["window"]
    first, last = w["first_open"], w["last_open"]
    if (last - first) % MINUTE or last < first:
        out.append("window not on the 1m grid")
    status = resp["status"]
    if requested is not None and set(status) != set(requested):
        out.append("status keys != requested set (missing %s, extra %s)" % (
            sorted(set(requested) - set(status))[:5], sorted(set(status) - set(requested))[:5]))
    for sym, st in status.items():
        for m in ("klines", "evidence", "absent", "missing", "synthetic", "covered_from"):
            if sym not in resp[m]:
                out.append("%s missing from %s" % (sym, m))
        if out and out[-1].startswith(sym):
            continue
        rows, ev = resp["klines"][sym], resp["evidence"][sym]
        if len(rows) != len(ev):
            out.append("%s evidence length %d != klines %d" % (sym, len(ev), len(rows)))
            continue
        if any(e not in ("final", "pending") for e in ev):
            out.append("%s evidence has a value outside final|pending" % sym)
        state = st.get("state")
        if state == "unknown":
            if rows or resp["absent"][sym] or resp["missing"][sym] or resp["covered_from"][sym] is not None:
                out.append("%s unknown but carries rows/absent/missing/covered_from" % sym)
            continue
        cf = resp["covered_from"][sym]
        times = [r[0] for r in rows]
        if times != sorted(set(times)):
            out.append("%s klines not strictly increasing" % sym)
        parts = times + list(resp["absent"][sym]) + list(resp["missing"][sym])
        if len(parts) != len(set(parts)):
            out.append("%s klines/absent/missing overlap" % sym)
        if cf is None:
            if parts:
                out.append("%s covered_from null but minutes present" % sym)
            continue
        lo = max(first, cf)
        expect = set(range(lo, last + 1, MINUTE)) if lo <= last else set()
        if set(parts) != expect:
            out.append("%s does not tile [%d,%d]: extra %s missing %s" % (
                sym, lo, last, sorted(set(parts) - expect)[:3], sorted(expect - set(parts))[:3]))
        syn = set(resp["synthetic"][sym])
        for t, e in zip(times, ev):
            if t in syn and e != "pending":
                out.append("%s synthetic row %d reported %s" % (sym, t, e))
        if not syn <= set(times):
            out.append("%s synthetic outside klines" % sym)
    return out


def minute_state(resp, sym, open_ms):
    """'final' | 'pending' | 'absent' | 'missing' | 'uncovered' | 'unknown' for one minute."""
    st = resp["status"].get(sym, {}).get("state")
    if st in (None, "unknown"):
        return "unknown"
    for row, ev in zip(resp["klines"][sym], resp["evidence"][sym]):
        if row[0] == open_ms:
            return ev
    if open_ms in resp["absent"][sym]:
        return "absent"
    if open_ms in resp["missing"][sym]:
        return "missing"
    return "uncovered"


def bridge_complete(resp, sym, first_open, last_open):
    """Every minute in [first_open, last_open] is a final row or an absent (client rule 4.1-2)."""
    for t in range(first_open, last_open + 1, MINUTE):
        if minute_state(resp, sym, t) not in ("final", "absent"):
            return False
    return True


class RollingBudget:
    """Weight spent in the trailing 60 s; `try_spend` refuses instead of exceeding the cap."""

    def __init__(self, cap_per_minute):
        self.cap = cap_per_minute
        self.events = deque()
        self.lock = threading.Lock()

    def _trim(self, t):
        while self.events and self.events[0][0] <= t - MINUTE:
            self.events.popleft()

    def used(self, t=None):
        t = now_ms() if t is None else t
        with self.lock:
            self._trim(t)
            return sum(w for _, w in self.events)

    def try_spend(self, weight, t=None):
        t = now_ms() if t is None else t
        with self.lock:
            self._trim(t)
            if sum(w for _, w in self.events) + weight > self.cap:
                return False
            self.events.append((t, weight))
            return True


class HourlyGzip:
    """Append JSON lines to <dir>/<prefix>YYYYMMDDHH.jsonl.gz, keyed by a record time in ms."""

    def __init__(self, directory, prefix=""):
        self.dir, self.prefix = directory, prefix
        os.makedirs(directory, exist_ok=True)
        self.lock = threading.Lock()
        self.name, self.fh, self.last_flush = None, None, 0.0

    def write(self, rec, t_ms=None):
        t_ms = now_ms() if t_ms is None else t_ms
        name = os.path.join(self.dir, self.prefix + time.strftime("%Y%m%d%H", time.gmtime(t_ms / 1000)) + ".jsonl.gz")
        line = json.dumps(rec, separators=(",", ":")) + "\n"
        with self.lock:
            if name != self.name:
                if self.fh:
                    self.fh.close()
                self.name, self.fh = name, gzip.open(name, "at")
            self.fh.write(line)
            if time.time() - self.last_flush > 5:
                self.fh.flush()
                self.last_flush = time.time()

    def close(self):
        with self.lock:
            if self.fh:
                self.fh.close()
                self.fh, self.name = None, None


# ---------------------------------------------------------------- RFC 6455 frames
OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0, 1, 2, 8, 9, 10


def ws_accept(key):
    return base64.b64encode(hashlib.sha1(key.encode() + WS_GUID).digest()).decode()


def ws_encode(opcode, payload, mask=False, fin=True):
    b0 = (0x80 if fin else 0) | opcode
    n = len(payload)
    mbit = 0x80 if mask else 0
    if n < 126:
        head = struct.pack("!BB", b0, mbit | n)
    elif n < 65536:
        head = struct.pack("!BBH", b0, mbit | 126, n)
    else:
        head = struct.pack("!BBQ", b0, mbit | 127, n)
    if not mask:
        return head + payload
    key = os.urandom(4)
    return head + key + ws_mask(payload, key)


def ws_mask(payload, key):
    if not payload:
        return b""
    n = len(payload)
    k = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(k, "big")).to_bytes(n, "big")


async def ws_read_frame(reader):
    """Read one frame; returns (fin, opcode, payload_unmasked, raw_bytes). Raises on EOF."""
    h = await reader.readexactly(2)
    fin, opcode = bool(h[0] & 0x80), h[0] & 0x0F
    masked, n = bool(h[1] & 0x80), h[1] & 0x7F
    raw = bytearray(h)
    if n == 126:
        ext = await reader.readexactly(2)
        raw += ext
        n = struct.unpack("!H", ext)[0]
    elif n == 127:
        ext = await reader.readexactly(8)
        raw += ext
        n = struct.unpack("!Q", ext)[0]
    key = b""
    if masked:
        key = await reader.readexactly(4)
        raw += key
    body = await reader.readexactly(n) if n else b""
    raw += body
    payload = ws_mask(body, key) if masked else body
    return fin, opcode, payload, bytes(raw)


def frame_topic(payload):
    """Topic of a combined-stream data frame ({"stream":"<topic>","data":...}) or None."""
    i = payload.find(b'"stream":"', 0, 200)
    if i < 0:
        return None
    j = payload.find(b'"', i + 10)
    return payload[i + 10:j].decode() if j > 0 else None


def topic_symbol(topic):
    """btcusdt_perpetual@continuousKline_1m -> BTCUSDT ; btcusdt@kline_1m -> BTCUSDT."""
    return topic.split("@", 1)[0].split("_", 1)[0].upper()


def symbol_topic(symbol, contract="PERPETUAL"):
    return "%s_%s@continuousKline_1m" % (symbol.lower(), contract.lower())
