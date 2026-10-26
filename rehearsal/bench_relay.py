#!/usr/bin/env python3
"""LOCAL ONLY: measure passthrough.py WS relay cost at full-market scale.

Starts mock_upstream.py (--coins N) and passthrough.py on spare loopback ports, opens
ceil(N/200) WS connections of <=200 continuousKline topics through the proxy (kp-1m's batch
size), reads for --seconds, and reports frames/s through the relay plus the proxy's CPU% and RSS
(ps samples). The mock and the reader run on the same host and are reported separately.
"""
import argparse
import asyncio
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def ps(pid):
    """(cumulative CPU seconds, RSS MB) from ps; CPU% is derived from deltas, not ps pcpu."""
    out = subprocess.run(["ps", "-o", "time=,rss=", "-p", str(pid)], stdout=subprocess.PIPE,
                         universal_newlines=True).stdout.split()
    if not out:
        return (None, None)
    secs = 0.0
    for part in out[0].replace("-", ":").split(":"):
        secs = secs * 60 + float(part)
    return (secs, int(out[1]) // 1024)


async def reader(port, topics, stop_at, count):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    key = L.base64.b64encode(os.urandom(16)).decode()
    w.write(("GET /market/stream?streams=%s HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
             "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % ("/".join(topics), key)).encode())
    await w.drain()
    await r.readuntil(b"\r\n\r\n")
    while time.time() < stop_at:
        try:
            await asyncio.wait_for(L.ws_read_frame(r), 2)
            count[0] += 1
        except asyncio.TimeoutError:
            pass
    w.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coins", type=int, default=750)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--log-dir", default=os.path.join(HERE, "local-run", "bench"))
    a = ap.parse_args()
    os.makedirs(a.log_dir, exist_ok=True)
    py = sys.executable
    mock = subprocess.Popen([py, "-I", os.path.join(HERE, "mock_upstream.py"), "--coins", str(a.coins),
                             "--rest", "127.0.0.1:18975", "--ws", "127.0.0.1:18976"],
                            stdout=open(os.path.join(a.log_dir, "mock.log"), "w"), stderr=subprocess.STDOUT)
    px = subprocess.Popen([py, "-I", os.path.join(HERE, "passthrough.py"), "--log-dir", a.log_dir,
                           "--rest-listen", "127.0.0.1:18985", "--ws-listen", "127.0.0.1:18986",
                           "--control-listen", "127.0.0.1:18987", "--rest-upstream", "http://127.0.0.1:18975",
                           "--ws-upstream", "ws://127.0.0.1:18976"],
                          stdout=open(os.path.join(a.log_dir, "px.log"), "w"), stderr=subprocess.STDOUT)
    try:
        time.sleep(2)
        import mock_upstream
        topics = [L.symbol_topic(s) for s in mock_upstream.universe(a.coins)]
        groups = [topics[i:i + 200] for i in range(0, len(topics), 200)]
        count = [0]
        stop_at = time.time() + a.seconds
        samples = []

        async def run():
            tasks = [asyncio.ensure_future(reader(18986, g, stop_at, count)) for g in groups]
            await asyncio.sleep(10)  # warm-up, then sample
            c0, t0 = count[0], time.time()
            samples.append((ps(px.pid), ps(mock.pid), t0))
            while time.time() < stop_at - 1:
                await asyncio.sleep(5)
            samples.append((ps(px.pid), ps(mock.pid), time.time()))
            rate = (count[0] - c0) / (time.time() - t0)
            await asyncio.gather(*tasks, return_exceptions=True)
            return rate
        rate = asyncio.new_event_loop().run_until_complete(run())
        (p0, _), (m0, _), w0 = samples[0]
        (p1, rss), (m1, _), w1 = samples[-1]
        print("coins=%d connections=%d relay_frames_per_s=%.0f proxy_cpu_pct_of_one_core=%.1f proxy_rss_mb=%s "
              "mock_cpu_pct_of_one_core=%.1f window_s=%.0f" % (a.coins, len(groups), rate, 100 * (p1 - p0) / (w1 - w0),
                                                             rss, 100 * (m1 - m0) / (w1 - w0), w1 - w0))
    finally:
        for p in (px, mock):
            p.terminate()
            p.wait(10)


if __name__ == "__main__":
    main()
