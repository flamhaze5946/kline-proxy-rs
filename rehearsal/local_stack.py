#!/usr/bin/env python3
"""LOCAL ONLY: start/stop mock upstream + passthrough + kp-1m on loopback for kit validation.

  python3 rehearsal/local_stack.py up   [--run DIR] [--coins 30]
  python3 rehearsal/local_stack.py down [--run DIR]
  python3 rehearsal/local_stack.py kp-stop|kp-start [--run DIR]

The kp-1m config is derived from rehearsal/kp-1m.json (the VPS sample) with only these local
overrides: listen, persistence.directory, rest.future_url, websocket.future_url (loopback ports),
market_api.metadata_refresh_seconds=30 and evidence.not_trading_retention_min=4 (so injections
6/10/12/13 finish in minutes). Every other value is the VPS value.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.path.join(ROOT, "target", "release", "kline-proxy")
PORTS = {"mock_rest": 18970, "mock_ws": 18971, "px_rest": 18980, "px_ws": 18981, "px_ctl": 18982, "kp": 18990}
LOCAL_OVERRIDES = {"metadata_refresh_seconds": 30, "not_trading_retention_min": 4}


def local_config(run):
    with open(os.path.join(HERE, "kp-1m.json")) as f:
        c = json.load(f)
    c["listen"] = "127.0.0.1:%d" % PORTS["kp"]
    c["persistence"]["directory"] = os.path.join(run, "state")
    c["rest"]["future_url"] = "http://127.0.0.1:%d" % PORTS["px_rest"]
    c["websocket"]["future_url"] = "ws://127.0.0.1:%d/market/stream" % PORTS["px_ws"]
    c["market_api"]["metadata_refresh_seconds"] = LOCAL_OVERRIDES["metadata_refresh_seconds"]
    c["evidence"]["not_trading_retention_min"] = LOCAL_OVERRIDES["not_trading_retention_min"]
    path = os.path.join(run, "kp-1m-local.json")
    with open(path, "w") as f:
        json.dump(c, f, indent=1)
    return path


def spawn(run, name, argv, env=None):
    log = open(os.path.join(run, name + ".log"), "a")
    p = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
    with open(os.path.join(run, name + ".pid"), "w") as f:
        f.write(str(p.pid))
    return p.pid


def pid_of(run, name):
    try:
        with open(os.path.join(run, name + ".pid")) as f:
            pid = int(f.read())
        os.kill(pid, 0)
        return pid
    except (OSError, ValueError):
        return None


def stop(run, name, timeout=60):
    pid = pid_of(run, name)
    if pid is None:
        return False
    os.kill(pid, signal.SIGTERM)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            os.kill(pid, 0)
        except OSError:
            break
        try:
            if os.waitpid(pid, os.WNOHANG)[0] == pid:
                break
        except ChildProcessError:
            pass
        time.sleep(0.2)
    else:
        os.kill(pid, signal.SIGKILL)
    os.unlink(os.path.join(run, name + ".pid"))
    return True


def kp_start(run):
    cfg = os.path.join(run, "kp-1m-local.json")
    env = dict(os.environ, RUST_LOG="info", KLINE_IO_WORKERS="2")
    return spawn(run, "kp-1m", [BIN, cfg], env)


def provision_breaker(run):
    state = os.path.join(run, "state")
    os.makedirs(state, exist_ok=True)
    path = os.path.join(state, "breaker.json")
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write('{"last_conflict_ms":0,"revised_total":0}')


def up(run, coins):
    os.makedirs(run, exist_ok=True)
    local_config(run)
    provision_breaker(run)
    py = sys.executable
    if not pid_of(run, "mock"):
        spawn(run, "mock", [py, "-I", os.path.join(HERE, "mock_upstream.py"), "--coins", str(coins),
                            "--rest", "127.0.0.1:%d" % PORTS["mock_rest"], "--ws", "127.0.0.1:%d" % PORTS["mock_ws"]])
    if not pid_of(run, "passthrough"):
        spawn(run, "passthrough", [py, "-I", os.path.join(HERE, "passthrough.py"), "--log-dir", os.path.join(run, "px"),
                                   "--rest-upstream", "http://127.0.0.1:%d" % PORTS["mock_rest"],
                                   "--ws-upstream", "ws://127.0.0.1:%d" % PORTS["mock_ws"]])
    time.sleep(1.0)
    if not pid_of(run, "kp-1m"):
        kp_start(run)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["up", "down", "kp-stop", "kp-start", "config"])
    ap.add_argument("--run", default=os.path.join(HERE, "local-run"))
    ap.add_argument("--coins", type=int, default=30)
    a = ap.parse_args()
    if a.cmd == "up":
        up(a.run, a.coins)
    elif a.cmd == "down":
        for n in ("kp-1m", "passthrough", "mock"):
            stop(a.run, n)
    elif a.cmd == "kp-stop":
        stop(a.run, "kp-1m")
    elif a.cmd == "kp-start":
        kp_start(a.run)
    else:
        print(local_config(a.run))


if __name__ == "__main__":
    main()
