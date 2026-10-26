# kp-1m evidence rehearsal (P3-S3) — VPS runbook

Host: Tokyo test VPS 45.32.255.251 (2 vCPU / 1.9 GiB). Never the 1h production proxy host (O8).
Spec: `scratch/design/P3-S0-kline-proxy-v8.md` §6 (resources), §7 (O7). Times are UTC.

Layout on the VPS (does not touch the running 0.4.24 instance `/opt/kp-1m/kline-proxy` +
`/etc/kp-1m/config-cont.json` on 127.0.0.1:1890):

| What | Where |
|---|---|
| binary (per release) | `/opt/kp-1m/evidence/<release>/kline-proxy`, `current` symlink |
| config (per release) | `/etc/kp-1m/evidence/<release>.json`, `current.json` symlink |
| kp-1m listener | `127.0.0.1:1891` |
| passthrough REST / WS / control | `127.0.0.1:18980` / `:18981` / `:18982` |
| kp-1m state (snapshots, breaker.json, retained.json, evidence.lock) | `/var/lib/kp-1m-evidence` |
| recordings, proxy logs, inject results | `/var/lib/kp-1m-o7/{rec,px,inject,load}` |
| units | `kp-1m-passthrough`, `kp-1m-evidence` (MemoryMax=1.4G), `kp-1m-o7-recorder` |

## 0. Preconditions (all must hold; otherwise do not start)
1. M10c sampler and the 0.4.24 pre-sample have finished: not before **2026-10-27 11:35Z**
   (`commands.sh` refuses earlier). Do not touch the VPS before that.
2. P3-S1 build under test is frozen: record `kline-proxy --version`, the source commit and the
   binary sha256 (`SHA256SUMS` in the stage). M10c branch selected = A (S1 handoff).
3. Same-IP REST budget (§6, ≤ 1800 weight/min summed over every process on the IP):

   | process | configured cap /min |
   |---|---|
   | kp-1m `rest.weight_per_minute` (fallback 120 is inside it) | 1200 |
   | recorder probes (`--recorder-weight-cap`; planned 4 coins × 3 deltas × (1+1+2) + 1 = 49) | 120 |
   | 0.4.24 pre-sample / M10c sampler | must be stopped (else pass `--other-weight` and the recorder asserts the sum) |
   | inject.py / load_f16.py | 0 (they only talk to kp-1m and the proxy control port) |
   | **total** | **1320 ≤ 1800** |

   The recorder asserts this table at start (exit on violation) and stops probing for the
   minute if the upstream `X-MBX-USED-WEIGHT-1M` exceeds 1800. The proxy logs that header on
   every kp-1m request, so the IP-wide usage is reconstructable from `px/rest/*.jsonl.gz`.
4. If the 0.4.24 instance on :1890 is still running, it shares the IP: stop it first or count
   its 1200/min in `--other-weight` (1200 + 1200 + 120 > 1800 → must stop it).

## 1. Deploy (workstation → VPS, by hand)
1. Workstation: build and stage (no remote action):
   `rehearsal/install.sh --release s3b-<shortsha> --build` (needs cargo-zigbuild; or
   `--binary <linux x86-64 kline-proxy>`). Output: `rehearsal/stage/kp-1m-<release>/`.
2. `scp -r rehearsal/stage/kp-1m-<release> root@45.32.255.251:/tmp/`
3. On the VPS as root: `sh /tmp/kp-1m-<release>/commands.sh` — checks the preconditions, verifies
   checksums, installs binary+config as a pair, `--check-config`, writes the initial
   `breaker.json` (`{"last_conflict_ms":0,"revised_total":0}`, the deployment's decision that WS
   finals are trusted, §5), installs units, starts passthrough then kp-1m, waits for
   `/health/ready`, prints one evidence response and the effective `MemoryMax`, then starts the
   recorder.
4. Acceptance of the deploy: `/health/ready` 200, `ws_final_trusted=true`, `revised_total=0`,
   `systemctl show -p MemoryMax kp-1m-evidence` ≈ 1.4 GiB (1503238553 bytes, possibly page-rounded),
   RSS (`ps -o rss`) < 0.8 GB after the first 15 min.
5. kp-1m waits at most 5 s for exchangeInfo on a cold start and exits otherwise (existing
   behaviour, `metadata.rs` QUERY_BUDGET); systemd restarts it after 30 s. A slow first
   exchangeInfo through the proxy therefore shows up as a restart, not as a hang.

Note: `kp-1m-evidence` has `Requires=kp-1m-passthrough`, so restarting the proxy also restarts
kp-1m, which is itself an injection-7-like event. Do not touch the proxy unit during the 24 h recording.

## 2. Resource watch (whole rehearsal)
Every 10 min (cron or a bounded loop): `ps -o rss,pcpu -C kline-proxy,python3`, `free -m`,
`/health/ready` (`fallback_*`, `routine_queued`, `ws_final_trusted`, `revised_total`), and the
max `weight_1m` in the last 10 min of `px/rest`. Stop conditions: RSS > 1.2 GB, IP weight > 1800,
passthrough CPU > 60 % of one core sustained (the relay is Python; see HANDOFF).

## 3. 24 h recording (G-a..G-d data)
- `kp-1m-o7-recorder.service` (started by `commands.sh`): 24 h, deltas 0.5/1.5/5 s, `limit=3`,
  4 probe coins (BTCUSDT, ETHUSDT + 2 daily-hashed coins) × 3 forms × 3 deltas.
- **No injections during these 24 h.** G-d (`revised_total=0`, trust true throughout) is judged
  on this window only.
- Optionally in the last hour: `load_f16.py --base http://127.0.0.1:1891 --minutes 30 --limit 100
  --symbols-file <P4 523-coin list> --out /var/lib/kp-1m-o7/load/f16.jsonl` (F=16; loopback only).
- At the end: `systemctl status kp-1m-o7-recorder` (exited 0), `python3 summarize_rec.py
  /var/lib/kp-1m-o7/rec` for a quick look.

## 4. Injection window (after the recording, same release)
`python3 -I /opt/kp-1m/rehearsal/inject.py --config /etc/kp-1m/evidence/current.json
 --backend systemd --unit kp-1m-evidence --binary /opt/kp-1m/evidence/current/kline-proxy
 --out /var/lib/kp-1m-o7/inject --scenario <list>` (root, for systemctl).

Suggested order and expected duration on the VPS (directory refresh = 0.8 × 300 s = 240 s):

| # | scenario | needs | ~min |
|---|---|---|---|
| 1 | WS disconnect | — | 2 |
| 3 | block 5 of the top-20 active topics before B | proxy activity stats (≥ 1 h of relay) | 2 |
| 5 | delay M+1 REST visibility | — | 1 |
| 11 | request in [B, B+300 ms) | kp-1m restart timed by the proxy (exchangeInfo held to B+50 ms) | 2 |
| 2 | resubscribe (topic silent > 120 s) | — | 4 |
| 4 | TCP stall 200 s (> 180 s inbound deadline) | — | 6 |
| 7 | restart with 0.4.24-format snapshot | — | 3 |
| 8 | cold load, klines 503 for 20 s so WS lands first | moves snapshots to `cold-backup-*` | 4 |
| 6 | status → SETTLING → back (W5 at the original expiry) | `--max-wait-s 15000` to wait 241 min for W5, else W5 is INCONCLUSIVE | 10 (+241) |
| 10 | not_trading → restart within retention | — | 10 |
| 12 | TRADING restored while stopped | — | 10 |
| 13 | retention expires while stopped | 241 min downtime with the canonical config | 250 |
| 9 | breaker trip → restart → offline reset | ends with an offline `--reset-breaker` | 4 |

Run 9 last (it leaves `revised_total` > 0 by design). Scenario 13 with the canonical 241-minute
retention needs a ~4 h stop; either schedule it overnight, or decide to run it with a short-
retention config release (`not_trading_retention_min` changed only, switched as a binary+config
pair like any release) — that is an orchestrator decision (HANDOFF open item).

Each scenario prints its checks and writes `inject-<ts>.json` (PASS / FAIL / INCONCLUSIVE).
INCONCLUSIVE means the precondition did not occur (e.g. no startup request landed in
[B, B+300 ms)); re-run, it is not a pass.

## 5. Collect and hand over to P3-S3a
1. `systemctl stop kp-1m-o7-recorder` (if still running), keep kp-1m running.
2. On the VPS: `tar czf /var/lib/kp-1m-o7-<release>.tgz -C /var/lib/kp-1m-o7 rec px inject load`
   plus `kline-proxy --version`, the config, `SHA256SUMS`, `journalctl -u kp-1m-evidence
   --since <start>` (gz).
3. Copy only that archive back (it is the whole evidence; no Binance data is re-fetched).
4. P3-S3a: `kline-oracle kp-evidence-compare` against the canonical 1m (P1). Recording format
   below; field names are this kit's and need to be aligned with the P3-S3a HANDOFF.

## 6. Rollback
- Binary and config roll back **together** (§6/W8: 0.4.24 rejects the `evidence` field via
  `deny_unknown_fields`). `rollback.sh` in the stage: stops recorder + kp-1m, `--check-config`s
  the previous pair, switches both symlinks, restarts; on a first install it leaves
  `kp-1m-evidence` stopped and disabled.
- Remove the rehearsal entirely: `systemctl stop kp-1m-o7-recorder kp-1m-evidence
  kp-1m-passthrough; systemctl disable ...; rm /etc/systemd/system/kp-1m-{evidence,passthrough,
  o7-recorder}.service; systemctl daemon-reload`. State and recordings stay under
  `/var/lib/kp-1m-evidence` and `/var/lib/kp-1m-o7` until collected.
- The breaker is never reset by deleting `breaker.json` (§5): stop → `kline-proxy --reset-breaker
  /etc/kp-1m/evidence/current.json` (as kp1m) → start.

## Recording format (to be aligned with P3-S3a)
`rec/YYYYMMDDHH.jsonl.gz` — one line per (T, delta), recorder_o7pre fields plus:
`kind:"bulk"`, `T` (ms, minute boundary), `delta` (s), `n_req`, `with_evidence:true`, `limit`,
`t_send_ms`, `elapsed_s`, `http_status`, `resp` (the kp-evidence-v1 body verbatim: contract,
number_type, limit_effective, window, ws_final_trusted, revised_total, status, klines, evidence,
absent, missing, synthetic, covered_from), `invariant` (≤ 20 server-invariant violations found by
the recorder, empty when clean), or `error`.

`rec/rest/YYYYMMDDHH.jsonl.gz` — one line per (T, delta, symbol, form): `kind:"rest"`, `T`,
`delta`, `symbol`, `form` ∈ {`em` limit=3 endTime=now; `st` startTime=M−(st_back+1)m,
limit=st_back+3, endTime=now; `cold` limit=241 endTime=now no startTime}, `path` (exact query
sent), `end_time`, `t_send_ms`, `t_recv_ms`, `http_status`, `weight_1m` (X-MBX-USED-WEIGHT-1M,
IP-wide), `resp` (Binance row arrays verbatim); or `budget_skip` / `skipped` / `error`. The
three forms of one (T, delta, symbol) are sent concurrently ("same moment").

`rec/health/YYYYMMDDHH.jsonl.gz` — `/health/ready` at T+30 s (`fallback_queued`,
`fallback_in_flight_blocked`, `fallback_concurrency_blocked`, `fallback_budget_blocked`,
`routine_queued`, `ws_final_trusted`, `revised_total`, ...): backlog and the three fallback-delay
classes (W4).

`px/rest/YYYYMMDDHH.jsonl.gz` — every REST exchange through the proxy: `client` (kp-1m |
recorder), `path`, `query`, `symbol`, `t_recv_ms`, `t_up_send_ms`, `t_up_done_ms`, `t_done_ms`,
`status`, `weight_1m`, `rows` {n, first_open, last_open, opens (≤ 12 rows)}, `applied`
(injections). `px/events.jsonl` — ws_open / ws_close / ws_cmd (SUBSCRIBE/UNSUBSCRIBE) /
ws_minute counts / inject / clear.
