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
5. **exchangeInfo cold-start budget (existing 0.4.24 behaviour, not a P3-S1 change).** On a
   cold start kp-1m waits at most 5 s (`metadata.rs` QUERY_BUDGET) for the first exchangeInfo
   and exits with `Fresh exchange metadata temporarily unavailable` otherwise. Handling:
   - systemd `Restart=on-failure` / `RestartSec=30` brings it back; the WS connect allowance is
     shared across restarts (state dir), so a few retries are harmless;
   - if it repeats: check the proxy (`systemctl status kp-1m-passthrough`, `px/events.jsonl`
     `ws_upstream_error`, `px/rest` exchangeInfo latency/status) and Binance reachability from
     the VPS (`curl -sS -o /dev/null -w '%{time_total}' https://fapi.binance.com/fapi/v1/time`);
     a stuck injection (`rest_hold` / `rest_stall` / `rest_timeout` on exchangeInfo) is cleared
     with `curl -X POST -d '{}' http://127.0.0.1:18982/clear`;
   - never hold exchangeInfo ≥ 5 s in an injection (inject.py scenario 11 keeps it ~2 s);
   - every restart is a restart event for the O7 data (bridge restore, startup syncs): note its
     time in the run log; during the 24 h recording it must not happen at all (G-d window).

Note: `kp-1m-evidence` has `Requires=kp-1m-passthrough`, so restarting the proxy also restarts
kp-1m, which is itself an injection-7-like event. Do not touch the proxy unit during the 24 h recording.

## 2. Resource watch (whole rehearsal)
Every 10 min (cron or a bounded loop): `ps -o rss,pcpu -C kline-proxy,python3`, `free -m`,
`/health/ready` (`fallback_*`, `routine_queued`, `ws_final_trusted`, `revised_total`), and the
max `weight_1m` in the last 10 min of `px/rest`. Stop conditions: RSS > 1.2 GB, IP weight > 1800,
passthrough CPU > 60 % of one core sustained (the relay is Python; see HANDOFF).

## 3. 24 h recording (G-a..G-d data)
- `kp-1m-o7-recorder.service` (started by `commands.sh`): 24 h, deltas 0.5/1.5/5 s, `limit=3`,
  4 probe coins (BTCUSDT, ETHUSDT + 2 daily-hashed coins) × 3 forms × 3 deltas, writes P3-S3a
  schema v1 (see "Recording format") to `/var/lib/kp-1m-o7/rec/`.
- Start it so that the recording covers whole UTC days if possible: G-b needs complete days
  ("该币该日的基准已发布且完整"), and the formal compare range `[from, to)` must be ≥ 1440 minutes.
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
| 6 | status → SETTLING → back (W5 at the original expiry) | night slot, `--max-wait-s 15000` (W5 at since + 241 min) | 10 (+241) |
| 10 | not_trading → restart within retention | — | 10 |
| 12 | TRADING restored while stopped | — | 10 |
| 13 | retention expires while stopped | night slot, `--max-wait-s 15000`, ~4 h kp-1m downtime with the canonical config | 250 |
| 9 | breaker trip → restart → offline reset | ends with an offline `--reset-breaker` | 4 |

Run 9 last (it leaves `revised_total` > 0 by design).

**Night slot for 6 (W5) and 13 (orchestrator ruling 2026-10-26: same binary and the canonical
241-minute config, no extra release).** Both need the real 241-minute retention:
- schedule them after the day-time scenarios, in the rehearsal's night hours, one after the
  other: `inject.py ... --scenario 6 --max-wait-s 15000` (≈ 4 h 15 min: W5 is checked at
  since + 241 min + one directory refresh), then `--scenario 13 --max-wait-s 15000` (≈ 4 h 20 min,
  kp-1m stopped for ~241 min);
- run them inside a bounded transient unit, e.g. `systemd-run --unit kp-1m-inject-night
  -p RuntimeMaxSec=10h python3 -I /opt/kp-1m/rehearsal/inject.py ...`, so a hang cannot outlive
  the window;
- scenario 13 stops kp-1m for ~4 h: nothing else may depend on kp-1m during that time (the 24 h
  recording is over by then); the same-IP budget is unaffected (a stopped kp-1m uses none).

Each scenario prints its checks and writes `inject-<ts>.json` (PASS / FAIL / INCONCLUSIVE).
INCONCLUSIVE means the precondition did not occur (e.g. no startup request landed in
[B, B+300 ms)); re-run, it is not a pass.

## 5. Formal O7 (order is mandatory)
P3-S3a fix2-H1: the canonical accepts only Vision Zip days and audited repairs; REST-tail minutes
are `rest_tail_unpublished` (BLOCKED). So the comparison waits for the daily archives.

1. **Record 24 h** (section 3). `systemctl status kp-1m-o7-recorder` → exited 0.
2. **Wait for the Vision daily archives** of every UTC day touched by the recording
   (`data.binance.vision/data/futures/um/daily/klines/<SYM>/1m/<SYM>-1m-<YYYY-MM-DD>.zip`;
   usually published the next day, check a few symbols before step 3).
3. **On EPYC, `kline-1m-sync` every day of the window into the canonical store**, universe =
   every symbol requested in the recording (`req_symbols` union), including coins listed or
   delisted during the window:
   `kline-1m-sync --symbols <union.txt> --root <store> --until <end of last day>`; then the audited
   `kline-1m-sync repair` (I70) where the sync reports gaps. TradFi / coins outside the canonical
   universe go into an explicit `--exclude-symbols` file with a reason (fix1-3), never silently.
4. **Re-pull the accepted absents** (G-b condition 3), after the last recorded UTC day has ended:
   on the VPS, `python3 -I /opt/kp-1m/rehearsal/repull.py --recording /var/lib/kp-1m-o7/rec
   --out /var/lib/kp-1m-o7/repull --rest-base http://127.0.0.1:18980 --kp-weight-per-minute 1200`
   (or `--kp-weight-per-minute 0` if kp-1m is stopped). Budget: `--weight-cap 120`/min, asserted
   with kp + other ≤ 1800; one weight-1 request per absent run (±2 min, once ±30 min if a side has
   no row). It refuses minutes whose UTC day has not ended.
5. **Collect**: `tar czf /var/lib/kp-1m-o7-<release>.tgz -C /var/lib/kp-1m-o7 rec repull px inject load`
   plus `kline-proxy --version`, the config, `SHA256SUMS`, `journalctl -u kp-1m-evidence --since
   <start>` (gz). Copy only that archive to EPYC.
6. **Compare in formal mode, no allow flags**:
   `kline-oracle kp-evidence-compare --recording rec --store-1m <store> --rest-repull repull
   --from <first T> --to <last T + 1m> [--exclude-symbols <file>] --out o7-report.json`
   (no `--allow-short-window`, no `--allow-resp-object`; range ≥ 1440 minutes). Exit 0 = PASS.

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

## Recording format = P3-S3a schema v1
Authority: `scratch/clones/p3-s3a/HANDOFF-P3-S3a.md` (§fix2 > §fix1 > §2). The recorder writes
only these three kinds into `rec/YYYYMMDDHH.jsonl.gz` (hour of `T`); every line has
`"schema": 1`, `"kind"`, `"T"` (minute boundary ms).

- `bulk` (§2.1, fix1-5, fix2-M1/M3): `T`, `delta`, `with_evidence: true`, `limit`,
  `req_symbols` (exact non-empty list sent, no duplicates), `n_req`, `t_send_ms` (wall clock just
  before the POST), `elapsed_s`, `http_status`, and exactly one of `resp_text` (raw HTTP body,
  2xx) / `error` (non-2xx with status and body head, transport error, or "sample thread did not
  finish"). Request: `POST /fapi/v1/klines/bulk?with_evidence=true&closed_only=true&limit=<n>`,
  body `{"interval":"1m","limit":n,"closed_only":true,"symbols":[...]}`.
- `rest_probe` (§2.2): `symbol`, `form` ∈ `end_m` (limit=3, endTime=now) / `start_time`
  (startTime=M−3m, limit=5, endTime=now) / `cold_page` (limit=241, endTime=now), `params`
  (the exact query ints), `t_send_ms`, `elapsed_s`, `http_status`, `used_weight_1m`
  (X-MBX-USED-WEIGHT-1M, IP-wide), `rows` (raw Binance rows) | `error`. The three forms of one
  (T, delta, symbol) are sent concurrently.
- `proxy_stats` (§2.3) for minute T, written at T+60.5 s. **Data source:** kp-1m
  `/health/ready`, sampled every 1 s (`--stats-period`). Its `fallback_*` fields are per-pass
  gauges (the number of due fallbacks blocked in that reconcile pass), not counters. Hence:
  `fallback_backlog` = max `fallback_queued` over the minute's samples; `fallback_delay.in_flight`
  / `.rate_limited` / `.concurrency_blocked` = sum over the samples of
  `fallback_in_flight_blocked` / `fallback_budget_blocked` / `fallback_concurrency_blocked`
  (unit coin × sample, this minute only; `rate_limited` is the 120/min fallback budget, transport
  token waits are not exposed by kp-1m); `rest_used_weight_1m` = max X-MBX-USED-WEIGHT-1M the
  passthrough relayed in the minute (`GET 127.0.0.1:18982/stats?from_ms=&to_ms=`), null if none.
  `/metrics` was not used: it has no fallback counters.

Re-pull directory `repull/repull_<YYYYMMDDHH>.jsonl.gz` (fix1-6, fix2-L1), written by
`repull.py`: `{"kind":"rest_repull","schema":1,"symbol","params":{"startTime","endTime","limit"},
"t_send_ms","http_status","for_minutes":[...],"rows":[...]}` or `error` instead of `rows`;
`t_send_ms` ≥ end of the proved minute's UTC day and `endTime` ≥ G + 1m + 300 ms.

Not read by the comparator (other directories): `px/rest/YYYYMMDDHH.jsonl.gz` — every REST
exchange through the proxy (`client` kp-1m | recorder | repull, `path`, `query`, timings,
`status`, `weight_1m`, row summary, `applied` injections); `px/events.jsonl` — ws_open /
ws_close / ws_cmd / ws_minute / inject / clear; `inject/inject-*.json`; `load/`.
