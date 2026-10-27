# HANDOFF · P3-S3b · kp-1m evidence-mode 24 h rehearsal kit

- **status: DONE** (details in "Result" at the end).
- Clone `scratch/clones/kp-s3` (kline-proxy-rs), branch `super-interval/p3-s3b`, base `ece092b`
  (P3-S1 fix2). Every new file is under `rehearsal/`. Nothing under `crates/`, `deploy/`,
  existing tests or git metadata was changed (`git status --short` → `?? rehearsal/` only).
  No VPS connection, no Binance access, no JOURNAL, no agents. Local runs used only
  `mock_upstream.py` on 127.0.0.1.
- Spec: `scratch/design/P3-S0-kline-proxy-v8.md` §6, §7 (and §2–§5 for the asserts).

## 1. Inventory (`rehearsal/`)

| File | Purpose |
|---|---|
| `kp-1m.json` | VPS kp-1m evidence config sample, §6 values (see 1.1). JSON, not TOML: `Config::load` is `serde_json` with `deny_unknown_fields`; `--check-config` passes |
| `systemd/kp-1m-evidence.service` | kp-1m unit, `MemoryMax=1.4G`, `MemorySwapMax=0`, `ExecStartPre=--check-config`, binary+config via `current` symlinks |
| `systemd/kp-1m-passthrough.service` | proxy unit (loopback listeners, MemoryMax 300M) |
| `systemd/kp-1m-o7-recorder.service` | 24 h recorder, `Restart=no`, `RuntimeMaxSec=25h` |
| `install.sh` | stages binary + config + units + scripts + initial `breaker.json` + `SHA256SUMS`, and writes `commands.sh` / `rollback.sh` to be run **by hand**; it never runs ssh/scp/systemctl. `--build` uses `scripts/build-linux.sh` (cargo-zigbuild) and checks the ELF is x86-64 |
| `recorder_evidence.py` | O7 recorder (extends `recorder_o7pre.py`): with_evidence bulk at T+{0.5,1.5,5} s (`--limit`, default 3); 3 REST forms per probe coin at the same moment via the proxy; `/health/ready` at T+30 s; hourly gzip; `--hours`; same-IP budget asserts |
| `passthrough.py` | REST + WS relay with injection hooks and event log; control API on `127.0.0.1:18982` |
| `inject.py` | injections 1–13 (prepare / act / assert / recover), PASS/FAIL/INCONCLUSIVE, `--backend systemd|process` |
| `load_f16.py` | F=16 concurrent clients at the minute boundary, explicit list (~523) and full-market shapes; latency, codes, acceptance |
| `summarize_rec.py` | quick look at a recording (not the gate) |
| `kp_lib.py` | shared: evidence window invariant (§4), minute state, rolling budget, weights, RFC 6455 codec |
| `mock_upstream.py`, `local_stack.py`, `bench_relay.py` | **local only**: Binance mock (REST + continuousKline WS), local start/stop, relay throughput bench |
| `test_kit.py` | 11 offline unit tests (invariant + 5 negative mutations on the pinned fixture, frame codec, budget asserts, config §6 values, unit MemoryMax) |
| `RUNBOOK.md` | VPS order: preconditions → deploy → 24 h recording → injection window → collect → P3-S3a; budget table; rollback; recording format |
| `.gitignore` | `local-run/`, `stage/`, `__pycache__/` (local artifacts) |

### 1.1 Config values (`kp-1m.json`) and where they come from
- §6 verbatim: persistence guards 2000/15000 ms; REST guards 3000/10000 ms (unchanged);
  `ws_trust_window_ms` 86 400 000 (1440 min, §2.4 default, ≥ 60 s); `fallback_weight_per_minute`
  120; `not_trading_retention_min` 241; `history_capacity` 241; continuous 1m futures only;
  `number_type` string; market API, funding/Vision warm-up and statistics off; explicit
  `latest_repair_workers`, `bulk_admission_wait_ms`, `http_admission_wait_ms`,
  `bulk_pre_boundary_wait_ms`; `final_wait_ms` non-zero.
- **My choices where §6 gives no number** (orchestrator may override):
  `final_wait_ms`=8000 (0.4.24 default; also the admission waits), `bulk_pre_boundary_wait_ms`=250
  (0.4.24 default), `latest_repair_workers`=2 (pacing at 50 ms/weight, not concurrency, binds at
  Tokyo RTT; validation allows 1..4), `rest.weight_per_minute`=1200, `listen`=127.0.0.1:1891
  (1890 is the running 0.4.24 instance), state dir `/var/lib/kp-1m-evidence`,
  `metadata_refresh_seconds`=300 (directory refresh every 240 s).
- `rest.future_url` = `http://127.0.0.1:18980`, `websocket.future_url` =
  `ws://127.0.0.1:18981/market/stream` (both through the proxy; loopback http/ws is accepted by
  `transport::endpoint` / `validate_stream`). kp-1m code unchanged.

### 1.2 Passthrough: Python, not Rust — why
Stdlib only (no new crate or workspace member, nothing to build or vendor on the VPS, editable in
place during the injection window), shares the frame codec and invariant code with the Python
recorder/injector. Cost measured with `bench_relay.py` at full-market scale (see 3.4): 3005
frames/s over 4 connections for 4.5 % of one core (Apple Silicon; expect 2–3× on the VPS vCPU),
RSS 34 MB. The relay forwards raw frames unchanged; it only parses a topic prefix per frame and
JSON-decodes x=true frames (activity ranking for "top-N active"). WS handshake is terminated on
both sides (own key upstream, no extension negotiation → no permessage-deflate).

Injection hooks (`POST /inject`; time-bounded by from_ms/until_ms or count; REST rules apply only
to client `kp-1m` unless named): `rest_error` (5xx), `rest_timeout` (sleep > 15 s transport
timeout), `rest_stall` (hold the connection, no response, no close), `rest_hold` (release at a wall
time), `rest_delay_successor` (hide rows ≥ B for the first N ms of each minute = delay M+1
visibility), `rest_status` (exchangeInfo status rewrite / removal), `rest_mutate` (conflict probe),
`ws_block` (drop topics in a window; `only_final`, `minute_open`), `ws_resubscribe` (drop until
kp-1m sends SUBSCRIBE), `ws_stall` (stop relaying, TCP open), `ws_disconnect` (abort).
Coin selection `top:N` ranks by quote volume of x=true frames over the last 60 min.

### 1.3 Injections → assertions (`inject.py`)
| # | act | asserts (design §7 pass conditions) |
|---|---|---|
| 1 | abort all WS mid-minute | reconnect ≤ 60 s; minutes around the drop final/absent at B1+8 s; revised_total unchanged, trust true |
| 2 | silence 3 topics until kp-1m resubscribes | UNSUBSCRIBE/SUBSCRIBE seen, no reconnect; blocked minutes final only with REST backing |
| 3 | drop all frames of top-5 active coins B−5 s..B+10 s | M final (REST-backed) or pending at B+8 s; ≤ 3 designated attempts in [B, B+6.5 s); final by B+70 s |
| 4 | stall all WS 200 s | M per minute final-with-REST or pending; kp-1m drops the connection (180 s inbound deadline) and reconnects; then final |
| 5 | drop M's x=true + hide M+1 for 4 s | attempts at T+1.5/3/6 s, first two hidden, exactly 3; a bulk sent at B+4.5 s returns final only after the maturing REST response; M final (R) |
| 6 | exchangeInfo status → SETTLING → back | not_trading{since}; retained record (since, expires = since+retention); history retained; WS without the coin; pre-since minutes final/absent; back: trading, record revoked, subscription restored; W5: at the original expiry + one refresh, no startup-form sync and no WS change for the coin, snapshot still present (needs `--max-wait-s` ≥ retention) |
| 7 | stop (snapshot dump) → start | snapshots present; 100-min bridge complete ≤ 120 s; for every trading coin, the first request with limit ≥ 100 after start is limit=241 without startTime |
| 8 | snapshots moved away, klines 503 for 20 s, start | WS delivered before the first startup sync succeeded; startup still limit=241/no startTime; bridge ≤ 120 s after release |
| 9 | mutate a W minute in a fallback response | breaker trips (trust false, revised_total +1), evidence carries false, breaker.json persisted; still tripped after restart; offline `--reset-breaker` → trusted, revised_total preserved |
| 10 | not_trading → restart within retention | still not_trading, since and retained record identical |
| 11 | restart timed so startup syncs are built in [B, B+300 ms) (exchangeInfo held to just before B; M's x=true dropped) | for each such coin, a per-coin bulk sent right after that response returns final only after a REST response with endTime ≥ B+300 ms (i.e. M stayed pending) |
| 12 | not_trading → stop → TRADING → start | trading, record revoked, subscription restored |
| 13 | not_trading → stop until past expiry → start | unknown; snapshot file and retained record removed; still unknown after another refresh (no restart of retention) |
Every scenario also runs the §4 window invariant on each evidence sample. Stop/start/reset go
through `Service` (systemctl, or local process control).

## 2. Verification

| Command | Exit | Key output |
|---|---|---|
| `cargo build --release -p kline-runtime --bin kline-proxy` (CARGO_TARGET_DIR=clone/target) | 0 | `Finished release` 1m27s |
| `cargo test --workspace` (same target) | 0 | 40 suites, **386 passed / 0 failed / 0 ignored** (`local-run/cargo-test.log`) |
| `kline-proxy --check-config rehearsal/kp-1m.json` | 0 | `configuration valid: 0 static instruments, 1 subscriptions` |
| `python3 -m py_compile rehearsal/*.py` | 0 | — |
| `python3 -m unittest rehearsal/test_kit.py` | 0 | `Ran 11 tests … OK` |
| `sh install.sh --release s3b-ece092b --out local-run/stage`; `sh -n` on generated `commands.sh`/`rollback.sh` | 0 | staged dir with SHA256SUMS; syntax ok (no binary passed: Mac build is not linux) |
| `git status --short` | — | `?? rehearsal/` only |

Toolchain: `rustc 1.92.0 (Homebrew)`, Python 3.9.12. Local stack: mock (32 PERPETUAL USDT coins
incl. QUIETUSDT zero-volume minutes and GAPUSDT real gaps, + XAUUSDT TRADIFI, + BADUSDT
pair≠symbol) → passthrough → kp-1m (release build, config = `kp-1m.json` with only loopback
ports, local state dir, `metadata_refresh_seconds`=30 and `not_trading_retention_min`=4).
kp-1m came up with 33 series (BADUSDT excluded with the expected warning), ready, trusted.

### 3.1 Recorder, 6 minutes (17:24–17:29Z)
`recorder_evidence.py --hours 0.0917 --probe-symbols BTCUSDT,ETHUSDT,QUIETUSDT,GAPUSDT` → exit 0;
`budget ok: recorder planned 49/min <= cap 120; kp 1200 + other 0 + cap 120 <= 1800`.
`summarize_rec.py local-run/rec`:
```
delta=0.5  n=6 status={'200': 6} invariant_violations=0 trusted=[True] revised_total=[0] accept_latest[min/p50]=1.0/1.0 latency_s[p50/max]=0.586/1.17
delta=1.5  n=6 status={'200': 6} invariant_violations=0 trusted=[True] revised_total=[0] accept_latest[min/p50]=1.0/1.0 latency_s[p50/max]=0.082/0.174
delta=5.0  n=6 status={'200': 6} invariant_violations=0 trusted=[True] revised_total=[0] accept_latest[min/p50]=1.0/1.0 latency_s[p50/max]=0.003/0.007
em   n=72 status={'200': 72} max_weight_1m=91 M_mature=66
st   n=72 status={'200': 72} max_weight_1m=92 M_mature=66
cold n=72 status={'200': 72} max_weight_1m=92 M_mature=66
same-moment triples with M mature in all three forms: 66, values identical: 66
```
(The T+0.5 s bulk is held until every trading coin's M is acceptable — §4 wait — hence
~0.59 s latency; the 6 non-mature probes are GAPUSDT's real-gap minutes.)

### 3.2 F=16 load, 2 minutes
`load_f16.py --minutes 2 --limit 100` → exit 0: explicit (33 coins) 16× 200, p50/p99 289/677 ms;
market 16× 200, p50/p99 252/645 ms; acceptance of the latest minute 1.0; 0 invariant violations.

### 3.3 Injections (local)
`inject.py --local --scenario …` (logs `local-run/inject-*.log`, results
`local-run/inject/inject-*.json`). The final verdict comes from the last run on the final code.
Earlier failed runs were bugs in the kit, fixed as noted in the last column. None was a kp-1m
defect.

| # | final verdict | key evidence | earlier runs |
|---|---|---|---|
| 1 | PASS | final run (18:29): WS aborted, reconnected within 60 s (run 1: 0.55 s per the kp-1m log); minutes around the abort final/absent at B1 (18:31:00)+8 s; revised_total unchanged, trust true | run 1 also PASS; the assert window now follows the actual disconnect time, then re-run |
| 2 | PASS | UNSUBSCRIBE+SUBSCRIBE for the 3 silent topics after 120.8 s, 0 reconnects, 0 unbacked finals | — |
| 3 | PASS | top-5 blocked B−5..B+10 s: M final with REST backing at B+8 s, 1 attempt per coin, final by B+70 s | — |
| 4 | PASS | stall 200 s: M final-with-REST at both boundaries; kp-1m dropped the stalled connection after 180.0 s (`client:IncompleteReadError`), reconnected, latest minutes final | — |
| 5 | PASS | attempts at T+1.56/3.06/6.06 s, first two hidden, exactly 3; a bulk sent at B+4.51 s returned final at B+6.07 s, after the maturing response | first run FAIL: the assert expected "pending" from a request that §4 holds; rewritten (finding 4.1) |
| 6 | PASS | final run (18:23–18:29): not_trading{since 18:23:25.528}, record expires = since+4 min, history kept, WS reconnected without the coin, pre-since minutes final, back to trading, record revoked, subscription restored; W5 at expiry+refresh: 0 startup syncs, 0 WS changes, snapshot kept | run 1: proxy did not apply `rest_status` (path filter bug, fixed); run 2: W5 compared `covered_from`, which slides with the window, replaced by "no startup sync / no WS change / snapshot kept" |
| 7 | PASS | final run (18:29): 33 shutdown-written snapshots loaded, 100-minute bridge complete 5.1 s after start (run 2: 38.2 s), all 33 trading coins' first limit ≥ 100 request is limit=241 without startTime | runs 1–2 FAIL on the shape check. I first suspected a routine sync from the stopping process (cursor moved after the stop), but the real cause was the target-set fallbacks right after restart (finding 4.4); the check now uses the first limit ≥ 100 request per coin |
| 8 | PASS | final run (18:22): WS open 18:22:37.5, first klines 200 at 18:22:57.5 (klines 503 for 20 s), every one of the 33 trading coins' first limit ≥ 100 request is limit=241 without startTime, bridge complete 20.3 s after release | run 1 also PASS under the old heuristic; re-run with the corrected shape check |
| 9 | PASS | mutated W minute → trust false, revised_total 0→1, evidence false, breaker.json persisted, still tripped after restart, `--reset-breaker` → "revised_total=1", trusted again with 1 preserved | — |
| 10 | PASS | not_trading → restart: since 1793037562249 and record (since, expires) identical | — |
| 11 | PASS | 23 coins with startup requests built in [B, B+300 ms) and M+1 in the response; each coin's own bulk sent at B+0.108 s returned final only at B+1.64–2.77 s, after a REST response with endTime ≥ B+300 ms | run 1: held exchangeInfo 6 s > the 5 s cold-start budget (finding 4.2); run 2: requests at B+374 ms, which led to the self-calibration |
| 12 | PASS | not_trading → stop → TRADING → start: trading, record revoked, subscription restored | — |
| 13 | PASS | not_trading → stop past expiry → start: unknown, snapshot removed (true→false), record gone, still unknown after another refresh | — |

Local deviation: `not_trading_retention_min`=4 and `metadata_refresh_seconds`=30. Everything else
uses the VPS values. Mock artifact: XAUUSDT WS frames carried `ct:PERPETUAL`, so kp-1m ignored
them and XAUUSDT went through the REST fallback every minute. This is fixed in
`mock_upstream.py` after the runs and does not affect any assert.

### 3.4 Relay bench
`bench_relay.py --coins 750 --seconds 60` → `coins=750 connections=4 relay_frames_per_s=3005
proxy_cpu_pct_of_one_core=4.5 proxy_rss_mb=34 mock_cpu_pct_of_one_core=5.2 window_s=50`.

## 4. Findings while building (for the orchestrator)
1. **Bulk wait vs. "pending" observation.** An evidence bulk sent before B+`final_wait_ms` is held
   until every requested trading coin's M is acceptable (§4). A mid-minute "pending" therefore
   cannot be read directly. Scenarios 5/11 instead check that a per-coin request returns final only
   after the REST response that legitimately matured M. The recorder's δ samples measure
   acceptance **at return time**, and their `elapsed_s` must be read together with δ (P4 picks δ
   from these).
2. **Cold start needs exchangeInfo within 5 s.** In kp-1m (existing behavior, `metadata.rs`
   QUERY_BUDGET), a cold start whose first exchangeInfo takes more than 5 s exits with "Fresh exchange metadata temporarily
   unavailable". systemd restarts it after 30 s. My first scenario-11 attempt held exchangeInfo for 6 s
   and hit this. Now noted in the RUNBOOK.
3. Local startup latency: from exchangeInfo release to the first startup klines request took about
   320 ms (33 coins). Scenario 11 calibrates itself (up to 3 restarts). On the VPS (about 750
   snapshots to restore) the first attempt will likely be late, and the auto-calibration absorbs it.

4. **Right after a restart, fallbacks run in parallel with the startup sync.** Restored rows are
   P, so every coin enters the target set right away. kp-1m sends E<M fallbacks (Urgent,
   startTime, limit ≤ 99) alongside the Background startup sync (limit=241); in the local
   kp-1m log, Background startup requests waited 1.7–2.0 s for pacing. From the proxy side a
   startup can only be identified by limit ≥ 100. A "narrowed" startup with limit ≤ 99 cannot be
   told apart from a fallback there, so that shape stays covered by the P3-S1 unit test
   (`startup_is_whole_window_even_when_ws_arrives_first_*`). On the VPS (about 750 coins, 1200
   weight/min) the 120/min fallback budget and the 1500-weight startup compete, so check
   scenario 7's bridge time against the "1–2 min" figure in §3.5.

## 5. Not covered / needs the real VPS
- Real Binance behaviour: maturity of early REST (§2.2/§9-a), E2 silent-but-traded minutes, the
  real `X-MBX-USED-WEIGHT-1M` interplay with the 1800 budget, Binance ping cadence through the
  relay, slow-consumer disconnects during scenario 4, TLS keep-alive behaviour of the REST relay.
- Resource criteria (§6): RSS < 0.8 GB with about 750 coins plus retained coins under `MemoryMax=1.4G`,
  2 vCPU headroom with the Python relay. Locally the series count is only 33.
- "Top-20 most active" (scenario 3) has meaning only with real volume. Locally it falls back to
  mock quote volume.
- Scenarios 6 (W5) and 13 with the canonical 241-minute retention: they need about 4 h of waiting or
  downtime. The orchestrator must choose: (a) run them overnight with `--max-wait-s 15000`, or (b)
  switch, for them only, to a release pair whose config changes only `not_trading_retention_min`.
  Locally they ran with 4 min.
- §7 statistics need the P3-S3a comparator: G-a/G-b/G-c against canonical, δ acceptance
  percentiles, fallback backlog and the three delay classes (the raw data is recorded: `/health`
  each minute, proxy REST log with timings). Field names are listed in RUNBOOK "Recording format"
  and still have to be aligned with the P3-S3a HANDOFF (it does not exist yet; the
  `clones/p3-s3a` tree has no handoff).
- `install.sh --build` was not run (cargo-zigbuild is not used locally). The staged
  `commands.sh` and `rollback.sh` were only syntax-checked, never executed.

## Result
**DONE.** Every item in the brief has been delivered under `rehearsal/`. Local verification:
release build, `cargo test --workspace` 386/0/0, `py_compile`, 11 unit tests, the recorder for
6 minutes, F=16 load, the relay bench, and all 13 injections **PASS** on the final code (final runs
`local-run/inject/inject-20261026T183108Z.json` (7, 1), `…T182916Z.json` (8, 6),
`…T181510Z.json` (11, 9, 3, 12, 10, 13, 2, 4; the 6/7 entries in that file are superseded)). Earlier failed runs were kit bugs and are listed in 3.3.

Open items for the orchestrator: (1) the config values I chose where §6 is silent (1.1);
(2) scenarios 6-W5/13 with 241 min on the VPS (overnight vs a short-retention release pair);
(3) align the recording field names with P3-S3a once its HANDOFF exists; (4) the VPS run
itself: deploy only after 2026-10-27 11:35Z, then the 24 h recording, then the injection
window (RUNBOOK). The config deviation is a JSON sample instead of `kp-1m.toml`, because
the loader only reads JSON.

---

## §fix1 (2026-10-26, on top of f7b44f6; same clone, only `rehearsal/`; no git metadata, no VPS, no Binance)

Where this section conflicts with the earlier ones (e.g. the old recording fields in §1/§3), this
section wins. Authority for the format: `scratch/clones/p3-s3a/HANDOFF-P3-S3a.md` (§fix2 > §fix1 > §2) and
`xtask/kline-oracle/src/kp_evidence.rs` (read only). Rulings applied: `final_wait_ms` 8000,
`latest_repair_workers` 2, `listen` 1891 adopted; the rewritten asserts of scenarios 5/11
(closed_only holds the bulk, no mid-minute pending) adopted; 6-W5 and 13 run in the rehearsal's
night slot with the canonical 241-minute config, no extra binary.

### Changes
| File | Change |
|---|---|
| `recorder_evidence.py` | rewritten to schema v1. `bulk`: `schema`, `T`, `delta`, `with_evidence`, `limit`, `req_symbols` (exact non-empty list), `n_req`, `t_send_ms`, `elapsed_s`, `http_status`, exactly one of `resp_text` (raw body) / `error`; empty universe refuses to record. `rest_probe`: forms `end_m` / `start_time` / `cold_page`, `params` ints, `used_weight_1m`, `rows` / `error`. `proxy_stats`: per minute from 1 s `/health/ready` samples + passthrough `/stats` (definition in RUNBOOK "Recording format"). Only these three kinds are written to the top-level hourly files (the old `rest/` and `health/` subdirectories are gone) |
| `kp_lib.py` | evidence POST URL is now `?with_evidence=true&closed_only=true&limit=<n>` (the P3-S2 client form, §2.1) |
| `passthrough.py` | `GET /stats?from_ms=&to_ms=`: max X-MBX-USED-WEIGHT-1M relayed in the interval (source of `rest_used_weight_1m`) |
| `repull.py` (new) | re-pull producer (fix1-6, fix2-L1): accepted absents from the `resp_text` bodies → one weight-1 request per consecutive run (±2 min, once ±30 min if a side lacks a row; runs split at UTC hours and at 35 minutes so limit ≤ 99), only for minutes whose UTC day has ended, endTime ≥ G+1m+300 ms asserted, `t_send_ms` always written, output `repull_<YYYYMMDDHH>.jsonl.gz` by proved hour; same-IP cap `--weight-cap` (120) with the kp + other + cap ≤ 1800 assertion; `--local-format-test` (loopback only) lifts the day-ended rule for a format check |
| `summarize_rec.py` | reads schema v1 |
| `install.sh` | stages `repull.py` |
| `test_kit.py` | +2 tests: bulk record has exactly one of resp_text/error and rejects an empty req_symbols; repull run splitting and day end (13 tests) |
| `RUNBOOK.md` | formal O7 order (§5: 24 h → Vision daily zips → EPYC `kline-1m-sync` every day incl. listed/delisted coins + audited repair → `repull.py` → `kp-evidence-compare` with no allow flags); exchangeInfo 5 s cold-start exit and its handling (§1 item 5); night slot for 6-W5 and 13 (§4); recording format replaced by the schema v1 description incl. the `proxy_stats` source |

### Verification
| Command | rc | Key output |
|---|---|---|
| `cargo build --release -p kline-oracle` in `clones/p3-s3a` (044808b8, `CARGO_TARGET_DIR=<p3-s3a>/target`) | 0 | Finished release 1m21s |
| `python3 -m py_compile rehearsal/*.py`; `python3 -m unittest test_kit.py` | 0 | 13 tests OK |
| `sh -n install.sh` | 0 | — |
| recorder 2.7 min (`local-run/rec-v1`) → `summarize_rec.py` | 0 | 12 bulk (all 200, `resp_text`, 0 invariant violations), 144 rest_probe (all 200, three-form values identical 48/48), 3 proxy_stats |
| `repull.py --local-format-test` on rec-v1 | 0 | 1 request (GAPUSDT absent 18:35Z): `startTime=G−2m`, `endTime=G+2m`, `limit=5`, rows at 18:33/18:34/18:36/18:37, no row at G |
| `kp-evidence-compare` rec-v1 + repull-v1, `--allow-short-window` | 1 | **FAIL `gd_revised_total_nonzero: 12`**. The local state carried `revised_total=1` from injection 9 (offline reset preserves it, by design). Format: `bad_lines` 0, `missing_slots` 0, `dup_key_checked` 12 / unchecked 0, `replayed_bodies` 0, `gc_contract_or_invariant` 0, `gc_duplicate_json_key` 0, rest_probe errors 0/0/0, repull loaded with no problem; BLOCKED only `trading:no_coverage` (empty store) and `gb_undetermined: 6` (no canonical; the local repull is same-day, so not evidence) |
| local test breaker re-provisioned like a fresh deployment (`{"last_conflict_ms":0,"revised_total":0}` with kp-1m stopped; local only), recorder 2 min again (`local-run/rec-v1b`) | 0 | 9 bulk, 108 rest_probe, 3 proxy_stats (last-minute stats now written), `revised_total` 0, trusted true |
| `kp-evidence-compare` rec-v1b + repull-v1, `--allow-short-window` | 1 | **BLOCKED only for the canonical**: `canonical_undetermined: 160`, `trading:no_coverage=160 minutes/32 symbols`. Format errors **0**: `bad_lines` 0, `missing_slots` 0, `duplicate_slots` 0, `records_outside_range` 0, `bulk_errors` 0, `dup_key_checked` 9 / unchecked 0, `replayed_bodies` 0; G-a / G-c / G-d violations all 0; rest_probe errors 0; proxy_stats 3 parsed |
| same, formal mode (no allow flags) | 1 | BLOCKED: the same canonical reason + `range spans 3 < 1440 minutes`. No `resp_text` reason (fix2-M3 satisfied) |

Expected BLOCKED: the local canonical store is empty (`local-run/empty-store/swap_1m`, no
Vision/sync for mock coins), so every covered minute is `no_coverage`, and G-b cannot be proven.
A real PASS needs the formal sequence in RUNBOOK §5 on EPYC.

### Open / not verified
- `repull.py` against Binance: not run (no network). Locally it is exercised only through the
  mock and `--local-format-test`; the day-ended rule was exercised by the normal run
  (0 requests, nothing eligible on the same day).
- `proxy_stats` units: blocked coin × 1 s sample per minute (gauge integration). If P3-S3a wants
  event counts, kp-1m would need counters; that is a `crates/` change, out of scope.
- The formal compare on real data (24 h, canonical synced after the Vision zips, repull
  after the day) is the VPS/EPYC step in RUNBOOK §5.

### Result
**DONE.** The recording is schema v1; the local comparator self-test has 0 format errors, and its
only BLOCKED reason is the expected empty canonical store. `repull.py` is added, and the RUNBOOK
covers the formal O7 order, the cold-start exit and the night slot.
