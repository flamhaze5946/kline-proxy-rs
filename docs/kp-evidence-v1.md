# kp-1m evidence mode

The evidence service is an opt-in futures continuous 1m instance. Existing configurations without `evidence` keep the 0.4.24 paths. No production configuration under `deploy/` is changed by this implementation.

Enable the object below alongside `number_type: "string"`, continuous 1m subscriptions with capacity 241, REST, and persistence:

```json
"evidence": {
  "ws_trust_window_ms": 86400000,
  "fallback_weight_per_minute": 120,
  "not_trading_retention_min": 241
}
```

The trust window is explicit and at least 60000ms. Set `rest.latest_repair_workers`, both persistence boundary guards, `bulk_admission_wait_ms`, `http_admission_wait_ms`, and `bulk_pre_boundary_wait_ms` explicitly. Keep `final_wait_ms` nonzero. The kp-1m deployment uses persistence guards 2000/15000ms and disables the market API, Vision warming and statistics. Roll back the configuration together with the executable because 0.4.24 rejects unknown configuration fields.

Request `GET /fapi/v1/klines/bulk?interval=1m&closed_only=true&limit=100&symbols=BTCUSDT,ETHUSDT&with_evidence=true`. For POST, put `with_evidence=true` in the URL and retain the existing JSON request body. The versioned response uses the exact field layout in `crates/kline-service/tests/fixtures/kp_evidence_v1_example.json`. Every requested symbol appears in every map, including empty arrays and explicit `covered_from: null`. `evidence` contains only `final` or `pending`; confirmed gaps appear in `absent`. A response is a fixed minute window; pending and missing minutes cannot be used as final history. The 75000-row bound counts only kline rows actually emitted for that fixed selection, including any retained history; unknown symbols and empty histories contribute no rows. Unrelated forming updates do not invalidate the closed evidence window. Normalized requested symbols have a separate ceiling of 100000, matching the existing directory capacity; exceeding it returns 400 with a reason before building a response.

The state directory is `persistence.directory`. It contains a stable `evidence.lock` advisory lock, `breaker.json` and `retained.json`, beside the existing snapshot files. The process holds the lock through its final snapshot/state writes. Lock-file existence is not lock state; the kernel releases the lock when the process exits. A second service or reset command fails immediately while the lock is held.

Provision the initial breaker before first start if the deployment has independently authorized WS final trust:

```json
{"last_conflict_ms":0,"revised_total":0}
```

A missing or unreadable breaker starts tripped, with `last_conflict_ms=now` and `revised_total=0`, and emits a warning. Before replacing an existing unreadable file, the service preserves it as `breaker.json.unreadable.<ms>`; reset also prints the preserved path. Failure to preserve an existing file refuses startup/reset. Deleting the file does not reset trust. Missing retained state is empty; unreadable retained state refuses startup. Both files use atomic replacement; failed writes remain dirty for retry. Retained records preserve their absolute expiry across restarts, and use the catalog symbol key within this futures/1m-only instance.

Reset only while offline, in this order:

1. Stop the service and wait for it to exit.
2. Run `kline-proxy --reset-breaker /path/to/config.json`.
3. Start the service with that same configuration.

Reset clears only `last_conflict_ms`; it preserves a readable `revised_total`. If the previous counter is unreadable, the command explicitly reports that it uses zero. It never changes retained records. It refuses to run against an active service.

Shutdown performs a final state flush after all producers and the snapshot writer finish. If a state write fails, shutdown keeps the directory locked and retries until that final flush succeeds.

Snapshots retain the 0.4.24 format. Confirmed absences are omitted and cannot be merged back from an older snapshot. Every restored row is pending until the startup full-window REST request or another qualifying observation provides fresh evidence.

REST pages publish within the instrument lifetime critical section shared by directory withdrawal. Withdrawal before publication discards the whole page; withdrawal after publication follows normal cleanup. Evidence requests retain the selected definition and tracking generation through waiting and encoding. Rechecks read immutable directory views outside the cache mutex and do not wait for directory writers. Full-market encoding also leaves the directory writer unlocked.
