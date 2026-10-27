//! Opt-in reconciliation. The 0.4.24 loop is intentionally left on its own path.
use super::*;
use crate::{
    evidence_recovery::{EvidenceRequest, TaskKind},
    evidence_schedule::{FallbackBudget, FallbackOrder, TaskRecord},
};
use kline_core::evidence::MINUTE_MS as MINUTE;

pub(super) async fn reconcile(
    engine: Arc<Engine>,
    api: Arc<RestApi>,
    config: RestConfig,
    health: Arc<Health>,
    stop: watch::Receiver<bool>,
) {
    reconcile_state(
        engine,
        api,
        config,
        health,
        stop,
        Duration::from_millis(250),
        Vec::new(),
    )
    .await;
}
async fn reconcile_state(
    engine: Arc<Engine>,
    api: Arc<RestApi>,
    config: RestConfig,
    health: Arc<Health>,
    mut stop: watch::Receiver<bool>,
    tick_period: Duration,
    mut records: Vec<Option<TaskRecord>>,
) {
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api,
        config: config.clone(),
    });
    let mut tasks: JoinSet<(
        usize,
        crate::evidence_schedule::Attempt,
        anyhow::Result<usize>,
    )> = JoinSet::new();
    let mut order = FallbackOrder::default();
    let mut fallback_running = 0;
    let mut sync_running = 0;
    let settings = engine.settings().evidence.as_ref().unwrap();
    let mut budget = FallbackBudget::new(settings.fallback_weight_per_minute);
    let mut tick = tokio::time::interval(tick_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let interval = (config.reconcile_seconds * 1000) as i64;
    'running: while !*stop.borrow() {
        let completed = tokio::select! {
            _ = stop.changed() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => completed,
            _ = tick.tick() => None,
        };
        for result in completed
            .into_iter()
            .chain(std::iter::from_fn(|| tasks.try_join_next()))
        {
            match result {
                Ok((id, attempt, result)) => {
                    let mut attempt: crate::evidence_schedule::Attempt = attempt;
                    if attempt.kind == TaskKind::History && engine.catalog.slot(id).is_trading() {
                        attempt.confirm_lifecycle = false;
                    }
                    if attempt.kind == TaskKind::Fallback {
                        fallback_running -= 1;
                    } else {
                        sync_running -= 1;
                    }
                    if let Some(record) = records.get_mut(id).and_then(Option::as_mut) {
                        record.finish(attempt, engine.now_ms(), result.is_ok(), interval);
                    }
                    if let Err(error) = result {
                        tracing::warn!(id, kind=?attempt.kind, %error, "evidence recovery will retry");
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "evidence recovery task panicked");
                    break 'running;
                }
            }
        }
        let slots = engine.catalog.slots();
        records.resize_with(slots.len(), || None);
        let feeds = health.feeds.load();
        let feed_digest = digest(feeds.iter().map(Feed::token));
        let mut epochs = vec![0_u64; slots.len()];
        for feed in feeds.iter() {
            if let Some(ids) = &feed.ids {
                for &id in ids.iter() {
                    if let Some(epoch) = epochs.get_mut(id) {
                        *epoch = epoch.wrapping_mul(1099511628211) ^ feed.token();
                    }
                }
            } else {
                for (id, _) in slots
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| slot.market == feed.market)
                {
                    epochs[id] = epochs[id].wrapping_mul(1099511628211) ^ feed.token();
                }
            }
        }
        let now = engine.now_ms();
        let latest = now.div_euclid(MINUTE) * MINUTE - MINUTE;
        let trusted = engine.ws_final_trusted();
        let mut available = vec![SeriesReadiness::default(); slots.len()];
        let (mut tracked, mut ready) = (0, 0);
        let mut fallback = Vec::new();
        let mut sync = Vec::new();
        let mut in_flight_blocked = 0;
        for (id, slot) in slots.iter().enumerate() {
            if !slot.is_tracked() {
                records[id] = None;
                order.forget(id);
                continue;
            }
            tracked += 1;
            if records[id]
                .as_ref()
                .is_none_or(|record| record.tracking_generation != slot.tracking_generation())
            {
                order.forget(id);
                let mut record = TaskRecord::new(now);
                record.tracking_generation = slot.tracking_generation();
                records[id] = Some(record);
            }
            let record = records[id].as_mut().unwrap();
            record.observe(now, epochs[id], slot.capacity());
            // J57: retain all legacy readiness accounting for nontrading slots,
            // while also allowing their startup and historical work to be queued.
            if !slot.is_trading() {
                ready += 1;
                record.established = true;
                available[id] = SeriesReadiness {
                    ready: true,
                    established: true,
                    capacity: slot.capacity(),
                    ..SeriesReadiness::default()
                };
            } else {
                let (_, fresh, _, provisional) =
                    slot.lifecycle_status(now, config.freshness_grace_ms as i64, latest);
                let current = record.initialized
                    && record.epoch == epochs[id]
                    && epochs[id] != 0
                    && record.gap == slot.gap_generation()
                    && (fresh || provisional);
                if current {
                    ready += 1;
                    record.established = true;
                }
                available[id] = SeriesReadiness {
                    ready: current,
                    established: record.established,
                    epoch: record.epoch,
                    gap: record.gap,
                    capacity: slot.capacity(),
                };
            }
            if slot.is_trading() {
                record.observe_latest(latest, engine.needs_verdict(id, latest));
                record.resolve_specified(|open| engine.needs_verdict(id, open));
                let earliest = slot.select_bars(|s| {
                    s.earliest_needs_verdict((latest - 96 * MINUTE).max(0), latest, trusted)
                });
                if let Some(earliest) = earliest
                    && let Some(due) =
                        record.fallback_due(now, earliest, latest, slot.last_rest_request_ms())
                {
                    if record.running {
                        in_flight_blocked += 1;
                    } else {
                        fallback.push((earliest, id, due));
                    }
                }
            }
            if !record.running
                && let Some(kind) = record.sync_due(now, slot.is_trading()).or_else(|| {
                    (slot.is_trading()
                        && record.initialized
                        && now >= record.history.due
                        && recovery.evidence_history_request(id, now).is_some())
                    .then_some(TaskKind::History)
                })
                && (kind == TaskKind::Startup || !recovery.evidence_guarded(now))
            {
                sync.push((kind, id));
            }
        }
        fallback.sort_by_key(|&(earliest, id, _)| (earliest, order.rank(id), id));
        let queued = fallback.len();
        let sync_queued = sync.len();
        let mut concurrency_blocked = 0;
        let mut budget_blocked = 0;
        if health.clock_ready() {
            // These counters are independent: background work never occupies a
            // latest_repair_workers permit, including during boundary guards.
            for (earliest, id, due) in fallback {
                if fallback_running >= config.latest_repair_workers {
                    concurrency_blocked += 1;
                    continue;
                }
                if !budget.available(now) {
                    budget_blocked += 1;
                    continue;
                }
                let Ok(gate) = slots[id].rest_gate().try_lock_owned() else {
                    in_flight_blocked += 1;
                    continue;
                };
                let record = records[id].as_mut().unwrap();
                let request = EvidenceRequest::fallback(earliest, latest, now);
                let attempt = record.begin(
                    TaskKind::Fallback,
                    request.range,
                    Some(due),
                    epochs[id],
                    slots[id].gap_generation(),
                );
                budget.consume(now);
                order.consume(id);
                fallback_running += 1;
                let r = recovery.clone();
                tasks.spawn(
                    async move { (id, attempt, r.execute_evidence(id, request, gate).await) },
                );
            }
            // Startup first; subsequent sync retries remain Background and use
            // their own exponentially backed-off demand deadlines.
            sync.sort_by_key(|&(kind, id)| (kind != TaskKind::Startup, id));
            for (kind, id) in sync {
                if sync_running >= config.workers {
                    break;
                }
                let record = records[id].as_mut().unwrap();
                if record.running {
                    continue;
                }
                let Ok(gate) = slots[id].rest_gate().try_lock_owned() else {
                    continue;
                };
                let request = if kind == TaskKind::History && slots[id].is_trading() {
                    let Some(request) = recovery.evidence_history_request(id, now) else {
                        continue;
                    };
                    request
                } else {
                    recovery.evidence_sync_request(id, kind != TaskKind::Startup, now)
                };
                let mut attempt = record.begin(
                    kind,
                    request.range,
                    None,
                    epochs[id],
                    slots[id].gap_generation(),
                );
                if kind == TaskKind::History && !slots[id].is_trading() {
                    attempt.confirm_lifecycle = true;
                }
                sync_running += 1;
                let r = recovery.clone();
                tasks.spawn(
                    async move { (id, attempt, r.execute_evidence(id, request, gate).await) },
                );
            }
        }
        health.series.store(Arc::new(available));
        health
            .acknowledged_catalog
            .store(slots.generation, Ordering::Release);
        health
            .acknowledged_gap
            .store(engine.gap_epoch(), Ordering::Release);
        health
            .acknowledged_feeds
            .store(feed_digest, Ordering::Release);
        health.ready.store(ready == tracked, Ordering::Release);
        if tracked > 0 && health.is_ready() {
            health.bootstrapped.store(true, Ordering::Release);
        }
        *health.detail.write() = serde_json::json!({
            "phase": if ready == tracked { "live" } else { "recovering" },
            "configured_series": tracked, "ready_series": ready, "recovering_series": tracked-ready,
            "rest_jobs": tasks.len(), "latest_repair_jobs": fallback_running,
            "initial_pending": records.iter().flatten().filter(|r| !r.initialized).count(),
            "routine_rest_jobs": sync_running, "routine_queued": sync_queued,
            "fallback_queued": queued, "fallback_in_flight_blocked": in_flight_blocked,
            "fallback_concurrency_blocked": concurrency_blocked, "fallback_budget_blocked": budget_blocked,
            "ws_final_trusted": trusted, "revised_total": engine.breaker_state().revised_total,
            "tail_rest_policy": "evidence", "observed_at_ms": now, "streams": feeds.len(),
        });
    }
    health.ready.store(false, Ordering::Release);
    health.stopped.store(true, Ordering::Release);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[cfg(test)]
mod fix2_tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{Query, State},
        routing::get,
    };
    use kline_core::{
        Bar, Interval, NumberType, Source, Update,
        evidence::{EvidenceState, EvidenceUpdate},
    };
    use kline_service::{Catalog, Instrument, Settings, evidence::EvidenceSettings};
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroUsize,
        sync::atomic::AtomicUsize,
    };
    const B: i64 = 6_000_000_000;
    struct TestClock(AtomicI64);
    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Acquire)
        }
    }
    fn bar(open: i64) -> Bar {
        let mut b = Bar {
            open_time: open,
            close_time: open + 59999,
            trades: 1,
            ..Bar::default()
        };
        b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
        b
    }
    fn engine(count: usize, clock: Arc<TestClock>) -> Arc<Engine> {
        Engine::new(
            Catalog::new(
                (0..count)
                    .map(|i| Instrument {
                        market: Market::Future,
                        symbol: format!("S{i:04}"),
                        interval: Interval::parse("1m").unwrap(),
                        trading: true,
                        continuous: Some((format!("S{i:04}"), "PERPETUAL".into())),
                        capacity: NonZeroUsize::new(241).unwrap(),
                    })
                    .collect(),
            )
            .unwrap(),
            clock,
            Settings {
                number_type: NumberType::String,
                evidence: Some(EvidenceSettings::default()),
                ..Settings::default()
            },
        )
    }
    fn config() -> RestConfig {
        RestConfig {
            workers: 1,
            latest_repair_workers: 2,
            sync_clock: false,
            boundary_guard_before_ms: 0,
            boundary_guard_after_ms: 0,
            hour_boundary_guard_before_ms: 0,
            hour_boundary_guard_after_ms: 0,
            freshness_grace_ms: 300000,
            ..RestConfig::default()
        }
    }
    #[tokio::test]
    async fn fix2_reconcile_dispatches_all_750_equal_e_symbols_despite_low_id_failures() {
        struct Mock {
            clock: Arc<TestClock>,
            seen: parking_lot::Mutex<HashSet<String>>,
            calls: AtomicUsize,
            progress: watch::Sender<usize>,
            stop: watch::Sender<bool>,
        }
        async fn handler(
            State(s): State<Arc<Mock>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            assert_eq!(q["limit"], "99");
            let call = s.calls.fetch_add(1, Ordering::AcqRel) + 1;
            s.clock.0.fetch_add(1000, Ordering::AcqRel);
            s.seen.lock().insert(q["symbol"].clone());
            s.progress.send_replace(call);
            if call >= 900 {
                let _ = s.stop.send(true);
            }
            let id = q["symbol"][1..].parse::<usize>().unwrap();
            if id < 60 {
                return Json(serde_json::json!([]));
            }
            let start = q["startTime"].parse::<i64>().unwrap();
            let end = q["endTime"].parse::<i64>().unwrap();
            let rows: Vec<_> = (start..=end)
                .step_by(60000)
                .take(99)
                .map(|t| serde_json::to_value(binance_wire::DisplayBar(bar(t))).unwrap())
                .collect();
            Json(serde_json::Value::Array(rows))
        }
        let clock = Arc::new(TestClock(AtomicI64::new(B + 10000)));
        let e = engine(750, clock.clone());
        for id in 0..750 {
            e.commit(
                id,
                Update {
                    bar: bar(B - 120 * MINUTE),
                    closed: false,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )
            .unwrap();
        }
        let (stop, rx) = watch::channel(false);
        let (progress, mut changes) = watch::channel(0);
        let state = Arc::new(Mock {
            clock,
            seen: parking_lot::Mutex::new(HashSet::new()),
            calls: AtomicUsize::new(0),
            progress,
            stop: stop.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/fapi/v1/klines", get(handler))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let api = RestApi::new_with_type(&url, &url, 6000, NumberType::String).unwrap();
        let health = Health::new(e.clone(), vec![], Arc::new(SyncedClock::new(0)), false);
        // Seed the already initialized scene from J74. Actual dispatch, sorting,
        // consumption, limiter, HTTP and completion all run through reconcile.
        let records = (0..750)
            .map(|_| {
                let mut r = TaskRecord::new(B + 10000);
                r.initialized = true;
                r.capacity = 241;
                r.routine.due = i64::MAX;
                r.history.due = i64::MAX;
                Some(r)
            })
            .collect();
        let task = tokio::spawn(reconcile_state(
            e,
            api,
            config(),
            health,
            rx,
            Duration::from_millis(1),
            records,
        ));
        tokio::time::timeout(Duration::from_secs(90), async {
            while state.seen.lock().len() < 750 && state.calls.load(Ordering::Acquire) < 900 {
                changes.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let _ = stop.send(true);
        task.await.unwrap();
        server.abort();
        assert_eq!(
            state.seen.lock().len(),
            750,
            "real reconcile starved symbols after {} attempts",
            state.calls.load(Ordering::Acquire)
        );
        assert!(state.calls.load(Ordering::Acquire) < 900);
    }
    #[tokio::test]
    async fn fix2_routine_failure_then_trading_history_success_cannot_confirm_readiness() {
        struct Mock {
            history: AtomicUsize,
        }
        async fn handler(
            State(s): State<Arc<Mock>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            if q["limit"] == "99" {
                return Json(serde_json::json!([]));
            }
            s.history.fetch_add(1, Ordering::AcqRel);
            Json(serde_json::json!([
                binance_wire::DisplayBar(bar(B - 200 * MINUTE)),
                binance_wire::DisplayBar(bar(B - 199 * MINUTE))
            ]))
        }
        let clock = Arc::new(TestClock(AtomicI64::new(B + 1500)));
        let e = engine(1, clock);
        e.commit(
            0,
            Update {
                bar: bar(B - 200 * MINUTE),
                closed: false,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            },
        )
        .unwrap();
        e.commit(
            0,
            Update {
                bar: bar(B - MINUTE),
                closed: true,
                source: Source::Stream,
                event_time: Some(B),
                sequence: 1,
            },
        )
        .unwrap();
        for open in (B - 98 * MINUTE..B).step_by(60000) {
            e.commit_evidence(0, EvidenceUpdate::Rest(bar(open)))
                .unwrap();
        }
        assert!(e.catalog.slot(0).gap_generation() > 0);
        let state = Arc::new(Mock {
            history: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/fapi/v1/klines", get(handler))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let status = Arc::new(StreamStatus::default());
        status.epoch.store(1, Ordering::Release);
        let feed = Feed {
            market: Market::Future,
            status,
            ids: Some(Arc::from([0])),
        };
        let epoch = feed.token();
        assert_ne!(epoch, 0);
        let health = Health::new(e.clone(), vec![feed], Arc::new(SyncedClock::new(0)), false);
        let mut record = TaskRecord::new(B + 1500);
        record.initialized = true;
        record.observe(B + 1500, epoch, 241);
        record.epoch = 0;
        record.gap = 0;
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(reconcile_state(
            e.clone(),
            RestApi::new_with_type(&url, &url, 6000, NumberType::String).unwrap(),
            config(),
            health.clone(),
            rx,
            Duration::from_millis(1),
            vec![Some(record)],
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.history.load(Ordering::Acquire) > 0
                    && e.catalog.slot(0).evidence_record(B - 200 * MINUTE).state
                        == EvidenceState::RestFinal
                    && health.detail.read()["rest_jobs"] == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(!health.series.load()[0].ready);
        assert_eq!(health.series.load()[0].epoch, 0);
        assert_eq!(health.series.load()[0].gap, 0);
        let _ = stop.send(true);
        task.await.unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn fix3_nontrading_history_relisted_during_http_cannot_confirm_readiness() {
        struct Mock {
            history: AtomicUsize,
            engine: Arc<Engine>,
        }
        async fn handler(
            State(s): State<Arc<Mock>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            if q["limit"] == "99" {
                return Json(serde_json::json!([]));
            }
            assert!(
                !s.engine.catalog.slot(0).is_trading(),
                "History must dispatch while nontrading"
            );
            let generation = s.engine.catalog.slot(0).tracking_generation();
            let mut definitions = s.engine.catalog.instruments();
            definitions[0].trading = true;
            s.engine.refresh_catalog(definitions).unwrap();
            assert_eq!(s.engine.catalog.slot(0).tracking_generation(), generation);
            s.history.fetch_add(1, Ordering::AcqRel);
            Json(serde_json::json!([
                binance_wire::DisplayBar(bar(B - 200 * MINUTE)),
                binance_wire::DisplayBar(bar(B - 199 * MINUTE))
            ]))
        }
        let clock = Arc::new(TestClock(AtomicI64::new(B + 1500)));
        let e = engine(1, clock);
        e.commit(
            0,
            Update {
                bar: bar(B - 200 * MINUTE),
                closed: false,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            },
        )
        .unwrap();
        e.commit(
            0,
            Update {
                bar: bar(B - MINUTE),
                closed: true,
                source: Source::Stream,
                event_time: Some(B),
                sequence: 1,
            },
        )
        .unwrap();
        for open in (B - 98 * MINUTE..B).step_by(60000) {
            e.commit_evidence(0, EvidenceUpdate::Rest(bar(open)))
                .unwrap();
        }
        assert!(e.catalog.slot(0).gap_generation() > 0);
        let mut definitions = e.catalog.instruments();
        definitions[0].trading = false;
        e.refresh_catalog(definitions).unwrap();
        let state = Arc::new(Mock {
            history: AtomicUsize::new(0),
            engine: e.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/fapi/v1/klines", get(handler))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let status = Arc::new(StreamStatus::default());
        status.epoch.store(1, Ordering::Release);
        let feed = Feed {
            market: Market::Future,
            status,
            ids: Some(Arc::from([0])),
        };
        let epoch = feed.token();
        assert_ne!(epoch, 0);
        let health = Health::new(e.clone(), vec![feed], Arc::new(SyncedClock::new(0)), false);
        let mut record = TaskRecord::new(B + 1500);
        record.initialized = true;
        record.observe(B + 1500, epoch, 241);
        record.epoch = 0;
        record.gap = 0;
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(reconcile_state(
            e.clone(),
            RestApi::new_with_type(&url, &url, 6000, NumberType::String).unwrap(),
            config(),
            health.clone(),
            rx,
            Duration::from_millis(1),
            vec![Some(record)],
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.history.load(Ordering::Acquire) > 0
                    && e.catalog.slot(0).evidence_record(B - 200 * MINUTE).state
                        == EvidenceState::RestFinal
                    && health.detail.read()["rest_jobs"] == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(!health.series.load()[0].ready);
        assert_eq!(health.series.load()[0].epoch, 0);
        assert_eq!(health.series.load()[0].gap, 0);
        let _ = stop.send(true);
        task.await.unwrap();
        server.abort();
    }
}
