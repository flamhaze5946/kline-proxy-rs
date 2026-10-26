use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use kline_core::{
    Bar, Interval, Market, NumberType, Source, Update,
    evidence::{EvidenceState, EvidenceUpdate},
};
use kline_runtime::{
    config::RestConfig,
    evidence_recovery::EvidenceRequest,
    lifecycle::{self, Health, SyncedClock},
    recovery::Recovery,
    rest::RestApi,
};
use kline_service::{
    Catalog, Clock, Engine, Instrument, Settings, evidence::EvidenceSettings, http::Readiness,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
const M: i64 = 60_000;
// Keep synthetic test timestamps beyond the default 24h trust window.
const B: i64 = 100_000 * M;
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn bar(open: i64, value: &str) -> Bar {
    let mut b = Bar {
        open_time: open,
        close_time: open + M - 1,
        trades: 1,
        ..Bar::default()
    };
    b.set_numbers(NumberType::String, [value; 8]).unwrap();
    b
}
fn engine(trading: bool, symbols: &[&str]) -> (Arc<Engine>, Arc<TestClock>) {
    let clock = Arc::new(TestClock(AtomicI64::new(B + 1500)));
    let catalog = Catalog::new(
        symbols
            .iter()
            .map(|symbol| Instrument {
                market: Market::Future,
                symbol: (*symbol).into(),
                interval: Interval::parse("1m").unwrap(),
                trading,
                continuous: Some(((*symbol).into(), "PERPETUAL".into())),
                capacity: NonZeroUsize::new(241).unwrap(),
            })
            .collect(),
    )
    .unwrap();
    (
        Engine::new(
            catalog,
            clock.clone(),
            Settings {
                number_type: NumberType::String,
                evidence: Some(EvidenceSettings::default()),
                ..Settings::default()
            },
        ),
        clock,
    )
}
fn observation(engine: &Engine, open: i64, value: &str, closed: bool) {
    engine
        .commit(
            0,
            Update {
                bar: bar(open, value),
                closed,
                source: Source::Stream,
                event_time: Some(1),
                sequence: 1,
            },
        )
        .unwrap();
}
#[derive(Default)]
struct Mock {
    rows: Mutex<Vec<Bar>>,
    queries: Mutex<Vec<HashMap<String, String>>>,
    block: AtomicBool,
    entered: Notify,
    resume: Notify,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    empty_startup: AtomicBool,
}
struct Server {
    api: Arc<RestApi>,
    mock: Arc<Mock>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(rows: Vec<Bar>) -> Server {
    let mock = Arc::new(Mock {
        rows: Mutex::new(rows),
        ..Mock::default()
    });
    async fn klines(
        State(s): State<Arc<Mock>>,
        Query(q): Query<HashMap<String, String>>,
    ) -> Json<Value> {
        s.queries.lock().push(q.clone());
        let running = s.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        s.peak.fetch_max(running, Ordering::SeqCst);
        if s.block.load(Ordering::SeqCst) {
            s.entered.notify_one();
            s.resume.notified().await;
        }
        let start = q.get("startTime").map(|v| v.parse::<i64>().unwrap());
        let end = q["endTime"].parse::<i64>().unwrap();
        let limit = q["limit"].parse::<usize>().unwrap();
        let mut rows: Vec<_> = s
            .rows
            .lock()
            .iter()
            .filter(|b| b.open_time <= end && start.is_none_or(|start| b.open_time >= start))
            .cloned()
            .collect();
        if start.is_some() {
            rows.truncate(limit);
        } else if rows.len() > limit {
            rows.drain(..rows.len() - limit);
        }
        if limit == 241 && s.empty_startup.load(Ordering::SeqCst) {
            rows.clear();
        }
        s.in_flight.fetch_sub(1, Ordering::SeqCst);
        Json(Value::Array(
            rows.iter()
                .map(|b| {
                    let fields = binance_wire::stored_numbers(b);
                    json!([
                        b.open_time,
                        fields[0],
                        fields[1],
                        fields[2],
                        fields[3],
                        fields[4],
                        b.close_time,
                        fields[5],
                        b.trades,
                        fields[6],
                        fields[7],
                        "0"
                    ])
                })
                .collect(),
        ))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/fapi/v1/klines", get(klines))
        .with_state(mock.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        api: RestApi::new_with_type(&url, &url, 6000, NumberType::String).unwrap(),
        mock,
        task,
    }
}
fn recovery(engine: Arc<Engine>, server: &Server) -> Recovery {
    Recovery {
        engine,
        api: server.api.clone(),
        config: RestConfig {
            boundary_guard_before_ms: 0,
            boundary_guard_after_ms: 0,
            sync_clock: false,
            workers: 1,
            latest_repair_workers: 1,
            ..RestConfig::default()
        },
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn startup_is_whole_window_even_when_ws_arrives_first_and_stale_pending_is_success() {
    let server = server(vec![bar(B - 240 * M, "1"), bar(B - 238 * M, "2")]).await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    observation(&engine, B - M, "3", true);
    recovery(engine.clone(), &server)
        .sync(0, false)
        .await
        .unwrap();
    let q = &server.mock.queries.lock()[0];
    assert_eq!(q["limit"], "241");
    assert!(!q.contains_key("startTime"));
    assert_eq!(q["endTime"], (B + 1500).to_string());
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 239 * M).state,
        EvidenceState::Absent
    );
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 238 * M).state,
        EvidenceState::Pending
    );
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - M).state,
        EvidenceState::WsFinal
    );
}
#[tokio::test]
async fn maturity_uses_end_time_on_every_request_and_requires_same_response_successor() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    let (engine, clock) = engine(true, &["BTCUSDT"]);
    clock.0.store(B + 299, Ordering::Relaxed);
    recovery(engine.clone(), &server)
        .sync(0, false)
        .await
        .unwrap();
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - M).state,
        EvidenceState::Pending
    );
    clock.0.store(B + 300, Ordering::Relaxed);
    recovery(engine.clone(), &server)
        .sync(0, false)
        .await
        .unwrap();
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - M).state,
        EvidenceState::RestFinal
    );
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B).state,
        EvidenceState::Pending
    );
}
#[tokio::test]
async fn repair_keeps_inflight_response_after_ws_and_compares_its_predecessor() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    server.mock.block.store(true, Ordering::SeqCst);
    let r = Arc::new(recovery(engine.clone(), &server));
    let task = {
        let r = r.clone();
        tokio::spawn(async move { r.repair_latest(0, B - M).await })
    };
    server.mock.entered.notified().await;
    observation(&engine, B - 2 * M, "9", true);
    observation(&engine, B - M, "2", true);
    server.mock.block.store(false, Ordering::SeqCst);
    server.mock.resume.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 2 * M).state,
        EvidenceState::RestFinal
    );
    assert_eq!(
        engine.catalog.slot(0).get(B - 2 * M).unwrap().0,
        bar(B - 2 * M, "1")
    );
    assert_eq!(engine.breaker_state().revised_total, 1);
    assert!(!engine.ws_final_trusted());
    assert_eq!(server.mock.queries.lock()[0]["limit"], "3");
    assert!(!server.mock.queries.lock()[0].contains_key("startTime"));
}
#[tokio::test]
async fn per_slot_gate_serializes_sync_and_repair_through_commit() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    server.mock.block.store(true, Ordering::SeqCst);
    let r = Arc::new(recovery(engine.clone(), &server));
    let first = {
        let r = r.clone();
        tokio::spawn(async move { r.sync(0, false).await })
    };
    server.mock.entered.notified().await;
    let second = {
        let r = r.clone();
        tokio::spawn(async move { r.repair_latest(0, B - M).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.mock.queries.lock().len(), 1);
    server.mock.block.store(false, Ordering::SeqCst);
    server.mock.resume.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(server.mock.peak.load(Ordering::SeqCst), 1);
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - M).state,
        EvidenceState::RestFinal
    );
}
#[tokio::test]
async fn routine_uses_98_rows_plus_left_anchor_and_gap_has_both_anchors() {
    let server = server(
        (0..=100)
            .filter(|i| *i != 20)
            .map(|i| bar(B - (100 - i) * M, "1"))
            .collect(),
    )
    .await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    for i in 0..=100 {
        engine
            .commit_evidence(0, EvidenceUpdate::Rest(bar(B - (100 - i) * M, "1")))
            .unwrap();
    }
    let r = recovery(engine.clone(), &server);
    let plan = r.evidence_sync_request(0, true, B + 1500);
    assert_eq!(plan.start, Some(B - 98 * M));
    assert_eq!(plan.limit, 99);
    engine
        .commit(
            0,
            Update {
                bar: bar(B - 80 * M, "2"),
                closed: true,
                source: Source::Stream,
                event_time: Some(9),
                sequence: 9,
            },
        )
        .unwrap();
    r.sync(0, true).await.unwrap();
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 80 * M).state,
        EvidenceState::Absent
    );
    assert!(engine.catalog.slot(0).get(B - 80 * M).is_none());
}
#[tokio::test]
async fn nontrading_since_caps_verdict_but_not_right_anchor_and_legacy_ready_count_survives() {
    let server = server(vec![bar(B - 3 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    let (engine, _) = engine(false, &["BTCUSDT"]);
    engine.catalog.slot(0).set_not_trading_since(Some(B - M));
    let r = recovery(engine.clone(), &server);
    r.sync(0, false).await.unwrap();
    assert!(engine.catalog.slot(0).get(B - M).is_none());
    let request = r.evidence_sync_request(0, true, B + 1500);
    assert_eq!(request.range.last, B - 2 * M);
    assert_eq!(request.end, B + 1500);
    r.sync(0, true).await.unwrap();
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 2 * M).state,
        EvidenceState::Absent
    );
    let health = Health::new(engine.clone(), vec![], Arc::new(SyncedClock::new(0)), false);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine,
        server.api.clone(),
        r.config,
        health.clone(),
        rx,
    ));
    until(|| health.details()["ready_series"] == 1).await;
    assert!(health.is_ready());
    assert!(health.is_serving());
    stop.send(true).unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn short_fallback_does_not_initialize_and_startup_failure_has_its_own_retry() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    server.mock.empty_startup.store(true, Ordering::SeqCst);
    let (engine, clock) = engine(true, &["BTCUSDT"]);
    let config = recovery(engine.clone(), &server).config;
    let health = Health::new(engine.clone(), vec![], Arc::new(SyncedClock::new(0)), false);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        config,
        health.clone(),
        rx,
    ));
    until(|| {
        server
            .mock
            .queries
            .lock()
            .iter()
            .any(|q| q["limit"] == "241")
    })
    .await;
    until(|| health.details()["rest_jobs"] == 0).await;
    assert_eq!(health.details()["initial_pending"], 1);
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - M).state,
        EvidenceState::RestFinal
    );
    assert_eq!(
        server
            .mock
            .queries
            .lock()
            .iter()
            .filter(|q| q["limit"] == "241")
            .count(),
        1
    );
    server.mock.empty_startup.store(false, Ordering::SeqCst);
    clock.0.store(B + 12_000, Ordering::Relaxed);
    until(|| health.details()["initial_pending"] == 0).await;
    assert_eq!(
        server
            .mock
            .queries
            .lock()
            .iter()
            .filter(|q| q["limit"] == "241")
            .count(),
        2
    );
    stop.send(true).unwrap();
    task.await.unwrap();
}
#[test]
fn fallback_forms_cover_97_minute_target_without_exceeding_weight_one() {
    let latest = B - M;
    let now = B + 1500;
    for distance in 0..97 {
        let p = EvidenceRequest::fallback(latest - distance * M, latest, now);
        assert_eq!(p.end, now);
        assert!(p.limit <= 99);
        if distance == 0 {
            assert_eq!(p.limit, 3);
            assert_eq!(p.start, None);
        } else {
            assert_eq!(p.start, Some(latest - (distance + 1) * M));
            assert_eq!(p.limit, (distance + 3) as usize);
        }
    }
}

#[tokio::test]
async fn fallback_has_an_independent_worker_while_another_symbols_startup_is_inflight() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2"), bar(B, "3")]).await;
    let (engine, clock) = engine(true, &["BTCUSDT", "ETHUSDT"]);
    clock.0.store(B + 1000, Ordering::Relaxed);
    for id in 0..2 {
        for step in 2..=97 {
            engine
                .commit_evidence(id, EvidenceUpdate::Rest(bar(B - step * M, "1")))
                .unwrap();
        }
        engine
            .commit(
                id,
                Update {
                    bar: bar(B - M, "2"),
                    closed: false,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )
            .unwrap();
    }
    let config = recovery(engine.clone(), &server).config;
    server.mock.block.store(true, Ordering::SeqCst);
    let health = Health::new(engine.clone(), vec![], Arc::new(SyncedClock::new(0)), false);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine,
        server.api.clone(),
        config,
        health.clone(),
        rx,
    ));
    until(|| server.mock.queries.lock().len() == 1).await;
    assert_eq!(server.mock.queries.lock()[0]["limit"], "241");
    clock.0.store(B + 1500, Ordering::Relaxed);
    until(|| server.mock.queries.lock().len() == 2).await;
    let queries = server.mock.queries.lock().clone();
    assert_eq!(queries[1]["limit"], "3");
    assert_ne!(queries[0]["symbol"], queries[1]["symbol"]);
    assert_eq!(health.details()["latest_repair_jobs"], 1);
    assert_eq!(health.details()["routine_rest_jobs"], 1);
    server.mock.block.store(false, Ordering::SeqCst);
    server.mock.resume.notify_waiters();
    stop.send(true).unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn unresolved_latest_gets_exactly_three_designated_attempts_before_target_spacing() {
    let server = server(vec![bar(B - 2 * M, "1"), bar(B - M, "2")]).await;
    let (engine, clock) = engine(true, &["BTCUSDT"]);
    observation(&engine, B - M, "2", false);
    let config = recovery(engine.clone(), &server).config;
    let health = Health::new(engine.clone(), vec![], Arc::new(SyncedClock::new(0)), false);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine,
        server.api.clone(),
        config,
        health.clone(),
        rx,
    ));
    let attempts = || {
        server
            .mock
            .queries
            .lock()
            .iter()
            .filter(|q| q["limit"] == "3")
            .count()
    };
    for (index, offset) in [1500, 3000, 6000].into_iter().enumerate() {
        clock.0.store(B + offset, Ordering::Relaxed);
        until(|| attempts() == index + 1 && health.details()["rest_jobs"] == 0).await;
    }
    clock.0.store(B + 10_999, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(attempts(), 3);
    clock.0.store(B + 11_000, Ordering::Relaxed);
    until(|| attempts() == 4).await;
    stop.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn invalid_decimal_anywhere_in_a_rest_response_prevents_partial_publication() {
    let server = server(vec![bar(B - M, "1"), bar(B, "invalid")]).await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    assert!(
        recovery(engine.clone(), &server)
            .sync(0, false)
            .await
            .is_err()
    );
    assert!(engine.catalog.slot(0).is_empty());
    assert_eq!(engine.catalog.slot(0).window_generation(), 0);
}
#[tokio::test]
async fn evidence_clock_discards_high_rtt_samples_and_keeps_the_600_second_freshness_rule() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/fapi/v1/time",
        get({
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"serverTime":kline_service::SystemClock{offset_ms:0}.now_ms()}))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let api = RestApi::new(&url, &url, 6000).unwrap();
    let clock = Arc::new(SyncedClock::new(0));
    let sampled = kline_service::SystemClock { offset_ms: 0 }.now_ms() - 1000;
    clock.synchronized_at.store(sampled, Ordering::Release);
    let (engine, _) = engine(true, &["BTCUSDT"]);
    let health = Health::new(engine, vec![], clock.clone(), true);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::sync_clock_with_evidence(
        api,
        clock.clone(),
        Market::Future,
        rx,
        true,
    ));
    until(|| calls.load(Ordering::SeqCst) >= 3).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    stop.send(true).unwrap();
    task.await.unwrap();
    server.abort();
    assert_eq!(clock.synchronized_at.load(Ordering::Acquire), sampled);
    assert_eq!(health.details()["clock_ready"], true);
    clock
        .synchronized_at
        .store(sampled - 600_000, Ordering::Release);
    assert_eq!(health.details()["clock_ready"], false);
}

#[tokio::test]
async fn old_inflight_response_cannot_repopulate_a_removed_and_readded_identity() {
    let server = server(vec![bar(B - M, "1"), bar(B, "2")]).await;
    server.mock.block.store(true, Ordering::SeqCst);
    let (engine, _) = engine(true, &["BTCUSDT"]);
    let definitions = engine.catalog.instruments();
    let r = Arc::new(recovery(engine.clone(), &server));
    let task = {
        let r = r.clone();
        tokio::spawn(async move { r.sync(0, false).await })
    };
    server.mock.entered.notified().await;
    engine.refresh_catalog(vec![]).unwrap();
    engine.refresh_catalog(definitions).unwrap();
    server.mock.block.store(false, Ordering::SeqCst);
    server.mock.resume.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(engine.catalog.slot(0).is_empty());
}

#[tokio::test]
async fn stale_cold_response_retains_its_actual_window_instead_of_discarding_old_history() {
    let server = server(vec![bar(B - 1000 * M, "1"), bar(B - 999 * M, "2")]).await;
    let (engine, _) = engine(true, &["BTCUSDT"]);
    recovery(engine.clone(), &server)
        .sync(0, false)
        .await
        .unwrap();
    assert_eq!(
        engine.catalog.slot(0).get(B - 1000 * M).unwrap().0,
        bar(B - 1000 * M, "1")
    );
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 1000 * M).state,
        EvidenceState::RestFinal
    );
    assert_eq!(
        engine.catalog.slot(0).evidence_record(B - 999 * M).state,
        EvidenceState::Pending
    );
    assert_eq!(server.mock.queries.lock()[0]["limit"], "241");
}
