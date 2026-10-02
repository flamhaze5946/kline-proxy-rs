use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_runtime::{
    config::RestConfig,
    lifecycle::{self, Feed, Health, SyncedClock},
    recovery::Recovery,
    rest::RestApi,
};
use kline_service::{
    BulkQuery, Catalog, Clock, Engine, Instrument, Settings, SystemClock,
    http::{self, Readiness},
    upstream::StreamStatus,
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
use tower::ServiceExt;
const M: i64 = 60_000;
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn bar(open: i64, price: f64, trades: u32) -> Bar {
    Bar {
        open_time: open,
        close_time: open + M - 1,
        trades,
        values: [price, price, price, price, 100., 100., 50., 50.],

        ..Bar::default()
    }
}
fn engine(now: i64, capacity: usize) -> (Arc<Engine>, Arc<TestClock>) {
    engine_with_interval(now, capacity, "1m")
}
fn engine_with_interval(
    now: i64,
    capacity: usize,
    interval: &str,
) -> (Arc<Engine>, Arc<TestClock>) {
    let clock = Arc::new(TestClock(AtomicI64::new(now)));
    let engine = Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Future,
            symbol: "BTCUSDT".into(),
            interval: Interval::parse(interval).unwrap(),
            trading: true,
            continuous: None,
            capacity: NonZeroUsize::new(capacity).unwrap(),
        }])
        .unwrap(),
        clock.clone(),
        Settings::default(),
    );
    (engine, clock)
}
fn commit(e: &Engine, bar: Bar, closed: bool, source: Source) {
    e.commit(
        0,
        Update {
            bar,
            closed,
            source,
            event_time: Some(1),
            sequence: 1,
        },
    )
    .unwrap();
}
fn config() -> RestConfig {
    RestConfig {
        weight_per_minute: 6000,
        boundary_guard_before_ms: 0,
        boundary_guard_after_ms: 0,
        sync_clock: false,
        retry_seconds: 1,
        ..RestConfig::default()
    }
}
#[derive(Default)]
struct Mock {
    bars: Mutex<Vec<Bar>>,
    queries: Mutex<Vec<HashMap<String, String>>>,
    block: AtomicBool,
    entered: Notify,
    resume: Notify,
    throttle: AtomicUsize,
    prefer_latest: AtomicBool,
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
async fn server(bars: Vec<Bar>) -> Server {
    let mock = Arc::new(Mock {
        bars: Mutex::new(bars),
        ..Mock::default()
    });
    async fn klines(
        State(s): State<Arc<Mock>>,
        Query(q): Query<HashMap<String, String>>,
    ) -> axum::response::Response {
        s.queries.lock().push(q.clone());
        if s.throttle
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")], "{}").into_response();
        }
        let start = q.get("startTime").map(|s| s.parse::<i64>().unwrap());
        let end = q["endTime"].parse::<i64>().unwrap();
        let limit = q["limit"].parse::<usize>().unwrap();
        let mut rows: Vec<_> = s
            .bars
            .lock()
            .iter()
            .filter(|b| b.open_time <= end && start.is_none_or(|t| b.open_time >= t))
            .cloned()
            .collect();
        if start.is_some() && !s.prefer_latest.load(Ordering::Relaxed) {
            rows.truncate(limit);
        } else if rows.len() > limit {
            rows.drain(..rows.len() - limit);
        }
        if s.block.load(Ordering::Acquire) {
            s.entered.notify_one();
            s.resume.notified().await;
        }
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|b| {
                json!([
                    b.open_time,
                    b.values[0].to_string(),
                    b.values[1].to_string(),
                    b.values[2].to_string(),
                    b.values[3].to_string(),
                    b.values[4].to_string(),
                    b.close_time,
                    b.values[5].to_string(),
                    b.trades,
                    b.values[6].to_string(),
                    b.values[7].to_string(),
                    "0"
                ])
            })
            .collect();
        Json(rows).into_response()
    }
    let router = Router::new()
        .route("/fapi/v1/klines", get(klines))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        api: RestApi::new(&url, &url, 6000).unwrap(),
        mock,
        task,
    }
}
#[tokio::test]
async fn shortened_historical_bar_preserves_close_and_does_not_force_full_refetch() {
    let mut shortened = bar(0, 100., 1);
    shortened.close_time = M / 4 - 1;
    let server = server(vec![
        shortened.clone(),
        bar(M, 101., 1),
        bar(2 * M, 102., 1),
    ])
    .await;
    let (engine, _) = engine(2 * M + 30_000, 3);
    let mut settings = config();
    settings.future_refresh_count = Some(1);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: settings,
    };
    recovery.sync(0, false).await.unwrap();
    assert_eq!(engine.catalog.slot(0).get(0).unwrap().0, shortened);
    assert_eq!(engine.catalog.slot(0).records().len(), 3);
    recovery.sync(0, true).await.unwrap();
    assert_eq!(
        server.mock.queries.lock().last().unwrap()["startTime"],
        M.to_string()
    );
}

#[tokio::test]
async fn shortened_bar_does_not_hide_missing_slots_or_overlapping_times() {
    let mut shortened = bar(0, 100., 1);
    shortened.close_time = M / 4 - 1;
    let server = server(vec![shortened, bar(2 * M, 102., 1)]).await;
    let (engine, _) = engine(2 * M + 30_000, 3);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    recovery.sync(0, false).await.unwrap();
    let filled = engine.catalog.slot(0).get(M).unwrap();
    assert_eq!(filled.0.close_time, 2 * M - 1);
    assert_eq!(filled.0.trades, 0);
    assert!(filled.1);
    let mut overlap = bar(0, 100., 2);
    overlap.close_time = M + 1;
    commit(&engine, overlap, true, Source::Rest);
    assert!(
        recovery
            .sync(0, true)
            .await
            .unwrap_err()
            .to_string()
            .contains("gap or overlap")
    );
}

#[tokio::test]
async fn cold_load_paginates_without_duplicates_and_keeps_forming_bar_unfinalized() {
    let server = server((0..1002).map(|i| bar(i * M, 100., 1)).collect()).await;
    let (engine, _) = engine(1001 * M + 30_000, 1000);
    // A forming WebSocket frame before bootstrap must not hide missing history.
    commit(&engine, bar(1001 * M, 101., 2), false, Source::Stream);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    assert_eq!(recovery.sync(0, false).await.unwrap(), 1000);
    let records = engine.catalog.slot(0).records();
    assert_eq!(records.len(), 1000);
    assert_eq!(records[0].0.open_time, 2 * M);
    assert!(records[..999].iter().all(|r| r.1));
    assert!(!records[999].1);
    assert_eq!(records[999].0.values[3], 101.);
    let queries = server.mock.queries.lock();
    assert_eq!(queries.len(), 3);
    assert_eq!(
        queries
            .iter()
            .map(|q| q["limit"].as_str())
            .collect::<Vec<_>>(),
        ["499", "499", "2"]
    );
}
#[tokio::test]
async fn recovery_fills_an_internal_hole_without_regressing_concurrent_stream_final() {
    let server = server(vec![bar(M, 101., 2), bar(3 * M, 103., 2)]).await;
    let (engine, _) = engine(3 * M + 30_000, 10);
    commit(&engine, bar(0, 100., 1), true, Source::Restore);
    commit(&engine, bar(3 * M, 103., 3), false, Source::Stream);
    let generation = engine.catalog.slot(0).gap_generation();
    assert!(generation > 0);
    server.mock.block.store(true, Ordering::Release);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    let task = tokio::spawn(async move { recovery.sync(0, true).await });
    server.mock.entered.notified().await;
    commit(&engine, bar(M, 109., 10), true, Source::Stream);
    server.mock.resume.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(engine.catalog.slot(0).get(M).unwrap().0.values[3], 109.);
    let filled = engine.catalog.slot(0).get(2 * M).unwrap();
    assert!(filled.1);
    assert_eq!(filled.0.trades, 0);
    assert_eq!(filled.0.values[4], 0.);
    assert!(!engine.catalog.slot(0).get(3 * M).unwrap().1);
}
#[tokio::test]
async fn long_outage_pages_bound_both_ends_and_keep_the_entire_retained_window() {
    let server = server((0..1002).map(|i| bar(i * M, 100. + i as f64, 1)).collect()).await;
    server.mock.prefer_latest.store(true, Ordering::Relaxed);
    let (engine, _) = engine(1001 * M + 30_000, 1000);
    commit(&engine, bar(0, 100., 1), true, Source::Restore);
    commit(&engine, bar(1001 * M, 1101., 2), false, Source::Stream);
    Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    }
    .sync(0, true)
    .await
    .unwrap();
    let records = engine.catalog.slot(0).records();
    assert_eq!(records.len(), 1000);
    for (index, (bar, _)) in records.iter().enumerate() {
        assert_eq!(bar.open_time, (index as i64 + 2) * M);
        assert!(bar.trades > 0);
    }
    assert_eq!(server.mock.queries.lock().len(), 3);
}
#[tokio::test]
async fn response_arriving_after_close_does_not_finalize_a_preclose_rest_snapshot() {
    let server = server(vec![bar(0, 100., 1)]).await;
    let (engine, clock) = engine(M - 2, 10);
    server.mock.block.store(true, Ordering::Release);
    let r = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    let task = tokio::spawn(async move { r.sync(0, false).await });
    server.mock.entered.notified().await;
    clock.0.store(M + 100, Ordering::Relaxed);
    server.mock.resume.notify_one();
    task.await.unwrap().unwrap();
    assert!(!engine.catalog.slot(0).get(0).unwrap().1);
}
#[tokio::test]
async fn empty_and_old_only_responses_do_not_complete_recovery() {
    let server = server(vec![]).await;
    let (engine, _) = engine(10 * M, 10);
    let r = Recovery {
        engine,
        api: server.api.clone(),
        config: config(),
    };
    assert!(r.sync(0, false).await.is_err());
    *server.mock.bars.lock() = vec![bar(0, 1., 1)];
    assert!(r.sync(0, false).await.is_err());
}
#[tokio::test]
async fn fresh_stream_tail_does_not_hide_a_gap_left_by_old_only_rest() {
    let server = server(vec![bar(0, 100., 1)]).await;
    let (engine, _) = engine(5 * M + 30_000, 10);
    commit(&engine, bar(0, 100., 1), true, Source::Restore);
    commit(&engine, bar(5 * M, 105., 2), false, Source::Stream);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    let error = recovery.sync(0, true).await.unwrap_err();
    assert!(error.to_string().contains("expired nonfinal"), "{error}");
    for i in 1..5 {
        let (bar, closed) = engine.catalog.slot(0).get(i * M).unwrap();
        assert_eq!(bar.trades, 0);
        assert!(!closed, "stream gap fill must await REST or a real final");
    }
    *server.mock.bars.lock() = (0..=5).map(|i| bar(i * M, 100. + i as f64, 1)).collect();
    recovery.sync(0, true).await.unwrap();
    assert_eq!(engine.catalog.slot(0).records().len(), 6);
}
#[tokio::test]
async fn calendar_month_recovery_preserves_real_month_boundaries() {
    const DAY: i64 = 86_400_000;
    let dates = [
        1_704_067_200_000,
        1_706_745_600_000,
        1_709_251_200_000,
        1_711_929_600_000,
    ];
    let bars: Vec<_> = dates
        .windows(2)
        .map(|pair| Bar {
            open_time: pair[0],
            close_time: pair[1] - 1,
            ..bar(pair[0], 100., 1)
        })
        .collect();
    let server = server(bars.clone()).await;
    let (engine, _) = engine_with_interval(dates[2] + 14 * DAY, 10, "1M");
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    assert_eq!(recovery.sync(0, false).await.unwrap(), 3);
    assert_eq!(recovery.sync(0, true).await.unwrap(), 3);
    let records = engine.catalog.slot(0).records();
    assert_eq!(
        records.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        bars
    );
    assert!(records[0].1 && records[1].1 && !records[2].1);
}
#[tokio::test]
async fn stream_final_releases_bulk_and_cancels_repair_during_shared_429_cooldown() {
    let server = server(vec![bar(0, 1., 1)]).await;
    let (engine, _) = engine(M + 100, 10);
    commit(&engine, bar(0, 1., 1), false, Source::Stream);
    server.mock.throttle.store(1, Ordering::Relaxed);
    let interval = Interval::parse("1m").unwrap();
    assert!(
        server
            .api
            .klines(Market::Future, "BTCUSDT", interval, None, M, 1)
            .await
            .is_err()
    );
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    });
    let repair = tokio::spawn({
        let recovery = recovery.clone();
        async move { recovery.repair_latest(0, 0).await }
    });
    let bulk = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .bulk(BulkQuery {
                    market: Market::Future,
                    interval: "1m".into(),
                    limit: Some(1),
                    closed_only: true,
                    symbols: vec!["BTCUSDT".into()],
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!repair.is_finished() && !bulk.is_finished());
    assert_eq!(server.mock.queries.lock().len(), 1);
    commit(&engine, bar(0, 2., 2), true, Source::Stream);
    let reply = tokio::time::timeout(Duration::from_millis(300), bulk)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(value["finalized"], true);
    assert_eq!(value["pending"], json!([]));
    assert_eq!(value["klines"]["BTCUSDT"][0][4], "2");
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), repair)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(
        server.mock.queries.lock().len(),
        1,
        "a WS final must not bypass the REST cooldown"
    );

    // An unrelated REST consumer still honors the original Retry-After window.
    let other = tokio::spawn({
        let api = server.api.clone();
        async move {
            api.klines(Market::Future, "BTCUSDT", interval, None, M, 1)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!other.is_finished());
    assert_eq!(server.mock.queries.lock().len(), 1);
    tokio::time::timeout(Duration::from_secs(2), other)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(server.mock.queries.lock().len(), 2);
    assert_eq!(engine.catalog.slot(0).get(0).unwrap().0.values[3], 2.);
}

#[tokio::test]
async fn retry_after_defers_other_requests_sharing_the_market_budget() {
    let server = server(vec![bar(0, 1., 1)]).await;
    server.mock.throttle.store(1, Ordering::Relaxed);
    let i = Interval::parse("1m").unwrap();
    assert!(
        server
            .api
            .klines(Market::Future, "BTCUSDT", i, None, M, 1)
            .await
            .is_err()
    );
    let started = std::time::Instant::now();
    server
        .api
        .klines(Market::Future, "BTCUSDT", i, None, M, 1)
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(950));
    assert_eq!(server.mock.queries.lock().len(), 2);
}
#[tokio::test]
async fn repaired_historical_final_invalidates_a_previously_cached_bulk_response() {
    let (engine, _) = engine(2 * M + 20_000, 10);
    commit(&engine, bar(M, 101., 2), true, Source::Stream);
    let query = || BulkQuery {
        market: Market::Future,
        interval: "1m".into(),
        limit: Some(2),
        closed_only: true,
        symbols: vec!["BTCUSDT".into()],
    };
    let before = engine.bulk(query()).await.unwrap();
    commit(&engine, bar(0, 100., 1), true, Source::Rest);
    let after = engine.bulk(query()).await.unwrap();
    assert_ne!(before.body, after.body);
    let json: Value = serde_json::from_slice(&after.body).unwrap();
    assert_eq!(json["klines"]["BTCUSDT"].as_array().unwrap().len(), 2);
}
async fn wait_ready(health: &Health) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !health.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn partial_cold_start_does_not_claim_established_service_availability() {
    let now = SystemClock { offset_ms: 0 }.now_ms();
    let boundary = now / M * M;
    let server = server(vec![bar(boundary - M, 100., 1), bar(boundary, 101., 1)]).await;
    let (engine, _) = engine(now, 2);
    let mut definitions = engine.catalog.instruments();
    let mut new = definitions[0].clone();
    new.symbol = "NEWUSDT".into();
    definitions.push(new);
    engine.refresh_catalog(definitions).unwrap();
    let mut feeds = Vec::new();
    for id in 0..2 {
        let status = Arc::new(StreamStatus::default());
        status.connected.store(id == 0, Ordering::Release);
        status.epoch.store(1, Ordering::Release);
        status.mapped_epoch.store(1, Ordering::Release);
        status.last_io_ms.store(now, Ordering::Release);
        feeds.push(Feed {
            market: Market::Future,
            status,
            ids: Some(Arc::from([id])),
        });
    }
    let missing_stream = feeds[1].status.clone();
    let health = Health::new(engine.clone(), feeds, Arc::new(SyncedClock::new(0)), false);
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        config(),
        health.clone(),
        stopped,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while health.details()["ready_series"] != 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!health.is_ready());
    assert!(!health.is_serving());
    assert!(!health.query_ready(&BulkQuery {
        market: Market::Future,
        interval: "1m".into(),
        limit: Some(1),
        closed_only: true,
        symbols: vec!["BTCUSDT".into()]
    }));
    missing_stream.connected.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !health.is_serving() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(health.is_ready());
    stop.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn a_new_listing_uses_reserved_recovery_capacity_without_blocking_existing_symbols() {
    let now = SystemClock { offset_ms: 0 }.now_ms();
    let boundary = now / M * M;
    let server = server(vec![bar(boundary - M, 100., 1), bar(boundary, 101., 1)]).await;
    let (engine, _) = engine(now, 2);
    let status = Arc::new(StreamStatus::default());
    status.connected.store(true, Ordering::Release);
    status.epoch.store(1, Ordering::Release);
    status.mapped_epoch.store(1, Ordering::Release);
    status.last_io_ms.store(now, Ordering::Release);
    let health = Health::new(
        engine.clone(),
        vec![Feed {
            market: Market::Future,
            status: status.clone(),
            ids: None,
        }],
        Arc::new(SyncedClock::new(0)),
        false,
    );
    assert!(
        !health.is_serving(),
        "cold startup must not become serving vacuously"
    );
    let router = http::router_with_readiness(engine.clone(), Some(health.clone()));
    let mut cfg = config();
    cfg.workers = 1;
    cfg.reconcile_seconds = 1;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        cfg,
        health.clone(),
        stopped,
    ));
    wait_ready(&health).await;
    assert!(health.is_serving());
    server.mock.block.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(3), server.mock.entered.notified())
        .await
        .unwrap();
    // The sole ordinary worker is held inside a routine HTTP request.
    let mut instruments = engine.catalog.instruments();
    let mut new = instruments[0].clone();
    new.symbol = "NEWUSDT".into();
    instruments.push(new);
    engine.refresh_catalog(instruments).unwrap();
    // This second HTTP request must start even while the first remains blocked.
    tokio::time::timeout(Duration::from_secs(1), server.mock.entered.notified())
        .await
        .expect("new listing should use the bounded recovery lane");
    assert!(!health.is_ready());
    assert!(health.is_serving());
    assert_eq!(health.details()["unready_reasons"]["initial_pending"], 1);
    let before = server.mock.queries.lock().len();
    for (uri, expected) in [
        ("/health/ready", 503),
        ("/health/serving", 200),
        (
            "/fapi/v1/klines/bulk?interval=1m&limit=1&symbols=BTCUSDT",
            200,
        ),
        (
            "/fapi/v1/klines/bulk?interval=1m&limit=1&symbols=NEWUSDT",
            503,
        ),
        ("/fapi/v1/klines/bulk?interval=1m&limit=1", 503),
    ] {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected, "{uri}");
    }
    assert_eq!(
        server.mock.queries.lock().len(),
        before,
        "queries must never trigger Kline REST recovery"
    );
    status.connected.store(false, Ordering::Release);
    assert!(
        !health.is_serving(),
        "established stream failure is still unavailable immediately"
    );
    status.connected.store(true, Ordering::Release);
    server.mock.block.store(false, Ordering::Release);
    server.mock.resume.notify_waiters();
    wait_ready(&health).await;
    assert!(health.is_serving());
    stop.send(true).unwrap();
    task.await.unwrap();
    assert!(!health.is_serving());
}
#[tokio::test]
async fn quiet_rechecks_do_not_call_rest_but_reconnect_still_requires_recovery() {
    let now = SystemClock { offset_ms: 0 }.now_ms();
    let boundary = now / M * M;
    let server = server(vec![bar(boundary - M, 100., 1), bar(boundary, 101., 1)]).await;
    let (engine, _) = engine(now, 2);
    let status = Arc::new(StreamStatus::default());
    status.connected.store(true, Ordering::Release);
    status.epoch.store(1, Ordering::Release);
    status.mapped_epoch.store(1, Ordering::Release);
    status.last_io_ms.store(now, Ordering::Relaxed);
    let clock = Arc::new(SyncedClock::new(0));
    let health = Health::new(
        engine.clone(),
        vec![Feed {
            market: Market::Future,
            status: status.clone(),
            ids: None,
        }],
        clock,
        false,
    );
    let router = http::router_with_readiness(engine.clone(), Some(health.clone()));
    let request = || {
        axum::http::Request::builder()
            .uri("/fapi/v1/klines/bulk?interval=1m&limit=1")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        503
    );
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        config(),
        health.clone(),
        stop_rx,
    ));
    wait_ready(&health).await;
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        200
    );
    // A low-volume topic can be quiet while its retained bars remain valid.
    // A quiet-topic hint must neither call REST nor reject healthy bulk traffic.
    server.mock.block.store(true, Ordering::Release);
    let gap = engine.gap_epoch();
    let before_hint = server.mock.queries.lock().len();
    engine.request_recheck(0);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(server.mock.queries.lock().len(), before_hint);
    assert_eq!(engine.gap_epoch(), gap);
    assert!(health.is_ready());
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        200
    );
    server.mock.throttle.store(1, Ordering::Relaxed);
    let before_reconnect = server.mock.queries.lock().len();
    status.connected.store(false, Ordering::Release);
    status.epoch.fetch_add(1, Ordering::Release);
    assert!(!health.is_ready());
    status.connected.store(true, Ordering::Release);
    status.epoch.fetch_add(1, Ordering::Release);
    status
        .mapped_epoch
        .store(status.epoch.load(Ordering::Acquire), Ordering::Release);
    assert!(!health.is_ready());
    tokio::time::timeout(Duration::from_secs(4), server.mock.entered.notified())
        .await
        .unwrap();
    assert_eq!(server.mock.queries.lock().len(), before_reconnect + 2);
    assert_eq!(router.oneshot(request()).await.unwrap().status(), 503);
    server.mock.block.store(false, Ordering::Release);
    server.mock.resume.notify_one();
    wait_ready(&health).await;
    stop_tx.send(true).unwrap();
    task.await.unwrap();
    assert!(!health.is_ready());
}

#[tokio::test]
async fn periodic_reconciliation_rechecks_finals_and_replaces_older_synthetic_fills() {
    for synthetic in [false, true] {
        let s = server(
            (0..=10)
                .map(|i| {
                    bar(
                        i * M,
                        if i == 5 { 200. } else { 100. },
                        if i == 5 { 2 } else { 1 },
                    )
                })
                .collect(),
        )
        .await;
        let (e, _) = engine(10 * M + 30_000, 20);
        for i in 0..=10 {
            let fill = synthetic && i == 5;
            commit(
                &e,
                bar(i * M, 100., if fill { 0 } else { 1 }),
                i < 10,
                if fill {
                    Source::Synthetic
                } else {
                    Source::Rest
                },
            );
        }
        let recovery = Recovery {
            engine: e.clone(),
            api: s.api.clone(),
            config: config(),
        };
        assert_eq!(recovery.sync(0, true).await.unwrap(), 11);
        let corrected = e.catalog.slot(0).get(5 * M).unwrap();
        assert_eq!(corrected.0.values[3], 200.);
        assert_eq!(corrected.0.trades, 2);
        assert!(corrected.1);
    }
}

#[tokio::test]
async fn latest_repair_ignores_history_guard_and_releases_bulk_wait_on_real_final() {
    let server = server(vec![bar(0, 1., 1), bar(M, 2., 2)]).await;
    let (engine, _) = engine(M + 600, 10);
    commit(&engine, bar(0, 1., 1), false, Source::Stream);
    let mut cfg = config();
    cfg.hour_boundary_guard_after_ms = 30_000;
    cfg.boundary_guard_after_ms = 30_000;
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: cfg,
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), recovery.repair_latest(0, 0))
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert!(engine.catalog.slot(0).get(0).unwrap().1);
    {
        let queries = server.mock.queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["limit"], "2");
    }
    assert_eq!(recovery.repair_latest(0, 0).await.unwrap(), 0);
    assert_eq!(server.mock.queries.lock().len(), 1);
}

#[tokio::test]
async fn stream_final_cancels_priority_rest_and_cannot_be_replaced_by_its_response() {
    let server = server(vec![bar(0, 1., 1)]).await;
    server.mock.block.store(true, Ordering::Release);
    let (engine, _) = engine(M + 600, 10);
    commit(&engine, bar(0, 1., 1), false, Source::Stream);
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    });
    let pending = tokio::spawn({
        let recovery = recovery.clone();
        async move { recovery.repair_latest(0, 0).await }
    });
    server.mock.entered.notified().await;
    commit(&engine, bar(0, 2., 2), true, Source::Stream);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    server.mock.resume.notify_one();
    assert_eq!(engine.catalog.slot(0).get(0).unwrap().0.values[3], 2.);
}

#[tokio::test]
async fn a_late_final_waits_for_websocket_without_immediate_rest_repair() {
    let server = server(vec![bar(0, 1., 1), bar(M, 2., 2)]).await;
    let (engine, clock) = engine(M + 30_000, 2);
    let now = SystemClock { offset_ms: 0 }.now_ms();
    let status = Arc::new(StreamStatus::default());
    status.connected.store(true, Ordering::Release);
    status.epoch.store(1, Ordering::Release);
    status.mapped_epoch.store(1, Ordering::Release);
    status.last_io_ms.store(now, Ordering::Release);
    let health = Health::new(
        engine.clone(),
        vec![Feed {
            market: Market::Future,
            status,
            ids: None,
        }],
        Arc::new(SyncedClock::new(0)),
        false,
    );
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut cfg = config();
    cfg.boundary_guard_after_ms = 30_000;
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        cfg,
        health.clone(),
        stop_rx,
    ));
    wait_ready(&health).await;
    server.mock.queries.lock().clear();
    *server.mock.bars.lock() = vec![bar(M, 2., 2), bar(2 * M, 3., 3)];
    clock.0.store(2 * M + 100, Ordering::Release);
    engine.request_recheck(0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        server.mock.queries.lock().is_empty(),
        "normal recovery must honor the boundary guard"
    );
    clock.0.store(2 * M + 600, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(300)).await;
    clock.0.store(2 * M + 31_000, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(server.mock.queries.lock().is_empty());
    assert!(!engine.catalog.slot(0).get(M).unwrap().1);
    assert!(!health.is_ready());
    assert_eq!(
        engine.catalog.slot(0).snapshot(1, false, 2 * M + 31_000)[0].open_time,
        M
    );
    commit(&engine, bar(M, 2., 2), true, Source::Stream);
    wait_ready(&health).await;
    assert_eq!(
        engine.catalog.slot(0).snapshot(1, false, 2 * M + 31_000)[0].open_time,
        2 * M
    );
    assert!(server.mock.queries.lock().is_empty());
    assert_eq!(engine.catalog.slot(0).len(), 2);
    stop_tx.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn quiet_forming_tail_uses_placeholder_until_scheduled_reconciliation() {
    let server = server(vec![bar(0, 1., 1), bar(M, 2., 2)]).await;
    let (engine, clock) = engine(M + 30_000, 2);
    let status = Arc::new(StreamStatus::default());
    status.connected.store(true, Ordering::Release);
    status.epoch.store(1, Ordering::Release);
    status.mapped_epoch.store(1, Ordering::Release);
    status
        .last_io_ms
        .store(SystemClock { offset_ms: 0 }.now_ms(), Ordering::Release);
    let health = Health::new(
        engine.clone(),
        vec![Feed {
            market: Market::Future,
            status,
            ids: None,
        }],
        Arc::new(SyncedClock::new(0)),
        false,
    );
    let mut cfg = config();
    cfg.workers = 1;
    cfg.reconcile_seconds = 2;
    cfg.boundary_guard_after_ms = 30_000;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(lifecycle::reconcile(
        engine.clone(),
        server.api.clone(),
        cfg,
        health.clone(),
        stop_rx,
    ));
    wait_ready(&health).await;
    server.mock.queries.lock().clear();
    *server.mock.bars.lock() = vec![bar(M, 2., 2), bar(2 * M, 3., 3)];
    clock.0.store(2 * M + 100, Ordering::Release);
    commit(&engine, bar(M, 2., 2), true, Source::Stream);
    engine.request_recheck(0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(server.mock.queries.lock().is_empty());
    assert!(!engine.catalog.slot(0).needs_final(M));
    server.mock.block.store(true, Ordering::Release);
    clock.0.store(2 * M + 16_000, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(server.mock.queries.lock().is_empty());
    // Query availability never invokes REST and does not make a placeholder durable.
    assert!(health.is_ready(), "{}", health.details());
    assert_eq!(health.details()["provisional_current"], 1);
    assert_eq!(health.details()["observed_stale_tail"], 1);
    assert!(!engine.catalog.slot(0).contains(2 * M));
    let placeholder = engine.catalog.slot(0).snapshot(1, false, 2 * M + 16_000);
    assert_eq!(placeholder[0].open_time, 2 * M);
    assert_eq!(placeholder[0].values, [2., 2., 2., 2., 0., 0., 0., 0.]);
    clock.0.store(2 * M + 31_000, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(4), server.mock.entered.notified())
        .await
        .unwrap();
    assert!(health.is_ready());
    assert_eq!(health.details()["latest_repair_jobs"], 0);
    assert_eq!(health.details()["forming_repair_jobs"], 0);
    server.mock.resume.notify_one();
    let recovered = tokio::time::timeout(Duration::from_secs(2), async {
        while !engine.catalog.slot(0).contains(2 * M) || !health.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    stop_tx.send(true).unwrap();
    task.await.unwrap();
    assert!(
        recovered.is_ok(),
        "scheduled reconciliation must still replace the placeholder with real data"
    );
    let queries = server.mock.queries.lock();
    assert_eq!(queries.len(), 1);
    // Regular reconciliation rechecks its configured history, not an urgent two-row tail.
    assert_eq!(queries[0]["limit"], "3");
    assert_eq!(queries[0]["startTime"], "0");
    assert!(engine.catalog.slot(0).get(M).unwrap().1);
    assert!(!engine.catalog.slot(0).get(2 * M).unwrap().1);
}

#[tokio::test]
async fn forming_tail_repair_rejects_old_only_rest_without_fabricating_a_bar() {
    let server = server(vec![bar(0, 1., 1)]).await;
    let (engine, _) = engine(M + 16_000, 10);
    commit(&engine, bar(0, 1., 1), true, Source::Stream);
    let recovery = Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    };
    let error = recovery.repair_forming(0).await.unwrap_err();
    assert!(error.to_string().contains("did not return a fresh bar"));
    assert_eq!(engine.catalog.slot(0).len(), 1);
    assert!(!engine.catalog.slot(0).contains(M));
    assert!(!engine.catalog.slot(0).fresh_tail(M + 16_000, 15_000));
    assert!(!engine.catalog.slot(0).needs_final(0));
    assert_eq!(server.mock.queries.lock()[0]["limit"], "2");
    let placeholder = engine.catalog.slot(0).snapshot(1, false, M + 16_000);
    assert_eq!(placeholder[0].open_time, M);
    assert_eq!(placeholder[0].values, [1., 1., 1., 1., 0., 0., 0., 0.]);
    assert_eq!(engine.catalog.slot(0).durable_snapshot(M * 2).1.len(), 1);
}

#[tokio::test]
async fn stream_forming_bar_cancels_inflight_tail_rest_without_overwriting_stream_data() {
    let server = server(vec![bar(0, 1., 1), bar(M, 2., 1)]).await;
    server.mock.block.store(true, Ordering::Release);
    let (engine, _) = engine(M + 16_000, 10);
    commit(&engine, bar(0, 1., 1), true, Source::Stream);
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    });
    let pending = tokio::spawn({
        let recovery = recovery.clone();
        async move { recovery.repair_forming(0).await }
    });
    tokio::time::timeout(Duration::from_secs(1), server.mock.entered.notified())
        .await
        .unwrap();
    commit(&engine, bar(M, 3., 2), false, Source::Stream);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    server.mock.resume.notify_one();
    let (latest, final_bar) = engine.catalog.slot(0).get(M).unwrap();
    assert_eq!(latest.values[3], 3.);
    assert!(!final_bar);
    assert_eq!(server.mock.queries.lock().len(), 1);
}

#[tokio::test]
async fn forming_tail_repair_cancels_while_rate_limited_and_preserves_shared_cooldown() {
    let server = server(vec![bar(0, 1., 1), bar(M, 2., 1)]).await;
    let (engine, _) = engine(M + 16_000, 10);
    commit(&engine, bar(0, 1., 1), true, Source::Stream);
    server.mock.throttle.store(1, Ordering::Relaxed);
    let interval = Interval::parse("1m").unwrap();
    assert!(
        server
            .api
            .klines(Market::Future, "BTCUSDT", interval, None, M, 1)
            .await
            .is_err()
    );
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api: server.api.clone(),
        config: config(),
    });
    let pending = tokio::spawn({
        let recovery = recovery.clone();
        async move { recovery.repair_forming(0).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!pending.is_finished());
    assert_eq!(server.mock.queries.lock().len(), 1);
    commit(&engine, bar(M, 3., 2), false, Source::Stream);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    let other = tokio::spawn({
        let api = server.api.clone();
        async move {
            api.klines(Market::Future, "BTCUSDT", interval, None, M, 1)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!other.is_finished());
    assert_eq!(server.mock.queries.lock().len(), 1);
    tokio::time::timeout(Duration::from_secs(2), other)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
