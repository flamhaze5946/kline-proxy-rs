use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::{BulkQuery, Catalog, Clock, Engine, Instrument, Settings};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
const H: i64 = 3_600_000;
struct Time(AtomicI64);
impl Clock for Time {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn spec(symbol: &str, capacity: usize) -> Instrument {
    Instrument {
        market: Market::Future,
        symbol: symbol.into(),
        interval: Interval::parse("1h").unwrap(),
        trading: true,
        continuous: None,
        capacity: NonZeroUsize::new(capacity).unwrap(),
    }
}
fn commit(engine: &Engine, id: usize, open: i64, closed: bool) {
    engine
        .commit(
            id,
            Update {
                bar: Bar {
                    open_time: open,
                    close_time: open + H - 1,
                    trades: 1,
                    values: [1.; 8],

                    ..Bar::default()
                },
                closed,
                source: Source::Stream,
                event_time: Some(open + H + 1),
                sequence: 1,
            },
        )
        .unwrap();
}
fn query() -> BulkQuery {
    BulkQuery {
        market: Market::Future,
        interval: "1h".into(),
        symbols: vec![],
        limit: Some(1),
        closed_only: true,
    }
}

#[tokio::test]
async fn directory_changes_preserve_ids_invalidate_responses_and_release_removed_waiters() {
    let clock = Arc::new(Time(AtomicI64::new(H + 1)));
    let engine = Engine::new(
        Catalog::new(vec![spec("B", 5)]).unwrap(),
        clock.clone(),
        Settings::default(),
    );
    commit(&engine, 0, 0, true);
    let before = engine.bulk(query()).await.unwrap();
    let slot = engine.catalog.slot(0);
    let change = engine
        .refresh_catalog(vec![spec("A", 4), spec("B", 5)])
        .unwrap();
    assert_eq!(change.added, 1);
    assert!(Arc::ptr_eq(&slot, &engine.catalog.slot(0)));
    assert_eq!(
        engine
            .catalog
            .find(Market::Future, Interval::parse("1h").unwrap(), "A"),
        Some(1)
    );
    commit(&engine, 1, 0, true);
    let after = engine.bulk(query()).await.unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    let data: serde_json::Value = serde_json::from_slice(&after.body).unwrap();
    assert_eq!(data["klines"].as_object().unwrap().len(), 2);
    commit(&engine, 0, H, false);
    clock.0.store(2 * H + 1, Ordering::Relaxed);
    let e = engine.clone();
    let pending = tokio::spawn(async move {
        e.bulk(BulkQuery {
            symbols: vec!["B".into()],
            ..query()
        })
        .await
    });
    tokio::task::yield_now().await;
    engine.refresh_catalog(vec![spec("A", 4)]).unwrap();
    tokio::time::timeout(Duration::from_millis(500), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!slot.is_tracked());
    assert!(slot.is_empty());
    commit(&engine, 0, H, true);
    assert!(
        slot.is_empty(),
        "late REST/WS job must not repopulate retired series"
    );
    engine
        .refresh_catalog(vec![spec("A", 4), spec("B", 3)])
        .unwrap();
    assert!(slot.is_tracked());
    assert_eq!(slot.capacity(), 3);
    assert!(slot.is_empty());
    assert!(
        !engine
            .refresh_catalog(vec![spec("A", 4), spec("B", 3)])
            .unwrap()
            .changed
    );
}

#[test]
fn invalid_directory_is_atomic_and_capacity_shrink_retains_newest_records() {
    let engine = Engine::new(
        Catalog::new(vec![spec("A", 5)]).unwrap(),
        Arc::new(Time(AtomicI64::new(10 * H))),
        Settings::default(),
    );
    for n in 0..5 {
        commit(&engine, 0, n * H, true)
    }
    let generation = engine.catalog.generation();
    assert!(
        engine
            .refresh_catalog(vec![spec("A", 2), spec("A", 2)])
            .is_err()
    );
    assert_eq!(engine.catalog.slot(0).len(), 5);
    assert_eq!(engine.catalog.generation(), generation);
    engine.refresh_catalog(vec![spec("A", 2)]).unwrap();
    assert_eq!(
        engine
            .catalog
            .slot(0)
            .records()
            .iter()
            .map(|r| r.0.open_time)
            .collect::<Vec<_>>(),
        vec![3 * H, 4 * H]
    );
}

#[test]
fn closed_bar_diagnostics_deduplicate_and_report_slowest_symbol() {
    let clock = Arc::new(Time(AtomicI64::new(H + 100)));
    let engine = Engine::new(
        Catalog::new(vec![spec("A", 3), spec("B", 3)]).unwrap(),
        clock,
        Settings::default(),
    );
    commit(&engine, 0, 0, false);
    commit(&engine, 1, 0, false);
    engine.diagnostics.record(
        &engine,
        0,
        0,
        kline_service::diagnostics::Observation {
            received_ms: H + 100,
            event_ms: Some(H),
            decode: Duration::from_micros(20),
            cache: Duration::from_micros(10),
        },
    );
    engine.diagnostics.record(
        &engine,
        0,
        0,
        kline_service::diagnostics::Observation {
            received_ms: H + 900,
            event_ms: Some(H),
            decode: Duration::ZERO,
            cache: Duration::ZERO,
        },
    );
    assert!(engine.diagnostics.summaries().is_empty());
    engine.diagnostics.record(
        &engine,
        1,
        0,
        kline_service::diagnostics::Observation {
            received_ms: H + 200,
            event_ms: Some(H),
            decode: Duration::from_micros(30),
            cache: Duration::from_micros(10),
        },
    );
    engine.diagnostics.maintain(&engine);
    let rows = engine.diagnostics.summaries();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.expected, 2);
    assert_eq!(row.arrived, 2);
    assert_eq!(row.last_symbol, "B");
    assert!(!row.timed_out);
    assert!((row.max_ms - 200.04).abs() < 1e-9);
    assert_eq!(row.receive_p99_ms, 200.);
}

#[test]
fn hourly_http_diagnostics_separate_boundary_requests_and_count_errors() {
    let metrics = kline_service::diagnostics::HttpMetrics::default();
    metrics.record_at(
        "/fapi/v1/klines/bulk",
        Duration::from_millis(250),
        200,
        H + 400,
        None,
    );
    metrics.record_at(
        "/fapi/v1/klines/bulk",
        Duration::from_millis(10),
        503,
        H + 500,
        None,
    );
    metrics.record_at(
        "/fapi/v1/klines/bulk",
        Duration::from_millis(2),
        200,
        H + 20_000,
        None,
    );
    let rows = metrics.hourly();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.requests, 3);
    assert_eq!(r.errors, 1);
    assert_eq!(r.boundary_requests, 1);
    assert_eq!(r.p99_ms, 250.);
    assert_eq!(r.last_boundary_response_ms, 400.);
    let mut text = String::new();
    metrics.render(&mut text);
    assert!(text.contains("http_server_requests_seconds_count{route=\"klines_bulk\"} 3"));
}

#[test]
fn disabling_detailed_latency_keeps_the_close_arrival_summary() {
    let engine = Engine::new(
        Catalog::new(vec![spec("A", 3)]).unwrap(),
        Arc::new(Time(AtomicI64::new(H + 123))),
        Settings {
            closed_bar_latency_enabled: false,
            ..Settings::default()
        },
    );
    let raw = br#"{"e":"kline","s":"A","E":3600100,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#;
    engine.ingest(Market::Future, raw).unwrap();
    engine.diagnostics.maintain(&engine);
    let rows = engine.diagnostics.summaries();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].detailed_latency_enabled);
    assert_eq!(rows[0].arrived, 1);
    assert_eq!(rows[0].receive_p99_ms, 123.);
}
