use binance_wire::DisplayBar;
use kline_core::{Bar, Interval, Market, NumberType, Source, Update};
use kline_service::{BulkQuery, Catalog, Clock, Engine, Instrument, Settings};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

const H: i64 = 3_600_000;
const B: i64 = 1_790_618_400_000; // 2026-09-28 18:00 UTC
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn fixture(market: Market, code: &str, now: i64, trading: bool) -> (Arc<Engine>, Arc<TestClock>) {
    fixture_with_mode(market, code, now, trading, NumberType::Double)
}
fn fixture_with_mode(
    market: Market,
    code: &str,
    now: i64,
    trading: bool,
    number_type: NumberType,
) -> (Arc<Engine>, Arc<TestClock>) {
    let clock = Arc::new(TestClock(AtomicI64::new(now)));
    let engine = Engine::new(
        Catalog::new(vec![Instrument {
            market,
            symbol: "BTCUSDT".into(),
            interval: Interval::parse(code).unwrap(),
            trading,
            continuous: None,
            capacity: NonZeroUsize::new(20).unwrap(),
        }])
        .unwrap(),
        clock.clone(),
        Settings {
            number_type,
            ..Settings::default()
        },
    );
    (engine, clock)
}
fn bar(open: i64, end: i64, mode: NumberType, close: &str, trades: u32) -> Bar {
    let mut bar = Bar {
        open_time: open,
        close_time: end,
        trades,
        ..Bar::default()
    };
    bar.set_numbers(
        mode,
        [
            "123", "124", "122", close, "12.5", "1000.25", "6.25", "500.125",
        ],
    )
    .unwrap();
    bar
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
fn query(market: Market, code: &str, limit: i32, closed_only: bool) -> BulkQuery {
    BulkQuery {
        market,
        interval: code.into(),
        limit: Some(limit),
        closed_only,
        symbols: vec!["BTCUSDT".into()],
    }
}
fn cached(e: &Engine, market: Market, code: &str, limit: usize) -> Vec<Bar> {
    kline_market::klines::cached(
        e,
        market,
        "BTCUSDT",
        Interval::parse(code).unwrap(),
        None,
        None,
        limit,
    )
}
fn display(bar: Bar) -> Value {
    serde_json::to_value(DisplayBar(bar)).unwrap()
}
fn assert_zero(bar: &Bar, open: i64, end: i64, previous: &Bar) {
    let row = display(bar.clone());
    let old = display(previous.clone());
    assert_eq!(row[0], open);
    assert_eq!(row[6], end);
    for i in [1, 2, 3, 4] {
        assert_eq!(row[i], old[4]);
    }
    for i in [5, 7, 9, 10] {
        assert_eq!(row[i], "0");
    }
    assert_eq!(row[8], 0);
    assert_eq!(bar.number_type, previous.number_type);
}

#[tokio::test]
async fn both_markets_all_numeric_modes_replace_temporary_tail_without_bulk_ttl_delay() {
    for market in [Market::Spot, Market::Future] {
        for mode in [
            NumberType::String,
            NumberType::Float,
            NumberType::Double,
            NumberType::BigDecimal,
        ] {
            for source in [Source::Rest, Source::Stream] {
                let (e, _) = fixture_with_mode(market, "1h", B + 30_000, true, mode);
                let previous = bar(B - H, B - 1, mode, "0.0000123400", 12);
                commit(&e, previous.clone(), true, Source::Stream);
                let rows = cached(&e, market, "1h", 1);
                assert_eq!(rows.len(), 1);
                assert_zero(&rows[0], B, B + H - 1, &previous);
                let bulk = e.bulk(query(market, "1h", 1, false)).await.unwrap();
                let body: Value = serde_json::from_slice(&bulk.body).unwrap();
                assert_eq!(body["klines"]["BTCUSDT"], json!([display(rows[0].clone())]));
                assert!(Arc::ptr_eq(
                    &bulk,
                    &e.bulk(query(market, "1h", 1, false)).await.unwrap()
                ));
                let closed = e.bulk(query(market, "1h", 1, true)).await.unwrap();
                let body: Value = serde_json::from_slice(&closed.body).unwrap();
                assert_eq!(body["klines"]["BTCUSDT"][0][0], B - H);
                let slot = e.catalog.slot(0);
                assert!(!slot.contains(B));
                assert!(!slot.fresh_tail(B + 30_000, 15_000));
                assert_eq!(slot.durable_snapshot(B + H + 1).1, vec![previous]);
                // A real zero-trade row also wins: no placeholder stored in the revision ledger.
                let real = bar(B, B + H - 1, mode, "0.00001235", 0);
                commit(&e, real.clone(), false, source);
                let replaced = e.bulk(query(market, "1h", 1, false)).await.unwrap();
                assert!(!Arc::ptr_eq(&bulk, &replaced));
                let body: Value = serde_json::from_slice(&replaced.body).unwrap();
                assert_eq!(body["klines"]["BTCUSDT"][0], display(real.clone()));
                assert_eq!(cached(&e, market, "1h", 1), vec![real]);
                assert!(!slot.get(B).unwrap().1);
            }
        }
    }
}

#[tokio::test]
async fn empty_or_unconfirmed_tail_is_not_extrapolated_and_final_arrival_invalidates_bulk() {
    for market in [Market::Spot, Market::Future] {
        let (e, _) = fixture(market, "1h", B + 30_000, true);
        let empty = e.bulk(query(market, "1h", 1, false)).await.unwrap();
        assert!(cached(&e, market, "1h", 1).is_empty());
        let previous = bar(B - H, B - 1, NumberType::Double, "1", 1);
        commit(&e, previous.clone(), false, Source::Stream);
        let forming = e.bulk(query(market, "1h", 1, false)).await.unwrap();
        assert!(!Arc::ptr_eq(&empty, &forming));
        assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B - H);
        assert!(
            !e.catalog
                .slot(0)
                .lifecycle_status(B + 30_000, 15_000, B - H)
                .3
        );
        commit(&e, previous, true, Source::Stream);
        let confirmed = e.bulk(query(market, "1h", 1, false)).await.unwrap();
        assert!(!Arc::ptr_eq(&forming, &confirmed));
        assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B);
    }
}

#[test]
fn historical_and_explicit_ranges_preserve_limits_and_placeholder_never_chains() {
    let market = Market::Future;
    let (e, clock) = fixture(market, "1h", B + 30_000, true);
    for i in (1..=12).rev() {
        commit(
            &e,
            bar(B - i * H, B - (i - 1) * H - 1, NumberType::Double, "100", 1),
            true,
            Source::Stream,
        );
    }
    let latest = cached(&e, market, "1h", 10);
    assert_eq!(latest.len(), 10);
    assert_eq!(latest[0].open_time, B - 9 * H);
    assert_eq!(latest[9].open_time, B);
    let range = |start, end| {
        kline_market::klines::cached(
            &e,
            market,
            "BTCUSDT",
            Interval::parse("1h").unwrap(),
            start,
            end,
            1,
        )
    };
    assert_eq!(range(Some(B), Some(B))[0].open_time, B);
    assert_eq!(range(None, Some(B - 1))[0].open_time, B - H);
    assert!(range(Some(B + H), Some(B + H)).is_empty());
    clock.0.store(B + H, Ordering::Relaxed);
    assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B - H);
}

#[test]
fn inactive_symbols_and_older_unconfirmed_bars_cannot_be_made_ready_by_placeholder() {
    let (inactive, _) = fixture(Market::Spot, "1h", B + 30_000, false);
    commit(
        &inactive,
        bar(B - H, B - 1, NumberType::Double, "1", 1),
        true,
        Source::Stream,
    );
    assert_eq!(cached(&inactive, Market::Spot, "1h", 1)[0].open_time, B - H);
    let (e, _) = fixture(Market::Future, "1h", B + 30_000, true);
    commit(
        &e,
        bar(B - 2 * H, B - H - 1, NumberType::Double, "1", 1),
        false,
        Source::Stream,
    );
    commit(
        &e,
        bar(B - H, B - 1, NumberType::Double, "2", 2),
        true,
        Source::Stream,
    );
    assert_eq!(cached(&e, Market::Future, "1h", 1)[0].open_time, B);
    assert!(
        !e.catalog
            .slot(0)
            .lifecycle_status(B + 30_000, 15_000, B - H)
            .3
    );
}

#[test]
fn fixed_intervals_and_weekly_grid_use_previous_close_timestamp() {
    for interval in Interval::all().filter(|i| i.code() != "1M") {
        let (e, clock) = fixture(Market::Future, interval.code(), B - 1, true);
        let previous = bar(B - interval.millis(), B - 1, NumberType::Double, "100", 1);
        commit(&e, previous.clone(), true, Source::Stream);
        let slot = e.catalog.slot(0);
        assert_eq!(slot.snapshot(1, false, B - 1), vec![previous.clone()]);
        clock.0.store(B, Ordering::Relaxed);
        assert_zero(
            &slot.snapshot(1, false, B)[0],
            B,
            B + interval.millis() - 1,
            &previous,
        );
        assert_eq!(
            slot.snapshot(1, false, B + interval.millis())[0].open_time,
            previous.open_time
        );
    }
}

#[test]
fn calendar_month_handles_leap_february_and_variable_lengths() {
    use chrono::DateTime;
    let time = |s| DateTime::parse_from_rfc3339(s).unwrap().timestamp_millis();
    for (before, open, end) in [
        (
            "2024-01-01T00:00:00Z",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
        ),
        (
            "2026-02-01T00:00:00Z",
            "2026-03-01T00:00:00Z",
            "2026-04-01T00:00:00Z",
        ),
        (
            "2026-11-01T00:00:00Z",
            "2026-12-01T00:00:00Z",
            "2027-01-01T00:00:00Z",
        ),
    ] {
        let (open, end) = (time(open), time(end));
        let (e, _) = fixture_with_mode(Market::Future, "1M", open, true, NumberType::String);
        let previous = bar(time(before), open - 1, NumberType::String, "123.4500", 1);
        commit(&e, previous.clone(), true, Source::Stream);
        let slot = e.catalog.slot(0);
        assert_zero(
            &slot.snapshot(1, false, end - 1)[0],
            open,
            end - 1,
            &previous,
        );
        assert_eq!(
            slot.snapshot(1, false, end)[0].open_time,
            previous.open_time
        );
    }
}

#[tokio::test]
async fn provisional_bulk_expires_on_calendar_rollover_even_inside_legacy_cache_boundary() {
    let open = 1_706_745_600_000; // February 2024
    let end = 1_709_251_200_000; // March 2024
    let (e, clock) = fixture(Market::Future, "1M", end - 1, true);
    let previous = bar(
        open - 31 * 86_400_000,
        open - 1,
        NumberType::Double,
        "100",
        1,
    );
    commit(&e, previous.clone(), true, Source::Stream);
    assert_eq!(cached(&e, Market::Future, "1M", 1)[0].open_time, open);
    let old = e.bulk(query(Market::Future, "1M", 1, false)).await.unwrap();
    clock.0.store(end, Ordering::Relaxed);
    let current = e.bulk(query(Market::Future, "1M", 1, false)).await.unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    let body: Value = serde_json::from_slice(&current.body).unwrap();
    assert_eq!(body["klines"]["BTCUSDT"][0][0], previous.open_time);
}

#[tokio::test]
async fn only_previous_websocket_x_true_can_authorize_provisional_current() {
    for market in [Market::Spot, Market::Future] {
        for source in [Source::Rest, Source::Restore, Source::Synthetic] {
            let (e, clock) = fixture(market, "1h", B + 30_000, true);
            let previous = bar(B - H, B - 1, NumberType::Double, "100", 1);
            commit(&e, previous.clone(), true, source);
            let before = e.bulk(query(market, "1h", 1, false)).await.unwrap();
            assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B - H);
            // Seeing x=false or x=true for a different candle is insufficient.
            commit(&e, previous.clone(), false, Source::Stream);
            commit(
                &e,
                bar(B - 2 * H, B - H - 1, NumberType::Double, "99", 1),
                true,
                Source::Stream,
            );
            assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B - H);
            let before_confirmation = e.bulk(query(market, "1h", 1, false)).await.unwrap();
            let generation = e.catalog.slot(0).window_generation();
            // Numerically identical x=true still changes eligibility and invalidates bulk.
            commit(&e, previous.clone(), true, Source::Stream);
            assert!(e.catalog.slot(0).window_generation() > generation);
            assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B);
            let after = e.bulk(query(market, "1h", 1, false)).await.unwrap();
            assert!(!Arc::ptr_eq(&after, &before));
            assert!(!Arc::ptr_eq(&after, &before_confirmation));
            // Snapshot recovery deliberately does not recover the stream confirmation.
            let (restarted, _) = fixture(market, "1h", B + 30_000, true);
            for row in e.catalog.slot(0).durable_snapshot(B + H).1 {
                commit(&restarted, row, true, Source::Restore);
            }
            assert_eq!(cached(&restarted, market, "1h", 1)[0].open_time, B - H);
            // Evidence must match the immediately preceding bar, not carry into the next hour.
            clock.0.store(B + H + 30_000, Ordering::Relaxed);
            let next = bar(B, B + H - 1, NumberType::Double, "101", 2);
            commit(&e, next.clone(), true, Source::Rest);
            assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B);
            commit(&e, next, true, Source::Stream);
            assert_eq!(cached(&e, market, "1h", 1)[0].open_time, B + H);
        }
    }
}
