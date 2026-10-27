use kline_core::{
    Bar, Interval, Market, NumberType, Source, Update,
    evidence::{EvidenceState, EvidenceUpdate},
};
use kline_service::{
    Catalog, Clock, Engine, Instrument, Settings,
    evidence::{BreakerState, EvidenceSettings},
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
const M: i64 = 60_000;
const NOW: i64 = 100_000 * M;
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn engine() -> (Arc<Engine>, Arc<TestClock>) {
    let c = Arc::new(TestClock(AtomicI64::new(NOW)));
    (
        Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "BTCUSDT".into(),
                interval: Interval::parse("1m").unwrap(),
                trading: true,
                continuous: Some(("BTCUSDT".into(), "PERPETUAL".into())),
                capacity: NonZeroUsize::new(241).unwrap(),
            }])
            .unwrap(),
            c.clone(),
            Settings {
                number_type: NumberType::String,
                evidence: Some(EvidenceSettings {
                    ws_trust_window_ms: 60_000,
                    ..EvidenceSettings::default()
                }),
                ..Settings::default()
            },
        ),
        c,
    )
}
fn bar(value: &str) -> Bar {
    let mut b = Bar {
        open_time: NOW - M,
        close_time: NOW - 1,
        trades: 1,
        ..Bar::default()
    };
    b.set_numbers(NumberType::String, [value; 8]).unwrap();
    b
}
fn ws(value: &str) -> EvidenceUpdate {
    EvidenceUpdate::Observation(Update {
        bar: bar(value),
        closed: true,
        source: Source::Stream,
        event_time: Some(NOW),
        sequence: 1,
    })
}
#[test]
fn pure_rest_revisions_count_and_ws_backed_or_late_challenges_trip_and_expire() {
    let (e, c) = engine();
    assert!(e.ws_final_trusted());
    e.commit_evidence(0, EvidenceUpdate::Rest(bar("1")))
        .unwrap();
    e.commit_evidence(0, EvidenceUpdate::Rest(bar("2")))
        .unwrap();
    assert_eq!(e.breaker_state().revised_total, 1);
    assert!(e.ws_final_trusted());
    e.commit_evidence(0, ws("2.000")).unwrap();
    let before = e.catalog.slot(0).window_generation();
    let durable = e.catalog.slot(0).durable_generation();
    e.commit_evidence(0, EvidenceUpdate::Rest(bar("2.00")))
        .unwrap();
    assert_eq!(before, e.catalog.slot(0).window_generation());
    assert_eq!(durable, e.catalog.slot(0).durable_generation());
    e.commit_evidence(0, EvidenceUpdate::Rest(bar("3")))
        .unwrap();
    assert!(!e.ws_final_trusted());
    assert_eq!(e.breaker_state().last_conflict_ms, NOW);
    c.0.store(NOW + 59_999, Ordering::Relaxed);
    assert!(!e.ws_final_trusted());
    c.0.store(NOW + 60_000, Ordering::Relaxed);
    assert!(e.ws_final_trusted());
    let before = e.catalog.slot(0).window_generation();
    e.commit_evidence(0, ws("9")).unwrap();
    assert_eq!(before, e.catalog.slot(0).window_generation());
    assert!(!e.ws_final_trusted());
    assert_eq!(e.breaker_state().revised_total, 3);
    let ring = e.evidence_conflicts();
    assert_eq!(ring.len(), 3);
    assert!(!ring[0].trips_breaker);
    assert!(ring[1].trips_breaker && ring[2].trips_breaker);
    assert_eq!(ring[2].previous.bar, Some(bar("3")));
    assert_eq!(ring[2].incoming.bar, Some(bar("9")));
}
#[test]
fn trust_changes_needs_verdict_without_mutating_row_or_window_generation() {
    let (e, c) = engine();
    e.commit_evidence(0, ws("1")).unwrap();
    let generation = e.catalog.slot(0).window_generation();
    assert!(!e.needs_verdict(0, NOW - M));
    e.restore_breaker(BreakerState {
        last_conflict_ms: NOW,
        revised_total: 7,
    });
    assert!(e.needs_verdict(0, NOW - M));
    assert_eq!(
        e.catalog.slot(0).evidence_record(NOW - M).state,
        EvidenceState::WsFinal
    );
    assert_eq!(generation, e.catalog.slot(0).window_generation());
    c.0.store(NOW + M, Ordering::Relaxed);
    assert!(!e.needs_verdict(0, NOW - M));
    assert_eq!(e.breaker_state().revised_total, 7);
}
#[test]
fn stream_gap_is_pending_synthetic_and_rest_absence_removes_it_atomically() {
    let (e, _) = engine();
    let mut first = bar("1");
    first.open_time = NOW - 3 * M;
    first.close_time = NOW - 2 * M - 1;
    e.commit(
        0,
        Update {
            bar: first,
            closed: true,
            source: Source::Stream,
            event_time: Some(1),
            sequence: 1,
        },
    )
    .unwrap();
    e.commit_evidence(0, ws("1")).unwrap();
    let gap = e.catalog.slot(0).evidence_record(NOW - 2 * M);
    assert!(gap.synthetic);
    assert_eq!(gap.state, EvidenceState::Pending);
    assert!(gap.bar.is_some());
    let generation = e.catalog.slot(0).window_generation();
    let durable = e.catalog.slot(0).durable_generation();
    e.commit_evidence(0, EvidenceUpdate::Absent(NOW - 2 * M))
        .unwrap();
    let gap = e.catalog.slot(0).evidence_record(NOW - 2 * M);
    assert_eq!(gap.state, EvidenceState::Absent);
    assert!(!gap.synthetic);
    assert!(gap.bar.is_none());
    assert!(e.catalog.slot(0).window_generation() > generation);
    assert!(e.catalog.slot(0).durable_generation() > durable);
}
#[test]
fn invalid_exact_decimal_and_off_grid_rows_do_not_change_series() {
    let (e, _) = engine();
    assert!(
        e.commit_evidence(0, EvidenceUpdate::Rest(bar("invalid")))
            .is_err()
    );
    let mut b = bar("1");
    b.open_time += 1;
    assert!(e.commit_evidence(0, EvidenceUpdate::Rest(b)).is_err());
    assert!(e.catalog.slot(0).is_empty());
}

#[test]
fn newly_published_slots_are_already_in_evidence_mode() {
    let (e, _) = engine();
    let mut definitions = e.catalog.instruments();
    definitions.push(Instrument {
        market: Market::Future,
        symbol: "ETHUSDT".into(),
        interval: Interval::parse("1m").unwrap(),
        trading: true,
        continuous: Some(("ETHUSDT".into(), "PERPETUAL".into())),
        capacity: NonZeroUsize::new(241).unwrap(),
    });
    e.refresh_catalog(definitions).unwrap();
    let id = e
        .catalog
        .find(Market::Future, Interval::parse("1m").unwrap(), "ETHUSDT")
        .unwrap();
    let mut old = bar("1");
    old.open_time = NOW - 300 * M;
    old.close_time = old.open_time + M - 1;
    e.commit(
        id,
        Update {
            bar: old,
            closed: true,
            source: Source::Restore,
            event_time: None,
            sequence: 0,
        },
    )
    .unwrap();
    e.commit(
        id,
        Update {
            bar: bar("2"),
            closed: true,
            source: Source::Restore,
            event_time: None,
            sequence: 0,
        },
    )
    .unwrap();
    assert!(e.catalog.slot(id).get(NOW - 300 * M).is_none());
    assert_eq!(
        e.catalog.slot(id).evidence_record(NOW - M).state,
        EvidenceState::Pending
    );
}

#[test]
fn matching_late_ws_keeps_the_legacy_provisional_tail_available() {
    let (e, _) = engine();
    e.commit_evidence(0, EvidenceUpdate::Rest(bar("1")))
        .unwrap();
    let slot = e.catalog.slot(0);
    let durable = slot.durable_generation();
    assert!(!slot.lifecycle_status(NOW + 30_000, 15_000, NOW - M).3);
    e.commit_evidence(0, ws("1.0")).unwrap();
    assert!(slot.lifecycle_status(NOW + 30_000, 15_000, NOW - M).3);
    assert_eq!(durable, slot.durable_generation());
}

#[tokio::test]
async fn fix1_invalid_ws_with_existing_history_cannot_fill_or_trim_or_invalidate() {
    let (e, clock) = engine();
    clock.0.store(NOW + 10000, Ordering::Relaxed);
    e.commit_evidence(0, ws("1")).unwrap();
    let query = || kline_service::BulkQuery {
        market: Market::Future,
        interval: "1m".into(),
        limit: Some(1),
        closed_only: true,
        symbols: vec!["BTCUSDT".into()],
    };
    let reply = e.bulk_with_evidence(query()).await.unwrap();
    let slot = e.catalog.slot(0);
    let before = (
        format!("{:?}", slot.records()),
        slot.durable_generation(),
        slot.window_generation(),
        slot.gap_generation(),
        slot.tracking_generation(),
        e.gap_epoch(),
        e.breaker_snapshot(),
        e.cache_sizes(),
    );
    let mut bad = bar("2");
    bad.open_time = NOW + 300 * M + 1;
    bad.close_time = bad.open_time + M - 1;
    assert!(
        e.commit(
            0,
            Update {
                bar: bad,
                closed: true,
                source: Source::Stream,
                event_time: Some(NOW + 1),
                sequence: 2
            }
        )
        .is_err()
    );
    assert_eq!(
        (
            format!("{:?}", slot.records()),
            slot.durable_generation(),
            slot.window_generation(),
            slot.gap_generation(),
            slot.tracking_generation(),
            e.gap_epoch(),
            e.breaker_snapshot(),
            e.cache_sizes()
        ),
        before
    );
    let off_grid = NOW + 300 * M + 1;
    let frame = serde_json::json!({"e":"continuous_kline","E":NOW+1,"ps":"BTCUSDT","ct":"PERPETUAL",
        "k":{"t":off_grid,"T":off_grid+M-1,"i":"1m","o":"2","h":"2","l":"2","c":"2","v":"2","q":"2","n":1,"V":"2","Q":"2","x":true}});
    assert!(
        e.ingest(Market::Future, &serde_json::to_vec(&frame).unwrap())
            .is_err()
    );
    assert_eq!(
        (
            format!("{:?}", slot.records()),
            slot.durable_generation(),
            slot.window_generation(),
            slot.gap_generation(),
            slot.tracking_generation(),
            e.gap_epoch(),
            e.breaker_snapshot(),
            e.cache_sizes()
        ),
        before
    );
    assert!(Arc::ptr_eq(
        &reply,
        &e.bulk_with_evidence(query()).await.unwrap()
    ));
}
