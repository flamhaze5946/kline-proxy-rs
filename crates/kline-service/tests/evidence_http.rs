use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use kline_core::{Interval, Market, NumberType, Source, Update, evidence::EvidenceUpdate};
use kline_service::{
    Catalog, Clock, Engine, Instrument, Settings,
    evidence::EvidenceSettings,
    evidence_bulk::{EvidenceResponse, RetainedRecord, SymbolStatus},
    http::{self, Readiness},
};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
use tower::ServiceExt;
const FIXTURE: &str = include_str!("fixtures/kp_evidence_v1_example.json");
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn fixture() -> (Arc<Engine>, Arc<TestClock>) {
    let expected: EvidenceResponse = serde_json::from_str(FIXTURE).unwrap();
    let clock = Arc::new(TestClock(AtomicI64::new(300500)));
    let catalog = Catalog::new(
        expected
            .status
            .iter()
            .filter(|(_, s)| !matches!(s, SymbolStatus::Unknown))
            .map(|(s, status)| Instrument {
                market: Market::Future,
                symbol: s.clone(),
                interval: Interval::parse("1m").unwrap(),
                trading: matches!(status, SymbolStatus::Trading),
                continuous: Some((s.clone(), "PERPETUAL".into())),
                capacity: NonZeroUsize::new(241).unwrap(),
            })
            .collect(),
    )
    .unwrap();
    let engine = Engine::new(
        catalog,
        clock.clone(),
        Settings {
            number_type: NumberType::String,
            evidence: Some(EvidenceSettings {
                ws_trust_window_ms: 60_000,
                ..EvidenceSettings::default()
            }),
            final_wait_ms: 1,
            pre_boundary_wait_ms: 0,
            ..Settings::default()
        },
    );
    engine.restore_retained(vec![RetainedRecord {
        symbol: "OLDUSDT".into(),
        pair: "OLDUSDT".into(),
        contract_type: "PERPETUAL".into(),
        since: 300000,
        delivery_ms: Some(300000),
        expires_ms: 600000,
    }]);
    for (symbol, rows) in &expected.klines {
        let Some(id) = engine
            .catalog
            .find(Market::Future, Interval::parse("1m").unwrap(), symbol)
        else {
            continue;
        };
        for row in rows {
            let b = binance_wire::parse_rest_bars_with_type(
                &serde_json::to_vec(&vec![row]).unwrap(),
                NumberType::String,
            )
            .unwrap()
            .remove(0);
            if expected.synthetic[symbol].contains(&b.open_time) {
                engine
                    .commit(
                        id,
                        Update {
                            bar: b,
                            closed: false,
                            source: Source::Synthetic,
                            event_time: None,
                            sequence: 0,
                        },
                    )
                    .unwrap();
            } else {
                engine.commit_evidence(id, EvidenceUpdate::Rest(b)).unwrap();
            }
        }
        for &open in &expected.absent[symbol] {
            engine
                .commit_evidence(id, EvidenceUpdate::Absent(open))
                .unwrap();
        }
    }
    (engine, clock)
}
fn query() -> String {
    "/fapi/v1/klines/bulk?interval=1m&limit=5&symbols=BTCUSDT,ETHUSDT,BNBUSDT,OLDUSDT,UNKNOWNUSDT&with_evidence=true".into()
}
async fn get(engine: Arc<Engine>, uri: &str) -> (StatusCode, Value) {
    let r = http::router(engine)
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    (
        r.status(),
        serde_json::from_slice(&r.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}
#[test]
fn shared_fixture_round_trips_through_the_server_wire_type() {
    let value: Value = serde_json::from_str(FIXTURE).unwrap();
    let typed: EvidenceResponse = serde_json::from_value(value.clone()).unwrap();
    typed.validate().unwrap();
    assert_eq!(serde_json::to_value(typed).unwrap(), value);
}
#[tokio::test]
async fn http_from_matching_internal_state_exactly_matches_shared_fixture() {
    let (e, _) = fixture();
    let (status, actual) = get(e, &query()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(actual, serde_json::from_str::<Value>(FIXTURE).unwrap());
}
#[tokio::test]
async fn get_and_post_url_parameter_share_the_contract_and_body_record_is_unchanged() {
    let (e, _) = fixture();
    let (_, expected) = get(e.clone(), &query()).await;
    let r=http::router(e.clone()).oneshot(Request::builder().method("POST").uri("/fapi/v1/klines/bulk?with_evidence=true").header("content-type","application/json").body(Body::from(json!({"interval":"1m","limit":5,"symbols":["BTCUSDT","ETHUSDT","BNBUSDT","OLDUSDT","UNKNOWNUSDT"]}).to_string())).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&r.into_body().collect().await.unwrap().to_bytes())
            .unwrap(),
        expected
    );
    let (status, body) = get(
        e,
        "/fapi/v1/klines/bulk?interval=1m&closed_only=false&with_evidence=true",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.get("contract").is_none());
}
#[tokio::test]
async fn identity_keeps_unknown_and_unfilled_symbols_and_normalizes_duplicates() {
    let (e, _) = fixture();
    let (_, a) = get(
        e.clone(),
        "/fapi/v1/klines/bulk?interval=1m&symbols=BTCUSDT,UNKNOWN_A&with_evidence=true",
    )
    .await;
    let (_, b) = get(
        e.clone(),
        "/fapi/v1/klines/bulk?interval=1m&symbols=BTCUSDT,UNKNOWN_B&with_evidence=true",
    )
    .await;
    assert!(a["status"].get("UNKNOWN_A").is_some());
    assert!(b["status"].get("UNKNOWN_B").is_some());
    assert!(b["status"].get("UNKNOWN_A").is_none());
    let mut definitions = e.catalog.instruments();
    definitions.push(Instrument {
        market: Market::Future,
        symbol: "EMPTYUSDT".into(),
        interval: Interval::parse("1m").unwrap(),
        trading: true,
        continuous: Some(("EMPTYUSDT".into(), "PERPETUAL".into())),
        capacity: NonZeroUsize::new(241).unwrap(),
    });
    e.refresh_catalog(definitions).unwrap();
    let (_, v) = get(
        e,
        "/fapi/v1/klines/bulk?interval=1m&symbols=%20EMPTYUSDT%20,EMPTYUSDT&with_evidence=true",
    )
    .await;
    assert_eq!(v["status"], json!({"EMPTYUSDT":{"state":"trading"}}));
    for key in ["klines", "evidence", "absent", "missing", "synthetic"] {
        assert_eq!(v[key], json!({"EMPTYUSDT":[]}));
    }
    assert_eq!(v["covered_from"], json!({"EMPTYUSDT":null}));
}
struct Recovering;
impl Readiness for Recovering {
    fn is_ready(&self) -> bool {
        false
    }
    fn details(&self) -> Value {
        json!({})
    }
    fn evidence_query_ready(&self) -> bool {
        true
    }
}
#[tokio::test]
async fn evidence_readiness_bypasses_legacy_bootstrapped_and_series_checks() {
    let (e, _) = fixture();
    let router = http::router_with_policy(e, Some(Arc::new(Recovering)), true);
    let r = router
        .clone()
        .oneshot(Request::builder().uri(query()).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = router
        .oneshot(
            Request::builder()
                .uri("/fapi/v1/klines/bulk?interval=1m")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
}
#[tokio::test]
async fn trust_recheck_invalidate_cached_replies_even_when_payload_is_none() {
    let (e, clock) = fixture();
    let (_, before) = get(e.clone(), &query()).await;
    assert_eq!(before["ws_final_trusted"], true);
    let id = e
        .catalog
        .find(Market::Future, Interval::parse("1m").unwrap(), "BTCUSDT")
        .unwrap();
    let mut b = e.catalog.slot(id).get(240000).unwrap().0;
    b.trades += 1;
    e.commit(
        id,
        Update {
            bar: b,
            closed: true,
            source: Source::Stream,
            event_time: Some(1),
            sequence: 1,
        },
    )
    .unwrap();
    let (_, after) = get(e.clone(), &query()).await;
    assert_eq!(after["ws_final_trusted"], false);
    assert_eq!(after["revised_total"], 1);
    clock.0.store(360500, Ordering::Relaxed);
    let (_, recovered) = get(e, &query()).await;
    assert_eq!(recovered["ws_final_trusted"], true);
}
#[tokio::test]
async fn retained_expiry_is_unknown_even_before_the_next_directory_refresh() {
    let (e, clock) = fixture();
    clock.0.store(600000, Ordering::Relaxed);
    let (_, body) = get(
        e,
        "/fapi/v1/klines/bulk?interval=1m&symbols=OLDUSDT&with_evidence=true",
    )
    .await;
    assert_eq!(body["status"]["OLDUSDT"]["state"], "unknown");
    assert_eq!(body["klines"]["OLDUSDT"], json!([]));
}
fn matches_source(engine: &Engine, packet: &EvidenceResponse) -> anyhow::Result<()> {
    packet.validate()?;
    let expected = serde_json::from_str::<EvidenceResponse>(FIXTURE)?;
    anyhow::ensure!(
        packet.status.keys().eq(expected.status.keys()),
        "requested symbol set changed"
    );
    for symbol in packet.status.keys() {
        let Some(id) = engine
            .catalog
            .find(Market::Future, Interval::parse("1m").unwrap(), symbol)
        else {
            continue;
        };
        let slot = engine.catalog.slot(id);
        for (row, kind) in packet.klines[symbol].iter().zip(&packet.evidence[symbol]) {
            let open = row[0].as_i64().unwrap();
            let source = slot.evidence_record(open);
            let bar = source
                .bar
                .ok_or_else(|| anyhow::anyhow!("wire row has no source bytes"))?;
            anyhow::ensure!(
                serde_json::to_value(row)? == serde_json::to_value(binance_wire::DisplayBar(bar))?,
                "wire value differs from source"
            );
            let expected_kind = if source.state.needs_verdict(engine.ws_final_trusted()) {
                kline_service::evidence_bulk::RowEvidence::Pending
            } else {
                kline_service::evidence_bulk::RowEvidence::Final
            };
            anyhow::ensure!(*kind == expected_kind, "pending source claimed final");
            anyhow::ensure!(
                packet.synthetic[symbol].contains(&open) == source.synthetic,
                "synthetic source flag changed"
            );
        }
        for &open in &packet.absent[symbol] {
            anyhow::ensure!(
                slot.evidence_record(open).state == kline_core::evidence::EvidenceState::Absent,
                "real source row claimed absent"
            );
        }
        for &open in &packet.missing[symbol] {
            anyhow::ensure!(
                slot.evidence_record(open).state == kline_core::evidence::EvidenceState::Empty,
                "source row claimed missing"
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn five_negative_cases_fail_contract_or_matching_source_snapshot() {
    let (engine, clock) = fixture();
    // Keep both a synthetic P and a real (non-synthetic) P in the same window.
    let id = engine
        .catalog
        .find(Market::Future, Interval::parse("1m").unwrap(), "BNBUSDT")
        .unwrap();
    let mut bar = engine.catalog.slot(id).get(240000).unwrap().0;
    bar.open_time = 300000;
    bar.close_time = 359999;
    engine
        .commit(
            id,
            Update {
                bar,
                closed: false,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            },
        )
        .unwrap();
    clock.0.store(360500, Ordering::Relaxed);
    let (_, body) = get(engine.clone(), &query()).await;
    let original: EvidenceResponse = serde_json::from_value(body).unwrap();
    matches_source(&engine, &original).unwrap();
    for case in 0..5 {
        let mut bad = original.clone();
        match case {
            0 => {
                let removed = bad.klines.get_mut("BTCUSDT").unwrap().remove(0);
                bad.evidence.get_mut("BTCUSDT").unwrap().remove(0);
                bad.absent
                    .get_mut("BTCUSDT")
                    .unwrap()
                    .push(removed[0].as_i64().unwrap());
            }
            1 => bad.klines.get_mut("BTCUSDT").unwrap()[0][1] = json!("10.001"),
            2 => {
                let index = bad.klines["BNBUSDT"]
                    .iter()
                    .position(|row| row[0] == 120000)
                    .unwrap();
                bad.evidence.get_mut("BNBUSDT").unwrap()[index] =
                    kline_service::evidence_bulk::RowEvidence::Final
            }
            3 => {
                let index = bad.klines["BNBUSDT"]
                    .iter()
                    .position(|row| row[0] == 300000)
                    .unwrap();
                bad.evidence.get_mut("BNBUSDT").unwrap()[index] =
                    kline_service::evidence_bulk::RowEvidence::Final;
            }
            _ => {
                bad.status.remove("BTCUSDT");
            }
        }
        assert!(
            matches_source(&engine, &bad).is_err(),
            "case {case} escaped the source comparison"
        );
    }
}
#[tokio::test]
async fn disabled_mode_only_true_url_parameter_changes_legacy_behavior() {
    let e = Engine::new(
        Catalog::new(vec![]).unwrap(),
        Arc::new(TestClock(AtomicI64::new(300500))),
        Settings::default(),
    );
    let (_, baseline) = get(e.clone(), "/fapi/v1/klines/bulk?interval=1m").await;
    for v in ["false", ""] {
        let (status, body) = get(
            e.clone(),
            &format!("/fapi/v1/klines/bulk?interval=1m&with_evidence={v}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, baseline);
    }
    assert_eq!(
        get(
            e.clone(),
            "/fapi/v1/klines/bulk?interval=1m&with_evidence=true"
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(e, "/fapi/v1/klines/bulk?interval=1m&with_evidence=%74rue")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn cached_not_trading_reply_expires_at_its_retention_deadline_within_the_same_minute() {
    let (e, clock) = fixture();
    let mut records = e.retained_records();
    records[0].expires_ms = 300600;
    e.restore_retained(records);
    let uri = "/fapi/v1/klines/bulk?interval=1m&symbols=OLDUSDT&with_evidence=true";
    assert_eq!(
        get(e.clone(), uri).await.1["status"]["OLDUSDT"]["state"],
        "not_trading"
    );
    clock.0.store(300600, Ordering::Relaxed);
    let (status, body) = get(e, uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"]["OLDUSDT"]["state"], "unknown");
    assert!(body["covered_from"]["OLDUSDT"].is_null());
}

#[tokio::test]
async fn evidence_requires_catalog_loaded_but_an_explicit_empty_catalog_is_valid() {
    let e = Engine::new(
        Catalog::new(vec![]).unwrap(),
        Arc::new(TestClock(AtomicI64::new(300500))),
        Settings {
            number_type: NumberType::String,
            evidence: Some(EvidenceSettings {
                ws_trust_window_ms: 60000,
                ..EvidenceSettings::default()
            }),
            ..Settings::default()
        },
    );
    let uri = "/fapi/v1/klines/bulk?interval=1m&symbols=UNKNOWN&with_evidence=true";
    assert_eq!(get(e.clone(), uri).await.0, StatusCode::SERVICE_UNAVAILABLE);
    e.refresh_catalog(vec![]).unwrap();
    let (status, body) = get(e, uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"]["UNKNOWN"]["state"], "unknown");
}

#[tokio::test]
async fn unchanged_ws_rows_change_wire_evidence_when_breaker_trips_and_expires() {
    let (source, clock) = fixture();
    let definition = source
        .catalog
        .instruments()
        .into_iter()
        .find(|i| i.symbol == "BTCUSDT")
        .unwrap();
    let e = Engine::new(
        Catalog::new(vec![definition]).unwrap(),
        clock.clone(),
        source.settings().clone(),
    );
    for (bar, _) in source
        .catalog
        .slot(
            source
                .catalog
                .find(Market::Future, Interval::parse("1m").unwrap(), "BTCUSDT")
                .unwrap(),
        )
        .records()
    {
        e.commit(
            0,
            Update {
                bar,
                closed: true,
                source: Source::Stream,
                event_time: Some(1),
                sequence: 1,
            },
        )
        .unwrap();
    }
    let generation = e.catalog.slot(0).window_generation();
    let uri = "/fapi/v1/klines/bulk?interval=1m&limit=5&symbols=BTCUSDT&with_evidence=true";
    let (_, before) = get(e.clone(), uri).await;
    assert!(
        before["evidence"]["BTCUSDT"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v == "final")
    );
    e.restore_breaker(kline_service::evidence::BreakerState {
        last_conflict_ms: 240600,
        revised_total: 7,
    });
    let (_, tripped) = get(e.clone(), uri).await;
    assert_eq!(tripped["ws_final_trusted"], false);
    assert!(
        tripped["evidence"]["BTCUSDT"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v == "pending")
    );
    clock.0.store(300600, Ordering::Relaxed);
    let (_, recovered) = get(e.clone(), uri).await;
    assert_eq!(recovered["ws_final_trusted"], true);
    assert_eq!(recovered["revised_total"], 7);
    assert_eq!(recovered["evidence"], before["evidence"]);
    assert_eq!(before["klines"], tripped["klines"]);
    assert_eq!(before["klines"], recovered["klines"]);
    assert_eq!(e.catalog.slot(0).window_generation(), generation);
}

#[tokio::test(start_paused = true)]
async fn unfilled_trading_series_waits_only_until_boundary_deadline_then_returns_dense_empty_maps()
{
    use std::time::Duration;
    let (source, clock) = fixture();
    let definition = source
        .catalog
        .instruments()
        .into_iter()
        .find(|i| i.symbol == "BTCUSDT")
        .unwrap();
    let settings = Settings {
        final_wait_ms: 3000,
        ..source.settings().clone()
    };
    let e = Engine::new(
        Catalog::new(vec![definition]).unwrap(),
        clock.clone(),
        settings,
    );
    let waiter = tokio::spawn(get(
        e.clone(),
        "/fapi/v1/klines/bulk?interval=1m&limit=5&symbols=BTCUSDT&with_evidence=true",
    ));
    for _ in 0..100 {
        if e.inflight_holders() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(e.inflight_holders(), 1);
    tokio::time::advance(Duration::from_millis(2499)).await;
    assert!(!waiter.is_finished());
    clock.0.store(303000, Ordering::Relaxed);
    tokio::time::advance(Duration::from_millis(1)).await;
    let (status, value) = waiter.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    let packet: EvidenceResponse = serde_json::from_value(value).unwrap();
    packet.validate().unwrap();
    assert_eq!(packet.status["BTCUSDT"], SymbolStatus::Trading);
    assert_eq!(packet.window.first_open, 0);
    assert_eq!(packet.window.last_open, 240000);
    assert!(packet.covered_from["BTCUSDT"].is_none());
    assert!(packet.klines["BTCUSDT"].is_empty());
    assert!(packet.missing["BTCUSDT"].is_empty());
}

#[tokio::test]
async fn absent_latest_releases_evidence_wait_but_a_missing_latest_does_not() {
    use std::time::Duration;
    let (source, clock) = fixture();
    let definition = source
        .catalog
        .instruments()
        .into_iter()
        .find(|i| i.symbol == "BTCUSDT")
        .unwrap();
    let settings = Settings {
        final_wait_ms: 30000,
        ..source.settings().clone()
    };
    let e = Engine::new(Catalog::new(vec![definition]).unwrap(), clock, settings);
    let mut bar = source
        .catalog
        .slot(
            source
                .catalog
                .find(Market::Future, Interval::parse("1m").unwrap(), "BTCUSDT")
                .unwrap(),
        )
        .get(180000)
        .unwrap()
        .0;
    e.commit_evidence(0, EvidenceUpdate::Rest(bar.clone()))
        .unwrap();
    let mut waiter = tokio::spawn(get(
        e.clone(),
        "/fapi/v1/klines/bulk?interval=1m&limit=2&symbols=BTCUSDT&with_evidence=true",
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while e.inflight_holders() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err()
    );
    e.commit_evidence(0, EvidenceUpdate::Absent(240000))
        .unwrap();
    let (status, value) = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["absent"]["BTCUSDT"], json!([240000]));
    assert_eq!(value["missing"]["BTCUSDT"], json!([]));
    // Absence has no bar row; a late WS challenge cannot inject its bytes.
    bar.open_time = 240000;
    bar.close_time = 299999;
    e.commit(
        0,
        Update {
            bar,
            closed: true,
            source: Source::Stream,
            event_time: Some(2),
            sequence: 2,
        },
    )
    .unwrap();
    let (_, late) = get(
        e,
        "/fapi/v1/klines/bulk?interval=1m&limit=2&symbols=BTCUSDT&with_evidence=true",
    )
    .await;
    assert_eq!(late["absent"]["BTCUSDT"], json!([240000]));
    assert_eq!(late["klines"]["BTCUSDT"].as_array().unwrap().len(), 1);
    assert_eq!(late["ws_final_trusted"], false);
    assert_eq!(late["revised_total"], 1);
}
