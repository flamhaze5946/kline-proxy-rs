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
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn fixture(wait: u64) -> (Arc<Engine>, Arc<TestClock>) {
    fixture_with(Settings {
        final_wait_ms: wait,
        ..Settings::default()
    })
}
fn fixture_with(settings: Settings) -> (Arc<Engine>, Arc<TestClock>) {
    let clock = Arc::new(TestClock(AtomicI64::new(H + 1)));
    let instruments = ["A", "B", "C", "D"]
        .into_iter()
        .map(|s| Instrument {
            market: Market::Future,
            symbol: s.to_owned(),
            interval: Interval::parse("1h").unwrap(),
            trading: s != "D",
            continuous: Some((format!("{s}_PAIR"), "PERPETUAL".into())),
            capacity: NonZeroUsize::new(1000).unwrap(),
        })
        .collect();
    (
        Engine::new(Catalog::new(instruments).unwrap(), clock.clone(), settings),
        clock,
    )
}
fn commit(e: &Engine, id: usize, open: i64, closed: bool, n: u32, close: f64) {
    e.commit(
        id,
        Update {
            bar: Bar {
                open_time: open,
                close_time: open + H - 1,
                trades: n,
                values: [close; 8],

                ..Bar::default()
            },
            closed,
            source: Source::Stream,
            event_time: Some(i64::from(n)),
            sequence: u64::from(n),
        },
    )
    .unwrap();
}
fn query(symbols: &[&str]) -> BulkQuery {
    BulkQuery {
        market: Market::Future,
        interval: "1h".into(),
        limit: Some(1),
        closed_only: true,
        symbols: symbols.iter().map(|s| (*s).into()).collect(),
    }
}
fn json(reply: &kline_service::BulkReply) -> serde_json::Value {
    serde_json::from_slice(&reply.body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_requests_share_wait_build_and_serialized_bytes() {
    let (engine, _) = fixture(1000);
    commit(&engine, 0, 0, false, 1, 100.);
    let mut requests = Vec::new();
    for _ in 0..96 {
        let e = engine.clone();
        requests.push(tokio::spawn(
            async move { e.bulk(query(&["A"])).await.unwrap() },
        ));
    }
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(engine.metrics.responses_built.load(Ordering::Relaxed), 0);
    commit(&engine, 0, 0, true, 2, 109.);
    let mut replies = Vec::new();
    for request in requests {
        replies.push(request.await.unwrap());
    }
    assert!(
        replies
            .iter()
            .all(|r| r.finalized && Arc::ptr_eq(r, &replies[0]))
    );
    assert_eq!(json(&replies[0])["klines"]["A"][0][4], "109");
    assert_eq!(engine.metrics.responses_built.load(Ordering::Relaxed), 1);
    assert_eq!(engine.cache_sizes().1, 0);
}

#[tokio::test]
async fn cap_pending_missing_and_nontrading_match_java() {
    let (engine, _) = fixture(25);
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 3, 0, false, 1, 100.);
    let result = engine
        .bulk(query(&[" A ", "D", "B", "unknown", "A"]))
        .await
        .unwrap();
    let body = json(&result);
    assert_eq!(body["pending"], serde_json::json!(["A"]));
    assert_eq!(body["not_trading"], serde_json::json!(["D"]));
    assert!(body["klines"]["B"].is_null());
    assert!(!result.finalized);
    assert!(result.waited_ms >= 20 && result.waited_ms < 200);
    assert_eq!(engine.cache_sizes(), (0, 0, 0));
    commit(&engine, 0, 0, true, 2, 109.);
    assert!(engine.bulk(query(&["A", "D"])).await.unwrap().finalized);
}

#[tokio::test]
async fn java_boundary_finality_is_separate_from_window_diagnostics_and_repair_invalidates_cache() {
    let (engine, clock) = fixture(1000);
    clock.0.store(3 * H + 1, Ordering::Relaxed);
    commit(&engine, 0, H, true, 1, 100.);
    let mut q = query(&["A"]);
    q.limit = Some(3);
    let missing = engine.bulk(q).await.unwrap();
    assert!(missing.finalized);
    assert_eq!(missing.waited_ms, 0);
    let body = json(&missing);
    assert_eq!(body["pending"], serde_json::json!([]));
    assert_eq!(
        body["data_status"]["missing_latest"],
        serde_json::json!(["A"])
    );
    assert_eq!(body["data_status"]["window_finalized"], true);
    let mut q = query(&["A"]);
    q.limit = Some(3);
    assert!(Arc::ptr_eq(&missing, &engine.bulk(q).await.unwrap()));

    commit(&engine, 0, 0, false, 1, 99.);
    commit(&engine, 0, 2 * H, true, 1, 101.);
    let mut q = query(&["A"]);
    q.limit = Some(3);
    let old_nonfinal = engine.bulk(q).await.unwrap();
    assert!(old_nonfinal.finalized);
    assert_eq!(old_nonfinal.waited_ms, 0);
    assert!(!Arc::ptr_eq(&missing, &old_nonfinal));
    let body = json(&old_nonfinal);
    assert_eq!(body["pending"], serde_json::json!([]));
    assert_eq!(body["data_status"]["missing_latest"], serde_json::json!([]));
    assert_eq!(
        body["data_status"]["nonfinal_symbols"],
        serde_json::json!(["A"])
    );
    assert_eq!(body["data_status"]["window_finalized"], false);
    commit(&engine, 0, 0, true, 2, 100.);
    let mut q = query(&["A"]);
    q.limit = Some(3);
    let repaired = json(&engine.bulk(q).await.unwrap());
    assert_eq!(repaired["data_status"]["window_finalized"], true);
    assert_eq!(
        repaired["data_status"]["nonfinal_symbols"],
        serde_json::json!([])
    );
    let mut q = query(&["A"]);
    q.closed_only = false;
    assert!(
        json(&engine.bulk(q).await.unwrap())
            .get("data_status")
            .is_none()
    );
}

#[tokio::test]
async fn http_java_coercions_null_bodies_and_rejections_keep_json_contract() {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture(0);
    commit(&engine, 0, 0, true, 1, 100.);
    let app = kline_service::http::router(engine);
    for prefix in ["/fapi/v1", "/api/v3"] {
        for (method, query, body, status) in [
            ("GET", "?interval=1h&limit=", "", 200),
            ("GET", "?interval=1h&closed_only=1", "", 200),
            ("GET", "?interval=1h&closed_only=off", "", 200),
            ("GET", "?interval=1h&closed_only=", "", 200),
            (
                "POST",
                "",
                r#"{"interval":"1h","limit":"1","closed_only":"true"}"#,
                200,
            ),
            (
                "POST",
                "",
                r#"{"interval":"1h","limit":1.9,"closed_only":0}"#,
                200,
            ),
            ("POST", "", " \n null \n ", 400),
            ("POST", "", "", 400),
            ("POST", "", "{", 500),
            ("POST", "", r#"{"interval":"1h","limit":2147483648}"#, 500),
            ("GET", "?interval=1h&limit=invalid", "", 500),
            ("GET", "?interval=1h&closed_only=invalid", "", 500),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(format!("{prefix}/klines/bulk{query}"))
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status().as_u16(),
                status,
                "{method} {query} {body}"
            );
            assert_eq!(response.headers()["content-type"], "application/json");
            let data: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            if status != 200 {
                assert!(data["code"].is_i64());
                assert!(data["msg"].is_string());
            }
            if body == " \n null \n " || (method == "POST" && body.is_empty()) {
                assert_eq!(data["code"], -1102);
            }
        }
    }
    for (content_type, body, status) in [
        ("text/plain", "{}".to_owned(), 500),
        ("application/json", " ".repeat(65537), 500),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/fapi/v1/klines/bulk")
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["content-type"], "application/json");
    }
}

#[tokio::test]
async fn cancellations_release_flights_and_dont_poison_followers() {
    let (engine, _) = fixture(1000);
    commit(&engine, 0, 0, false, 1, 100.);
    for _ in 0..300 {
        let e = engine.clone();
        let task = tokio::spawn(async move { e.bulk(query(&["A"])).await });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        assert_eq!(engine.cache_sizes().1, 0);
    }
    let a = engine.clone();
    let leader = tokio::spawn(async move { a.bulk(query(&["A"])).await });
    tokio::task::yield_now().await;
    let b = engine.clone();
    let follower = tokio::spawn(async move { b.bulk(query(&["A"])).await });
    tokio::task::yield_now().await;
    leader.abort();
    let _ = leader.await;
    commit(&engine, 0, 0, true, 2, 109.);
    assert!(follower.await.unwrap().unwrap().finalized);
    assert_eq!(engine.cache_sizes().1, 0);
}

#[tokio::test]
async fn revision_and_boundary_invalidate_and_forming_does_not_regress_final() {
    let (engine, clock) = fixture(0);
    commit(&engine, 0, 0, true, 2, 109.);
    let before = engine.bulk(query(&["A"])).await.unwrap();
    commit(&engine, 0, 0, true, 3, 110.);
    let revised = engine.bulk(query(&["A"])).await.unwrap();
    assert!(!Arc::ptr_eq(&before, &revised));
    assert_eq!(json(&revised)["klines"]["A"][0][4], "110");
    commit(&engine, 0, 0, false, 4, 111.);
    assert_eq!(engine.catalog.slot(0).get(0).unwrap().0.values[3], 110.);
    commit(&engine, 0, H, false, 1, 120.);
    clock.0.store(2 * H + 1, Ordering::Relaxed);
    let next = engine.bulk(query(&["A"])).await.unwrap();
    assert!(!next.finalized);
    assert_eq!(json(&next)["klines"]["A"][0][4], "120");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_racing_registration_never_loses_wakeup() {
    let (engine, clock) = fixture(500);
    for hour in 1..101 {
        clock.0.store(hour * H + 1, Ordering::Relaxed);
        let open = (hour - 1) * H;
        commit(&engine, 0, open, false, 1, 100.);
        let e = engine.clone();
        let request = tokio::spawn(async move { e.bulk(query(&["A"])).await.unwrap() });
        if hour % 2 == 0 {
            tokio::task::yield_now().await;
        }
        commit(&engine, 0, open, true, 2, 109.);
        let reply = tokio::time::timeout(Duration::from_millis(150), request)
            .await
            .unwrap()
            .unwrap();
        assert!(reply.finalized);
    }
}

#[tokio::test]
async fn unrelated_final_does_not_release_requested_series() {
    let (engine, _) = fixture(500);
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 100.);
    let e = engine.clone();
    let task = tokio::spawn(async move { e.bulk(query(&["A"])).await.unwrap() });
    tokio::task::yield_now().await;
    commit(&engine, 1, 0, true, 2, 109.);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
    commit(&engine, 0, 0, true, 2, 109.);
    assert!(task.await.unwrap().finalized);
}

#[test]
fn continuous_pair_metadata_routes_to_actual_symbol_and_rejects_ambiguity() {
    let (engine, _) = fixture(0);
    let raw = br#"{"e":"continuous_kline","ps":"A_PAIR","ct":"PERPETUAL","E":1,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#;
    assert_eq!(engine.ingest(Market::Future, raw).unwrap().unwrap().id, 0);
    assert!(engine.ingest(Market::Spot, raw).unwrap().is_none());
    let instrument = |symbol| Instrument {
        market: Market::Future,
        symbol: String::from(symbol),
        interval: Interval::parse("1h").unwrap(),
        trading: true,
        continuous: Some(("PAIR".into(), "PERPETUAL".into())),
        capacity: NonZeroUsize::new(1).unwrap(),
    };
    assert!(Catalog::new(vec![instrument("A"), instrument("B")]).is_err());
}

#[tokio::test]
async fn http_routes_validate_queries_and_return_real_json() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture(0);
    commit(&engine, 0, 0, true, 1, 109.);
    let app = kline_service::http::router(engine);
    let request = Request::builder()
        .uri("/fapi/v1/klines/bulk?interval=1h&symbols=A&limit=1")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let data: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(data["klines"]["A"][0][4], "109");
    for (uri, status) in [
        ("/fapi/v1/klines/bulk", 500),
        ("/fapi/v1/klines/bulk?interval=bad", 400),
        ("/fapi/v1/fundingRate/bulk", 500),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status()
                .as_u16(),
            status
        );
    }
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/fapi/v1/klines/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"interval":"1h","symbols":[null,"A","A"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn first_publication_changes_normalized_cache_membership() {
    let (engine, _) = fixture(0);
    let empty = engine.bulk(query(&["A"])).await.unwrap();
    assert_eq!(json(&empty)["klines"], serde_json::json!({}));
    commit(&engine, 0, 0, true, 1, 109.);
    let populated = engine.bulk(query(&["A"])).await.unwrap();
    assert_eq!(json(&populated)["klines"]["A"][0][4], "109");
    assert!(!Arc::ptr_eq(&empty, &populated));
}

#[tokio::test]
async fn java_availability_policy_serves_retained_data_while_health_reports_recovery() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    struct Recovering;
    impl kline_service::http::Readiness for Recovering {
        fn is_ready(&self) -> bool {
            false
        }
        fn details(&self) -> serde_json::Value {
            serde_json::json!({"ready":false})
        }
    }
    let (engine, _) = fixture(0);
    commit(&engine, 0, 0, true, 1, 100.);
    for strict in [false, true] {
        let app = kline_service::http::router_with_policy(
            engine.clone(),
            Some(Arc::new(Recovering)),
            strict,
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/fapi/v1/klines/bulk?symbols=A&interval=1h&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), if strict { 503 } else { 200 });
        let health = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status().as_u16(), 503);
    }
}

#[test]
fn stream_gaps_preserve_numeric_policy_and_never_fabricate_finality() {
    for mode in [
        kline_core::NumberType::Double,
        kline_core::NumberType::Float,
        kline_core::NumberType::String,
        kline_core::NumberType::BigDecimal,
    ] {
        let (original, clock) = fixture(0);
        let engine = Engine::new(
            Catalog::new(original.catalog.instruments()).unwrap(),
            clock,
            Settings {
                number_type: mode,
                ..Settings::default()
            },
        );
        let frame = |n: i64| {
            format!(
                r#"{{"e":"kline","s":"A","k":{{"t":{},"T":{},"i":"1h","o":"1.5000","h":"2","l":"1","c":"1.5000","v":"3","q":"4","V":"1","Q":"2","n":1,"x":true}}}}"#,
                n * H,
                (n + 1) * H - 1
            )
        };
        engine.ingest(Market::Future, frame(0).as_bytes()).unwrap();
        engine.ingest(Market::Future, frame(3).as_bytes()).unwrap();
        for n in 1..3 {
            let (bar, closed) = engine.catalog.slot(0).get(n * H).unwrap();
            assert_eq!(bar.number_type, mode);
            assert!(!closed);
            let value = serde_json::to_value(binance_wire::DisplayBar(bar)).unwrap();
            assert_eq!(
                value[1],
                if matches!(
                    mode,
                    kline_core::NumberType::String | kline_core::NumberType::BigDecimal
                ) {
                    "1.5000"
                } else {
                    "1.5"
                }
            );
            assert_eq!(value[5], "0");
        }
        engine.ingest(Market::Future, frame(1).as_bytes()).unwrap();
        assert!(engine.catalog.slot(0).get(H).unwrap().1);
    }
}

#[tokio::test]
async fn payload_reuse_refreshes_metadata_and_ignores_unrelated_revisions() {
    let (engine, clock) = fixture(0);
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 1, 0, true, 1, 200.);
    let first = engine.bulk(query(&["A"])).await.unwrap();
    commit(&engine, 1, 0, true, 2, 201.);
    assert!(Arc::ptr_eq(
        &first,
        &engine.bulk(query(&["A"])).await.unwrap()
    ));
    // The envelope's one-second TTL expires, while the unchanged data survives.
    tokio::time::sleep(Duration::from_millis(1010)).await;
    clock.0.store(H + 2000, Ordering::Relaxed);
    let next = engine.bulk(query(&["A"])).await.unwrap();
    assert_eq!(json(&next)["klines"], json(&first)["klines"]);
    assert_eq!(json(&next)["ts_ms"], H + 2000);
    assert_eq!(engine.metrics.payloads_built.load(Ordering::Relaxed), 1);
    assert_eq!(engine.metrics.payload_reuses.load(Ordering::Relaxed), 1);
    commit(&engine, 0, 0, true, 2, 101.);
    assert_eq!(
        json(&engine.bulk(query(&["A"])).await.unwrap())["klines"]["A"][0][4],
        "101"
    );
}

#[tokio::test]
async fn removed_series_and_backward_clock_cannot_reuse_old_window() {
    let (engine, clock) = fixture(0);
    let instruments = engine.catalog.instruments();
    commit(&engine, 0, 0, true, 1, 100.);
    engine.bulk(query(&["A"])).await.unwrap();
    engine
        .refresh_catalog(
            instruments
                .iter()
                .filter(|i| i.symbol != "A")
                .cloned()
                .collect(),
        )
        .unwrap();
    engine.refresh_catalog(instruments).unwrap();
    commit(&engine, 0, 0, false, 1, 101.);
    let result = engine.bulk(query(&["A"])).await.unwrap();
    assert!(!result.finalized);
    assert_eq!(json(&result)["klines"]["A"][0][4], "101");
    commit(&engine, 0, 0, true, 2, 102.);
    engine.bulk(query(&["A"])).await.unwrap();
    clock.0.store(H - 2, Ordering::Relaxed);
    assert!(json(&engine.bulk(query(&["A"])).await.unwrap())["klines"]["A"].is_null());
}

#[tokio::test]
async fn live_price_ticks_preserve_closed_payload_but_new_rows_invalidate_it() {
    let (engine, _) = fixture(0);
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 0, H, false, 1, 101.);
    let first = engine.bulk(query(&["A"])).await.unwrap();
    for n in 2..100 {
        commit(&engine, 0, H, false, n, n as f64);
    }
    assert!(Arc::ptr_eq(
        &first,
        &engine.bulk(query(&["A"])).await.unwrap()
    ));
}

#[tokio::test]
async fn early_close_time_change_invalidates_a_prepared_window() {
    let (engine, clock) = fixture(0);
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 0, H, false, 1, 101.);
    engine.bulk(query(&["A"])).await.unwrap();
    clock.0.store(H + 2000, Ordering::Relaxed);
    let mut bar = engine.catalog.slot(0).latest().unwrap().0;
    bar.close_time = H + 1000;
    bar.trades = 2;
    engine
        .commit(
            0,
            Update {
                bar,
                closed: false,
                source: Source::Stream,
                event_time: Some(H + 2000),
                sequence: 2,
            },
        )
        .unwrap();
    let result = engine.bulk(query(&["A"])).await.unwrap();
    // The previous boundary bar is final (Java contract); the early-closing
    // current bar must still invalidate the window and remain visibly nonfinal.
    assert!(result.finalized);
    assert_eq!(json(&result)["pending"], serde_json::json!([]));
    assert_eq!(json(&result)["data_status"]["window_finalized"], false);
    assert_eq!(
        json(&result)["data_status"]["nonfinal_symbols"],
        serde_json::json!(["A"])
    );
    assert_eq!(json(&result)["klines"]["A"][0][0], H);
}

#[tokio::test]
async fn calendar_openings_do_not_invent_a_missing_fixed_boundary_final() {
    const DAY: i64 = 86_400_000;
    const JAN: i64 = 1_704_067_200_000;
    const FEB: i64 = 1_706_745_600_000;
    const MAR: i64 = 1_709_251_200_000;
    for (code, starts, boundary) in [
        ("1M", vec![JAN, FEB, MAR], MAR),
        (
            "1w",
            vec![JAN - 14 * DAY, JAN - 7 * DAY, JAN, JAN + 7 * DAY],
            JAN + 7 * DAY,
        ),
        ("3d", vec![JAN, JAN + 3 * DAY, JAN + 6 * DAY], JAN + 6 * DAY),
    ] {
        let engine = Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "A".into(),
                interval: Interval::parse(code).unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(10).unwrap(),
            }])
            .unwrap(),
            Arc::new(TestClock(AtomicI64::new(boundary + 100))),
            Settings {
                final_wait_ms: 0,
                ..Settings::default()
            },
        );
        for pair in starts.windows(2) {
            engine
                .commit(
                    0,
                    Update {
                        bar: Bar {
                            open_time: pair[0],
                            close_time: pair[1] - 1,
                            values: [100.; 8],
                            trades: 1,
                            ..Bar::default()
                        },
                        closed: true,
                        source: Source::Rest,
                        event_time: None,
                        sequence: 0,
                    },
                )
                .unwrap();
        }
        let reply = engine
            .bulk(BulkQuery {
                interval: code.into(),
                limit: Some(10),
                ..query(&["A"])
            })
            .await
            .unwrap();
        assert!(
            reply.finalized,
            "{code}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        assert_eq!(json(&reply)["pending"], serde_json::json!([]));
        assert_eq!(
            json(&reply)["klines"]["A"].as_array().unwrap().len(),
            starts.len() - 1
        );
    }
}

fn admission(limit: usize, queue: usize, wait_ms: u64) -> Settings {
    Settings {
        final_wait_ms: 5_000,
        inflight_limit: limit,
        admission_queue: queue,
        admission_wait_ms: wait_ms,
        ..Settings::default()
    }
}
fn many(n: usize, settings: Settings) -> (Arc<Engine>, Arc<TestClock>) {
    let clock = Arc::new(TestClock(AtomicI64::new(H + 1)));
    let instruments = (0..n)
        .map(|i| Instrument {
            market: Market::Future,
            symbol: format!("S{i}"),
            interval: Interval::parse("1h").unwrap(),
            trading: true,
            continuous: Some((format!("S{i}_PAIR"), "PERPETUAL".into())),
            capacity: NonZeroUsize::new(10).unwrap(),
        })
        .collect();
    (
        Engine::new(Catalog::new(instruments).unwrap(), clock.clone(), settings),
        clock,
    )
}
async fn until(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(
            std::time::Instant::now() < deadline,
            "condition not reached"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
/// Yields without letting paused time move (builds run on worker threads) until `done`.
async fn settle(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(
            std::time::Instant::now() < deadline,
            "condition not reached"
        );
        tokio::task::yield_now().await;
    }
}
/// Gives `task` 200 ms of real time to finish (paused time does not move) and checks it did not.
async fn still_waiting<T>(task: &tokio::task::JoinHandle<T>) {
    let until = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < until {
        assert!(!task.is_finished(), "finished before its deadline");
        tokio::task::yield_now().await;
    }
}
fn spawn(
    e: &Arc<Engine>,
    symbols: &[&str],
) -> tokio::task::JoinHandle<Result<Arc<kline_service::BulkReply>, kline_service::ServiceError>> {
    let e = e.clone();
    let q = query(symbols);
    tokio::spawn(async move { e.bulk(q).await })
}
fn counters(e: &Engine) -> (u64, u64) {
    (
        e.metrics.admission_queued.load(Ordering::Relaxed),
        e.metrics.admission_rejected.load(Ordering::Relaxed),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_settings_serve_a_boundary_burst_beyond_256_keys() {
    // 2026-09-30: 392 distinct keys at the hour lost 136 to the old fixed 256-key cap.
    let (engine, _) = many(
        300,
        Settings {
            final_wait_ms: 5_000,
            ..Settings::default()
        },
    );
    for id in 0..300 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let names: Vec<String> = (0..300).map(|i| format!("S{i}")).collect();
    let requests: Vec<_> = names
        .iter()
        .map(|n| spawn(&engine, &[n.as_str()]))
        .collect();
    until(|| engine.cache_sizes().1 == 256 && engine.admission_waiting() == 44).await;
    for id in 0..300 {
        commit(&engine, id, 0, true, 2, 101.);
    }
    for request in requests {
        assert!(request.await.unwrap().unwrap().finalized);
    }
    assert_eq!(counters(&engine), (44, 0));
    until(|| engine.cache_sizes().1 == 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_key_waits_for_a_slot_then_for_its_own_final() {
    let (engine, _) = fixture_with(admission(1, 8, 5_000));
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    let second = spawn(&engine, &["B"]);
    until(|| engine.admission_waiting() == 1).await;
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(first.await.unwrap().unwrap().finalized);
    // B now holds the slot and waits for its own final, which has not arrived yet.
    until(|| engine.admission_waiting() == 0 && engine.cache_sizes().1 == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!second.is_finished());
    commit(&engine, 1, 0, true, 2, 201.);
    let b = second.await.unwrap().unwrap();
    assert!(b.finalized);
    assert_eq!(json(&b)["klines"]["B"][0][4], "201");
    assert_eq!(counters(&engine), (1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_queue_answers_busy_at_once_without_counting_as_queued() {
    let (engine, _) = fixture_with(admission(1, 1, 5_000));
    for id in 0..3 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    let second = spawn(&engine, &["B"]);
    until(|| engine.admission_waiting() == 1).await;
    // Busy on the first poll: the request never waits.
    let busy = engine.bulk(query(&["C"]));
    tokio::pin!(busy);
    assert!(matches!(
        futures_util::poll!(&mut busy),
        std::task::Poll::Ready(Err(kline_service::ServiceError::Busy))
    ));
    assert_eq!(counters(&engine), (1, 1));
    commit(&engine, 0, 0, true, 2, 101.);
    commit(&engine, 1, 0, true, 2, 201.);
    assert!(first.await.unwrap().unwrap().finalized);
    assert!(second.await.unwrap().unwrap().finalized);
    assert_eq!(engine.admission_waiting(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_admission_wait_answers_busy() {
    let (engine, _) = fixture_with(admission(1, 8, 50));
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    let started = std::time::Instant::now();
    assert!(matches!(
        engine.bulk(query(&["B"])).await,
        Err(kline_service::ServiceError::Busy)
    ));
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(counters(&engine), (1, 1));
    assert_eq!(engine.admission_waiting(), 0);
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(first.await.unwrap().unwrap().finalized);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn followers_of_an_inflight_key_need_no_slot() {
    let (engine, _) = fixture_with(admission(1, 0, 0));
    commit(&engine, 0, 0, false, 1, 100.);
    let leader = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    // Followers register on the leader's flight while the final is missing (so none can be a
    // cache hit); with no queue at all, needing a slot would make them Busy at once.
    let followers: Vec<_> = (0..4).map(|_| spawn(&engine, &["A"])).collect();
    until(|| engine.inflight_holders() == 5).await;
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(leader.await.unwrap().unwrap().finalized);
    for follower in followers {
        assert!(follower.await.unwrap().unwrap().finalized);
    }
    assert_eq!(counters(&engine), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_queued_request_leaves_the_queue() {
    let (engine, _) = fixture_with(admission(1, 1, 5_000));
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    let queued = spawn(&engine, &["B"]);
    until(|| engine.admission_waiting() == 1).await;
    queued.abort();
    until(|| engine.admission_waiting() == 0).await;
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(first.await.unwrap().unwrap().finalized);
    commit(&engine, 1, 0, true, 2, 201.);
    assert!(engine.bulk(query(&["B"])).await.unwrap().finalized);
    until(|| engine.cache_sizes().1 == 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_completion_and_cancellation_never_strand_a_slot() {
    let (engine, clock) = fixture_with(admission(3, 64, 5_000));
    for hour in 1..=40i64 {
        let open = (hour - 1) * H;
        clock.0.store(hour * H + 1, Ordering::Relaxed);
        for id in 0..3 {
            commit(&engine, id, open, false, 1, 100.);
        }
        let mut tasks = Vec::new();
        for symbol in ["A", "B", "C"] {
            for _ in 0..16 {
                tasks.push(spawn(&engine, &[symbol]));
            }
        }
        until(|| engine.cache_sizes().1 == 3).await;
        let mut aborted = std::collections::HashSet::new();
        for (i, task) in tasks.iter().enumerate() {
            if (i as i64 + hour) % 3 == 0 {
                task.abort();
                aborted.insert(i);
            }
        }
        for id in 0..3 {
            commit(&engine, id, open, true, 2, 101.);
        }
        for (i, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(result) => assert!(result.unwrap().finalized),
                // Only tasks this test aborted may fail, and only by cancellation (no panics).
                Err(error) => assert!(aborted.contains(&i) && error.is_cancelled(), "{error}"),
            }
        }
        until(|| engine.cache_sizes().1 == 0 && engine.inflight_holders() == 0).await;
    }
    assert_eq!(counters(&engine).1, 0);
}

#[tokio::test(start_paused = true)]
async fn a_follower_taking_over_a_cancelled_leader_keeps_the_boundary_deadline() {
    let (engine, clock) = fixture_with(Settings {
        final_wait_ms: 300,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    let leader = spawn(&engine, &["A"]);
    until(|| engine.inflight_holders() == 1).await;
    let follower = spawn(&engine, &["A"]);
    until(|| engine.inflight_holders() == 2).await;
    // 250 ms of the 300 ms final wait have passed when the leader is cancelled; the follower
    // registered at the start, so restarting from its arrival would wait another ~299 ms.
    tokio::time::advance(Duration::from_millis(250)).await;
    clock.0.store(H + 251, Ordering::Relaxed);
    leader.abort();
    let took_over = tokio::time::Instant::now();
    let reply = follower.await.unwrap().unwrap();
    assert!(!reply.finalized);
    let waited = took_over.elapsed();
    assert!(
        waited <= Duration::from_millis(60),
        "the follower restarted the final wait: {waited:?}"
    );
}

#[tokio::test]
async fn http_answers_503_1008_when_no_slot_can_be_had() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture_with(admission(1, 0, 0));
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    let response = kline_service::http::router(engine.clone())
        .oneshot(
            Request::builder()
                .uri("/fapi/v1/klines/bulk?interval=1h&symbols=B&limit=1&closed_only=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["code"], -1008);
    assert_eq!(counters(&engine), (0, 1));
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(first.await.unwrap().unwrap().finalized);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_http_route_group_queues_requests_instead_of_rejecting() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let get = |symbol: &str| {
        Request::builder()
            .uri(format!(
                "/fapi/v1/klines/bulk?interval=1h&symbols={symbol}&limit=1&closed_only=true"
            ))
            .body(Body::empty())
            .unwrap()
    };
    let (engine, _) = fixture_with(Settings {
        final_wait_ms: 5_000,
        http_concurrency_limit: 1,
        http_admission_queue: 8,
        http_admission_wait_ms: 5_000,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let app = kline_service::http::router(engine.clone());
    let first = tokio::spawn(app.clone().oneshot(get("A")));
    until(|| engine.cache_sizes().1 == 1).await;
    let second = tokio::spawn(app.clone().oneshot(get("B")));
    until(|| {
        engine
            .metrics
            .http_admission_waiting
            .load(Ordering::Relaxed)
            == 1
    })
    .await;
    commit(&engine, 1, 0, true, 2, 201.);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !second.is_finished(),
        "B must wait for A's place in the route group"
    );
    commit(&engine, 0, 0, true, 2, 101.);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
    let response = second.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let data: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(data["klines"]["B"][0][4], "201");
    assert_eq!(
        engine.metrics.http_admission_queued.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        engine
            .metrics
            .http_admission_rejected
            .load(Ordering::Relaxed),
        0
    );

    let (engine, _) = fixture_with(Settings {
        final_wait_ms: 5_000,
        http_concurrency_limit: 1,
        http_admission_queue: 0,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    let app = kline_service::http::router(engine.clone());
    let first = tokio::spawn(app.clone().oneshot(get("A")));
    until(|| engine.cache_sizes().1 == 1).await;
    let response = app.clone().oneshot(get("B")).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["code"], -1008);
    assert_eq!(body["msg"], "Request capacity exceeded");
    commit(&engine, 0, 0, true, 2, 101.);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_settings_serve_a_burst_beyond_both_http_and_bulk_limits() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    // 600 concurrent closed_only bulk requests for distinct keys: 512 enter the route group and
    // 256 of them hold bulk in-flight slots until the finals arrive; everything else queues.
    let (engine, _) = many(
        600,
        Settings {
            final_wait_ms: 5_000,
            ..Settings::default()
        },
    );
    for id in 0..600 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let app = kline_service::http::router(engine.clone());
    let requests: Vec<_> = (0..600)
        .map(|i| {
            tokio::spawn(
                app.clone().oneshot(
                    Request::builder()
                        .uri(format!(
                            "/fapi/v1/klines/bulk?interval=1h&symbols=S{i}&limit=1&closed_only=true"
                        ))
                        .body(Body::empty())
                        .unwrap(),
                ),
            )
        })
        .collect();
    until(|| {
        engine
            .metrics
            .http_admission_waiting
            .load(Ordering::Relaxed)
            == 88
            && engine.cache_sizes().1 == 256
            && engine.admission_waiting() == 256
    })
    .await;
    for id in 0..600 {
        commit(&engine, id, 0, true, 2, 101.);
    }
    for request in requests {
        assert_eq!(request.await.unwrap().unwrap().status(), 200);
    }
    assert_eq!(
        engine.metrics.http_admission_queued.load(Ordering::Relaxed),
        88
    );
    assert_eq!(
        engine
            .metrics
            .http_admission_rejected
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(counters(&engine).1, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_routes_bypass_a_full_route_group() {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture_with(Settings {
        final_wait_ms: 5_000,
        http_concurrency_limit: 1,
        http_admission_queue: 0,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    let app = kline_service::http::router(engine.clone());
    let get = |uri: &str| Request::builder().uri(uri).body(Body::empty()).unwrap();
    let busy = tokio::spawn(app.clone().oneshot(get(
        "/fapi/v1/klines/bulk?interval=1h&symbols=A&limit=1&closed_only=true",
    )));
    until(|| engine.cache_sizes().1 == 1).await;
    assert_eq!(
        app.clone()
            .oneshot(get("/fapi/v1/klines/bulk?interval=1h&symbols=B&limit=1"))
            .await
            .unwrap()
            .status(),
        503
    );
    let rejected = || {
        engine
            .metrics
            .http_admission_rejected
            .load(Ordering::Relaxed)
    };
    assert_eq!(rejected(), 1);
    for uri in ["/health/live", "/actuator/health", "/metrics"] {
        // Health may legitimately answer 503 (low disk space, for example); only a refusal by
        // admission is wrong, and with no queue that would count as a rejection.
        let response = app.clone().oneshot(get(uri)).await.unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            !String::from_utf8_lossy(&body).contains("Request capacity exceeded"),
            "{uri} was refused by admission"
        );
    }
    assert_eq!(rejected(), 1, "management routes never meet admission");
    commit(&engine, 0, 0, true, 2, 101.);
    assert_eq!(busy.await.unwrap().unwrap().status(), 200);
}

fn bulk_get(symbol: &str) -> axum::http::Request<axum::body::Body> {
    axum::http::Request::builder()
        .uri(format!(
            "/fapi/v1/klines/bulk?interval=1h&symbols={symbol}&limit=1&closed_only=true"
        ))
        .body(axum::body::Body::empty())
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_bulk_request_never_queues_for_a_route_place_past_its_bulk_budget() {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture_with(Settings {
        final_wait_ms: 5_000,
        admission_wait_ms: 100,
        http_concurrency_limit: 1,
        http_admission_queue: 8,
        http_admission_wait_ms: 8_000,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let app = kline_service::http::router(engine.clone());
    let first = tokio::spawn(app.clone().oneshot(bulk_get("A")));
    settle(|| engine.cache_sizes().1 == 1).await;
    // B's wait for an in-flight slot would share its 100 ms budget with this queue, so the queue
    // ends there rather than after the route group's eight seconds.
    let started = tokio::time::Instant::now();
    let response = app.clone().oneshot(bulk_get("B")).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_millis(100));
    assert_eq!(response.status(), 503);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["msg"], "Request capacity exceeded");
    let m = &engine.metrics;
    assert_eq!(m.http_admission_queued.load(Ordering::Relaxed), 1);
    assert_eq!(m.http_admission_rejected.load(Ordering::Relaxed), 1);
    commit(&engine, 0, 0, true, 2, 101.);
    assert_eq!(first.await.unwrap().unwrap().status(), 200);
}

#[tokio::test(start_paused = true)]
async fn a_key_admitted_after_queueing_only_waits_out_the_rest_of_the_final_wait() {
    let (engine, clock) = fixture_with(Settings {
        final_wait_ms: 300,
        inflight_limit: 1,
        admission_queue: 8,
        admission_wait_ms: 5_000,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.);
    commit(&engine, 1, 0, false, 1, 200.);
    let first = spawn(&engine, &["A"]);
    settle(|| engine.cache_sizes().1 == 1).await;
    let second = spawn(&engine, &["B"]);
    settle(|| engine.admission_waiting() == 1).await;
    // 250 ms into the 300 ms final wait, A's final frees the slot; B's own final never comes.
    tokio::time::advance(Duration::from_millis(250)).await;
    clock.0.store(H + 251, Ordering::Relaxed);
    commit(&engine, 0, 0, true, 2, 101.);
    settle(|| first.is_finished() && engine.admission_waiting() == 0).await;
    assert!(first.await.unwrap().unwrap().finalized);
    // B, admitted at 250 ms, waits out the remaining 49 ms of the final wait: no more, no less.
    tokio::time::advance(Duration::from_millis(48)).await;
    still_waiting(&second).await;
    tokio::time::advance(Duration::from_millis(2)).await;
    settle(|| second.is_finished()).await;
    let reply = second.await.unwrap().unwrap();
    assert!(!reply.finalized);
    // Also measured inside the final wait, so a slow build worker cannot hide a skipped wait.
    assert!((49..=50).contains(&reply.waited_ms), "{}", reply.waited_ms);
}

#[tokio::test(start_paused = true)]
async fn a_stale_retry_keeps_the_admission_deadline_it_started_with() {
    let (engine, clock) = fixture_with(admission(1, 8, 300));
    for id in 0..3 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let started = tokio::time::Instant::now();
    let first = spawn(&engine, &["A"]);
    settle(|| engine.cache_sizes().1 == 1).await;
    let retried = spawn(&engine, &["B"]);
    settle(|| engine.admission_waiting() == 1).await;
    // At 200 ms A's final frees the slot for B, which then waits for its own final.
    tokio::time::advance(Duration::from_millis(200)).await;
    clock.0.store(H + 201, Ordering::Relaxed);
    commit(&engine, 0, 0, true, 2, 101.);
    settle(|| first.is_finished() && engine.admission_waiting() == 0).await;
    assert!(first.await.unwrap().unwrap().finalized);
    // C queues for the slot B holds; its own deadline is at 500 ms.
    let queued = spawn(&engine, &["C"]);
    settle(|| engine.admission_waiting() == 1).await;
    // At 350 ms, past B's 300 ms deadline, the next hour has begun when B's final arrives: B's
    // reply is stale at once, and its retry (a new key) finds the slot handed on to C.
    tokio::time::advance(Duration::from_millis(150)).await;
    clock.0.store(2 * H + 1, Ordering::Relaxed);
    commit(&engine, 1, 0, true, 2, 201.);
    settle(|| retried.is_finished()).await;
    assert!(matches!(
        retried.await.unwrap(),
        Err(kline_service::ServiceError::Busy)
    ));
    assert_eq!(started.elapsed(), Duration::from_millis(350));
    assert_eq!(counters(&engine), (2, 1), "the retry must not queue again");
    queued.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_granted_for_a_key_built_meanwhile_goes_back_at_once() {
    let (engine, _) = fixture_with(admission(1, 8, 5_000));
    for id in 0..3 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let first = spawn(&engine, &["A"]);
    until(|| engine.cache_sizes().1 == 1).await;
    // B is not in flight, so two requests for it queue separately instead of joining.
    let b1 = spawn(&engine, &["B"]);
    until(|| engine.admission_waiting() == 1).await;
    let b2 = spawn(&engine, &["B"]);
    until(|| engine.admission_waiting() == 2).await;
    commit(&engine, 1, 0, true, 2, 201.);
    commit(&engine, 0, 0, true, 2, 101.);
    assert!(first.await.unwrap().unwrap().finalized);
    assert!(b1.await.unwrap().unwrap().finalized);
    // b2's slot comes free only once b1's reply is cached, so b2 is normally a cache hit (shown
    // deterministically in cache.rs); it rebuilds instead if descheduled past the 1 s cache TTL.
    assert!(b2.await.unwrap().unwrap().finalized);
    until(|| engine.cache_sizes().1 == 0).await;
    // b2's unused slot went back: a new key registers without queueing (a leaked slot would
    // queue it, and five seconds later answer Busy).
    commit(&engine, 2, 0, true, 2, 301.);
    assert!(engine.bulk(query(&["C"])).await.unwrap().finalized);
    assert_eq!(counters(&engine), (2, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_counters_and_gauges_are_exported() {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let (engine, _) = fixture_with(Settings {
        final_wait_ms: 5_000,
        inflight_limit: 1,
        admission_queue: 8,
        admission_wait_ms: 5_000,
        http_concurrency_limit: 2,
        http_admission_queue: 1,
        http_admission_wait_ms: 5_000,
        ..Settings::default()
    });
    for id in 0..3 {
        commit(&engine, id, 0, false, 1, 100.);
    }
    let app = kline_service::http::router(engine.clone());
    let metrics = || async {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap()
    };
    let has = |text: &str, line: &str| assert!(text.lines().any(|l| l == line), "{line}\n{text}");
    // A holds the in-flight slot and B waits for it; both route places are taken, so C queues
    // for one, and the next request finds that one-place queue full.
    let a = tokio::spawn(app.clone().oneshot(bulk_get("A")));
    until(|| engine.cache_sizes().1 == 1).await;
    let b = tokio::spawn(app.clone().oneshot(bulk_get("B")));
    until(|| engine.admission_waiting() == 1).await;
    let c = tokio::spawn(app.clone().oneshot(bulk_get("C")));
    until(|| {
        engine
            .metrics
            .http_admission_waiting
            .load(Ordering::Relaxed)
            == 1
    })
    .await;
    assert_eq!(
        app.clone().oneshot(bulk_get("A")).await.unwrap().status(),
        503
    );
    let text = metrics().await;
    for line in [
        "kline_bulk_admission_queued_total 1",
        "kline_bulk_admission_rejected_total 0",
        "kline_bulk_admission_waiting 1",
        "kline_http_admission_queued_total 1",
        "kline_http_admission_rejected_total 1",
        "kline_http_admission_waiting 1",
    ] {
        has(&text, line);
    }
    for id in 0..3 {
        commit(&engine, id, 0, true, 2, 101.);
    }
    for request in [a, b, c] {
        assert_eq!(request.await.unwrap().unwrap().status(), 200);
    }
    let text = metrics().await;
    has(&text, "kline_bulk_admission_waiting 0");
    has(&text, "kline_http_admission_waiting 0");
}

/// Hour [0, H) final, hour [H, 2H) still forming, clock `before` ms short of 2H.
fn just_before_boundary(before: i64) -> (Arc<Engine>, Arc<TestClock>) {
    let (engine, clock) = fixture(5_000);
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 0, H, false, 2, 200.);
    clock.0.store(2 * H - before, Ordering::Relaxed);
    (engine, clock)
}

#[tokio::test(start_paused = true)]
async fn a_closed_only_request_just_before_the_boundary_waits_for_it() {
    let (engine, _) = just_before_boundary(100);
    let started = tokio::time::Instant::now();
    let request = spawn(&engine, &["A"]);
    // The request sleeps until the boundary. The manual clock is left 100 ms short of it, as a
    // clock running a moment behind would be: the request must still count as after it.
    for _ in 0..10 {
        tokio::task::yield_now().await; // let it reach the sleep
    }
    assert_eq!(
        engine.cache_sizes().1,
        0,
        "nothing registered before the boundary"
    );
    tokio::time::advance(Duration::from_millis(100)).await;
    settle(|| engine.cache_sizes().1 == 1).await;
    // It waits for the bar that closes at the boundary (given real time to finish otherwise).
    still_waiting(&request).await;
    commit(&engine, 0, H, true, 3, 201.);
    settle(|| request.is_finished()).await;
    let reply = request.await.unwrap().unwrap();
    assert!(reply.finalized);
    let bar = &json(&reply)["klines"]["A"][0];
    assert_eq!(
        bar[0], H,
        "the bar that closed at the boundary, not the hour before"
    );
    assert_eq!(bar[4], "201");
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(engine.metrics.pre_boundary_waits.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn a_request_further_from_the_boundary_does_not_wait() {
    let (engine, _) = just_before_boundary(400);
    let started = tokio::time::Instant::now();
    let reply = engine.bulk(query(&["A"])).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        json(&reply)["klines"]["A"][0][0],
        0,
        "the last bar closed before the request"
    );
    assert_eq!(engine.metrics.pre_boundary_waits.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn a_forming_request_just_before_the_boundary_does_not_wait() {
    let (engine, _) = just_before_boundary(100);
    let started = tokio::time::Instant::now();
    let mut forming = query(&["A"]);
    forming.closed_only = false;
    engine.bulk(forming).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(engine.metrics.pre_boundary_waits.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn the_pre_boundary_wait_can_be_turned_off() {
    let (engine, clock) = fixture_with(Settings {
        final_wait_ms: 5_000,
        pre_boundary_wait_ms: 0,
        ..Settings::default()
    });
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 0, H, false, 2, 200.);
    clock.0.store(2 * H - 100, Ordering::Relaxed);
    let started = tokio::time::Instant::now();
    let reply = engine.bulk(query(&["A"])).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(json(&reply)["klines"]["A"][0][0], 0);
}

#[tokio::test(start_paused = true)]
async fn a_queued_request_keeps_its_boundary_when_the_clock_steps_back() {
    let (engine, clock) = fixture_with(admission(1, 8, 5_000));
    // A: hours [0, H) and [H, 2H) final. B: [H, 2H) not final yet.
    commit(&engine, 0, 0, true, 1, 100.);
    commit(&engine, 0, H, true, 2, 201.);
    commit(&engine, 1, H, false, 1, 300.);
    clock.0.store(2 * H + 5, Ordering::Relaxed);
    let blocker = spawn(&engine, &["B"]);
    settle(|| engine.cache_sizes().1 == 1).await;
    // A selects the boundary 2H, then waits for B's slot.
    let request = spawn(&engine, &["A"]);
    settle(|| engine.admission_waiting() == 1).await;
    // While it waits, the clock is stepped back across the boundary.
    clock.0.store(2 * H - 10, Ordering::Relaxed);
    commit(&engine, 1, H, true, 2, 301.);
    settle(|| request.is_finished()).await;
    let reply = request.await.unwrap().unwrap();
    assert_eq!(
        json(&reply)["klines"]["A"][0][0],
        H,
        "the bar that closed at 2H, not the hour before"
    );
    assert!(blocker.await.unwrap().unwrap().finalized);
}

#[tokio::test(start_paused = true)]
async fn a_takeover_after_the_clock_steps_back_keeps_the_flights_deadline() {
    let (engine, clock) = fixture(300);
    commit(&engine, 0, 0, false, 1, 100.); // A's final never comes
    let leader = spawn(&engine, &["A"]);
    settle(|| engine.inflight_holders() == 1).await;
    // 290 ms into the 300 ms final wait the clock is stepped back 290 ms (still after the
    // boundary, so the same key); a follower joins, by its own reading with 299 ms to wait, and
    // the leader is cancelled. The follower takes over and waits out the flight's ~10 ms.
    tokio::time::advance(Duration::from_millis(290)).await;
    clock.0.store(H + 1, Ordering::Relaxed);
    let follower = spawn(&engine, &["A"]);
    settle(|| engine.inflight_holders() == 2).await;
    leader.abort();
    let took_over = tokio::time::Instant::now();
    let reply = follower.await.unwrap().unwrap();
    assert!(!reply.finalized);
    let waited = took_over.elapsed();
    assert!(waited <= Duration::from_millis(20), "{waited:?}");
}

#[test]
fn bulk_requests_count_with_the_hour_they_were_answered_for() {
    let metrics = kline_service::Metrics::default();
    let bulk = "/fapi/v1/klines/bulk";
    let record = |elapsed_ms: u64, finished: i64, answered_for: Option<i64>| {
        metrics.http.record_at(
            bulk,
            Duration::from_millis(elapsed_ms),
            200,
            finished,
            answered_for,
        )
    };
    // Waited for 2H after arriving 100 ms early; and queued from 400 ms early, then waited.
    record(150, 2 * H + 50, Some(2 * H));
    record(450, 2 * H + 50, Some(2 * H));
    // A forming request 100 ms before 2H is answered for the hour before and stays there.
    record(10, 2 * H - 90, Some(H));
    // Without an answered-for period (not a bulk answer), arrival decides.
    record(10, 2 * H - 90, None);
    let hours = metrics.http.hourly();
    let at = |boundary: i64| {
        hours
            .iter()
            .find(|h| h.market == "future" && h.boundary == boundary)
            .unwrap()
    };
    let new_hour = at(2 * H);
    assert_eq!((new_hour.requests, new_hour.boundary_requests), (2, 2));
    assert_eq!(new_hour.max_ms, 450.0, "with their real latency");
    assert_eq!(at(H).requests, 2);
    assert_eq!(at(H).boundary_requests, 0, "they arrived late in that hour");
}

#[tokio::test(start_paused = true)]
async fn http_statistics_count_a_request_that_waited_with_the_new_hour() {
    use tower::ServiceExt;
    let (engine, _) = just_before_boundary(100);
    let app = kline_service::http::router(engine.clone());
    let request = tokio::spawn(app.oneshot(bulk_get("A")));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_millis(100)).await;
    settle(|| engine.cache_sizes().1 == 1).await;
    commit(&engine, 0, H, true, 3, 201.);
    settle(|| request.is_finished()).await;
    assert_eq!(request.await.unwrap().unwrap().status(), 200);
    // The application clock never reached 2H here, yet the request counts with 2H.
    let hours = engine.metrics.http.hourly();
    let new_hour = hours
        .iter()
        .find(|h| h.boundary == 2 * H)
        .expect("counted with 2H");
    assert_eq!((new_hour.requests, new_hour.boundary_requests), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn a_queued_request_keeps_its_final_deadline_when_the_clock_steps_back() {
    let (engine, clock) = fixture_with(Settings {
        final_wait_ms: 300,
        inflight_limit: 1,
        admission_queue: 8,
        admission_wait_ms: 5_000,
        ..Settings::default()
    });
    commit(&engine, 0, 0, false, 1, 100.); // A's final never comes
    commit(&engine, 1, 0, false, 1, 200.);
    let blocker = spawn(&engine, &["B"]);
    settle(|| engine.cache_sizes().1 == 1).await;
    // A selects H at H+1 and queues behind B.
    let request = spawn(&engine, &["A"]);
    settle(|| engine.admission_waiting() == 1).await;
    // 290 ms later the clock is stepped back to H+1 and B's final frees the slot: A starts its
    // flight with the ~10 ms left of its budget, not a fresh 299 ms.
    tokio::time::advance(Duration::from_millis(290)).await;
    clock.0.store(H + 1, Ordering::Relaxed);
    commit(&engine, 1, 0, true, 2, 201.);
    settle(|| blocker.is_finished() && engine.admission_waiting() == 0).await;
    let admitted = tokio::time::Instant::now();
    let reply = request.await.unwrap().unwrap();
    assert!(!reply.finalized);
    let waited = admitted.elapsed();
    assert!(waited <= Duration::from_millis(20), "{waited:?}");
}
