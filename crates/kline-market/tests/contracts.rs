use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use kline_core::{Bar, Interval, Market, NumberType, Source, Update};
use kline_market::{
    config::Config,
    funding::{self, H, Query as FundingQuery},
    http::{self, App},
    transport::RestApi,
};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
use tower::ServiceExt;

#[derive(Default)]
struct Mock {
    calls: Mutex<Vec<(String, BTreeMap<String, String>)>>,
    hold_funding: AtomicBool,
    large_metadata: AtomicBool,
    funding_started: Notify,
    release_funding: Notify,
    ticker_price: Mutex<Option<String>>,
    all_prices: Mutex<Option<Vec<Value>>>,
    hold_price: AtomicBool,
    price_started: Notify,
    release_price: Notify,
    hold_metadata: AtomicBool,
    metadata_started: Notify,
    release_metadata: Notify,
    archive: Mutex<Vec<u8>>,
    missing_archive: AtomicBool,
    failed_archive: AtomicBool,
}
async fn upstream(
    State(mock): State<Arc<Mock>>,
    uri: Uri,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    let path = uri.path();
    mock.calls.lock().await.push((path.into(), q.clone()));
    if path.ends_with(".zip") {
        if path.contains("ETHUSDT") && mock.missing_archive.load(Ordering::Relaxed) {
            return StatusCode::NOT_FOUND.into_response();
        }
        if path.contains("ETHUSDT") && mock.failed_archive.load(Ordering::Relaxed) {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        return mock.archive.lock().await.clone().into_response();
    }
    let symbol = q.get("symbol").map(String::as_str).unwrap_or("BTCUSDT");
    let row = json!({"symbol":symbol,"priceChange":"1.2300","lastPrice":"100.50000000","volume":"500.00","quoteVolume":"50250.0000","openPrice":"99.0000","highPrice":"101.00","lowPrice":"98.00","openTime":1,"closeTime":2,"count":12,"unknown":"discard"});
    let value = if path.ends_with("exchangeInfo") {
        if mock.hold_metadata.load(Ordering::Acquire) {
            mock.metadata_started.notify_one();
            mock.release_metadata.notified().await;
        }
        let unknown = if mock.large_metadata.load(Ordering::Relaxed) {
            "x".repeat(17 * 1024 * 1024)
        } else {
            "discard".into()
        };
        json!({"timezone":"UTC","serverTime":1,"unknown":unknown,"rateLimits":[],"exchangeFilters":[],"symbols":[
            {"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"1.0E-8","unknown":1}],"unknown":"discard"},
            {"symbol":"ETHUSDT","pair":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},
            {"symbol":"HALTUSDT","status":"HALT","quoteAsset":"USDT"}
        ]})
    } else if path.ends_with("fundingRate") {
        if mock.hold_funding.load(Ordering::Acquire) {
            mock.funding_started.notify_one();
            mock.release_funding.notified().await;
        }
        let start = q
            .get("startTime")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(H);
        if symbol == "PAGEUSDT" {
            Value::Array(
                (0..1000)
                    .map(|i| json!({"symbol":symbol,"fundingTime":start+i*H,"fundingRate":"0.01"}))
                    .collect(),
            )
        } else {
            json!([
                {"symbol":symbol,"fundingTime":start+1,"fundingRate":"1.00E-4","markPrice":"100.5000"},
                {"symbol":symbol,"fundingTime":start+2,"fundingRate":"2E-4","markPrice":"101.0"}
            ])
        }
    } else if path.ends_with("ticker/24hr") {
        if q.contains_key("symbol") {
            row
        } else {
            json!([row])
        }
    } else if matches!(path, "/fapi/v2/ticker/price" | "/api/v3/ticker/price") {
        if mock.hold_price.load(Ordering::Acquire) {
            mock.price_started.notify_one();
            mock.release_price.notified().await;
        }
        let price = mock
            .ticker_price
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| "100.50000000".into());
        let row = json!({"symbol":symbol,"price":price,"time":123});
        if q.contains_key("symbol") {
            row
        } else {
            Value::Array(
                mock.all_prices
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| vec![row]),
            )
        }
    } else if path.ends_with("premiumIndex") {
        let row = json!({"symbol":symbol,"markPrice":"1.2300E2","lastFundingRate":"1.00E-4","time":99,"unknown":true});
        if q.contains_key("symbol") {
            row
        } else {
            json!([row])
        }
    } else if path.ends_with("klines") {
        json!([[
            0,
            "1.00000000",
            "2",
            "0.5",
            "1.5",
            "3",
            H - 1,
            "4",
            5,
            "6",
            "7",
            "0"
        ]])
    } else if path.ends_with("catalog/list/query") {
        json!({"code":"000000","success":true,"message":null,"unknown":true,"data":{"total":1,"articles":[{"id":1,"title":"fixture","body":"text","unknown":0}]}})
    } else if path.ends_with("article/list/query") {
        json!({"code":"000000","success":true,"data":{"catalogs":[{"catalogId":48,"catalogName":"news","articles":[{"id":1,"title":"fixture","unknown":0}],"unknown":0}]}})
    } else if path == "/alt" {
        return "<script>chartdata[30] = {\"labels\":{\"all\":[\"2020-01-01\",\"2026-01-01\"]},\"values\":{\"all\":[10,75]}}; </script>".into_response();
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    axum::Json(value).into_response()
}
struct Fixture {
    app: Arc<App>,
    mock: Arc<Mock>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn fixture() -> Fixture {
    fixture_with_history(true).await
}
async fn fixture_with_history(populate: bool) -> Fixture {
    fixture_with_history_and_type(populate, NumberType::Double).await
}
async fn fixture_with_history_and_type(populate: bool, number_type: NumberType) -> Fixture {
    let mock = Arc::new(Mock::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(upstream).with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut definitions = vec![];
    for market in [Market::Future, Market::Spot] {
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            for interval in ["1h", "1d"] {
                definitions.push(Instrument {
                    market,
                    symbol: symbol.into(),
                    interval: Interval::parse(interval).unwrap(),
                    trading: true,
                    continuous: None,
                    capacity: NonZeroUsize::new(100).unwrap(),
                });
            }
        }
    }
    let engine = Engine::new(
        Catalog::new(definitions).unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings {
            final_wait_ms: 0,
            number_type,
            ..Settings::default()
        },
    );
    let now = funding::now();
    for (id, slot) in engine.catalog.slots().iter().enumerate() {
        if !populate {
            continue;
        }
        let p = slot.interval.millis();
        let base = now.div_euclid(p) * p;
        for i in 0..70 {
            let open = base - (70 - i) * p;
            let price = 100. + i as f64;
            engine
                .commit(
                    id,
                    Update {
                        bar: Bar {
                            open_time: open,
                            close_time: open + p - 1,
                            trades: 10,
                            values: [price, price + 1., price - 1., price, 5., 500., 1., 100.],

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
    }
    let api = RestApi::new_with_type(&root, &root, 6000, number_type).unwrap();
    let mut config = Config {
        cms_url: root.clone(),
        ..Config::default()
    };
    config.statistics.altcoin_url = format!("{root}/alt");
    config.funding.vision_url = root;
    config.funding.vision_days = 1;
    config.statistics.timezone_offset_minutes = Some(0);
    Fixture {
        app: App::new(engine, api, config),
        mock,
        server,
    }
}
async fn request(app: &Arc<App>, method: &str, path: &str, body: &str) -> (StatusCode, Vec<u8>) {
    let router = kline_service::http::router(app.engine.clone()).merge(http::router(app.clone()));
    let response = router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    (
        response.status(),
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
}
async fn get(app: &Arc<App>, path: &str) -> Value {
    let (status, body) = request(app, "GET", path, "").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{path}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn all_java_http_operations_are_wired_with_expected_shapes() {
    let f = fixture().await;
    let app = &f.app;
    for prefix in ["/fapi/v1", "/api/v3"] {
        assert!(
            get(app, &format!("{prefix}/time")).await["serverTime"]
                .as_i64()
                .unwrap()
                > 0
        );
        let meta = get(app, &format!("{prefix}/exchangeInfo")).await;
        assert!(meta.get("unknown").is_none());
        assert!(meta["symbols"][0].get("unknown").is_none());
        assert_eq!(meta["symbols"][0]["filters"][0]["tickSize"], "0.000000010");
        let bars = get(
            app,
            &format!("{prefix}/klines?symbol=BTCUSDT&interval=1h&limit=2"),
        )
        .await;
        assert_eq!(bars.as_array().unwrap().len(), 2);
        assert_eq!(bars[0].as_array().unwrap().len(), 12);
        assert!(bars[0][1].is_string());
        let price = get(app, &format!("{prefix}/ticker/price?symbol=BTCUSDT")).await;
        assert_eq!(price["price"], "169");
        assert_eq!(price.get("time").is_some(), prefix == "/fapi/v1");
        let ticker = get(app, &format!("{prefix}/ticker/24hr?symbol=BTCUSDT")).await;
        assert_eq!(ticker["lastPrice"], "100.50000000");
        assert!(ticker.get("unknown").is_none());
    }
    let rates = get(
        app,
        "/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=3600000&endTime=7200000&limit=2",
    )
    .await;
    assert_eq!(rates[0]["fundingRate"], "0.000100");
    let path =
        "/fapi/v1/fundingRate/bulk?symbols=BTCUSDT&since_ms=3600000&until_ms=7200000&limit=1";
    let a = get(app, path).await;
    let (status, body) = request(
        app,
        "POST",
        "/fapi/v1/fundingRate/bulk",
        r#"{"symbols":["BTCUSDT"],"since_ms":3600000,"until_ms":7200000,"limit":1}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let b: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(a["fundingRates"], b["fundingRates"]);
    assert_eq!(a["fundingRates"]["BTCUSDT"].as_array().unwrap().len(), 1);
    let premium = get(app, "/fapi/v1/premiumIndex?symbol=BTCUSDT").await;
    assert_eq!(premium["markPrice"], "123.00");
    assert!(premium.get("unknown").is_none());
    let bulk = get(app, "/fapi/v1/klines/bulk?interval=1h&symbols=BTCUSDT").await;
    let (status, body) = request(
        app,
        "POST",
        "/fapi/v1/klines/bulk",
        r#"{"interval":"1h","symbols":["BTCUSDT"]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let posted: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(bulk["klines"], posted["klines"]);
    for path in [
        "/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10",
        "/bapi/composite/v1/public/cms/article/list/query?catalogId=48&type=1&pageNo=1&pageSize=10",
    ] {
        let data = get(app, path).await;
        assert_eq!(data["success"], true);
        assert!(data.get("unknown").is_none());
        assert!(!data.to_string().contains("unknown"));
    }
    assert_eq!(
        get(app, "/statistic/getAltCoinIndex").await,
        json!({"2026-01-01":0.75})
    );
    for name in ["Yama01", "Yama02", "YamaAgg"] {
        let data = get(app, &format!("/statistic/get{name}AltCoinIndex")).await;
        assert!(!data.as_object().unwrap().is_empty());
        let (status, image) = request(
            app,
            "GET",
            &format!("/statistic/pic/get{name}AltCoinIndex"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let decoder = png::Decoder::new(std::io::Cursor::new(image));
        let png = decoder.read_info().unwrap();
        assert_eq!((png.info().width, png.info().height), (1024, 768));
    }
    let (status, body) = request(app, "GET", "/hello/helloWorld", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"Hello World!");
    let response = http::router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/hello/whatsMyIp")
                .header("x-forwarded-for", "203.0.113.1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "203.0.113.1"
    );
    assert!(get(app, "/health/diagnostics").await.is_array());
}

#[tokio::test]
async fn spot_query_validation_mini_status_and_raw_timezone_monthly() {
    let f = fixture().await;
    for (path, code) in [
        (
            "/api/v3/ticker/24hr?symbol=BTCUSDT&symbols=%5B%22BTCUSDT%22%5D",
            -1128,
        ),
        ("/api/v3/ticker/24hr?symbols=oops", -1100),
        ("/api/v3/ticker/24hr?type=SMALL", -1139),
        ("/api/v3/ticker/24hr?symbolStatus=PAUSED", -1122),
        (
            "/api/v3/ticker/24hr?symbol=BTCUSDT&symbolStatus=HALT",
            -1220,
        ),
        ("/api/v3/ticker/price?symbol=UNKNOWN", -1121),
        ("/api/v3/klines?symbol=BTCUSDT&interval=bad", -1120),
        ("/api/v3/klines?interval=1h", -1000),
    ] {
        let (status, body) = request(&f.app, "GET", path, "").await;
        assert_eq!(
            status,
            if code == -1000 {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::BAD_REQUEST
            },
            "{path}"
        );
        let data: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["code"], code, "{path}");
    }
    assert_eq!(
        get(&f.app, "/api/v3/ticker/24hr?symbols=%5B%5D").await,
        json!([])
    );
    let mini = get(&f.app, "/api/v3/ticker/24hr?symbol=BTCUSDT&type=MINI").await;
    assert!(mini.get("priceChange").is_none());
    assert_eq!(mini["lastPrice"], "100.50000000");
    for suffix in ["interval=1h&timeZone=08:00", "interval=1M"] {
        assert_eq!(
            get(&f.app, &format!("/api/v3/klines?symbol=BTCUSDT&{suffix}")).await[0][1],
            "1.00000000"
        );
    }
    let calls = f.mock.calls.lock().await;
    assert!(calls.iter().any(
        |(path, q)| path == "/api/v3/klines" && q.get("timeZone").is_some_and(|v| v == "08:00")
    ));
}

#[tokio::test]
async fn funding_preserves_explicit_blank_symbol_without_validating_it() {
    let f = fixture().await;
    for (suffix, expected) in [
        ("", None),
        ("?symbol=", Some("")),
        ("?symbol=%20%20", Some("  ")),
    ] {
        let (status, _) =
            request(&f.app, "GET", &format!("/fapi/v1/fundingRate{suffix}"), "").await;
        assert_eq!(status, StatusCode::OK);
        let calls = f.mock.calls.lock().await;
        let (_, params) = calls
            .iter()
            .rev()
            .find(|(path, _)| path.ends_with("fundingRate"))
            .unwrap();
        assert_eq!(params.get("symbol").map(String::as_str), expected);
    }
}

#[tokio::test]
async fn optional_numbers_and_funding_json_null_match_java_request_binding() {
    let f = fixture().await;
    for prefix in ["/api/v3", "/fapi/v1"] {
        let base = format!("{prefix}/klines?symbol=BTCUSDT&interval=1h");
        let absent = get(&f.app, &base).await;
        let empty = get(&f.app, &format!("{base}&limit=&startTime=&endTime=")).await;
        assert_eq!(absent, empty);
    }
    let (status, body) = request(&f.app, "POST", "/fapi/v1/fundingRate/bulk", "null").await;
    assert_eq!(status, StatusCode::OK);
    let baseline: Value = serde_json::from_slice(&body).unwrap();
    for body in [" \n null \n ", "", "{}"] {
        let (status, bytes) = request(&f.app, "POST", "/fapi/v1/fundingRate/bulk", body).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["fundingRates"], baseline["fundingRates"]);
    }
    let path = "/fapi/v1/fundingRate/bulk";
    let (status, bytes) = request(
        &f.app,
        "POST",
        path,
        r#"{"symbols":["BTCUSDT"],"since_ms":"3600000","until_ms":"7200000","limit":"1"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text: Value = serde_json::from_slice(&bytes).unwrap();
    let (status, bytes) = request(
        &f.app,
        "POST",
        path,
        r#"{"symbols":["BTCUSDT"],"since_ms":3600000,"until_ms":7200000,"limit":1}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let typed: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(text["fundingRates"], typed["fundingRates"]);
    assert_eq!(text["fundingRates"]["BTCUSDT"].as_array().unwrap().len(), 1);
    for body in ["{", r#"{"since_ms":9223372036854775808}"#] {
        let (status, bytes) = request(&f.app, "POST", path, body).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(error["code"], -1000);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn funding_singleflight_does_not_block_kline_requests_or_cancel_followers() {
    let f = fixture().await;
    f.mock.hold_funding.store(true, Ordering::Release);
    let q = FundingQuery {
        symbols: Some(vec![Some(" BTCUSDT ".into()), None, Some("BTCUSDT".into())]),
        since_ms: Some(H),
        until_ms: Some(2 * H),
        limit: Some(1),
    };
    let mut clients = vec![];
    for _ in 0..192 {
        let funding = f.app.funding.clone();
        let q = q.clone();
        clients.push(tokio::spawn(async move { funding.bulk(q).await }));
    }
    tokio::time::timeout(Duration::from_secs(2), f.mock.funding_started.notified())
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        get(&f.app, "/fapi/v1/klines/bulk?interval=1h&symbols=BTCUSDT"),
    )
    .await
    .unwrap();
    assert!(reply["pending"].as_array().unwrap().is_empty());
    assert!(clients.iter().all(|c| !c.is_finished()));
    // Cancellation of any follower must leave the shared async load usable.
    clients.pop().unwrap().abort();
    f.mock.release_funding.notify_one();
    for client in clients {
        let bytes = client.await.unwrap().unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["fundingRates"]["BTCUSDT"][0]["fundingTime"], H + 2);
    }
    assert_eq!(f.app.funding.upstream_loads.load(Ordering::Relaxed), 1);
    assert_eq!(
        f.mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|(p, _)| p.ends_with("fundingRate"))
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_recent_funding_combinations_share_only_the_same_symbol_and_limit() {
    let f = fixture().await;
    f.mock.hold_funding.store(true, Ordering::Release);
    let mut clients = Vec::new();
    for i in 0..64 {
        let funding = f.app.funding.clone();
        clients.push(tokio::spawn(async move {
            funding
                .bulk(FundingQuery {
                    symbols: Some(if i % 2 == 0 {
                        vec![Some("BTCUSDT".into())]
                    } else {
                        vec![Some("BTCUSDT".into()), Some("ETHUSDT".into())]
                    }),
                    limit: Some(1),
                    ..Default::default()
                })
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let count = f
                .mock
                .calls
                .lock()
                .await
                .iter()
                .filter(|(p, _)| p.ends_with("fundingRate"))
                .count();
            if count >= 2 {
                assert_eq!(count, 2);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    f.mock.hold_funding.store(false, Ordering::Release);
    f.mock.release_funding.notify_waiters();
    for client in clients {
        let value: Value = serde_json::from_slice(&client.await.unwrap().unwrap()).unwrap();
        assert_eq!(
            value["fundingRates"]["BTCUSDT"].as_array().unwrap().len(),
            1
        );
        assert_eq!(value["fundingRates"]["BTCUSDT"][0]["fundingTime"], H + 2);
    }
    assert_eq!(f.app.funding.upstream_loads.load(Ordering::Relaxed), 2);
    let value: Value = serde_json::from_slice(
        &f.app
            .funding
            .bulk(FundingQuery {
                symbols: Some(vec![Some("BTCUSDT".into())]),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        value["fundingRates"]["BTCUSDT"].as_array().unwrap().len(),
        2
    );
    assert_eq!(f.app.funding.upstream_loads.load(Ordering::Relaxed), 3);
    // The ordinary proxy endpoint continues to forward upstream independently.
    f.app
        .funding
        .raw(Some("BTCUSDT"), None, None, Some(1))
        .await
        .unwrap();
    assert_eq!(f.app.funding.upstream_loads.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn funding_rejects_ambiguous_truncated_ranges_and_obeys_exclusive_end() {
    let f = fixture().await;
    let q = FundingQuery {
        symbols: Some(vec![Some("BTCUSDT".into())]),
        since_ms: Some(H + 1),
        until_ms: Some(H + 2),
        limit: Some(1000),
    };
    let value: Value = serde_json::from_slice(&f.app.funding.bulk(q).await.unwrap()).unwrap();
    assert_eq!(
        value["fundingRates"]["BTCUSDT"].as_array().unwrap().len(),
        1
    );
    assert_eq!(value["fundingRates"]["BTCUSDT"][0]["fundingTime"], H + 1);
    let q = FundingQuery {
        symbols: Some(vec![Some("PAGEUSDT".into())]),
        since_ms: Some(H),
        until_ms: Some(2000 * H),
        limit: Some(1),
    };
    assert_eq!(f.app.funding.bulk(q).await.unwrap_err().code, -1130);
    let q = FundingQuery {
        since_ms: Some(i64::MIN),
        until_ms: Some(0),
        ..Default::default()
    };
    let value: Value = serde_json::from_slice(&f.app.funding.bulk(q).await.unwrap()).unwrap();
    assert_eq!(value["fundingRates"], json!({}));
}

#[test]
fn vision_archive_is_bounded_validated_and_preserves_decimal_scale() {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(vec![]));
    zip.start_file("BTCUSDT.csv", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"calc_time,funding_interval_hours,last_funding_rate\n 3600000 ,8, 1.00E-4 ,extra\nbad,8,0.1\n3600010,8,NaN\nshort\n7200000,8,-0.0001\n").unwrap();
    let data = zip.finish().unwrap().into_inner();
    let rates = kline_market::vision::parse_archive(&data, "BTCUSDT", H, 2 * H).unwrap();
    assert_eq!(rates.len(), 1);
    assert_eq!(rates[0].funding_rate.as_deref(), Some("0.000100"));
    assert!(rates[0].mark_price.is_none());
    assert!(kline_market::vision::parse_archive(b"bad archive", "BTCUSDT", H, 2 * H).is_err());
}

#[test]
fn ticker_decimal_contract_is_distinct_from_prices() {
    let value = kline_market::ticker::display(
        &json!({"e":"24hrTicker","s":"BTCUSDT","c":"1.2300E-5","C":123,"Q":"2.0"}),
        true,
    )
    .unwrap();
    assert_eq!(
        value,
        json!({"symbol":"BTCUSDT","lastPrice":"0.000012300","closeTime":123,"lastQty":"2.0"})
    );
    assert!(kline_market::decimal("1e99999").is_err());
}

#[tokio::test]
async fn delayed_ticker_frames_and_rest_snapshots_do_not_regress_the_latest_value() {
    let f = fixture().await;
    for (time, price) in [(123, "100.50000000"), (122, "99.00000000")] {
        let frame = json!({"stream":"btcusdt@ticker","data":{"e":"24hrTicker","s":"BTCUSDT","C":time,"c":price}});
        f.app
            .tickers
            .ingest(Market::Spot, &serde_json::to_vec(&frame).unwrap())
            .unwrap();
    }
    f.app.tickers.refresh(Market::Spot).await.unwrap();
    let rows = get(&f.app, "/api/v3/ticker/24hr").await;
    assert_eq!(rows[0]["closeTime"], 123);
    assert_eq!(rows[0]["lastPrice"], "100.50000000");
    assert_eq!(f.app.tickers.status()["spot"]["frames"], 2);
}

#[tokio::test]
async fn large_exchange_directory_is_shaped_once_and_shares_wire_bytes() {
    let f = fixture().await;
    f.mock.large_metadata.store(true, Ordering::Relaxed);
    let (prefix, a) = f.app.metadata.body(Market::Spot, 123).await.unwrap();
    let (_, b) = f.app.metadata.body(Market::Spot, 124).await.unwrap();
    assert_eq!(a.as_ptr(), b.as_ptr());
    assert_eq!(prefix, b"{\"serverTime\":123,"[..]);
    assert!(a.len() < 4096);
    assert_eq!(f.mock.calls.lock().await.len(), 1);
    assert_eq!(
        f.mock.calls.lock().await[0].1["showPermissionSets"],
        "false"
    );
}

#[test]
fn ordinary_query_fallback_preserves_gaps_and_atr_excludes_forming_bar() {
    let interval = Interval::parse("1h").unwrap();
    assert_eq!(
        kline_market::klines::range(0, interval, Some(1), Some(4 * H), 2),
        (H, 2 * H)
    );
    let engine = Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Spot,
            symbol: "BTCUSDT".into(),
            interval,
            trading: true,
            continuous: None,
            capacity: NonZeroUsize::new(5).unwrap(),
        }])
        .unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    );
    for open in [0, 4 * H] {
        engine
            .commit(
                0,
                Update {
                    bar: Bar {
                        open_time: open,
                        close_time: open + H - 1,
                        trades: 1,
                        values: [1.; 8],

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
    let bars =
        kline_market::klines::cached(&engine, Market::Spot, "BTCUSDT", interval, None, None, 2);
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].open_time, 4 * H);
    let row = |open, high, low, close| Bar {
        open_time: open,
        close_time: open + H - 1,
        trades: 1,
        values: [close, high, low, close, 1., 1., 1., 1.],

        ..Bar::default()
    };
    let atr = kline_market::statistics::atr(
        &[
            row(0, 12., 8., 10.),
            row(H, 15., 12., 13.),
            row(2 * H, 999., 1., 500.),
        ],
        48,
    )
    .unwrap();
    assert_eq!(atr.as_deref(), Some("0.39130434"));
}

#[tokio::test]
async fn ticker_price_keeps_precision_and_uses_future_v2_for_single_and_all_symbols() {
    for raw in ["100.50000000", "0.000000001", "123456789.123456789"] {
        let f = fixture_with_history(false).await;
        *f.mock.ticker_price.lock().await = Some(raw.into());
        for prefix in ["/api/v3", "/fapi/v1"] {
            assert_eq!(
                get(&f.app, &format!("{prefix}/ticker/price?symbol=BTCUSDT")).await["price"],
                raw
            );
            let all = get(&f.app, &format!("{prefix}/ticker/price")).await;
            assert_eq!(all[0]["price"], raw);
            if prefix == "/fapi/v1" {
                assert_eq!(all[0]["time"], 123);
            }
        }
        let calls = f.mock.calls.lock().await;
        let price_calls: Vec<_> = calls
            .iter()
            .filter(|(path, _)| path.ends_with("/ticker/price"))
            .collect();
        assert_eq!(price_calls.len(), 4);
        for path in ["/api/v3/ticker/price", "/fapi/v2/ticker/price"] {
            assert!(price_calls.iter().any(|(p, q)| p == path && q.is_empty()));
            assert!(
                price_calls
                    .iter()
                    .any(|(p, q)| { p == path && q.get("symbol").is_some_and(|s| s == "BTCUSDT") })
            );
        }
        assert!(!calls.iter().any(|(p, _)| p.ends_with("/ticker/24hr")));
    }
}

fn commit_price(
    app: &App,
    market: Market,
    symbol: &str,
    interval: &str,
    mode: NumberType,
    price: &str,
    sequence: u64,
) -> Value {
    let interval = Interval::parse(interval).unwrap();
    let open = interval.boundary(app.engine.now_ms());
    let mut bar = Bar {
        open_time: open,
        close_time: open + interval.millis() - 1,
        trades: sequence as u32,
        ..Bar::default()
    };
    bar.set_numbers(mode, [price, price, price, price, "1", "1", "0", "0"])
        .unwrap();
    let expected = serde_json::to_value(binance_wire::DisplayBar(bar.clone())).unwrap()[4].clone();
    let id = app.engine.catalog.find(market, interval, symbol).unwrap();
    app.engine
        .commit(
            id,
            Update {
                bar,
                closed: false,
                source: Source::Stream,
                event_time: None,
                sequence,
            },
        )
        .unwrap();
    expected
}

#[tokio::test]
async fn single_price_reads_latest_bars_in_all_numeric_modes_without_rest_or_ttl_delay() {
    for mode in [
        NumberType::String,
        NumberType::Float,
        NumberType::Double,
        NumberType::BigDecimal,
    ] {
        let f = fixture_with_history_and_type(false, mode).await;
        for market in [Market::Future, Market::Spot] {
            f.app.metadata.get(market).await.unwrap();
        }
        f.mock.calls.lock().await.clear();
        f.mock.hold_price.store(true, Ordering::Release);
        for (market, prefix) in [(Market::Future, "/fapi/v1"), (Market::Spot, "/api/v3")] {
            // An old observation cannot satisfy Live::get's three-second gate.
            let old = funding::now() - 10_000;
            let frame = if market == Market::Future {
                json!({"e":"aggTrade","s":"BTCUSDT","p":"999","E":old,"T":old,"a":1})
            } else {
                json!({"e":"24hrTicker","s":"BTCUSDT","c":"999","E":old,"C":old})
            };
            f.app
                .tickers
                .ingest(market, &serde_json::to_vec(&frame).unwrap())
                .unwrap();
            for (i, close) in ["0.0000123400", "123456789.123456789"]
                .into_iter()
                .enumerate()
            {
                let expected =
                    commit_price(&f.app, market, "BTCUSDT", "1h", mode, close, i as u64 + 1);
                let before = f.app.engine.now_ms();
                let value = tokio::time::timeout(
                    Duration::from_millis(250),
                    get(&f.app, &format!("{prefix}/ticker/price?symbol=BTCUSDT")),
                )
                .await
                .expect("cached close must bypass a blocked price upstream");
                assert_eq!(value["price"], expected);
                assert_eq!(value["symbol"], "BTCUSDT");
                if market == Market::Future {
                    assert!(
                        (before..=f.app.engine.now_ms()).contains(&value["time"].as_i64().unwrap())
                    );
                } else {
                    assert!(value.get("time").is_none());
                    let array =
                        get(&f.app, "/api/v3/ticker/price?symbols=%5B%22BTCUSDT%22%5D").await;
                    assert_eq!(array, json!([value]));
                }
            }
        }
        assert!(f.mock.calls.lock().await.is_empty());
        for market in ["future", "spot"] {
            let status = f.app.tickers.status();
            assert_eq!(status[market]["single_rest_loads"], 0);
            assert!(status[market]["kline_price_hits"].as_u64().unwrap() >= 2);
        }
    }
}

#[tokio::test]
async fn single_price_walks_configured_intervals_without_mixing_symbols_or_markets() {
    let f = fixture_with_history_and_type(false, NumberType::String).await;
    f.app.tickers.set_price_intervals(std::array::from_fn(|_| {
        vec![
            Interval::parse("1d").unwrap(),
            Interval::parse("1h").unwrap(),
        ]
    }));
    for market in [Market::Future, Market::Spot] {
        f.app.metadata.get(market).await.unwrap();
    }
    commit_price(
        &f.app,
        Market::Future,
        "ETHUSDT",
        "1d",
        NumberType::String,
        "999",
        1,
    );
    commit_price(
        &f.app,
        Market::Spot,
        "BTCUSDT",
        "1d",
        NumberType::String,
        "888",
        1,
    );
    let path = "/fapi/v1/ticker/price?symbol=BTCUSDT";
    // Neither a different symbol nor the other market is a local hit.
    assert_eq!(get(&f.app, path).await["price"], "100.50000000");
    let loads = f.app.tickers.status()["future"]["single_rest_loads"].clone();
    commit_price(
        &f.app,
        Market::Future,
        "BTCUSDT",
        "1h",
        NumberType::String,
        "111.00",
        1,
    );
    f.mock.calls.lock().await.clear();
    // A just-loaded REST cache must not hide newer local data for its 500ms TTL.
    assert_eq!(get(&f.app, path).await["price"], "111.00");
    commit_price(
        &f.app,
        Market::Future,
        "BTCUSDT",
        "1d",
        NumberType::String,
        "222.00",
        1,
    );
    assert_eq!(get(&f.app, path).await["price"], "222.00");
    assert_eq!(
        get(&f.app, "/api/v3/ticker/price?symbol=BTCUSDT").await["price"],
        "888"
    );
    assert!(f.mock.calls.lock().await.is_empty());
    assert_eq!(f.app.tickers.status()["future"]["single_rest_loads"], loads);
}

#[tokio::test]
async fn local_close_does_not_authorize_zero_bars_or_poison_ws_transaction_order() {
    let f = fixture().await;
    let interval = Interval::parse("1h").unwrap();
    let now = funding::now();
    for (market, prefix) in [(Market::Future, "/fapi/v1"), (Market::Spot, "/api/v3")] {
        f.app.metadata.get(market).await.unwrap();
        let id = f
            .app
            .engine
            .catalog
            .find(market, interval, "BTCUSDT")
            .unwrap();
        let slot = f.app.engine.catalog.slot(id);
        let previous = slot.latest().unwrap().0;
        let count = slot.len();
        // This fixture has only REST-confirmed previous bars, no WS x=true.
        assert_eq!(
            get(&f.app, &format!("{prefix}/ticker/price?symbol=BTCUSDT")).await["price"],
            "169"
        );
        assert_eq!(slot.snapshot(1, false, now)[0], previous);
        assert_eq!(slot.len(), count);
        let frame = if market == Market::Future {
            json!({"e":"aggTrade","s":"BTCUSDT","p":"321.0000","E":now,"T":now-23,"a":8})
        } else {
            json!({"e":"24hrTicker","s":"BTCUSDT","c":"321.0000","E":now,"C":now-23})
        };
        f.app
            .tickers
            .ingest(market, &serde_json::to_vec(&frame).unwrap())
            .unwrap();
        let value = get(&f.app, &format!("{prefix}/ticker/price?symbol=BTCUSDT")).await;
        assert_eq!(value["price"], "321.0000");
        if market == Market::Future {
            assert_eq!(value["time"], now - 23);
        }
        assert_eq!(slot.len(), count);
    }
}

#[tokio::test]
async fn local_prices_keep_symbol_and_status_validation() {
    let f = fixture().await;
    for (path, code) in [
        ("/api/v3/ticker/price?symbol=UNKNOWN", -1121),
        ("/fapi/v1/ticker/price?symbol=UNKNOWN", -1121),
        (
            "/api/v3/ticker/24hr?symbol=BTCUSDT&symbolStatus=HALT",
            -1220,
        ),
    ] {
        let (status, body) = request(&f.app, "GET", path, "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["code"],
            code
        );
    }
    assert_eq!(f.app.tickers.status()["spot"]["single_rest_loads"], 0);
}

#[tokio::test]
async fn an_early_ws_increment_does_not_replace_the_full_ticker_baseline() {
    let f = fixture().await;
    f.app
        .tickers
        .ingest(
            Market::Future,
            br#"{"e":"24hrTicker","s":"ETHUSDT","c":"7.000","C":100}"#,
        )
        .unwrap();
    let mut tasks = vec![];
    for _ in 0..16 {
        let app = f.app.clone();
        tasks.push(tokio::spawn(async move {
            get(&app, "/fapi/v1/ticker/24hr").await
        }));
    }
    for task in tasks {
        let rows = task.await.unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
        assert_eq!(rows[0]["symbol"], "BTCUSDT");
        assert_eq!(rows[1]["symbol"], "ETHUSDT");
        assert_eq!(rows[1]["lastPrice"], "7.000");
    }
    assert_eq!(
        f.mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|(p, _)| p.ends_with("/ticker/24hr"))
            .count(),
        1
    );
}

#[tokio::test]
async fn vision_ignores_missing_archives_but_keeps_month_atomicity_on_server_errors() {
    use std::io::{Cursor, Write};
    for (missing, failed, expected_hours) in [(false, false, 1), (true, false, 1), (false, true, 0)]
    {
        let f = fixture().await;
        let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
        archive
            .start_file("funding.csv", zip::write::SimpleFileOptions::default())
            .unwrap();
        writeln!(
            archive,
            "calc_time,funding_interval_hours,last_funding_rate\n{},8,0.0001",
            funding::now() - H
        )
        .unwrap();
        *f.mock.archive.lock().await = archive.finish().unwrap().into_inner();
        f.mock.missing_archive.store(missing, Ordering::Relaxed);
        f.mock.failed_archive.store(failed, Ordering::Relaxed);
        assert_eq!(
            kline_market::vision::warm(f.app.funding.clone())
                .await
                .unwrap(),
            expected_hours
        );
    }
}

#[tokio::test]
async fn every_numeric_mode_reaches_ordinary_and_bulk_http_without_losing_its_contract() {
    use kline_core::NumberType;
    for mode in [
        NumberType::Double,
        NumberType::Float,
        NumberType::String,
        NumberType::BigDecimal,
    ] {
        let f = fixture().await;
        let engine = Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "BTCUSDT".into(),
                interval: Interval::parse("1h").unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(4).unwrap(),
            }])
            .unwrap(),
            Arc::new(SystemClock { offset_ms: 0 }),
            Settings {
                number_type: mode,
                final_wait_ms: 0,
                ..Settings::default()
            },
        );
        let open = funding::now().div_euclid(H) * H - H;
        let frame=serde_json::to_vec(&json!({"e":"kline","s":"BTCUSDT","E":open+H,"k":{"t":open,"T":open+H-1,"i":"1h","o":"1.0000","h":"1.0000","l":"0.000000001","c":"0.000000001","v":"5.00","q":"5.00","V":"2.00","Q":"2.00","n":1,"x":true}})).unwrap();
        engine.ingest(Market::Future, &frame).unwrap().unwrap();
        let app = App::new(engine, f.app.api.clone(), Config::default());
        let expected = match mode {
            NumberType::Double | NumberType::Float => "0",
            _ => "0.000000001",
        };
        assert_eq!(
            get(&app, "/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=1").await[0][4],
            expected
        );
        assert_eq!(
            get(
                &app,
                "/fapi/v1/klines/bulk?symbols=BTCUSDT&interval=1h&limit=1"
            )
            .await["klines"]["BTCUSDT"][0][4],
            expected
        );
    }
    assert_eq!(kline_market::decimal("0E+5").unwrap(), "0");
}

#[tokio::test]
async fn warm_price_requests_do_not_wait_for_refresh_and_background_is_cancellable() {
    let f = fixture().await;
    let query = || {
        f.app
            .tickers
            .query(Market::Future, true, None, vec![], false, None)
    };
    let first = query().await.unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    f.mock.hold_price.store(true, Ordering::Release);
    let task = tokio::spawn(f.app.tickers.clone().run(Market::Future, false, stopped));
    tokio::time::timeout(Duration::from_secs(2), f.mock.price_started.notified())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), query())
            .await
            .unwrap()
            .unwrap(),
        first
    );
    *f.mock.ticker_price.lock().await = Some("101.2500".into());
    f.mock.hold_price.store(false, Ordering::Release);
    f.mock.release_price.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let body: Value = serde_json::from_slice(&query().await.unwrap()).unwrap();
            if body[0]["price"] == "101.2500" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    f.mock.hold_price.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), f.mock.price_started.notified())
        .await
        .unwrap();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn live_prices_and_single_tickers_do_not_wait_for_rest_or_metadata_refresh() {
    let f = fixture().await;
    for market in [Market::Future, Market::Spot] {
        f.app.metadata.get(market).await.unwrap();
    }
    f.mock.hold_metadata.store(true, Ordering::Release);
    let refresh = tokio::spawn({
        let metadata = f.app.metadata.clone();
        async move { metadata.refresh(Market::Future).await }
    });
    tokio::time::timeout(Duration::from_secs(1), f.mock.metadata_started.notified())
        .await
        .unwrap();
    let now = funding::now();
    let ticker = json!({"e":"24hrTicker","s":"BTCUSDT","c":"123.45000000","E":now,"C":now-10});
    for market in [Market::Future, Market::Spot] {
        f.app
            .tickers
            .ingest(market, &serde_json::to_vec(&ticker).unwrap())
            .unwrap();
    }
    f.app
        .tickers
        .ingest(
            Market::Future,
            &serde_json::to_vec(
                &json!({"e":"aggTrade","s":"BTCUSDT","p":"123.456789123","E":now,"T":now-23,"a":8}),
            )
            .unwrap(),
        )
        .unwrap();
    let before = f.mock.calls.lock().await.len();
    for _ in 0..2 {
        for path in [
            "/fapi/v1/ticker/price?symbol=BTCUSDT",
            "/api/v3/ticker/price?symbol=BTCUSDT",
            "/fapi/v1/ticker/24hr?symbol=BTCUSDT",
            "/api/v3/ticker/24hr?symbol=BTCUSDT",
        ] {
            let result = tokio::time::timeout(Duration::from_millis(100), get(&f.app, path))
                .await
                .unwrap();
            if path.starts_with("/fapi") && path.contains("/price") {
                assert_eq!(result["price"], "123.456789123");
                assert_eq!(result["time"], now - 23);
            } else if path.contains("/price") {
                assert_eq!(result["price"], "123.45000000");
                assert!(result.get("time").is_none());
            } else {
                assert_eq!(result["lastPrice"], "123.45000000");
            }
        }
        tokio::time::sleep(Duration::from_millis(550)).await;
    }
    assert_eq!(f.mock.calls.lock().await.len(), before);
    refresh.abort();
}

#[tokio::test]
async fn a_stream_price_releases_a_request_already_waiting_for_rest() {
    let f = fixture_with_history(false).await;
    f.app.metadata.get(Market::Future).await.unwrap();
    f.mock.hold_price.store(true, Ordering::Release);
    let request = tokio::spawn({
        let app = f.app.clone();
        async move { get(&app, "/fapi/v1/ticker/price?symbol=BTCUSDT").await }
    });
    tokio::time::timeout(Duration::from_secs(1), f.mock.price_started.notified())
        .await
        .unwrap();
    let now = funding::now();
    f.app
        .tickers
        .ingest(
            Market::Future,
            &serde_json::to_vec(
                &json!({"e":"aggTrade","s":"BTCUSDT","p":"321.0000","E":now,"T":now-7,"a":9}),
            )
            .unwrap(),
        )
        .unwrap();
    let value = tokio::time::timeout(Duration::from_millis(100), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["price"], "321.0000");
    assert_eq!(value["time"], now - 7);
}

#[tokio::test]
async fn all_prices_overlay_live_transactions_and_stale_frames_fall_back_to_v2() {
    let f = fixture_with_history(false).await;
    let baseline = get(&f.app, "/fapi/v1/ticker/price").await;
    assert_eq!(baseline[0]["time"], 123);
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(f.app.tickers.clone().run(Market::Future, false, stopped));
    let now = funding::now();
    let trade = json!({"e":"aggTrade","s":"BTCUSDT","p":"222.1200","E":now,"T":now-5,"a":10});
    f.app
        .tickers
        .ingest(Market::Future, &serde_json::to_vec(&trade).unwrap())
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let all = get(&f.app, "/fapi/v1/ticker/price").await;
    assert_eq!(all[0]["price"], "222.1200");
    assert_eq!(all[0]["time"], now - 5);
    let old = json!({"e":"aggTrade","s":"ETHUSDT","p":"1","E":now-10000,"T":now-10001,"a":11});
    f.app
        .tickers
        .ingest(Market::Future, &serde_json::to_vec(&old).unwrap())
        .unwrap();
    let fallback = get(&f.app, "/fapi/v1/ticker/price?symbol=ETHUSDT").await;
    assert_eq!(fallback["price"], "100.50000000");
    assert!(
        f.mock
            .calls
            .lock()
            .await
            .iter()
            .any(|(path, q)| path == "/fapi/v2/ticker/price"
                && q.get("symbol").is_some_and(|s| s == "ETHUSDT"))
    );
    stop.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn repeated_price_subsets_keep_valid_symbols_absent_from_the_full_price_list() {
    let f = fixture_with_history(false).await;
    // Metadata and the single-symbol endpoint know ETH, while the complete
    // price endpoint currently only lists BTC (e.g. a newly available quote).
    *f.mock.all_prices.lock().await = Some(vec![
        json!({"symbol":"BTCUSDT","price":"100.50000000","time":123}),
    ]);
    for market in [Market::Future, Market::Spot] {
        for _ in 0..2 {
            let body = f.app.tickers.query(market, true, None, vec!["ETHUSDT".into(), "BTCUSDT".into()], false, None).await
                .expect("a known valid symbol absent from the ordering baseline must retain its per-symbol lookup");
            let rows: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(rows[0]["symbol"], "BTCUSDT");
            assert_eq!(rows[1]["symbol"], "ETHUSDT");
            assert_eq!(rows[1]["price"], "100.50000000");
        }
    }
}

#[tokio::test]
async fn established_metadata_reads_do_not_join_refresh_and_do_not_extend_body_ttl() {
    let f = fixture().await;
    let metadata = kline_market::metadata::Metadata::new(f.app.api.clone(), 1);
    let (_, original) = metadata.body(Market::Spot, 10).await.unwrap();
    f.mock.hold_metadata.store(true, Ordering::Release);
    let refresh = tokio::spawn({
        let metadata = metadata.clone();
        async move { metadata.refresh(Market::Spot).await }
    });
    tokio::time::timeout(Duration::from_secs(1), f.mock.metadata_started.notified())
        .await
        .unwrap();
    let (prefix, body) =
        tokio::time::timeout(Duration::from_millis(100), metadata.body(Market::Spot, 20))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(body.as_ptr(), original.as_ptr());
    let json: Value = serde_json::from_slice(&[prefix.as_ref(), body.as_ref()].concat()).unwrap();
    assert_eq!(json["serverTime"], 20);
    assert_eq!(json["symbols"].as_array().unwrap().len(), 3);
    metadata
        .validate(Market::Spot, &["BTCUSDT".into()])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1050)).await;
    let error = tokio::time::timeout(Duration::from_millis(100), metadata.body(Market::Spot, 30))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.status, 503,
        "expired bodies must not acquire a fresh timestamp"
    );
    assert_eq!(
        metadata
            .validate(Market::Spot, &["BTCUSDT".into()])
            .await
            .unwrap_err()
            .status,
        503
    );
    assert!(
        metadata.query_snapshot(Market::Spot).await.is_ok(),
        "symbol lookup retains its separate original grace"
    );
    f.mock.hold_metadata.store(false, Ordering::Release);
    f.mock.release_metadata.notify_waiters();
    refresh.await.unwrap().unwrap();
    assert!(metadata.body(Market::Spot, 40).await.is_ok());
}

#[tokio::test]
async fn live_price_subsets_do_not_refresh_missing_order_or_wait_on_rest() {
    let f = fixture_with_history(false).await;
    for market in [Market::Future, Market::Spot] {
        f.mock.hold_price.store(false, Ordering::Release);
        f.app.metadata.get(market).await.unwrap();
        f.app
            .tickers
            .query(market, true, None, vec![], false, None)
            .await
            .unwrap();
        let now = funding::now();
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let row = if market == Market::Future {
                json!({"e":"aggTrade","s":symbol,"p":"222.0000","E":now,"T":now-7,"a":9})
            } else {
                json!({"e":"24hrTicker","s":symbol,"c":"222.0000","E":now})
            };
            f.app
                .tickers
                .ingest(market, &serde_json::to_vec(&row).unwrap())
                .unwrap();
        }
        f.mock.hold_price.store(true, Ordering::Release);
        let before = f.mock.calls.lock().await.len();
        for _ in 0..2 {
            let body = tokio::time::timeout(
                Duration::from_millis(100),
                f.app.tickers.query(
                    market,
                    true,
                    None,
                    vec!["ETHUSDT".into(), "BTCUSDT".into()],
                    false,
                    None,
                ),
            )
            .await
            .unwrap()
            .unwrap();
            let rows: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(rows[0]["symbol"], "BTCUSDT");
            assert_eq!(rows[1]["symbol"], "ETHUSDT");
            assert_eq!(rows[1]["price"], "222.0000");
            if market == Market::Future {
                assert_eq!(rows[1]["time"], now - 7);
            }
        }
        assert_eq!(f.mock.calls.lock().await.len(), before);
    }
}

#[tokio::test]
async fn quiet_price_subsets_use_the_same_valid_baseline_as_the_complete_response() {
    let f = fixture_with_history(false).await;
    *f.mock.all_prices.lock().await = Some(vec![
        json!({"symbol":"BTCUSDT","price":"100.50000000","time":123}),
        json!({"symbol":"ETHUSDT","price":"200.00","time":234}),
    ]);
    for market in [Market::Future, Market::Spot] {
        f.mock.hold_price.store(false, Ordering::Release);
        f.app.metadata.get(market).await.unwrap();
        let all = f
            .app
            .tickers
            .query(market, true, None, vec![], false, None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        f.mock.hold_price.store(true, Ordering::Release);
        let before = f.mock.calls.lock().await.len();
        let subset = tokio::time::timeout(
            Duration::from_millis(100),
            f.app.tickers.query(
                market,
                true,
                None,
                vec!["ETHUSDT".into(), "BTCUSDT".into()],
                false,
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&subset).unwrap(),
            serde_json::from_slice::<Value>(&all).unwrap()
        );
        assert_eq!(f.mock.calls.lock().await.len(), before);
    }
}

#[tokio::test]
async fn price_maintenance_prepares_and_renews_idle_snapshots_before_the_first_query() {
    let f = fixture_with_history(false).await;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(f.app.tickers.clone().run(Market::Spot, false, stopped));
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let status = f.app.tickers.status();
            if status["spot"]["price_snapshot"]["rest_loads"]
                .as_u64()
                .unwrap()
                >= 2
                && status["spot"]["price_snapshot"]["rest_age_ms"]
                    .as_u64()
                    .unwrap_or(u64::MAX)
                    < 500
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("idle snapshots should refresh without any request");
    let before = f.app.tickers.status()["spot"]["price_snapshot"]["rest_loads"].clone();
    let rows = f
        .app
        .tickers
        .query(Market::Spot, true, None, vec![], false, None)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&rows).unwrap()[0]["price"],
        "100.50000000"
    );
    assert_eq!(
        f.app.tickers.status()["spot"]["price_snapshot"]["rest_loads"],
        before
    );
    stop.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn full_price_lists_keep_retired_pairs_and_publish_ws_while_rest_is_blocked() {
    for market in [Market::Future, Market::Spot] {
        let f = fixture().await;
        *f.mock.all_prices.lock().await = Some(vec![
            json!({"symbol":"BTCUSDT","price":"100.50000000","time":123}),
            json!({"symbol":"HALTUSDT","price":"7.00000001","time":122}),
            json!({"symbol":"REMOVEDUSDT","price":"0.000000001","time":121}),
        ]);
        let query = || f.app.tickers.query(market, true, None, vec![], false, None);
        let initial: Value = serde_json::from_slice(&query().await.unwrap()).unwrap();
        assert_eq!(initial.as_array().unwrap().len(), 3);
        f.mock.hold_price.store(true, Ordering::Release);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(f.app.tickers.clone().run(market, false, stopped));
        tokio::time::timeout(Duration::from_secs(2), f.mock.price_started.notified())
            .await
            .unwrap();
        // Reproduces the previous foreground barrier. Its 100ms cache has also expired.
        tokio::time::sleep(Duration::from_millis(3100)).await;
        let now = funding::now();
        let frame = if market == Market::Future {
            json!({"e":"aggTrade","s":"BTCUSDT","p":"222.1200","E":now,"T":now-5,"a":10})
        } else {
            json!({"e":"24hrTicker","s":"BTCUSDT","c":"222.1200","E":now,"C":now-5})
        };
        f.app
            .tickers
            .ingest(market, &serde_json::to_vec(&frame).unwrap())
            .unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let rows: Value = serde_json::from_slice(&query().await.unwrap()).unwrap();
                assert_eq!(rows.as_array().unwrap().len(), 3);
                assert_eq!(rows[1], initial[1]);
                assert_eq!(rows[2], initial[2]);
                if rows[0]["price"] == "222.1200" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a queued REST load must not block the list or live publication");
        let rest_calls = f
            .mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|(p, _)| p.ends_with("/ticker/price"))
            .count();
        assert_eq!(
            rest_calls, 2,
            "initial load and one shared background refresh"
        );
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();
        f.mock.release_price.notify_one();
    }
}

#[tokio::test]
async fn concurrent_cold_price_lists_share_one_rest_load() {
    let f = fixture().await;
    f.mock.hold_price.store(true, Ordering::Release);
    let mut tasks = vec![];
    for _ in 0..32 {
        let app = f.app.clone();
        tasks.push(tokio::spawn(async move {
            app.tickers
                .query(Market::Spot, true, None, vec![], false, None)
                .await
                .unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), f.mock.price_started.notified())
        .await
        .unwrap();
    f.mock.hold_price.store(false, Ordering::Release);
    f.mock.release_price.notify_one();
    let mut bodies = vec![];
    for task in tasks {
        bodies.push(task.await.unwrap());
    }
    assert!(bodies.iter().all(|body| body == &bodies[0]));
    assert_eq!(
        f.mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|(p, _)| p.ends_with("/ticker/price"))
            .count(),
        1
    );
}

#[tokio::test]
async fn metadata_maintenance_renews_idle_markets_without_a_subscription_directory() {
    let f = fixture().await;
    let metadata = kline_market::metadata::Metadata::new(f.app.api.clone(), 1);
    metadata.body(Market::Future, 1).await.unwrap();
    let (stop, rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(metadata.clone().run(rx));
    tokio::time::sleep(Duration::from_millis(1250)).await;
    metadata.body(Market::Future, 2).await.unwrap();
    assert!(
        f.mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|(path, _)| path == "/fapi/v1/exchangeInfo")
            .count()
            >= 2
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}
