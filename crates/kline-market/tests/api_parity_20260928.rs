use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{Request, Uri},
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_market::{config::Config, http, transport::RestApi};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use tower::ServiceExt;

const NOW: i64 = 1_790_000_000_000;
struct FixedClock;
impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        NOW
    }
}
#[derive(Default)]
struct Mock {
    // 0: empty all-market lists/null single objects; 1: normal 24hr baseline; 2: malformed JSON;
    // 3: null for both single-symbol and all-market responses; 4: wrong schema.
    mode: AtomicU8,
    metadata: std::sync::Mutex<Option<Value>>,
    ticker: std::sync::Mutex<Option<Value>>,
}
async fn upstream(
    State(mock): State<Arc<Mock>>,
    uri: Uri,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    if uri.path().ends_with("exchangeInfo") {
        if let Some(metadata) = mock.metadata.lock().unwrap().clone() {
            return axum::Json(metadata).into_response();
        }
        return axum::Json(json!({"symbols":[
            {"symbol":"BTCUSDT","status":"TRADING"},
            {"symbol":"ETHUSDT","status":"TRADING"}
        ]}))
        .into_response();
    }
    if mock.mode.load(Ordering::Acquire) == 2 {
        return ([("content-type", "application/json")], "{bad-json").into_response();
    }
    if mock.mode.load(Ordering::Acquire) == 4 {
        return axum::Json(json!("not a ticker object")).into_response();
    }
    let single = query.contains_key("symbol");
    if let Some(row) = mock.ticker.lock().unwrap().clone() {
        return axum::Json(if single { row } else { json!([row]) }).into_response();
    }
    if uri.path().ends_with("ticker/24hr") && mock.mode.load(Ordering::Acquire) == 1 {
        let row =
            json!({"symbol":"BTCUSDT","lastPrice":"99.27000000","closeTime":123,"volume":"5.0000"});
        return axum::Json(if single { row } else { json!([row]) }).into_response();
    }
    axum::Json(if single || mock.mode.load(Ordering::Acquire) == 3 {
        Value::Null
    } else {
        json!([])
    })
    .into_response()
}
struct Fixture {
    app: Arc<http::App>,
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
    let mock = Arc::new(Mock::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(upstream).with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let instruments = [Market::Future, Market::Spot]
        .into_iter()
        .flat_map(|market| {
            ["1d", "1h"].map(|interval| Instrument {
                market,
                symbol: "BTCUSDT".into(),
                interval: Interval::parse(interval).unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(8).unwrap(),
            })
        })
        .collect();
    let engine = Engine::new(
        Catalog::new(instruments).unwrap(),
        Arc::new(FixedClock),
        Settings::default(),
    );
    for (id, slot) in engine.catalog.slots().iter().enumerate() {
        if !populate {
            continue;
        }
        let open = slot.interval.boundary(NOW);
        let close = if slot.interval.code() == "1h" {
            101.25
        } else {
            88.0
        };
        engine
            .commit(
                id,
                Update {
                    bar: Bar {
                        open_time: open,
                        close_time: open + slot.interval.millis() - 1,
                        values: [close; 8],
                        ..Bar::default()
                    },
                    closed: false,
                    source: Source::Stream,
                    event_time: Some(NOW),
                    sequence: 1,
                },
            )
            .unwrap();
    }
    Fixture {
        app: http::App::new(
            engine,
            RestApi::new(&base, &base, 6000).unwrap(),
            Config::default(),
        ),
        mock,
        server,
    }
}
async fn get(app: &Arc<http::App>, path: &str) -> (u16, Value) {
    let response = http::router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn legal_empty_price_responses_use_last_in_memory_close_like_java() {
    let f = fixture().await;
    for (market, prefix) in [(Market::Future, "/fapi/v1"), (Market::Spot, "/api/v3")] {
        let mut expected = json!({"symbol":"BTCUSDT","price":"101.25"});
        if market == Market::Future {
            expected["time"] = NOW.into();
        }
        assert_eq!(
            get(&f.app, &format!("{prefix}/ticker/price")).await,
            (200, json!([expected])),
            "{market:?}: a non-null empty list uses the current Kline close"
        );
    }
    assert_eq!(
        get(
            &f.app,
            "/api/v3/ticker/price?symbols=%5B%22ETHUSDT%22,%22BTCUSDT%22%5D"
        )
        .await,
        (200, json!([{"symbol":"BTCUSDT","price":"101.25"}])),
        "empty multi-symbol response skips a known symbol without a local bar"
    );
}

#[tokio::test]
async fn legal_empty_ticker_lists_use_previous_cache_like_java() {
    let f = fixture().await;
    f.mock.mode.store(1, Ordering::Release);
    for market in [Market::Future, Market::Spot] {
        f.app.tickers.refresh(market).await.unwrap();
    }
    f.mock.mode.store(0, Ordering::Release);
    for market in [Market::Future, Market::Spot] {
        f.app.tickers.refresh(market).await.unwrap();
    }
    let expected =
        json!({"symbol":"BTCUSDT","lastPrice":"99.27000000","closeTime":123,"volume":"5.0000"});
    for prefix in ["/fapi/v1", "/api/v3"] {
        assert_eq!(
            get(&f.app, &format!("{prefix}/ticker/24hr")).await,
            (200, json!([expected.clone()]))
        );
    }
    assert_eq!(
        get(
            &f.app,
            "/api/v3/ticker/24hr?symbols=%5B%22ETHUSDT%22,%22BTCUSDT%22%5D"
        )
        .await,
        (200, json!([expected]))
    );
}

#[tokio::test]
async fn malformed_upstream_is_not_silently_replaced_with_cached_data() {
    for mode in [2, 4] {
        let f = fixture_with_history(false).await;
        f.mock.mode.store(1, Ordering::Release);
        f.app.tickers.refresh(Market::Future).await.unwrap();
        f.mock.mode.store(mode, Ordering::Release);
        for endpoint in ["price", "24hr"] {
            let (status, body) = get(
                &f.app,
                &format!("/fapi/v1/ticker/{endpoint}?symbol=BTCUSDT"),
            )
            .await;
            assert_eq!(status, 502);
            assert_eq!(body["code"], -1001);
        }
    }
}

#[tokio::test]
async fn cached_empty_upstream_does_not_freeze_the_kline_fallback() {
    let f = fixture().await;
    let path = "/fapi/v1/ticker/price";
    assert_eq!(get(&f.app, path).await.1[0]["price"], "101.25");
    let id = f
        .app
        .engine
        .catalog
        .find(Market::Future, Interval::parse("1h").unwrap(), "BTCUSDT")
        .unwrap();
    let (mut bar, _) = f.app.engine.catalog.slot(id).latest().unwrap();
    bar.values[3] = 102.5;
    f.app
        .engine
        .commit(
            id,
            Update {
                bar,
                closed: false,
                source: Source::Stream,
                event_time: Some(NOW + 1),
                sequence: 2,
            },
        )
        .unwrap();
    assert_eq!(get(&f.app, path).await.1[0]["price"], "102.5");
}

#[tokio::test]
async fn null_responses_preserve_upstream_errors_when_local_prices_are_missing() {
    let f = fixture_with_history(false).await;
    f.mock.mode.store(3, Ordering::Release);
    for prefix in ["/fapi/v1", "/api/v3"] {
        for endpoint in ["price", "24hr"] {
            for suffix in ["", "?symbol=BTCUSDT", "?symbol=ETHUSDT"] {
                let (status, body) =
                    get(&f.app, &format!("{prefix}/ticker/{endpoint}{suffix}")).await;
                assert_eq!(status, 502, "{prefix}/{endpoint}{suffix}");
                assert_eq!(body["code"], -1000);
                assert_eq!(body["msg"], "body from call is null.");
            }
        }
    }
}

#[tokio::test]
async fn metadata_integer_and_long_strings_match_java_dto_types() {
    let f = fixture().await;
    *f.mock.metadata.lock().unwrap() = Some(json!({
        "rateLimits":[{"intervalNum":"1","limit":"2400"}],
        "symbols":[{"symbol":"BTCUSDT","status":"TRADING","pricePrecision":"8",
            "onboardDate":"1569398400000","filters":[{"filterType":"PERCENT_PRICE",
                "multiplierDecimal":"4","limit":"12"}]}]
    }));
    let (status, body) = get(&f.app, "/fapi/v1/exchangeInfo").await;
    assert_eq!(status, 200);
    assert_eq!(body["symbols"][0]["filters"][0]["multiplierDecimal"], 4);
    assert_eq!(body["symbols"][0]["filters"][0]["limit"], 12);
    assert_eq!(body["symbols"][0]["pricePrecision"], 8);
    assert_eq!(body["symbols"][0]["onboardDate"], 1_569_398_400_000_i64);
    assert_eq!(body["rateLimits"][0]["intervalNum"], 1);
    assert_eq!(body["rateLimits"][0]["limit"], 2400);
}

#[tokio::test]
async fn metadata_boolean_strings_and_integers_match_java_dto_types() {
    let f = fixture().await;
    *f.mock.metadata.lock().unwrap() = Some(json!({"symbols":[{
        "symbol":"BTCUSDT","status":"TRADING","icebergAllowed":"true",
        "ocoAllowed":"False","isSpotTradingAllowed":2,"isMarginTradingAllowed":0,
        "filters":[{"filterType":"NOTIONAL","applyMinToMarket":"TRUE","applyMaxToMarket":"FALSE"}]
    }]}));
    let (status, body) = get(&f.app, "/api/v3/exchangeInfo").await;
    assert_eq!(status, 200);
    let symbol = &body["symbols"][0];
    assert_eq!(symbol["icebergAllowed"], true);
    assert_eq!(symbol["ocoAllowed"], false);
    assert_eq!(symbol["isSpotTradingAllowed"], true);
    assert_eq!(symbol["isMarginTradingAllowed"], false);
    assert_eq!(symbol["filters"][0]["applyMinToMarket"], true);
    assert_eq!(symbol["filters"][0]["applyMaxToMarket"], false);
}

#[tokio::test]
async fn metadata_null_coercions_are_omitted_and_scalar_strings_are_typed() {
    let f = fixture().await;
    *f.mock.metadata.lock().unwrap() = Some(json!({"symbols":[{
        "symbol":123,"status":true,"baseAssetPrecision":"null",
        "quoteAssetPrecision":"","icebergAllowed":"null","ocoAllowed":"",
        "permissions":["SPOT",123,true,null]
    }]}));
    let (status, body) = get(&f.app, "/api/v3/exchangeInfo").await;
    assert_eq!(status, 200);
    assert_eq!(
        body["symbols"][0],
        json!({
            "symbol":"123","status":"true","permissions":["SPOT","123","true",null]
        })
    );
}

#[tokio::test]
async fn metadata_float_integer_conversion_preserves_java_bounds() {
    let f = fixture().await;
    *f.mock.metadata.lock().unwrap() = Some(json!({"symbols":[{
        "symbol":"BTCUSDT","status":"TRADING","pricePrecision":4.9,"onboardDate":123.9
    }]}));
    let (status, body) = get(&f.app, "/fapi/v1/exchangeInfo").await;
    assert_eq!(status, 200);
    assert_eq!(body["symbols"][0]["pricePrecision"], 4);
    assert_eq!(body["symbols"][0]["onboardDate"], 123);

    let f = fixture().await;
    *f.mock.metadata.lock().unwrap() = Some(json!({"symbols":[{
        "symbol":"BTCUSDT","pricePrecision":2_147_483_647.9
    }]}));
    assert_eq!(get(&f.app, "/fapi/v1/exchangeInfo").await.0, 502);
}

#[tokio::test]
async fn invalid_typed_metadata_is_rejected_instead_of_published() {
    for symbol in [
        json!({"symbol":"BTCUSDT","baseAssetPrecision":"nope"}),
        json!({"symbol":"BTCUSDT","baseAssetPrecision":"2147483648"}),
        json!({"symbol":"BTCUSDT","icebergAllowed":"tRuE"}),
        json!({"symbol":"BTCUSDT","icebergAllowed":1.0}),
        json!({"symbol":{},"status":"TRADING"}),
    ] {
        let f = fixture().await;
        *f.mock.metadata.lock().unwrap() = Some(json!({"symbols":[symbol]}));
        let (status, body) = get(&f.app, "/api/v3/exchangeInfo").await;
        assert_eq!(
            status, 502,
            "malformed metadata must not become a valid snapshot: {body}"
        );
        assert_eq!(body["code"], -1001);
    }
}

#[tokio::test]
async fn ticker_dto_scalar_coercions_match_java_conversion() {
    let f = fixture_with_history(false).await;
    *f.mock.ticker.lock().unwrap() = Some(json!({
        "symbol":123,"price":12.5,"time":" 456 ","lastPrice":" 1.20 ",
        "openPrice":"null","volume":"","closeTime":"123","count":1.9
    }));
    assert_eq!(
        get(&f.app, "/fapi/v1/ticker/price?symbol=BTCUSDT").await,
        (200, json!({"symbol":"123","price":"12.5","time":456}))
    );
    assert_eq!(
        get(&f.app, "/api/v3/ticker/24hr?symbol=BTCUSDT").await,
        (
            200,
            json!({"symbol":"123","lastPrice":"1.20","closeTime":123,"count":1})
        )
    );
    let f = fixture_with_history(false).await;
    *f.mock.ticker.lock().unwrap() = Some(json!({"symbol":"BTCUSDT","price":"1.20"}));
    assert_eq!(
        get(&f.app, "/fapi/v1/ticker/price?symbol=BTCUSDT").await,
        (200, json!({"symbol":"BTCUSDT","price":"1.20","time":0}))
    );
}

#[tokio::test]
async fn ticker_dto_errors_are_io_but_null_price_conversion_is_internal() {
    for (endpoint, row, expected_status, expected_code) in [
        (
            "24hr",
            json!({"symbol":"BTCUSDT","closeTime":"not-an-integer"}),
            502,
            -1001,
        ),
        (
            "24hr",
            json!({"symbol":"BTCUSDT","lastPrice":true}),
            502,
            -1001,
        ),
        (
            "price",
            json!({"symbol":"BTCUSDT","price":false}),
            502,
            -1001,
        ),
        (
            "price",
            json!({"symbol":"BTCUSDT","price":null}),
            500,
            -1000,
        ),
        ("price", json!({"symbol":"BTCUSDT","price":" "}), 500, -1000),
    ] {
        let f = fixture_with_history(false).await;
        *f.mock.ticker.lock().unwrap() = Some(row);
        let (status, body) =
            get(&f.app, &format!("/api/v3/ticker/{endpoint}?symbol=BTCUSDT")).await;
        assert_eq!(status, expected_status, "{body}");
        assert_eq!(body["code"], expected_code, "{body}");
    }
}

#[tokio::test]
async fn metadata_nullable_fields_and_wrong_dto_shapes_match_java() {
    for raw in [json!({}), json!({"symbols":null})] {
        let f = fixture().await;
        *f.mock.metadata.lock().unwrap() = Some(raw);
        assert_eq!(
            get(&f.app, "/api/v3/exchangeInfo").await,
            (200, json!({"serverTime":NOW}))
        );
    }
    for raw in [
        json!("object expected"),
        json!([]),
        json!({"symbols":["object expected"]}),
    ] {
        let f = fixture().await;
        *f.mock.metadata.lock().unwrap() = Some(raw);
        let (status, body) = get(&f.app, "/api/v3/exchangeInfo").await;
        assert_eq!(status, 502, "{body}");
        assert_eq!(body["code"], -1001);
    }
}
