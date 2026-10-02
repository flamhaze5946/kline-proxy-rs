//! Java-facing funding/CMS/statistic contracts and upstream forwarding regressions.
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use kline_core::{Bar, Interval, Market};
use kline_market::{
    config::Config,
    funding::{H, Query as FundingQuery},
    http::{self, App},
    statistics,
    transport::RestApi,
};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use serde_json::{Value, json};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};
use tokio::sync::Mutex;
use tower::ServiceExt;

#[derive(Default)]
struct Mock {
    calls: Mutex<Vec<(String, BTreeMap<String, String>)>>,
    funding: Mutex<Value>,
    premium: Mutex<Value>,
}
async fn upstream(
    State(mock): State<Arc<Mock>>,
    uri: Uri,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    mock.calls.lock().await.push((uri.path().into(), q));
    let value = if uri.path().ends_with("exchangeInfo") {
        json!({"timezone":"UTC","symbols":[
            {"symbol":"BTCUSDT","status":"TRADING","quoteAsset":"USDT"},
            {"symbol":"ETHUSDT","status":"TRADING","quoteAsset":"USDT"}
        ]})
    } else if uri.path().ends_with("fundingRate") {
        mock.funding.lock().await.clone()
    } else if uri.path().ends_with("premiumIndex") {
        mock.premium.lock().await.clone()
    } else if uri.path().contains("/cms/") {
        json!({"code":"000000","success":true,"data":{"total":0,"articles":[]}})
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
    let mock = Arc::new(Mock::default());
    *mock.funding.lock().await = json!([]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(upstream).with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let catalog = Catalog::new(vec![Instrument {
        market: Market::Future,
        symbol: "BTCUSDT".into(),
        interval: Interval::parse("1h").unwrap(),
        trading: true,
        continuous: None,
        capacity: NonZeroUsize::new(10).unwrap(),
    }])
    .unwrap();
    let engine = Engine::new(
        catalog,
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    );
    let config = Config {
        cms_url: root.clone(),
        ..Config::default()
    };
    Fixture {
        app: App::new(engine, RestApi::new(&root, &root, 6000).unwrap(), config),
        mock,
        server,
    }
}
async fn get(f: &Fixture, path: &str) -> (StatusCode, Value) {
    let response = http::router(f.app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn ordinary_funding_preserves_signed_integer_limit_and_empty_arrays() {
    let f = fixture().await;
    for limit in ["-1", "0", "2147483647"] {
        let (status, value) = get(
            &f,
            &format!("/fapi/v1/fundingRate?symbol=BTCUSDT&limit={limit}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, json!([]));
        let calls = f.mock.calls.lock().await;
        let (_, args) = calls.last().unwrap();
        assert_eq!(args["limit"], limit);
    }
    let (status, _) = get(&f, "/fapi/v1/fundingRate?limit=2147483648").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let (status, _) = get(&f, "/fapi/v1/fundingRate?limit=-2147483649").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    *f.mock.funding.lock().await = Value::Null;
    let (status, value) = get(&f, "/fapi/v1/fundingRate").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(value["code"], -1000);
}

#[tokio::test]
async fn funding_window_keeps_newest_rows_and_excludes_end_boundary() {
    let f = fixture().await;
    *f.mock.funding.lock().await = json!([
        {"symbol":"BTCUSDT","fundingTime":H+2,"fundingRate":"2.00E-4","markPrice":"100.500"},
        {"symbol":"BTCUSDT","fundingTime":H,"fundingRate":"1.00E-4"},
        {"symbol":"BTCUSDT","fundingTime":H+3,"fundingRate":"3.00E-4"},
        {"symbol":"BTCUSDT","fundingTime":2*H,"fundingRate":"9.00E-4"},
        {"symbol":"BTCUSDT","fundingRate":"8.00E-4"},
        {"symbol":" ","fundingTime":H+1,"fundingRate":"7.00E-4"},
        {"symbol":"DELISTEDUSDT","fundingTime":H+1,"fundingRate":"4.00E-4"}
    ]);
    let result = f
        .app
        .funding
        .bulk(FundingQuery {
            since_ms: Some(H),
            until_ms: Some(H + 3),
            limit: Some(1),
            ..FundingQuery::default()
        })
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(
        value["fundingRates"],
        json!({
            "BTCUSDT":[{"symbol":"BTCUSDT","fundingTime":H+2,"fundingRate":"0.000200","markPrice":"100.500"}],
            "DELISTEDUSDT":[{"symbol":"DELISTEDUSDT","fundingTime":H+1,"fundingRate":"0.000400"}]
        })
    );
    let calls = f.mock.calls.lock().await;
    assert_eq!(
        calls.len(),
        1,
        "All-symbol bounded history uses the hour chunk without metadata filtering"
    );
    assert_eq!(calls[0].1["startTime"], H.to_string());
    assert_eq!(calls[0].1["endTime"], (2 * H).to_string());
    assert_eq!(calls[0].1["limit"], "1000");
}

#[tokio::test]
async fn pathological_funding_windows_fail_before_consuming_upstream_capacity() {
    let f = fixture().await;
    for (start, end) in [(i64::MIN + H, 0), (H, H * 1002)] {
        let error = f
            .app
            .funding
            .bulk(FundingQuery {
                since_ms: Some(start),
                until_ms: Some(end),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!((error.status, error.code), (400, -1130));
    }
    assert!(f.mock.calls.lock().await.is_empty());
}

#[tokio::test]
async fn funding_body_symbols_follow_jackson_scalars_and_java_whitespace() {
    let f = fixture().await;
    let query: FundingQuery = serde_json::from_value(json!({
        "symbols": [123, true, null, "\u{00a0}", " BTCUSDT ", "\u{3000}"],
        "since_ms": H,
        "until_ms": H + 1
    }))
    .unwrap();
    let result: Value = serde_json::from_slice(&f.app.funding.bulk(query).await.unwrap()).unwrap();
    assert_eq!(
        result["fundingRates"],
        json!({"123": [], "true": [], "\u{00a0}": [], "BTCUSDT": []})
    );
}

#[tokio::test]
async fn premium_null_payload_is_an_error_but_null_rows_keep_java_empty_shape() {
    let f = fixture().await;
    for path in [
        "/fapi/v1/premiumIndex",
        "/fapi/v1/premiumIndex?symbol=BTCUSDT",
    ] {
        let (status, value) = get(&f, path).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(value["code"], -1000);
    }
    *f.mock.premium.lock().await =
        json!([null, {"symbol":"BTCUSDT","markPrice":"1.2300E2","unknown":5}]);
    assert_eq!(
        get(&f, "/fapi/v1/premiumIndex").await,
        (
            StatusCode::OK,
            json!([[], {"symbol":"BTCUSDT","markPrice":"123.00"}])
        )
    );
}

#[tokio::test]
async fn cms_integer_binding_normalizes_values_and_rejects_bad_input_before_upstream() {
    let f = fixture().await;
    let path = "/bapi/composite/v1/public/cms/article/catalog/list/query";
    let (status, _) = get(
        &f,
        &format!("{path}?catalogId=&pageNo=%2B001&pageSize=%20-2%20"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let calls = f.mock.calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1["catalogId"], "");
    assert_eq!(calls[0].1["pageNo"], "1");
    assert_eq!(calls[0].1["pageSize"], "-2");
    drop(calls);
    for bad in ["", "bad", "1.5", "2147483648"] {
        let (status, _) = get(&f, &format!("{path}?catalogId=48&pageNo={bad}&pageSize=10")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{bad}");
    }
    assert_eq!(f.mock.calls.lock().await.len(), 1);
}

#[test]
fn statistics_sparse_series_and_missing_btc_keep_java_denominators() {
    let bar = |open_time, open, volume| Bar {
        open_time,
        values: [open, open, open, open, 1.0, volume, 1.0, 1.0],
        ..Bar::default()
    };
    let (a, b) = statistics::calculate(
        &[
            (
                "ETHUSDT".into(),
                vec![bar(1, 10.0, 100.0), bar(2, 15.0, 100.0)],
            ),
            ("BTCUSDT".into(), vec![bar(2, 10.0, 50.0)]),
        ],
        2,
        2,
        2,
    )
    .unwrap();
    assert_eq!(a, BTreeMap::from([(1, 1.0), (2, 1.0)]));
    assert_eq!(b, BTreeMap::from([(1, 0.0), (2, 0.25)]));
    assert_eq!(
        statistics::calculate(&[], 2, 2, 2).unwrap(),
        (BTreeMap::new(), BTreeMap::new())
    );
}

#[test]
fn statistics_values_match_fresh_java_oracle_across_sparse_and_tied_series() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/statistics-data-cases.json")).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/statistics-data-java.json")).unwrap();
    let mut differences = vec![];
    for case in cases.as_array().unwrap() {
        let rows: Vec<_> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|series| {
                let bars = series["bars"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| Bar {
                        open_time: row["t"].as_i64().unwrap(),
                        exact: Some(Arc::new(kline_core::ExactNumbers::Strings([
                            row["open"].as_str().unwrap().into(),
                            "0".into(),
                            "0".into(),
                            "0".into(),
                            "0".into(),
                            row["quote_volume"].as_str().unwrap().into(),
                            "0".into(),
                            "0".into(),
                        ]))),
                        ..Bar::default()
                    })
                    .collect();
                (series["symbol"].as_str().unwrap().into(), bars)
            })
            .collect();
        let (a, b) = statistics::calculate(
            &rows,
            case["days"].as_u64().unwrap() as usize,
            case["volume_days"].as_u64().unwrap() as usize,
            case["rank"].as_u64().unwrap() as usize,
        )
        .unwrap();
        let actual: Value = serde_json::from_slice(
            &serde_json::to_vec(&BTreeMap::from([("yama01", a), ("yama02", b)])).unwrap(),
        )
        .unwrap();
        let name = case["name"].as_str().unwrap();
        if actual != expected[name] {
            differences.push(json!({"case":name,"rust":actual,"java":expected[name]}));
        }
    }
    assert!(
        differences.is_empty(),
        "{}",
        serde_json::to_string_pretty(&differences).unwrap()
    );
}
