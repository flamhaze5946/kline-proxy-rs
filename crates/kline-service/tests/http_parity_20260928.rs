use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, sync::Arc};
use tower::ServiceExt;

struct Fixed;
impl Clock for Fixed {
    fn now_ms(&self) -> i64 {
        18_000_001
    }
}
fn router() -> axum::Router {
    let engine = Engine::new(
        Catalog::new(
            ["BTCUSDT", "ETHUSDT"]
                .into_iter()
                .map(|s| Instrument {
                    market: Market::Future,
                    symbol: s.into(),
                    interval: Interval::parse("1h").unwrap(),
                    trading: true,
                    continuous: None,
                    capacity: NonZeroUsize::new(8).unwrap(),
                })
                .collect(),
        )
        .unwrap(),
        Arc::new(Fixed),
        Settings {
            final_wait_ms: 0,
            ..Settings::default()
        },
    );
    for id in 0..2 {
        for n in 0..6 {
            engine
                .commit(
                    id,
                    Update {
                        bar: Bar {
                            open_time: n * 3_600_000,
                            close_time: (n + 1) * 3_600_000 - 1,
                            values: [100.; 8],
                            ..Bar::default()
                        },
                        closed: n < 5,
                        source: Source::Stream,
                        event_time: Some(n),
                        sequence: n as u64,
                    },
                )
                .unwrap();
        }
    }
    kline_service::http::router(engine)
}
async fn request(app: &axum::Router, query: &str, body: Option<&str>) -> (u16, Value) {
    let request = Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(format!("/fapi/v1/klines/bulk{query}"))
        .header("content-type", "application/json")
        .body(Body::from(body.unwrap_or_default().to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn repeated_query_parameters_preserve_servlet_binding_and_symbol_selection() {
    let app = router();
    let (status, body) = request(&app, "?interval=1h&symbols=BTCUSDT&symbols=ETHUSDT&limit=2&limit=bad&closed_only=false&closed_only=true", None).await;
    assert_eq!(status, 200);
    assert_eq!(body["klines"].as_object().unwrap().len(), 2);
    assert_eq!(body["klines"]["BTCUSDT"].as_array().unwrap().len(), 2);
    assert_eq!(body["klines"]["BTCUSDT"][1][0], 18_000_000);
    assert_eq!(
        request(&app, "?interval=1h&limit=bad&limit=2", None)
            .await
            .0,
        500
    );
    assert_eq!(
        request(&app, "?interval=1h&interval=1d", None).await.1["code"],
        -1120
    );
}

#[tokio::test]
async fn spring_query_numbers_accept_hex_and_internal_whitespace_but_json_does_not() {
    let app = router();
    for value in ["0x2", "%232", "%202%20", "0%202"] {
        let (status, body) = request(
            &app,
            &format!("?interval=1h&symbols=BTCUSDT&limit={value}"),
            None,
        )
        .await;
        assert_eq!(status, 200, "{value}: {body}");
        assert_eq!(body["klines"]["BTCUSDT"].as_array().unwrap().len(), 2);
    }
    for value in ["0x2", "0 2"] {
        assert_eq!(
            request(
                &app,
                "",
                Some(&json!({"interval":"1h","limit":value}).to_string())
            )
            .await
            .0,
            500
        );
    }
    for value in [
        "%2B%2B0x2",
        "-%2B0x2",
        "--0x2",
        "%2B-0x2",
        "%2B0x2",
        "%2B%232",
    ] {
        assert_eq!(
            request(&app, &format!("?interval=1h&limit={value}"), None)
                .await
                .0,
            500
        );
    }
}

#[tokio::test]
async fn nonbreaking_space_symbols_do_not_expand_to_all_market() {
    let app = router();
    for query in [
        "?interval=1h&symbols=%C2%A0",
        "?interval=1h&symbols=%C2%A0BTCUSDT%C2%A0",
    ] {
        assert_eq!(request(&app, query, None).await.1["klines"], json!({}));
    }
    assert_eq!(
        request(&app, "", Some(r#"{"interval":"1h","symbols":["\u00a0"]}"#))
            .await
            .1["klines"],
        json!({})
    );
}

#[tokio::test]
async fn jackson_body_coercions_do_not_accept_arrays_or_overflowing_fractional_limits() {
    let app = router();
    let (_, body) = request(&app, "", Some(r#"{"interval":"bad","interval":"1h","symbols":[1,true,null,"BTCUSDT"],"limit":1,"limit":2}"#)).await;
    assert_eq!(body["klines"].as_object().unwrap().len(), 1);
    assert_eq!(body["klines"]["BTCUSDT"].as_array().unwrap().len(), 2);
    for value in [
        r#"["1h",2,true,[]]"#,
        r#"{"interval":"1h","limit":2147483647.9}"#,
        r#"{"interval":"1h","closed_only":1.0}"#,
    ] {
        assert_eq!(request(&app, "", Some(value)).await.0, 500, "{value}");
    }
    assert_eq!(
        request(&app, "", Some(r#"{"interval":1}"#)).await.1["code"],
        -1120
    );
    assert_eq!(request(&app, "?interval=", None).await.1["code"], -1120);
    assert_eq!(
        request(&app, "", Some(r#"{"interval":""}"#)).await.1["code"],
        -1102
    );
}

#[tokio::test]
async fn actuator_discovery_and_prometheus_name_filter_are_available() {
    let app = router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/actuator")
                .header("host", "mirror.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        body["_links"]["prometheus"]["href"],
        "http://mirror.test/actuator/prometheus"
    );
    for (query, expected) in [
        ("includedNames=kline_frames_total", "kline_frames_total"),
        ("includedNames=unknown", ""),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/actuator/prometheus?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        if expected.is_empty() {
            assert!(body.is_empty());
        } else {
            assert!(body.contains(expected));
            assert!(!body.contains("kline_cache"));
        }
    }
}
