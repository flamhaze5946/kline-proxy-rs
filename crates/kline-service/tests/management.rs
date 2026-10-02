use axum::{
    Json, Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
    response::IntoResponse,
};
use http_body_util::BodyExt;
use kline_core::{Interval, Market};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, sync::Arc};
use tower::ServiceExt;

fn router() -> Router {
    let catalog = Catalog::new(vec![Instrument {
        market: Market::Future,
        symbol: "BTCUSDT".into(),
        interval: Interval::parse("1h").unwrap(),
        trading: true,
        continuous: None,
        capacity: NonZeroUsize::new(10).unwrap(),
    }])
    .unwrap();
    kline_service::http::router(Engine::new(
        catalog,
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    ))
}
async fn request(
    app: &Router,
    method: &str,
    path: &str,
    accept: Option<&str>,
) -> (u16, axum::http::HeaderMap, Vec<u8>) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "market.test");
    if let Some(a) = accept {
        req = req.header("accept", a);
    }
    let result = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = result.status().as_u16();
    let headers = result.headers().clone();
    let body = result
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, body)
}
#[tokio::test]
async fn actuator_negotiation_and_component_visibility_match_boot_probe() {
    let app = router();
    for (accept, expected) in [
        (None, "application/vnd.spring-boot.actuator.v3+json"),
        (Some("application/json"), "application/json"),
        (
            Some("application/vnd.spring-boot.actuator.v2+json"),
            "application/vnd.spring-boot.actuator.v2+json",
        ),
        (
            Some("application/*+json"),
            "application/vnd.spring-boot.actuator.v3+json",
        ),
        (
            Some("application/json;q=0.9,application/vnd.spring-boot.actuator.v3+json;q=0.1"),
            "application/json",
        ),
    ] {
        let (status, headers, body) = request(&app, "GET", "/actuator/info", accept).await;
        assert_eq!(status, 200);
        assert_eq!(headers["content-type"], expected);
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), json!({}));
    }
    let (_, _, body) = request(&app, "GET", "/actuator", None).await;
    let links = &serde_json::from_slice::<Value>(&body).unwrap()["_links"];
    assert_eq!(links["self"]["href"], "http://market.test/actuator");
    assert_eq!(links["health-path"]["templated"], true);
    for path in [
        "/actuator/health/ping",
        "/actuator/health/diskSpace",
        "/actuator/health/readiness",
        "/actuator/health/a/b",
    ] {
        let (status, headers, body) = request(&app, "GET", path, None).await;
        assert_eq!(status, 404);
        assert!(!headers.contains_key("content-type"));
        assert!(body.is_empty());
    }
    let (status, headers, body) = request(&app, "GET", "/actuator/info", Some("text/plain")).await;
    assert_eq!(status, 406);
    assert!(!headers.contains_key("content-type"));
    assert!(body.is_empty());
    assert_eq!(
        request(&app, "GET", "/actuator/info", Some("garbage"))
            .await
            .0,
        500
    );
}
#[tokio::test]
async fn native_process_health_does_not_claim_market_readiness() {
    let app = router();
    let (status, _, body) = request(&app, "GET", "/health/ready", None).await;
    assert_eq!(status, 503);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["cache_populated"],
        false
    );
    let (status, headers, body) = request(&app, "GET", "/actuator/health", None).await;
    let expected = rustix::fs::statvfs(".")
        .ok()
        .is_some_and(|s| s.f_bavail.saturating_mul(s.f_frsize) >= 10 * 1024 * 1024);
    assert_eq!(status, if expected { 200 } else { 503 });
    assert_eq!(
        headers["content-type"],
        "application/vnd.spring-boot.actuator.v3+json"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["status"],
        if expected { "UP" } else { "DOWN" }
    );
}
#[tokio::test]
async fn methods_unknown_paths_and_direct_error_follow_framework_contract() {
    let app = router();
    for path in [
        "/actuator",
        "/actuator/info",
        "/actuator/health",
        "/actuator/health/ping",
        "/actuator/prometheus",
    ] {
        let (status, headers, body) = request(&app, "OPTIONS", path, None).await;
        assert_eq!(status, 200);
        assert_eq!(headers["allow"], "GET,HEAD,OPTIONS");
        assert!(body.is_empty());
        let (status, headers, body) = request(&app, "POST", path, None).await;
        assert!(!headers.contains_key("allow"));
        assert_eq!(status, 500);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["msg"],
            "Request method 'POST' is not supported"
        );
        assert!(request(&app, "HEAD", path, None).await.2.is_empty());
    }
    assert_eq!(
        request(&app, "OPTIONS", "/fapi/v1/klines/bulk", None)
            .await
            .1["allow"],
        "GET,HEAD,POST,OPTIONS"
    );
    let (status, _, body) = request(&app, "GET", "/not-found", None).await;
    assert_eq!(status, 500);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"code":-1000,"msg":"No static resource not-found."})
    );
    assert!(request(&app, "HEAD", "/not-found", None).await.2.is_empty());
    let (status, _, body) = request(&app, "GET", "/error", None).await;
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, 500);
    assert_eq!(json["status"], 999);
    assert_eq!(json["error"], "None");
    assert!(json["timestamp"].is_i64());
}
#[tokio::test]
async fn exporter_filters_real_samples_and_supports_openmetrics() {
    let app = router();
    let (status, headers, body) = request(
        &app,
        "GET",
        "/actuator/prometheus?includedNames=kline_frames_total",
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        headers["content-type"],
        "text/plain;version=0.0.4;charset=utf-8"
    );
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("kline_frames_total 0"));
    assert!(!text.contains("kline_finals_total"));
    let (_, headers, body) = request(
        &app,
        "GET",
        "/actuator/prometheus?includedNames=kline_frames_total",
        Some("text/plain;q=0.9,application/openmetrics-text;q=0.1"),
    )
    .await;
    assert_eq!(
        headers["content-type"],
        "application/openmetrics-text;version=1.0.0;charset=utf-8"
    );
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("# TYPE kline_frames counter\n"));
    assert!(text.ends_with("# EOF\n"));
    assert_eq!(
        request(
            &app,
            "GET",
            "/actuator/prometheus",
            Some("text/plain;version=9")
        )
        .await
        .0,
        406
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/actuator/prometheus",
            Some("application/json")
        )
        .await
        .0,
        500
    );
    assert!(
        request(
            &app,
            "GET",
            "/actuator/prometheus?includedNames=jvm_memory_used_bytes",
            None
        )
        .await
        .2
        .is_empty()
    );
    assert!(
        !request(&app, "GET", "/actuator/prometheus?includedNames=", None)
            .await
            .2
            .is_empty()
    );
    assert!(
        request(&app, "GET", "/actuator/prometheus?includedNames=%20", None)
            .await
            .2
            .is_empty()
    );
    let (_,_,body)=request(&app,"GET","/actuator/prometheus?includedNames=kline_frames_total,kline_finals_total&includedNames=kline_cache_bytes",None).await;
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("kline_cache_bytes"));
    assert!(!text.contains("kline_frames_total"));
}
#[tokio::test]
async fn bulk_get_and_post_missing_interval_keep_distinct_java_errors() {
    let app = router();
    let (status, _, body) = request(&app, "GET", "/fapi/v1/klines/bulk", None).await;
    assert_eq!(status, 500);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["code"],
        -1000
    );
    let (status, _, body) = request(&app, "POST", "/fapi/v1/klines/bulk", None).await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["code"],
        -1102
    );
}

#[tokio::test]
async fn converter_media_and_exception_fallbacks_match_real_boot_probe() {
    use kline_service::management::{fixed_content_type, get, negotiate};
    let app = router().merge(
        Router::new()
            .route("/hello", get(|| async { "Hello World!" }))
            .route("/json", get(|| async { Json(json!({"value":1})) }))
            .route(
                "/fixed",
                get(|| async { fixed_content_type(Json(json!({"value":1})).into_response()) }),
            )
            .route(
                "/png",
                get(|| async { ([("content-type", "image/png")], vec![1_u8, 2, 3]) }),
            )
            .layer(middleware::from_fn(negotiate)),
    );
    for (path, accept, status, media) in [
        ("/hello", "application/json", 200, Some("application/json")),
        ("/hello", "text/html", 200, Some("text/html;charset=UTF-8")),
        (
            "/hello",
            "application/octet-stream",
            200,
            Some("text/plain;charset=UTF-8"),
        ),
        (
            "/json",
            "application/vnd.test+json",
            200,
            Some("application/vnd.test+json"),
        ),
        ("/json", "text/plain", 406, None),
        ("/json", "garbage", 500, None),
        ("/json", "application/json;charset=ISO-8859-1", 406, None),
        ("/fixed", "garbage", 200, Some("application/json")),
        ("/png", "application/json", 200, Some("image/png")),
        ("/fapi/v1/klines/bulk", "text/plain", 400, None),
    ] {
        let (s, headers, _) = request(&app, "GET", path, Some(accept)).await;
        assert_eq!(s, status, "{path} {accept}");
        assert_eq!(
            headers.get("content-type").map(|h| h.to_str().unwrap()),
            media
        );
    }
    // Explicit ApiException and a missing Spring parameter have different native
    // fallback statuses when their error JSON cannot be negotiated.
    assert_eq!(
        request(&app, "POST", "/fapi/v1/klines/bulk", Some("text/plain"))
            .await
            .0,
        500
    );
    assert_eq!(
        request(&app, "POST", "/fapi/v1/klines/bulk", Some("garbage"))
            .await
            .0,
        400
    );
    assert_eq!(request(&app, "GET", "/actuator/health/", None).await.0, 200);
    assert_eq!(
        request(&app, "GET", "/actuator/health//", None).await.0,
        200
    );
    for path in ["/hello", "/json", "/actuator/health", "/fixed", "/png"] {
        for accept in [None, Some("application/json;charset=UTF-16LE")] {
            let (status, headers, body) = request(&app, "HEAD", path, accept).await;
            assert_eq!(status, 200);
            assert!(body.is_empty());
            assert!(!headers.contains_key("content-length"), "{path} {accept:?}");
        }
    }
}

#[tokio::test]
async fn alternate_charset_streams_across_utf8_boundaries_without_eager_collection() {
    use kline_service::management::{get, negotiate};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let polls = Arc::new(AtomicUsize::new(0));
    let shared = polls.clone();
    let app = Router::new()
        .route(
            "/stream",
            get(move || {
                let polls = shared.clone();
                async move {
                    let parts = [vec![b'"', 0xe4], vec![0xbd], vec![0xa0, b'"']];
                    let stream = futures_util::stream::iter(parts.into_iter().map(move |part| {
                        polls.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, std::convert::Infallible>(part)
                    }));
                    (
                        [("content-type", "application/json")],
                        Body::from_stream(stream),
                    )
                }
            }),
        )
        .layer(middleware::from_fn(negotiate));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/stream")
                .header("accept", "application/json;charset=UTF-16LE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], &[b'"', 0, 0x60, 0x4f, b'"', 0]);
    assert_eq!(polls.load(Ordering::Relaxed), 3);
}
