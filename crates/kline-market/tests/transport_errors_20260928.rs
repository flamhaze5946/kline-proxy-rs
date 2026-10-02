//! Actual Java ClientUtil/JacksonConverter outputs are the expected contracts.
use axum::{
    Router,
    body::Body,
    http::{Response, StatusCode},
};
use kline_core::Market;
use kline_market::{
    error::ApiError,
    funding::Rate,
    transport::{RestApi, decode_json},
};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Fixture {
    api: Arc<RestApi>,
    url: String,
    calls: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn fixture(status: u16, body: String) -> Fixture {
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback({
        let calls = calls.clone();
        move || {
            let calls = calls.clone();
            let body = body.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Response::builder()
                    .status(StatusCode::from_u16(status).unwrap())
                    .header("content-type", "application/json")
                    .header("retry-after", "1")
                    .body(Body::from(body))
                    .unwrap()
            }
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Fixture {
        api: RestApi::new(&url, &url, 6000).unwrap(),
        url,
        calls,
        server,
    }
}
fn compare(name: &str, actual: Result<Value, anyhow::Error>, expected: &Value) {
    if expected["status"] == 200 {
        assert_eq!(actual.unwrap(), expected["body"], "{name}");
    } else {
        let error = ApiError::from(actual.unwrap_err());
        assert_eq!(
            i64::from(error.status),
            expected["status"].as_i64().unwrap(),
            "{name}: {error:?}"
        );
        assert_eq!(
            error.code,
            expected["code"].as_i64().unwrap(),
            "{name}: {error:?}"
        );
        if error.code != -1001 {
            assert_eq!(error.message, expected["msg"].as_str().unwrap(), "{name}");
        } else {
            // Parser/runtime-specific IOException descriptions are not byte-identical.
            assert!(!error.message.is_empty(), "{name}");
        }
    }
}
#[tokio::test]
async fn json_and_external_errors_match_real_java_client_oracle() {
    let inputs: Value =
        serde_json::from_str(include_str!("fixtures/upstream-inputs.json")).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/upstream-java.json")).unwrap();
    for (input, expected) in inputs
        .as_array()
        .unwrap()
        .iter()
        .zip(expected.as_array().unwrap())
    {
        if input["disconnect"] == true {
            continue;
        }
        let name = input["name"].as_str().unwrap();
        let f = fixture(
            input["status"].as_u64().unwrap() as u16,
            input["body"].as_str().unwrap().into(),
        )
        .await;
        let typed = input["typed"] == true;
        let result = if typed {
            f.api
                .json::<Vec<Rate>>(Market::Future, "/payload", &[], 1)
                .await
                .map(|rows| serde_json::to_value(rows).unwrap())
        } else {
            f.api
                .json::<Value>(Market::Future, "/payload", &[], 1)
                .await
        };
        compare(name, result, expected);
        if input["status"] == 503 {
            assert_eq!(
                f.calls.load(Ordering::Relaxed),
                3,
                "Server-error retry behavior is retained"
            );
        }
        let result = match f
            .api
            .external(&format!("{}/payload", f.url), &[], 2 * 1024 * 1024)
            .await
        {
            Ok(bytes) if typed => {
                decode_json::<Vec<Rate>>(&bytes).map(|rows| serde_json::to_value(rows).unwrap())
            }
            Ok(bytes) => decode_json::<Value>(&bytes),
            Err(error) => Err(error),
        };
        compare(&format!("external {name}"), result, expected);
    }
}
#[tokio::test]
async fn null_and_invalid_large_json_keep_the_same_errors_on_cpu_workers() {
    for (suffix, code) in [("null", -1000), ("{", -1001)] {
        let f = fixture(200, format!("{}{suffix}", " ".repeat(70_000))).await;
        let error = ApiError::from(
            f.api
                .json::<Value>(Market::Future, "/payload", &[], 1)
                .await
                .unwrap_err(),
        );
        assert_eq!((error.status, error.code), (502, code));
    }
}
#[tokio::test]
async fn external_404_keeps_reqwest_status_for_missing_vision_archives() {
    let f = fixture(404, "No such archive".into()).await;
    let error = f
        .api
        .external(&format!("{}/archive.zip", f.url), &[], 1000)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status),
        Some(StatusCode::NOT_FOUND)
    );
    let error = ApiError::from(error);
    assert_eq!(
        (error.status, error.code, error.message.as_str()),
        (404, 404, "No such archive")
    );
}
#[tokio::test]
async fn connection_failure_uses_java_io_error_after_existing_retries() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        }
    });
    let api = RestApi::new(&url, &url, 6000).unwrap();
    let error = ApiError::from(
        api.json::<Value>(Market::Future, "/payload", &[], 1)
            .await
            .unwrap_err(),
    );
    server.abort();
    assert_eq!((error.status, error.code), (502, -1001));
}
#[tokio::test]
async fn rest_bar_null_and_malformed_json_are_classified_before_domain_validation() {
    for (body, code) in [("null", -1000), ("[", -1001)] {
        let f = fixture(200, body.into()).await;
        let error = ApiError::from(
            f.api
                .klines(
                    Market::Future,
                    "BTCUSDT",
                    kline_core::Interval::parse("1h").unwrap(),
                    None,
                    3_600_000,
                    1,
                )
                .await
                .unwrap_err(),
        );
        assert_eq!((error.status, error.code), (502, code));
    }
}
#[test]
fn nested_io_errors_preserve_typed_mapping_and_optional_targets_cannot_bypass_null() {
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "connection ended",
    ))
    .context("read upstream");
    assert_eq!(ApiError::from(error).code, -1001);
    let error = ApiError::from(decode_json::<Option<Value>>(b"null []").unwrap_err());
    assert_eq!(
        (error.status, error.code, error.message.as_str()),
        (502, -1000, "body from call is null.")
    );
    assert_eq!(
        decode_json::<Value>(b"\xef\xbb\xbf[] {}").unwrap(),
        serde_json::json!([])
    );
}
