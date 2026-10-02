use axum::{
    Router,
    body::Body,
    http::{Request, Response},
};
use http_body_util::BodyExt;
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_market::{
    config::Config,
    http::{self, App},
    transport::RestApi,
};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use std::{num::NonZeroUsize, sync::Arc, sync::atomic::Ordering, time::Duration};
use tokio::time::Instant;
use tower::ServiceExt;

const H: i64 = 3_600_000;
/// One second into an hour, so a closed_only bulk request waits for the previous hour's final.
struct AfterBoundary;
impl Clock for AfterBoundary {
    fn now_ms(&self) -> i64 {
        H + 1_000
    }
}
fn app(queue: usize, wait_ms: u64) -> Arc<App> {
    let engine = Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Future,
            symbol: "BTCUSDT".into(),
            interval: Interval::parse("1h").unwrap(),
            trading: true,
            continuous: None,
            capacity: NonZeroUsize::new(10).unwrap(),
        }])
        .unwrap(),
        Arc::new(AfterBoundary),
        Settings {
            http_concurrency_limit: 1,
            http_admission_queue: queue,
            http_admission_wait_ms: wait_ms,
            ..Settings::default()
        },
    );
    // Nothing listens on port 9: the routes used below never reach the upstream.
    let api = RestApi::new("http://127.0.0.1:9", "http://127.0.0.1:9", 6000).unwrap();
    App::new(engine, api, Config::default())
}
fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}
fn hello() -> Request<Body> {
    get("/hello/helloWorld")
}
async fn body(response: Response<Body>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}
fn counts(app: &App) -> (u64, u64) {
    let m = &app.engine.metrics;
    (
        m.http_admission_queued.load(Ordering::Relaxed),
        m.http_admission_rejected.load(Ordering::Relaxed),
    )
}
/// Yields (paused time does not move) until `done`; fails instead of hanging.
async fn settle(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not reached");
}

#[tokio::test(start_paused = true)]
async fn a_full_market_route_group_queues_instead_of_rejecting() {
    let app = app(8, 5_000);
    let router = http::router(app.clone());
    let held = app
        .admission()
        .enter(&app.engine.metrics, Instant::now(), None)
        .await
        .unwrap();
    let queued = tokio::spawn(router.clone().oneshot(hello()));
    settle(|| app.admission().waiting() == 1).await;
    assert!(!queued.is_finished());
    drop(held);
    let response = queued.await.unwrap().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(body(response).await, b"Hello World!");
    assert_eq!(counts(&app), (1, 0));
}

#[tokio::test]
async fn a_full_market_route_group_without_a_queue_answers_503_1008() {
    let app = app(0, 5_000);
    let router = http::router(app.clone());
    let _held = app
        .admission()
        .enter(&app.engine.metrics, Instant::now(), None)
        .await
        .unwrap();
    let response = router.oneshot(hello()).await.unwrap();
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert_eq!(body["code"], -1008);
    assert_eq!(counts(&app), (0, 1));
}

#[tokio::test(start_paused = true)]
async fn a_market_query_never_queues_past_its_five_second_budget() {
    let app = app(8, 8_000);
    let router = http::router(app.clone());
    let _held = app
        .admission()
        .enter(&app.engine.metrics, Instant::now(), None)
        .await
        .unwrap();
    // Only ticker queries have a budget of their own; everything else (exchange metadata, still
    // to load here, included) waits the group's full eight seconds.
    let routes = [
        ("/fapi/v1/ticker/price?symbol=BTCUSDT", 5),
        ("/api/v3/ticker/24hr?symbol=BTCUSDT", 5),
        ("/api/v3/exchangeInfo", 8),
        ("/fapi/v1/premiumIndex?symbol=BTCUSDT", 8),
        ("/api/v3/time", 8),
        ("/hello/helloWorld", 8),
    ];
    for (uri, waited) in routes {
        let started = Instant::now();
        let response = router.clone().oneshot(get(uri)).await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(waited), "{uri}");
        assert_eq!(response.status(), 503, "{uri}");
        let body: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
        assert_eq!(body["msg"], "Request capacity exceeded", "{uri}");
    }
    let n = routes.len() as u64;
    assert_eq!(counts(&app), (n, n));
}

#[tokio::test]
async fn market_health_bypasses_a_full_route_group() {
    let app = app(0, 5_000);
    let router = http::router(app.clone());
    let _held = app
        .admission()
        .enter(&app.engine.metrics, Instant::now(), None)
        .await
        .unwrap();
    assert_eq!(router.clone().oneshot(hello()).await.unwrap().status(), 503);
    let response = router.oneshot(get("/health/market")).await.unwrap();
    assert_eq!(response.status(), 200);
    let status: serde_json::Value = serde_json::from_slice(&body(response).await).unwrap();
    assert!(status.get("metadata").is_some());
    assert_eq!(counts(&app), (0, 1), "only /hello met admission");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_route_groups_do_not_share_admission() {
    let app = app(8, 5_000);
    let engine = app.engine.clone();
    let update = |closed| Update {
        bar: Bar {
            open_time: 0,
            close_time: H - 1,
            trades: 1,
            values: [100.; 8],
            ..Bar::default()
        },
        closed,
        source: Source::Stream,
        event_time: Some(1),
        sequence: u64::from(closed) + 1,
    };
    engine.commit(0, update(false)).unwrap();
    // The process serves both groups from one router, as kline-proxy does.
    let router: Router =
        kline_service::http::router(engine.clone()).merge(http::router(app.clone()));
    // The core group is full: a closed_only bulk request holds its only place until the final.
    let waiting = tokio::spawn(router.clone().oneshot(get(
        "/fapi/v1/klines/bulk?interval=1h&symbols=BTCUSDT&limit=1&closed_only=true",
    )));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while engine.cache_sizes().1 != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the bulk request never waited"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let response = router.clone().oneshot(hello()).await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "the market group has its own places"
    );
    engine.commit(0, update(true)).unwrap();
    assert_eq!(waiting.await.unwrap().unwrap().status(), 200);
    // The market group is full: the core group still serves at once.
    let _held = app
        .admission()
        .enter(&engine.metrics, Instant::now(), None)
        .await
        .unwrap();
    let response = router
        .clone()
        .oneshot(get(
            "/fapi/v1/klines/bulk?interval=1h&symbols=BTCUSDT&limit=1",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "the core group has its own places");
    assert_eq!(counts(&app), (0, 0), "nothing queued in either group");
}
