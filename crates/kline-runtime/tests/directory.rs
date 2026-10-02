use axum::{Json, Router, extract::State, routing::get};
use kline_core::{Interval, Market};
use kline_market::{metadata::Metadata, transport::RestApi};
use kline_runtime::{config::Config, directory};
use kline_service::{Engine, Settings, SystemClock};
use parking_lot::RwLock;
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn discovery_tracks_new_and_delisted_symbols_with_regex_and_capacity_overrides() {
    let exchange = Arc::new(RwLock::new(json!({"symbols":[
        {"symbol":"BTCUSDT","status":"TRADING","pair":"BTCUSDT","contractType":"PERPETUAL"},
        {"symbol":"BTCUSDC","status":"TRADING","pair":"BTCUSDC","contractType":"PERPETUAL"},
        {"symbol":"OLDUSDT","status":"BREAK","pair":"OLDUSDT","contractType":"PERPETUAL"}
    ]})));
    async fn metadata(State(s): State<Arc<RwLock<Value>>>) -> Json<Value> {
        Json(s.read().clone())
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/fapi/v1/exchangeInfo", get(metadata))
        .with_state(exchange.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let config:Config=serde_json::from_value(json!({"listen":"127.0.0.1:0","subscriptions":[{"market":"future","interval":"1h","continuous":true,"symbol_patterns":[".*?USDT"],"history_capacity":9000,"symbol_capacities":{"BTCUSDT":10000}}],"rest":{"future_url":root,"spot_url":root}})).unwrap();
    let engine = Engine::new(
        config.catalog().unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    );
    let metadata = Metadata::new(RestApi::new(&root, &root, 6000).unwrap(), 300);
    let initial = directory::discover(&config, &metadata).await.unwrap();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].capacity.get(), 10000);
    assert_eq!(initial[0].continuous.as_ref().unwrap().0, "BTCUSDT");
    engine.refresh_catalog(initial).unwrap();
    let btc = engine.catalog.slot(0);
    *exchange.write() = json!({"symbols":[
        {"symbol":"BTCUSDT","status":"TRADING","pair":"BTCUSDT","contractType":"PERPETUAL"},
        {"symbol":"NEWUSDT","status":"TRADING","pair":"NEWUSDT","contractType":"PERPETUAL"}
    ]});
    metadata.refresh(Market::Future).await.unwrap();
    let change = engine
        .refresh_catalog(directory::discover(&config, &metadata).await.unwrap())
        .unwrap();
    assert_eq!(change.added, 1);
    assert!(Arc::ptr_eq(&btc, &engine.catalog.slot(0)));
    exchange.write()["symbols"][0]["status"] = "BREAK".into();
    metadata.refresh(Market::Future).await.unwrap();
    let change = engine
        .refresh_catalog(directory::discover(&config, &metadata).await.unwrap())
        .unwrap();
    assert_eq!(change.removed, 1);
    assert!(!btc.is_tracked());
    assert!(
        engine
            .catalog
            .find(Market::Future, Interval::parse("1h").unwrap(), "BTCUSDT")
            .is_none()
    );
    task.abort();
}

#[test]
fn bad_dynamic_configuration_fails_before_network_or_allocation() {
    for subscription in [
        json!({"market":"future","interval":"1s"}),
        json!({"market":"spot","interval":"1h","continuous":true}),
        json!({"market":"spot","interval":"1h","symbol_patterns":["["]}),
        json!({"market":"spot","interval":"1h","symbol_capacities":{"BTCUSDT":1}}),
    ] {
        let config: Config = serde_json::from_value(
            json!({"listen":"127.0.0.1:0","subscriptions":[subscription],"rest":{}}),
        )
        .unwrap();
        assert!(config.catalog().is_err());
    }
}
