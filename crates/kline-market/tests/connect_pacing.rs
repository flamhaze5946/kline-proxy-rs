use kline_core::{Interval, Market};
use kline_market::{metadata::Metadata, ticker::Tickers, transport::RestApi};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock, connect_pacer::SPACING};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

#[tokio::test]
async fn ticker_stream_attempts_take_a_turn_on_the_engine_pacer() {
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
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    );
    let api = RestApi::new("http://127.0.0.1:9", "http://127.0.0.1:9", 6000).unwrap();
    let tickers = Tickers::with_engine(api.clone(), Metadata::new(api, 300), engine.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    // A kline connection has just started: the ticker connection waits one spacing for its turn.
    engine.connect_pacer().turn().await;
    let turn_taken = std::time::Instant::now();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(tickers.stream(Market::Future, url, stop_rx));
    let (_stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let waited = turn_taken.elapsed();
    assert!(waited >= SPACING - Duration::from_millis(5), "{waited:?}");
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
