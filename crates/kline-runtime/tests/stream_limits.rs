use kline_runtime::config::Config;
use serde_json::json;

fn with_explicit_stream(streams: usize) -> anyhow::Result<()> {
    let topics: Vec<String> = (0..streams).map(|i| format!("s{i}usdt@kline_1h")).collect();
    explicit(format!(
        "wss://fstream.binance.com/market/stream?streams={}",
        topics.join("/")
    ))
}
fn with_raw_streams(streams: usize) -> anyhow::Result<()> {
    let topics: Vec<String> = (0..streams).map(|i| format!("s{i}usdt@kline_1h")).collect();
    explicit(format!(
        "wss://fstream.binance.com/market/ws/{}",
        topics.join("/")
    ))
}
fn explicit(url: String) -> anyhow::Result<()> {
    let config: Config = serde_json::from_value(json!({
        "listen": "127.0.0.1:0",
        "streams": [{"market": "future", "url": url}]
    }))
    .unwrap();
    config.catalog().map(|_| ())
}

#[test]
fn an_explicit_stream_url_may_not_exceed_binances_1024_streams() {
    let error = with_explicit_stream(1025).unwrap_err().to_string();
    assert!(error.contains("at most 1024 streams"), "{error}");
    if let Err(error) = with_explicit_stream(1024) {
        assert!(!error.to_string().contains("1024 streams"), "{error}");
    }
}

#[test]
fn raw_streams_listed_in_the_path_count_too() {
    let error = with_raw_streams(1025).unwrap_err().to_string();
    assert!(error.contains("at most 1024 streams"), "{error}");
    if let Err(error) = with_raw_streams(1024) {
        assert!(!error.to_string().contains("1024 streams"), "{error}");
    }
}

#[test]
fn connection_attempts_are_journalled_in_the_persistence_directory() {
    let config: Config = serde_json::from_value(json!({
        "listen": "127.0.0.1:0",
        "persistence": {"directory": "/var/lib/kline-proxy-rs"}
    }))
    .unwrap();
    assert_eq!(
        config.engine_settings().unwrap().connect_journal,
        Some("/var/lib/kline-proxy-rs/ws-connect-attempts.json".into())
    );
    let config: Config = serde_json::from_value(json!({"listen": "127.0.0.1:0"})).unwrap();
    assert_eq!(config.engine_settings().unwrap().connect_journal, None);
}
