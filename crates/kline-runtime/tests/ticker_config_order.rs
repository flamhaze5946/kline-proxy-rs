use kline_core::Market;
use kline_runtime::config::Config;
use serde_json::json;

#[test]
fn fallback_uses_first_subscription_per_market_without_sorting() {
    let config: Config = serde_json::from_value(json!({"listen":"127.0.0.1:0","subscriptions":[
        {"market":"future","interval":"1d","symbol_patterns":[".*"],"history_capacity":100},
        {"market":"spot","interval":"4h","symbol_patterns":[".*"],"history_capacity":100},
        {"market":"future","interval":"1h","symbol_patterns":[".*"],"history_capacity":100},
        {"market":"spot","interval":"1m","symbol_patterns":[".*"],"history_capacity":100}
    ]}))
    .unwrap();
    let intervals = config.ticker_fallback_intervals().unwrap();
    assert_eq!(intervals[Market::Future as usize].code(), "1d");
    assert_eq!(intervals[Market::Spot as usize].code(), "4h");
    let periods = config.ticker_price_intervals().unwrap();
    assert_eq!(
        periods[0].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["1d", "1h"]
    );
    assert_eq!(
        periods[1].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["4h", "1m"]
    );
}

#[test]
fn static_interval_order_and_empty_config_follow_java_fallback_selection() {
    let config: Config = serde_json::from_value(json!({"listen":"127.0.0.1:0","instruments":[
        {"market":"future","symbol":"BTCUSDT","intervals":["1d","1h"]},
        {"market":"spot","symbol":"BTCUSDT","intervals":["4h","1m"]}
    ]}))
    .unwrap();
    let intervals = config.ticker_fallback_intervals().unwrap();
    assert_eq!(intervals[0].code(), "1d");
    assert_eq!(intervals[1].code(), "4h");
    let periods = config.ticker_price_intervals().unwrap();
    assert_eq!(
        periods[0].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["1d", "1h"]
    );
    assert_eq!(
        periods[1].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["4h", "1m"]
    );
    let empty: Config = serde_json::from_value(json!({"listen":"127.0.0.1:0"})).unwrap();
    assert!(
        empty
            .ticker_fallback_intervals()
            .unwrap()
            .iter()
            .all(|interval| interval.code() == "1h")
    );
}

#[test]
fn price_intervals_keep_unique_subscription_and_static_periods_in_order() {
    let config: Config = serde_json::from_value(json!({"listen":"127.0.0.1:0",
        "subscriptions":[
            {"market":"future","interval":"1d","symbol_patterns":[".*"],"history_capacity":100},
            {"market":"future","interval":"1d","symbol_patterns":["BTC.*"],"history_capacity":100}
        ],
        "instruments":[
            {"market":"future","symbol":"BTCUSDT","intervals":["1h","1d"]},
            {"market":"future","symbol":"ETHUSDT","intervals":["4h","1h"]}
        ]
    }))
    .unwrap();
    let periods = config.ticker_price_intervals().unwrap();
    assert_eq!(
        periods[0].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["1d", "1h", "4h"]
    );
    assert_eq!(
        periods[1].iter().map(|p| p.code()).collect::<Vec<_>>(),
        ["1h"]
    );
}
