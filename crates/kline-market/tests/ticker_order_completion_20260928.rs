use axum::{
    Router,
    extract::{Query, State},
    http::Uri,
};
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_market::{config::Config, http::App, transport::RestApi};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct SourceData {
    // Deliberately not sorted by symbol or request order.
    symbols: Vec<String>,
    calls: parking_lot::Mutex<Vec<(String, BTreeMap<String, String>)>>,
    empty: bool,
    hold_ticker: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
async fn upstream(
    State(data): State<Arc<SourceData>>,
    uri: Uri,
    Query(query): Query<BTreeMap<String, String>>,
) -> axum::Json<Value> {
    data.calls.lock().push((uri.path().into(), query.clone()));
    if uri.path().ends_with("exchangeInfo") {
        return axum::Json(
            json!({"symbols":data.symbols.iter().map(|symbol| json!({"symbol":symbol,"status":"TRADING"})).collect::<Vec<_>>()}),
        );
    }
    if uri.path().ends_with("ticker/24hr") && data.hold_ticker.swap(false, Ordering::AcqRel) {
        data.entered.notify_one();
        data.release.notified().await;
    }
    let single = query.get("symbol");
    if data.empty {
        return axum::Json(if single.is_some() {
            Value::Null
        } else {
            json!([])
        });
    }
    let rows: Vec<_> = data
        .symbols
        .iter()
        .filter(|symbol| single.is_none_or(|s| s == *symbol))
        .map(|symbol| {
            if uri.path().ends_with("ticker/price") {
                json!({"symbol":symbol,"price":"100.00"})
            } else {
                json!({"symbol":symbol,"lastPrice":"100.00","closeTime":1})
            }
        })
        .collect();
    axum::Json(if single.is_some() {
        rows.into_iter().next().unwrap_or(Value::Null)
    } else {
        json!(rows)
    })
}
struct Fixture {
    app: Arc<App>,
    data: Arc<SourceData>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(empty: bool) -> Fixture {
    let data = Arc::new(SourceData {
        symbols: (0..251)
            .map(|i| format!("S{:03}USDT", (i * 53) % 251))
            .collect(),
        calls: parking_lot::Mutex::new(vec![]),
        empty,
        hold_ticker: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(upstream).with_state(data.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let instruments = [Market::Future, Market::Spot]
        .into_iter()
        .flat_map(|market| {
            ["1d", "1h"].map(|interval| Instrument {
                market,
                symbol: "S000USDT".into(),
                interval: Interval::parse(interval).unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(8).unwrap(),
            })
        })
        .collect();
    let engine = Engine::new(
        Catalog::new(instruments).unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    );
    for (id, slot) in engine.catalog.slots().iter().enumerate() {
        let open = slot.interval.boundary(engine.now_ms());
        let close = if slot.interval.code() == "1d" {
            88.0
        } else {
            101.25
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
                    event_time: Some(engine.now_ms()),
                    sequence: 1,
                },
            )
            .unwrap();
    }
    Fixture {
        app: App::new(
            engine,
            RestApi::new(&url, &url, 6000).unwrap(),
            Config::default(),
        ),
        data,
        task,
    }
}
async fn query(app: &App, price: bool, symbols: Vec<String>) -> Value {
    let bytes = app
        .tickers
        .query(Market::Spot, price, None, symbols, false, None)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
async fn scattered_requests(f: &Fixture, price: bool, expected_price: &str) {
    let mut tasks = vec![];
    for client in 0..200 {
        let requested: Vec<_> = (0..5)
            .map(|i| f.data.symbols[(client * 13 + i * 47) % 251].clone())
            .rev()
            .collect();
        let expected: Vec<_> = f
            .data
            .symbols
            .iter()
            .filter(|symbol| requested.contains(symbol))
            .cloned()
            .collect();
        let app = f.app.clone();
        let expected_price = expected_price.to_owned();
        tasks.push(tokio::spawn(async move {
            let values = query(&app, price, requested).await;
            let rows = values.as_array().unwrap();
            let actual: Vec<_> = rows
                .iter()
                .map(|row| row["symbol"].as_str().unwrap())
                .collect();
            assert_eq!(actual, expected);
            for row in rows {
                assert_eq!(
                    row[if price { "price" } else { "lastPrice" }],
                    expected_price
                );
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn two_hundred_cold_scattered_clients_share_one_upstream_order_snapshot() {
    for price in [true, false] {
        let f = fixture(false).await;
        scattered_requests(&f, price, "100.00").await;
        let calls = f.data.calls.lock();
        let ticker: Vec<_> = calls
            .iter()
            .filter(|(path, _)| path.contains("/ticker/"))
            .collect();
        assert_eq!(
            ticker.len(),
            1,
            "cold symbol combinations must share the baseline: {ticker:?}"
        );
        assert!(
            ticker[0].1.is_empty(),
            "the single load covers every request combination"
        );
    }
}

#[tokio::test]
async fn two_hundred_hot_scattered_clients_do_not_wait_for_background_rest() {
    let f = fixture(false).await;
    query(&f.app, true, vec![]).await;
    f.app.tickers.refresh(Market::Spot).await.unwrap();
    let now = kline_market::funding::now();
    let frames: Vec<_> = f
        .data
        .symbols
        .iter()
        .map(|symbol| json!({"e":"24hrTicker","s":symbol,"E":now,"C":now,"c":"222.5000"}))
        .collect();
    f.app
        .tickers
        .ingest(Market::Spot, &serde_json::to_vec(&frames).unwrap())
        .unwrap();
    f.data.hold_ticker.store(true, Ordering::Release);
    let app = f.app.clone();
    let refresh = tokio::spawn(async move { app.tickers.refresh(Market::Spot).await });
    f.data.entered.notified().await;
    f.data.calls.lock().clear();
    tokio::time::timeout(Duration::from_secs(1), async {
        scattered_requests(&f, true, "222.5000").await;
        scattered_requests(&f, false, "222.5000").await;
    })
    .await
    .expect("hot queries must finish while the background REST response is withheld");
    assert!(
        f.data.calls.lock().is_empty(),
        "hot 200 x 5 requests must make zero REST calls"
    );
    f.data.release.notify_one();
    refresh.await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_price_uses_the_configured_first_interval_even_if_catalog_sorts_it() {
    let f = fixture(true).await;
    for interval in ["1d", "1h"] {
        f.app
            .tickers
            .set_fallback_intervals([Interval::parse(interval).unwrap(); 2]);
        let expected = if interval == "1d" { "88" } else { "101.25" };
        for market in [Market::Future, Market::Spot] {
            let bytes = f
                .app
                .tickers
                .query(market, true, None, vec![], false, None)
                .await
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value[0]["price"], expected);
        }
    }
}
