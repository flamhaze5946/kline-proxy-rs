//! Stream-owned observations. Dropping a connection immediately invalidates its rows.
use super::{prices::PriceRow, wire::Trade};
use crate::{
    decimal,
    error::{ApiError, Result},
};
use kline_core::Market;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

const FRESH: Duration = Duration::from_secs(3);
const INTEREST: Duration = Duration::from_secs(300);

pub(super) struct Connection(pub Arc<AtomicBool>);
impl Connection {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct Observation<T> {
    value: T,
    event_ms: i64,
    sequence: i64,
    received: Instant,
    connection: Arc<AtomicBool>,
}
impl<T> Observation<T> {
    fn fresh(&self) -> bool {
        self.fresh_at(Instant::now(), crate::funding::now())
    }
    fn fresh_at(&self, now: Instant, wall_ms: i64) -> bool {
        self.connection.load(Ordering::Acquire)
            && now.saturating_duration_since(self.received) <= FRESH
            && wall_ms.saturating_sub(self.event_ms) <= FRESH.as_millis() as i64
    }
}
struct TickerValue {
    display: Arc<Value>,
    price_body: Option<bytes::Bytes>,
}
struct TradePrice {
    price: String,
    time: i64,
    body: bytes::Bytes,
}
fn price_body(symbol: &str, price: &str, time: Option<i64>) -> Result<bytes::Bytes> {
    #[derive(Serialize)]
    struct Price<'a> {
        price: &'a str,
        symbol: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        time: Option<i64>,
    }
    serde_json::to_vec(&Price {
        price,
        symbol,
        time,
    })
    .map(bytes::Bytes::from)
    .map_err(ApiError::internal)
}
#[derive(Default)]
struct Interest {
    all: Option<Instant>,
    symbols: BTreeMap<String, Instant>,
}
pub(super) struct Live {
    tickers: [RwLock<BTreeMap<String, Observation<TickerValue>>>; 2],
    prices: RwLock<BTreeMap<String, Observation<TradePrice>>>,
    rest_time: RwLock<BTreeMap<String, i64>>,
    interest: RwLock<Interest>,
    pub demand: Notify,
    pub changed: [watch::Sender<u64>; 2],
    pub trade_frames: AtomicU64,
}
impl Live {
    pub fn new() -> Self {
        Self {
            tickers: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            prices: RwLock::new(BTreeMap::new()),
            rest_time: RwLock::new(BTreeMap::new()),
            interest: RwLock::new(Interest::default()),
            demand: Notify::new(),
            changed: std::array::from_fn(|_| watch::channel(0).0),
            trade_frames: AtomicU64::new(0),
        }
    }
    pub fn want(&self, symbols: &[String]) {
        let now = Instant::now();
        let mut interest = self.interest.write();
        let mut changed = false;
        if symbols.is_empty() {
            changed = interest
                .all
                .is_none_or(|t| now.duration_since(t) > INTEREST);
            interest.all = Some(now);
        } else {
            for symbol in symbols {
                let previous = interest.symbols.insert(symbol.clone(), now);
                changed |= previous.is_none_or(|t| now.duration_since(t) > INTEREST);
            }
        }
        drop(interest);
        if changed {
            self.demand.notify_one();
        }
    }
    pub fn wanted(&self, trading: &[String]) -> Vec<String> {
        let mut interest = self.interest.write();
        interest.symbols.retain(|_, t| t.elapsed() <= INTEREST);
        let all = interest.all.is_some_and(|t| t.elapsed() <= INTEREST);
        trading
            .iter()
            .filter(|s| all || interest.symbols.contains_key(*s))
            .cloned()
            .collect()
    }
    pub fn ticker(
        &self,
        market: Market,
        event_ms: Option<i64>,
        value: Arc<Value>,
        connection: &Arc<AtomicBool>,
    ) -> Result<()> {
        let Some(event_ms) = event_ms else {
            return Ok(());
        };
        let Some(symbol) = value["symbol"].as_str() else {
            return Ok(());
        };
        if !valid_event(event_ms) {
            return Ok(());
        }
        let mut rows = self.tickers[market as usize].write();
        if rows.get(symbol).is_some_and(|r| r.event_ms > event_ms) {
            return Ok(());
        }
        let body = if market == Market::Spot {
            value["lastPrice"]
                .as_str()
                .map(|price| price_body(symbol, price, None))
                .transpose()?
        } else {
            None
        };
        let old = rows.get_mut(symbol);
        let key = old.is_none().then(|| symbol.to_owned());
        let observation = Observation {
            value: TickerValue {
                display: value,
                price_body: body,
            },
            event_ms,
            sequence: 0,
            received: Instant::now(),
            connection: connection.clone(),
        };
        if let Some(old) = old {
            *old = observation;
        } else {
            rows.insert(key.unwrap(), observation);
        }
        Ok(())
    }
    pub fn trade(&self, trade: Trade<'_>, connection: &Arc<AtomicBool>) -> Result<()> {
        let Trade {
            symbol,
            event_ms,
            time,
            sequence,
            price,
        } = trade;
        if time <= 0 || time > event_ms.saturating_add(5000) {
            return Err(ApiError::internal("trade missing transaction time"));
        }
        let price = decimal(&price)?;
        if !valid_event(event_ms) {
            return Ok(());
        }
        let mut rows = self.prices.write();
        if rows
            .get(symbol)
            .is_some_and(|r| (r.value.time, r.sequence) >= (time, sequence))
        {
            return Ok(());
        }
        let body = price_body(symbol, &price, Some(time))?;
        let observation = Observation {
            value: TradePrice { price, time, body },
            event_ms,
            sequence,
            received: Instant::now(),
            connection: connection.clone(),
        };
        if let Some(old) = rows.get_mut(symbol) {
            *old = observation;
        } else {
            rows.insert(symbol.to_owned(), observation);
        }
        self.trade_frames.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    pub fn publish(&self, market: Market) {
        // Subscribers attach before checking observations, so a reader that starts
        // after this check sees the already committed value without a notification.
        let changed = &self.changed[market as usize];
        if changed.receiver_count() != 0 {
            changed.send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }
    pub fn get(&self, market: Market, symbol: &str, price: bool) -> Option<Value> {
        if market == Market::Future && price {
            let floor = self.rest_time.read().get(symbol).copied().unwrap_or(0);
            return self
                .prices
                .read()
                .get(symbol)
                .filter(|r| r.fresh() && r.value.time > floor)
                .map(|r| serde_json::json!({"symbol":symbol,"price":r.value.price,"time":r.value.time}));
        }
        let rows = self.tickers[market as usize].read();
        let row = rows.get(symbol).filter(|r| r.fresh())?;
        if price {
            let price = row.value.display["lastPrice"].as_str()?;
            Some(serde_json::json!({"symbol":symbol,"price":price}))
        } else {
            Some((*row.value.display).clone())
        }
    }
    pub fn observed_rest_price(&self, market: Market, value: &Value) {
        if market != Market::Future {
            return;
        }
        if let (Some(symbol), Some(time)) = (value["symbol"].as_str(), value["time"].as_i64()) {
            let mut floor = self.rest_time.write();
            let previous = floor.entry(symbol.to_owned()).or_default();
            *previous = (*previous).max(time);
        }
    }
    pub fn observed_baseline(&self, market: Market, baseline: &[PriceRow]) {
        if market != Market::Future {
            return;
        }
        let mut floors = self.rest_time.write();
        for row in baseline {
            if let (Some(symbol), Some(time)) = (&row.symbol, row.time) {
                if let Some(previous) = floors.get_mut(symbol) {
                    *previous = (*previous).max(time);
                } else {
                    floors.insert(symbol.clone(), time.max(0));
                }
            }
        }
    }
    /// Stitch immutable, already encoded rows under one market read lock. Freshness
    /// uses one sampled clock; the REST list still defines full coverage and order.
    pub fn encode_prices(
        &self,
        market: Market,
        baseline: &[PriceRow],
    ) -> Result<(bytes::Bytes, usize)> {
        let mut output = Vec::with_capacity(baseline.len().saturating_mul(64));
        output.push(b'[');
        let now = Instant::now();
        let wall_ms = crate::funding::now();
        let mut ws_rows = 0;
        if market == Market::Future {
            let floors = self.rest_time.read();
            let prices = self.prices.read();
            for (index, rest) in baseline.iter().enumerate() {
                let live = rest.symbol.as_deref().and_then(|symbol| {
                    prices.get(symbol).filter(|row| {
                        let time = row.value.time;
                        row.fresh_at(now, wall_ms)
                            && time > floors.get(symbol).copied().unwrap_or(0)
                            && time >= rest.time.unwrap_or(0)
                    })
                });
                let body = if let Some(live) = live {
                    ws_rows += 1;
                    &live.value.body
                } else {
                    &rest.body
                };
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(body);
            }
        } else {
            let tickers = self.tickers[market as usize].read();
            for (index, rest) in baseline.iter().enumerate() {
                let live = rest.symbol.as_deref().and_then(|symbol| {
                    tickers
                        .get(symbol)
                        .filter(|row| row.fresh_at(now, wall_ms))
                        .and_then(|row| row.value.price_body.as_ref())
                });
                let body = if let Some(live) = live {
                    ws_rows += 1;
                    live
                } else {
                    &rest.body
                };
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(body);
            }
        }
        output.push(b']');
        Ok((output.into(), ws_rows))
    }
    pub fn retain(&self, market: Market, valid: &std::collections::BTreeSet<String>) {
        self.tickers[market as usize]
            .write()
            .retain(|s, _| valid.contains(s));
        if market == Market::Future {
            self.prices.write().retain(|s, _| valid.contains(s));
            self.rest_time.write().retain(|s, _| valid.contains(s));
        }
    }
}
fn valid_event(time: i64) -> bool {
    let age = crate::funding::now().saturating_sub(time);
    time > 0 && (-5000..=FRESH.as_millis() as i64).contains(&age)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn trade(book: &Live, raw: &Value, connection: &Arc<AtomicBool>) -> Result<()> {
        let text = serde_json::to_string(raw).unwrap();
        let event: super::super::wire::Event<'_> = serde_json::from_str(&text).unwrap();
        book.trade(event.trade()?, connection)
    }

    #[tokio::test(start_paused = true)]
    async fn quotes_preserve_transaction_time_expire_and_follow_connection_lifetime() {
        let book = Live::new();
        let future = Connection::new();
        let spot = Connection::new();
        let now = crate::funding::now();
        let ticker = json!({"E":now,"s":"BTCUSDT","C":now-5,"c":"999.00"});
        let displayed = json!({"symbol":"BTCUSDT","lastPrice":"999.00","closeTime":now-5});
        book.ticker(
            Market::Future,
            ticker["E"].as_i64(),
            Arc::new(displayed.clone()),
            &future.0,
        )
        .unwrap();
        book.ticker(
            Market::Spot,
            ticker["E"].as_i64(),
            Arc::new(displayed),
            &spot.0,
        )
        .unwrap();
        assert!(
            book.get(Market::Future, "BTCUSDT", true).is_none(),
            "statistics time is not transaction time"
        );
        assert!(
            book.get(Market::Spot, "BTCUSDT", true)
                .unwrap()
                .get("time")
                .is_none()
        );
        let frame =
            json!({"e":"aggTrade","s":"BTCUSDT","E":now,"T":now-17,"a":20,"p":"0.0000000012300"});
        trade(&book, &frame, &future.0).unwrap();
        let quote = book.get(Market::Future, "BTCUSDT", true).unwrap();
        assert_eq!(quote["price"], "0.0000000012300");
        assert_eq!(quote["time"], now - 17);
        let older = json!({"s":"BTCUSDT","E":now,"T":now-18,"a":999,"p":"1"});
        trade(&book, &older, &future.0).unwrap();
        assert_eq!(book.get(Market::Future, "BTCUSDT", true).unwrap(), quote);
        drop(future);
        assert!(book.get(Market::Future, "BTCUSDT", true).is_none());
        assert!(
            book.get(Market::Spot, "BTCUSDT", true).is_some(),
            "another connection remains usable"
        );
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(book.get(Market::Spot, "BTCUSDT", true).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn old_frames_cannot_become_fresh_and_price_subscriptions_expire() {
        let book = Live::new();
        let connection = Connection::new();
        let now = crate::funding::now();
        let old = json!({"s":"BTCUSDT","E":now-10000,"T":now-10001,"a":1,"p":"1"});
        trade(&book, &old, &connection.0).unwrap();
        assert!(book.get(Market::Future, "BTCUSDT", true).is_none());
        let trading = vec!["BTCUSDT".into(), "ETHUSDT".into()];
        book.want(&["BTCUSDT".into()]);
        assert_eq!(book.wanted(&trading), vec!["BTCUSDT"]);
        tokio::time::advance(Duration::from_secs(301)).await;
        assert!(book.wanted(&trading).is_empty());
        book.want(&[]);
        assert_eq!(book.wanted(&trading), trading);
    }

    #[tokio::test]
    async fn an_older_stream_transaction_cannot_replace_a_newer_rest_price() {
        let book = Live::new();
        let connection = Connection::new();
        let now = crate::funding::now();
        book.observed_rest_price(Market::Future, &json!({"symbol":"BTCUSDT","time":now-10}));
        book.observed_rest_price(Market::Future, &json!({"symbol":"BTCUSDT","time":now-30}));
        trade(
            &book,
            &json!({"s":"BTCUSDT","E":now,"T":now-20,"a":1,"p":"1"}),
            &connection.0,
        )
        .unwrap();
        assert!(book.get(Market::Future, "BTCUSDT", true).is_none());
        trade(
            &book,
            &json!({"s":"BTCUSDT","E":now,"T":now-5,"a":2,"p":"2"}),
            &connection.0,
        )
        .unwrap();
        assert_eq!(
            book.get(Market::Future, "BTCUSDT", true).unwrap()["price"],
            "2"
        );
    }
}
