//! Local price reads and Java's empty-response compatibility helpers.
use super::Tickers;
use crate::error::{ApiError, Result};
use kline_core::{Bar, Market};
use serde_json::{Value, json};

impl Tickers {
    /// A last-known price is usable even when a quiet WS stream exceeds its TTL.
    /// Read actual bars only: no synthetic bar, REST load, or duplicate price cache.
    pub(super) fn cached_single_price(
        &self,
        market: Market,
        symbol: &str,
    ) -> Result<Option<Value>> {
        let Some(engine) = &self.engine else {
            return Ok(None);
        };
        let intervals = self.price_intervals.read();
        for &interval in &intervals[market as usize] {
            let Some(id) = engine.catalog.find(market, interval, symbol) else {
                continue;
            };
            let slot = engine.catalog.slot(id);
            if !slot.is_tracked() {
                continue;
            }
            if let Some(value) = slot.select_bars(|series| {
                series
                    .latest_bar()
                    .map(|bar| price_from_bar(market, symbol, bar, engine.now_ms()))
            }) {
                return value.map(Some);
            }
        }
        Ok(None)
    }
    pub(super) fn fallback_ticker(&self, market: Market, symbol: &str) -> Option<Value> {
        self.cached_ticker(market, symbol, std::time::Duration::from_secs(86_400))
    }
    pub(super) fn cached_ticker(
        &self,
        market: Market,
        symbol: &str,
        max_age: std::time::Duration,
    ) -> Option<Value> {
        self.rows[market as usize]
            .read()
            .get(symbol)
            .filter(|row| row.observed.elapsed() < max_age)
            .map(|row| (*row.value).clone())
    }
    pub(super) fn fallback_prices(&self, market: Market, symbols: &[String]) -> Result<Vec<Value>> {
        let Some(engine) = &self.engine else {
            return Ok(vec![]);
        };
        let slots = engine.catalog.slots();
        let interval = self.price_intervals.read()[market as usize][0];
        let now = engine.now_ms();
        let mut selected: Vec<_> = slots
            .iter()
            .filter(|slot| {
                slot.market == market
                    && slot.is_tracked()
                    && slot.interval == interval
                    && (symbols.is_empty() || symbols.iter().any(|s| s == slot.symbol.as_ref()))
            })
            .collect();
        if !symbols.is_empty() {
            // Java's empty-upstream fallback iterates the caller's symbols.
            selected.sort_by_key(|slot| {
                symbols
                    .iter()
                    .position(|s| s == slot.symbol.as_ref())
                    .unwrap()
            });
        }
        selected
            .into_iter()
            .filter_map(|slot| slot.latest().map(|(bar, _)| (slot, bar)))
            .map(|(slot, bar)| price_from_bar(market, &slot.symbol, &bar, now))
            .collect()
    }
}

fn price_from_bar(market: Market, symbol: &str, bar: &Bar, now: i64) -> Result<Value> {
    let price = if let Some(value) = binance_wire::exact_display(bar, 3) {
        value.into_owned()
    } else {
        binance_wire::display_number(bar.values[3])
            .map_err(ApiError::internal)?
            .to_string()
    };
    let mut value = json!({"symbol":symbol,"price":price});
    if market == Market::Future {
        // Java's local-price contract uses the query clock, not transaction time.
        // Never feed this timestamp into the live stream's REST ordering floor.
        value["time"] = now.into();
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metadata::Metadata, transport::RestApi};
    use std::{sync::Arc, time::Duration};

    #[tokio::test(start_paused = true)]
    async fn empty_response_ticker_fallback_expires_after_java_one_day_ttl() {
        let api = RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        let tickers = Tickers::new(api.clone(), Metadata::new(api, 300));
        let row = json!({"symbol":"BTCUSDT","lastPrice":"100.00","closeTime":1});
        tickers.merge(Market::Future, vec![Arc::new(row.clone())]);
        tickers.baseline_ready[Market::Future as usize]
            .store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(
            tickers
                .query(Market::Future, false, None, vec![], false, None)
                .await
                .unwrap(),
            serde_json::to_vec(&vec![row.clone()]).unwrap()
        );
        assert_eq!(
            tickers.fallback_ticker(Market::Future, "BTCUSDT"),
            Some(row.clone())
        );
        tokio::time::advance(Duration::from_secs(86_400)).await;
        assert!(tickers.fallback_ticker(Market::Future, "BTCUSDT").is_none());
        tickers.rendered.invalidate_all();
        assert_eq!(
            tickers
                .query(Market::Future, false, None, vec![], false, None)
                .await
                .unwrap(),
            b"[]".as_slice()
        );
        tickers.merge(Market::Future, vec![Arc::new(row.clone())]);
        assert_eq!(
            tickers.fallback_ticker(Market::Future, "BTCUSDT"),
            Some(row)
        );
    }
}
