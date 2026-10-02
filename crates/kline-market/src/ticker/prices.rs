//! Immutable all-market price responses. Network refresh never owns the publication lock.
use super::{Tickers, price_display, ticker_path};
use crate::error::{ApiError, Result};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use kline_core::Market;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    sync::{Mutex, Notify, watch},
    time::{Duration, Instant},
};

const REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const PUBLISH_INTERVAL: Duration = Duration::from_millis(100);
const MAX_RENDER_AGE: Duration = Duration::from_millis(250);
// A bounded grace for rows without a current stream observation, including retired pairs.
// Re-encoding a response must never reset this clock. Single-symbol freshness is unchanged.
const MAX_REST_AGE: Duration = Duration::from_secs(10);
const ACTIVE_MS: u64 = 5_000;

pub(super) struct PriceRow {
    pub symbol: Option<String>,
    pub time: Option<i64>,
    pub body: Bytes,
}
impl PriceRow {
    fn new(value: Value) -> Result<Self> {
        Ok(Self {
            symbol: value["symbol"].as_str().map(str::to_owned),
            time: value["time"].as_i64(),
            body: serde_json::to_vec(&value)
                .map(Bytes::from)
                .map_err(ApiError::internal)?,
        })
    }
}
struct Baseline {
    rows: Vec<PriceRow>,
    order: std::collections::BTreeMap<String, usize>,
    loaded: Instant,
}
struct Published {
    body: Bytes,
    baseline: Arc<Baseline>,
    rendered: Instant,
    ws_rows: usize,
}
pub(super) struct Prices {
    baseline: ArcSwapOption<Baseline>,
    published: ArcSwapOption<Published>,
    refresh: Mutex<()>,
    publish: Mutex<()>,
    access: AtomicU64,
    wake_refresh: Notify,
    wake_publish: Notify,
    rest_loads: AtomicU64,
    publications: AtomicU64,
    unavailable: AtomicU64,
}
impl Prices {
    pub(super) fn new() -> Self {
        Self {
            baseline: ArcSwapOption::empty(),
            published: ArcSwapOption::empty(),
            refresh: Mutex::new(()),
            publish: Mutex::new(()),
            access: AtomicU64::new(0),
            wake_refresh: Notify::new(),
            wake_publish: Notify::new(),
            rest_loads: AtomicU64::new(0),
            publications: AtomicU64::new(0),
            unavailable: AtomicU64::new(0),
        }
    }
    fn active(&self) -> bool {
        let access = self.access.load(Ordering::Relaxed);
        access != 0 && (crate::funding::now() as u64).saturating_sub(access) <= ACTIVE_MS
    }
    fn cached(&self) -> Option<Bytes> {
        self.published
            .load()
            .as_ref()
            .filter(|s| {
                s.baseline.loaded.elapsed() < MAX_REST_AGE && s.rendered.elapsed() < MAX_RENDER_AGE
            })
            .map(|s| s.body.clone())
    }
    pub(super) fn status(&self) -> Value {
        let baseline = self.baseline.load();
        let published = self.published.load();
        json!({
            "rows":baseline.as_ref().map_or(0, |s| s.rows.len()),
            "rest_age_ms":baseline.as_ref().map(|s| s.loaded.elapsed().as_millis() as u64),
            "encoded_age_ms":published.as_ref().map(|s| s.rendered.elapsed().as_millis() as u64),
            "encoded_rest_age_ms":published.as_ref().map(|s| s.baseline.loaded.elapsed().as_millis() as u64),
            "ws_rows":published.as_ref().map_or(0, |s| s.ws_rows),
            "max_rest_age_ms":MAX_REST_AGE.as_millis() as u64,
            "rest_loads":self.rest_loads.load(Ordering::Relaxed),
            "publications":self.publications.load(Ordering::Relaxed),
            "unavailable":self.unavailable.load(Ordering::Relaxed),
            "refresh_priority":"PriceRefresh",
            "idle_refresh_ms":IDLE_REFRESH_INTERVAL.as_millis() as u64,
        })
    }
}
impl Tickers {
    async fn refresh_prices(&self, market: Market) -> Result<()> {
        let prices = &self.prices[market as usize];
        let _refresh = prices.refresh.lock().await;
        if prices
            .baseline
            .load()
            .as_ref()
            .is_some_and(|s| s.loaded.elapsed() < REFRESH_INTERVAL)
        {
            return Ok(());
        }
        prices.rest_loads.fetch_add(1, Ordering::Relaxed);
        let raw: Vec<Value> = self
            .api
            .json_with_priority(
                market,
                ticker_path(market, true),
                &[],
                if market == Market::Future { 2 } else { 4 },
                crate::transport::Priority::PriceRefresh,
            )
            .await
            .map_err(ApiError::from)?;
        let rows = self
            .api
            .cpu
            .run(move || {
                raw.into_iter()
                    .map(|row| price_display(&row, market).and_then(PriceRow::new))
                    .collect::<Result<Vec<_>>>()
            })
            .await
            .map_err(ApiError::from)??;
        self.live.observed_baseline(market, &rows);
        let order = rows
            .iter()
            .enumerate()
            .filter_map(|(rank, row)| row.symbol.as_ref().map(|symbol| (symbol.clone(), rank)))
            .collect();
        prices.baseline.store(Some(Arc::new(Baseline {
            rows,
            order,
            loaded: Instant::now(),
        })));
        prices.wake_publish.notify_one();
        Ok(())
    }
    pub(super) async fn order_price_symbols(
        &self,
        market: Market,
        symbols: &mut [String],
    ) -> Result<()> {
        let prices = &self.prices[market as usize];
        prices
            .access
            .store(crate::funding::now() as u64, Ordering::Relaxed);
        prices.wake_refresh.notify_one();
        if prices.baseline.load().is_none() {
            // Bootstrap alone needs the authoritative ordering. An established
            // list can omit a newly available symbol: keep stable known order,
            // append unknown rows in caller order, and refresh in the background.
            // Missing order must not prevent a live quote from being read.
            self.refresh_prices(market).await?;
        }
        if let Some(baseline) = prices.baseline.load().as_ref() {
            symbols.sort_by_key(|symbol| baseline.order.get(symbol).copied().unwrap_or(usize::MAX));
        }
        Ok(())
    }
    pub(super) fn price_baseline_empty(&self, market: Market) -> bool {
        self.prices[market as usize]
            .baseline
            .load()
            .as_ref()
            .is_some_and(|baseline| baseline.rows.is_empty())
    }
    pub(super) fn recent_baseline_price(&self, market: Market, symbol: &str) -> Option<Value> {
        let baseline = self.prices[market as usize].baseline.load();
        let baseline = baseline
            .as_ref()
            // Subsets have the same freshness contract as the complete response.
            // Requiring a 500ms REST row caused quiet symbols to load REST even
            // while the already-published all-price response was valid.
            .filter(|baseline| baseline.loaded.elapsed() < MAX_REST_AGE)?;
        let row = baseline.rows.get(*baseline.order.get(symbol)?)?;
        serde_json::from_slice(&row.body).ok()
    }
    async fn publish_prices(&self, market: Market) -> Result<()> {
        let prices = &self.prices[market as usize];
        let _publish = prices.publish.lock().await;
        let Some(baseline) = prices.baseline.load_full() else {
            return Ok(());
        };
        if baseline.loaded.elapsed() >= MAX_REST_AGE {
            return Ok(());
        }
        if prices.published.load().as_ref().is_some_and(|s| {
            Arc::ptr_eq(&s.baseline, &baseline) && s.rendered.elapsed() < PUBLISH_INTERVAL
        }) {
            return Ok(());
        }
        let live = self.live.clone();
        let source = baseline.clone();
        let (body, ws_rows) = self
            .api
            .cpu
            .run(move || live.encode_prices(market, &source.rows))
            .await
            .map_err(ApiError::from)??;
        prices.published.store(Some(Arc::new(Published {
            body,
            baseline,
            rendered: Instant::now(),
            ws_rows,
        })));
        prices.publications.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    pub(super) async fn all_prices(&self, market: Market) -> Result<Bytes> {
        let prices = &self.prices[market as usize];
        let now = crate::funding::now() as u64;
        let previous = prices.access.swap(now, Ordering::Relaxed);
        if previous == 0 || now.saturating_sub(previous) > ACTIVE_MS {
            prices.wake_refresh.notify_one();
            prices.wake_publish.notify_one();
        }
        if market == Market::Future {
            self.live.want(&[]);
        }
        if let Some(body) = prices.cached() {
            return self.with_empty_price_fallback(market, body);
        }
        // Only the first bootstrap may wait for REST. Maintenance also runs
        // while idle, so the first request after a quiet period is already warm.
        // An expired established snapshot fails explicitly, without joining a
        // queued REST job or giving stale rows a new freshness deadline.
        if prices.baseline.load().is_none() {
            self.refresh_prices(market).await?;
        }
        self.publish_prices(market).await?;
        let body = prices.cached().ok_or_else(|| {
            prices.wake_refresh.notify_one();
            prices.unavailable.fetch_add(1, Ordering::Relaxed);
            unavailable()
        })?;
        self.with_empty_price_fallback(market, body)
    }
    fn with_empty_price_fallback(&self, market: Market, body: Bytes) -> Result<Bytes> {
        if body.as_ref() == b"[]" {
            super::encode(self.fallback_prices(market, &[])?)
        } else {
            Ok(body)
        }
    }
    pub(super) async fn run_price_refresh(&self, market: Market, mut stop: watch::Receiver<bool>) {
        let prices = &self.prices[market as usize];
        while !*stop.borrow() {
            let due = prices.baseline.load().as_ref().is_none_or(|baseline| {
                baseline.loaded.elapsed()
                    >= if prices.active() {
                        REFRESH_INTERVAL
                    } else {
                        IDLE_REFRESH_INTERVAL
                    }
            });
            if due {
                tokio::select! {
                    _ = stop.changed() => break,
                    result = self.refresh_prices(market) => {
                        if let Err(error) = result { tracing::warn!(?market,%error,"price snapshot refresh failed"); }
                    }
                }
            }
            tokio::select! {
                _ = stop.changed() => break,
                _ = prices.wake_refresh.notified() => {},
                _ = tokio::time::sleep(PUBLISH_INTERVAL) => {},
            }
        }
    }
    pub(super) async fn run_price_publisher(
        &self,
        market: Market,
        mut stop: watch::Receiver<bool>,
    ) {
        let prices = &self.prices[market as usize];
        while !*stop.borrow() {
            if prices.active() {
                tokio::select! {
                    _ = stop.changed() => break,
                    result = self.publish_prices(market) => {
                        if let Err(error) = result { tracing::warn!(?market,%error,"price snapshot publication failed"); }
                    }
                }
            }
            tokio::select! {
                _ = stop.changed() => break,
                _ = prices.wake_publish.notified() => {},
                _ = tokio::time::sleep(PUBLISH_INTERVAL) => {},
            }
        }
    }
}

fn unavailable() -> ApiError {
    ApiError {
        negotiation_fallback: None,
        status: 503,
        code: -1008,
        message: "Fresh market data temporarily unavailable".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_twenty_second_refresh_stall_cannot_hold_established_queries() {
        for market in [Market::Future, Market::Spot] {
            let api = crate::transport::RestApi::new("http://127.0.0.1", "http://127.0.0.1", 1200)
                .unwrap();
            let tickers = Tickers::new(api.clone(), crate::metadata::Metadata::new(api, 300));
            let prices = &tickers.prices[market as usize];
            prices.baseline.store(Some(Arc::new(Baseline {
                rows: vec![PriceRow::new(json!({"symbol":"HALTED","price":"0.00000001"})).unwrap()],
                order: [("HALTED".into(), 0)].into(),
                loaded: Instant::now(),
            })));
            tickers.publish_prices(market).await.unwrap();
            // The maintenance owner holds this mutex during both pacing and HTTP.
            let _blocked_refresh = prices.refresh.lock().await;
            tokio::time::advance(Duration::from_secs(4)).await;
            // Paused Tokio time must not race the real CPU worker thread.
            tickers.publish_prices(market).await.unwrap();
            let body = tokio::time::timeout(Duration::from_millis(100), tickers.all_prices(market))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()[0]["price"],
                "0.00000001"
            );
            tokio::time::advance(Duration::from_secs(16)).await;
            let start = Instant::now();
            for _ in 0..200 {
                let error =
                    tokio::time::timeout(Duration::from_millis(100), tickers.all_prices(market))
                        .await
                        .expect("no request may join the refresh mutex")
                        .unwrap_err();
                assert_eq!(
                    error.status, 503,
                    "expired REST-only rows must not be advertised as fresh"
                );
            }
            assert_eq!(start.elapsed(), Duration::ZERO);
            assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 0);
            assert_eq!(prices.status()["unavailable"], 200);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn publishing_cannot_extend_the_rest_baseline_deadline() {
        let api =
            crate::transport::RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        let tickers = Tickers::new(api.clone(), crate::metadata::Metadata::new(api, 300));
        let prices = &tickers.prices[Market::Spot as usize];
        prices.baseline.store(Some(Arc::new(Baseline {
            rows: vec![PriceRow::new(json!({"symbol":"HALTED","price":"0.00000001"})).unwrap()],
            order: [("HALTED".into(), 0)].into(),
            loaded: Instant::now(),
        })));
        tickers.publish_prices(Market::Spot).await.unwrap();
        assert!(prices.cached().is_some());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(
            prices.cached().is_none(),
            "unmaintained rendered responses expire"
        );
        tickers.publish_prices(Market::Spot).await.unwrap();
        assert!(
            prices.cached().is_some(),
            "REST refresh can wait during the bounded grace"
        );
        tokio::time::advance(Duration::from_secs(6)).await;
        tickers.publish_prices(Market::Spot).await.unwrap();
        assert!(
            prices.cached().is_none(),
            "re-encoding never makes an expired baseline fresh"
        );
        assert_eq!(prices.status()["encoded_rest_age_ms"], 10_000);
    }
}
