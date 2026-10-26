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
const MAX_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
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
    idle_refresh_ms: AtomicU64,
    wake_refresh: Notify,
    wake_publish: Notify,
    rest_loads: AtomicU64,
    publications: AtomicU64,
    unavailable: AtomicU64,
    #[cfg(test)]
    refresh_checks: AtomicU64,
    #[cfg(test)]
    publish_checks: AtomicU64,
}
impl Prices {
    pub(super) fn new() -> Self {
        Self {
            baseline: ArcSwapOption::empty(),
            published: ArcSwapOption::empty(),
            refresh: Mutex::new(()),
            publish: Mutex::new(()),
            access: AtomicU64::new(0),
            idle_refresh_ms: AtomicU64::new(IDLE_REFRESH_INTERVAL.as_millis() as u64),
            wake_refresh: Notify::new(),
            wake_publish: Notify::new(),
            rest_loads: AtomicU64::new(0),
            publications: AtomicU64::new(0),
            unavailable: AtomicU64::new(0),
            #[cfg(test)]
            refresh_checks: AtomicU64::new(0),
            #[cfg(test)]
            publish_checks: AtomicU64::new(0),
        }
    }
    fn active(&self) -> bool {
        let access = self.access.load(Ordering::Relaxed);
        access != 0 && (crate::funding::now() as u64).saturating_sub(access) <= ACTIVE_MS
    }
    fn reset_idle_refresh(&self) {
        self.idle_refresh_ms
            .store(IDLE_REFRESH_INTERVAL.as_millis() as u64, Ordering::Relaxed);
    }
    fn backoff_idle_refresh(&self) {
        let _ = self
            .idle_refresh_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |ms| {
                Some((ms + 1_000).min(MAX_IDLE_REFRESH_INTERVAL.as_millis() as u64))
            });
    }
    fn refresh_interval(&self) -> Duration {
        if self.active() {
            self.reset_idle_refresh();
            REFRESH_INTERVAL
        } else {
            Duration::from_millis(self.idle_refresh_ms.load(Ordering::Relaxed))
        }
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
            "idle_refresh_ms":self.idle_refresh_ms.load(Ordering::Relaxed),
            "max_idle_refresh_ms":MAX_IDLE_REFRESH_INTERVAL.as_millis() as u64,
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
        prices.reset_idle_refresh();
        prices.wake_refresh.notify_one();
        prices.wake_publish.notify_one();
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
        prices.reset_idle_refresh();
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
        let mut retry_after = None;
        while !*stop.borrow() {
            #[cfg(test)]
            prices.refresh_checks.fetch_add(1, Ordering::Relaxed);
            let interval = prices.refresh_interval();
            let due = prices
                .baseline
                .load()
                .as_ref()
                .map_or_else(Instant::now, |baseline| baseline.loaded + interval);
            // Demand can shorten the idle interval, but cannot bypass a failed
            // attempt's retry floor (nor turn an expired baseline into a spin).
            let due = retry_after.map_or(due, |retry: Instant| due.max(retry));
            tokio::select! {
                _ = stop.changed() => break,
                _ = prices.wake_refresh.notified() => continue,
                _ = wait_until_due(due) => {},
            }
            // Demand may have expired while this active deadline was sleeping.
            // Recheck before starting REST rather than doing one extra active refresh.
            if prices
                .baseline
                .load()
                .as_ref()
                .is_some_and(|baseline| baseline.loaded.elapsed() < prices.refresh_interval())
            {
                continue;
            }
            let had_baseline = prices.baseline.load().is_some();
            tokio::select! {
                _ = stop.changed() => break,
                result = self.refresh_prices(market) => {
                    match result {
                        Err(error) => {
                            prices.reset_idle_refresh();
                            retry_after = Some(Instant::now() + PUBLISH_INTERVAL);
                            tracing::warn!(?market,%error,"price snapshot refresh failed");
                        }
                        Ok(()) => {
                            retry_after = None;
                            if had_baseline && !prices.active() {
                                prices.backoff_idle_refresh();
                            } else {
                                prices.reset_idle_refresh();
                            }
                        }
                    }
                }
            }
        }
    }
    pub(super) async fn run_price_publisher(
        &self,
        market: Market,
        mut stop: watch::Receiver<bool>,
    ) {
        let prices = &self.prices[market as usize];
        let mut retry_after = None;
        while !*stop.borrow() {
            #[cfg(test)]
            prices.publish_checks.fetch_add(1, Ordering::Relaxed);
            let due = if prices.active() {
                prices
                    .baseline
                    .load_full()
                    .filter(|baseline| baseline.loaded.elapsed() < MAX_REST_AGE)
                    .map(|baseline| {
                        let due = prices
                            .published
                            .load()
                            .as_ref()
                            .filter(|published| Arc::ptr_eq(&published.baseline, &baseline))
                            .map_or_else(Instant::now, |published| {
                                published.rendered + PUBLISH_INTERVAL
                            });
                        retry_after.map_or(due, |retry: Instant| due.max(retry))
                    })
            } else {
                None
            };
            tokio::select! {
                _ = stop.changed() => break,
                _ = prices.wake_publish.notified() => continue,
                _ = async {
                    match due {
                        Some(due) => wait_until_due(due).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {},
            }
            if !prices.active() {
                continue;
            }
            tokio::select! {
                _ = stop.changed() => break,
                result = self.publish_prices(market) => {
                    retry_after = if let Err(error) = result {
                        tracing::warn!(?market,%error,"price snapshot publication failed");
                        Some(Instant::now() + PUBLISH_INTERVAL)
                    } else {
                        None
                    };
                }
            }
        }
    }
}

async fn wait_until_due(due: Instant) {
    // Already-due work needs no timer registration (also avoids timer rounding
    // delaying bootstrap when notifications arrive before the worker starts).
    if due > Instant::now() {
        tokio::time::sleep_until(due).await;
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
            for _ in 0..8 {
                prices.backoff_idle_refresh();
            }
            assert_eq!(prices.refresh_interval(), Duration::from_secs(5));
            prices.baseline.store(Some(Arc::new(Baseline {
                rows: vec![PriceRow::new(json!({"symbol":"HALTED","price":"0.00000001"})).unwrap()],
                order: [("HALTED".into(), 0)].into(),
                loaded: Instant::now(),
            })));
            tickers.publish_prices(market).await.unwrap();
            // The maintenance owner holds this mutex during both pacing and HTTP.
            let _blocked_refresh = prices.refresh.lock().await;
            tokio::time::advance(Duration::from_secs(6)).await;
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
            assert_eq!(prices.idle_refresh_ms.load(Ordering::Relaxed), 2_000);
            assert_eq!(prices.refresh_interval(), REFRESH_INTERVAL);
            tokio::time::advance(Duration::from_secs(14)).await;
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

#[cfg(test)]
mod idle_backoff_tests {
    use super::*;
    #[test]
    fn idle_refresh_backoff_is_bounded_and_demand_restores_active_cadence() {
        let prices = Prices::new();
        assert_eq!(prices.refresh_interval(), Duration::from_secs(2));
        for expected in [3, 4, 5, 5] {
            prices.backoff_idle_refresh();
            assert_eq!(prices.refresh_interval(), Duration::from_secs(expected));
        }
        prices
            .access
            .store(crate::funding::now() as u64, Ordering::Relaxed);
        assert_eq!(prices.refresh_interval(), REFRESH_INTERVAL);
        assert_eq!(prices.idle_refresh_ms.load(Ordering::Relaxed), 2_000);
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    async fn settle() {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
    }
    // Paused Tokio time and real network/CPU threads must not auto-advance each
    // other's deadlines. Keep the executor runnable until the observation arrives.
    async fn observe(mut ready: impl FnMut() -> bool) {
        let end = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while !ready() {
            assert!(
                std::time::Instant::now() < end,
                "background observation timed out"
            );
            tokio::task::yield_now().await;
        }
    }
    async fn fixture(fail: bool) -> (Arc<Tickers>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", listener.local_addr().unwrap());
        let router = axum::Router::new().route(
            "/api/v3/ticker/price",
            axum::routing::get(move || async move {
                if fail {
                    (
                        axum::http::StatusCode::BAD_REQUEST,
                        axum::Json(json!({"code":-1,"msg":"test failure"})),
                    )
                } else {
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(json!([{"symbol":"TESTUSDT","price":"1"}])),
                    )
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let api = crate::transport::RestApi::new(&root, &root, 6000).unwrap();
        (
            Tickers::new(api.clone(), crate::metadata::Metadata::new(api, 300)),
            server,
        )
    }
    fn seed(prices: &Prices, loaded: Instant) {
        prices.baseline.store(Some(Arc::new(Baseline {
            rows: vec![PriceRow::new(json!({"symbol":"TESTUSDT","price":"1"})).unwrap()],
            order: [("TESTUSDT".into(), 0)].into(),
            loaded,
        })));
    }

    #[tokio::test]
    async fn idle_refresh_sleeps_to_backoff_deadlines_and_demand_shortens_the_deadline() {
        let (tickers, server) = fixture(false).await;
        let prices = &tickers.prices[Market::Spot as usize];
        let (stop, rx) = watch::channel(false);
        let t = tickers.clone();
        tokio::time::pause();
        let task = tokio::spawn(async move { t.run_price_refresh(Market::Spot, rx).await });
        observe(|| {
            prices.baseline.load().is_some() && prices.refresh_checks.load(Ordering::Relaxed) >= 2
        })
        .await;
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 1);
        for (index, seconds) in [2_u64, 3, 4, 5].into_iter().enumerate() {
            let checks = prices.refresh_checks.load(Ordering::Relaxed);
            tokio::time::advance(Duration::from_millis(seconds * 1000 - 1)).await;
            settle().await;
            assert_eq!(
                prices.refresh_checks.load(Ordering::Relaxed),
                checks,
                "idle refresh must not poll every 100ms"
            );
            tokio::time::advance(Duration::from_millis(2)).await;
            observe(|| prices.refresh_checks.load(Ordering::Relaxed) > checks).await;
            assert_eq!(prices.rest_loads.load(Ordering::Relaxed), index as u64 + 2);
        }
        assert_eq!(prices.idle_refresh_ms.load(Ordering::Relaxed), 5_000);
        let loaded = prices.rest_loads.load(Ordering::Relaxed);
        let mut symbols = vec!["TESTUSDT".into()];
        tickers
            .order_price_symbols(Market::Spot, &mut symbols)
            .await
            .unwrap();
        settle().await;
        tokio::time::advance(Duration::from_millis(499)).await;
        settle().await;
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), loaded);
        tokio::time::advance(Duration::from_millis(2)).await;
        observe(|| prices.rest_loads.load(Ordering::Relaxed) == loaded + 1).await;
        stop.send(true).unwrap();
        observe(|| task.is_finished()).await;
        task.await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn publisher_waits_while_idle_and_consumes_pre_wait_demand_notification() {
        let (tickers, server) = fixture(false).await;
        let prices = &tickers.prices[Market::Spot as usize];
        tokio::time::pause();
        seed(prices, Instant::now());
        let (stop, rx) = watch::channel(false);
        let t = tickers.clone();
        let task = tokio::spawn(async move { t.run_price_publisher(Market::Spot, rx).await });
        observe(|| prices.publish_checks.load(Ordering::Relaxed) == 1).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(prices.publish_checks.load(Ordering::Relaxed), 1);
        // Stop/restart with demand signalled before the new owner can wait.
        stop.send(true).unwrap();
        observe(|| task.is_finished()).await;
        task.await.unwrap();
        let mut symbols = vec!["TESTUSDT".into()];
        tickers
            .order_price_symbols(Market::Spot, &mut symbols)
            .await
            .unwrap();
        let (stop, rx) = watch::channel(false);
        let t = tickers.clone();
        let task = tokio::spawn(async move { t.run_price_publisher(Market::Spot, rx).await });
        observe(|| prices.publications.load(Ordering::Relaxed) == 1).await;
        assert!(prices.cached().is_some());
        tokio::time::advance(PUBLISH_INTERVAL + Duration::from_millis(1)).await;
        observe(|| prices.publications.load(Ordering::Relaxed) == 2).await;
        // Access age uses UTC; explicitly expire demand while virtual timers run.
        prices.access.store(
            crate::funding::now() as u64 - ACTIVE_MS - 1,
            Ordering::Relaxed,
        );
        tokio::time::advance(PUBLISH_INTERVAL + Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(prices.publications.load(Ordering::Relaxed), 2);
        let checks = prices.publish_checks.load(Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(10)).await;
        settle().await;
        assert_eq!(prices.publish_checks.load(Ordering::Relaxed), checks);
        assert!(
            prices.cached().is_none(),
            "hard REST/render expiry still applies while sleeping"
        );
        // Expired data plus demand must not create an immediate-deadline loop.
        prices
            .access
            .store(crate::funding::now() as u64, Ordering::Relaxed);
        prices.wake_publish.notify_one();
        settle().await;
        let checks = prices.publish_checks.load(Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(prices.publish_checks.load(Ordering::Relaxed), checks);
        stop.send(true).unwrap();
        observe(|| task.is_finished()).await;
        task.await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn demand_notifications_cannot_bypass_failed_refresh_retry_floor() {
        let (tickers, server) = fixture(true).await;
        let prices = &tickers.prices[Market::Spot as usize];
        tokio::time::pause();
        seed(prices, Instant::now() - Duration::from_secs(20));
        let (stop, rx) = watch::channel(false);
        let t = tickers.clone();
        let task = tokio::spawn(async move { t.run_price_refresh(Market::Spot, rx).await });
        observe(|| prices.refresh_checks.load(Ordering::Relaxed) >= 2).await;
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 1);
        for _ in 0..20 {
            prices.wake_refresh.notify_one();
            settle().await;
        }
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(99)).await;
        settle().await;
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(2)).await;
        observe(|| prices.rest_loads.load(Ordering::Relaxed) == 2).await;
        stop.send(true).unwrap();
        observe(|| task.is_finished()).await;
        task.await.unwrap();
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stop_cancels_a_refresh_waiting_for_the_maintenance_lock() {
        let api =
            crate::transport::RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        let tickers = Tickers::new(api.clone(), crate::metadata::Metadata::new(api, 300));
        let prices = &tickers.prices[Market::Spot as usize];
        let _blocked = prices.refresh.lock().await;
        let (stop, rx) = watch::channel(false);
        let t = tickers.clone();
        let task = tokio::spawn(async move { t.run_price_refresh(Market::Spot, rx).await });
        settle().await;
        assert_eq!(prices.rest_loads.load(Ordering::Relaxed), 0);
        stop.send(true).unwrap();
        observe(|| task.is_finished()).await;
        task.await.unwrap();
    }
}
