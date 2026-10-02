//! Immutable exchange metadata. Readers never join a maintenance refresh.
use crate::{
    error::{ApiError, Result},
    transport::RestApi,
};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use kline_core::Market;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Mutex, time::Instant};

struct Snapshot {
    value: Arc<Value>,
    encoded: Bytes,
    symbols: BTreeSet<String>,
    loaded: Instant,
}
impl Snapshot {
    fn validate(&self, symbols: &[String]) -> Result<()> {
        if symbols.iter().any(|symbol| !self.symbols.contains(symbol)) {
            Err(ApiError::bad(-1121, "Invalid symbol."))
        } else {
            Ok(())
        }
    }
}
pub struct Metadata {
    api: Arc<RestApi>,
    snapshots: [ArcSwapOption<Snapshot>; 2],
    refresh: [Mutex<()>; 2],
    scheduled: [AtomicBool; 2],
    loads: [AtomicU64; 2],
    failures: [AtomicU64; 2],
    retry_after: [parking_lot::Mutex<Instant>; 2],
    max_age: Duration,
}
impl Metadata {
    pub fn new(api: Arc<RestApi>, refresh_seconds: u64) -> Arc<Self> {
        Arc::new(Self {
            api,
            snapshots: std::array::from_fn(|_| ArcSwapOption::empty()),
            refresh: std::array::from_fn(|_| Mutex::new(())),
            scheduled: std::array::from_fn(|_| AtomicBool::new(false)),
            loads: std::array::from_fn(|_| AtomicU64::new(0)),
            failures: std::array::from_fn(|_| AtomicU64::new(0)),
            retry_after: std::array::from_fn(|_| parking_lot::Mutex::new(Instant::now())),
            max_age: Duration::from_secs(refresh_seconds.max(1)),
        })
    }
    // Leave headroom before the existing TTL expires; never extend the body's
    // age limit to conceal a slow or failed upstream refresh.
    pub fn refresh_interval(&self) -> Duration {
        self.max_age.mul_f64(0.8)
    }
    /// Maintain already-used markets even when there is no Kline subscription
    /// directory or ticker stream. Idle traffic must not create an expiry gap.
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        let period = self.max_age.mul_f64(0.1).min(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = stop.changed() => break,
                _ = tokio::time::sleep(period) => {}
            }
            for market in [Market::Future, Market::Spot] {
                if self.snapshots[market as usize]
                    .load()
                    .as_ref()
                    .is_some_and(|s| s.loaded.elapsed() >= self.refresh_interval())
                {
                    self.schedule(market);
                }
            }
        }
    }
    async fn load(&self, market: Market) -> Result<Arc<Snapshot>> {
        let index = market as usize;
        let previous = self.snapshots[index].load_full();
        let _refresh = self.refresh[index].lock().await;
        if let Some(current) = self.snapshots[index].load_full()
            && previous
                .as_ref()
                .is_none_or(|old| !Arc::ptr_eq(old, &current))
            && current.loaded.elapsed() < self.max_age
        {
            return Ok(current);
        }
        self.loads[index].fetch_add(1, Ordering::Relaxed);
        let path = if market == Market::Future {
            "/fapi/v1/exchangeInfo"
        } else {
            "/api/v3/exchangeInfo"
        };
        let value: Value = self
            .api
            .json_with_priority(
                market,
                path,
                &if market == Market::Spot {
                    vec![("showPermissionSets".into(), "false".into())]
                } else {
                    vec![]
                },
                if market == Market::Spot { 20 } else { 1 },
                crate::transport::Priority::Metadata,
            )
            .await
            .map_err(ApiError::from)?;
        let snapshot = self
            .api
            .cpu
            .run_large(move || {
                let value = crate::metadata_shape::shape(
                    if market == Market::Future {
                        "BinanceFutureExchange"
                    } else {
                        "BinanceSpotExchange"
                    },
                    &value,
                )?;
                let fields: std::collections::BTreeMap<_, _> = value
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| key.as_str() != "serverTime")
                    .collect();
                let encoded = Bytes::from(serde_json::to_vec(&fields).map_err(ApiError::internal)?);
                let symbols = value["symbols"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|row| row["symbol"].as_str().map(str::to_owned))
                    .collect();
                Ok::<_, ApiError>(Arc::new(Snapshot {
                    value: Arc::new(value),
                    encoded: encoded.slice(1..),
                    symbols,
                    loaded: Instant::now(),
                }))
            })
            .await
            .map_err(ApiError::from)??;
        // Publish data, body, symbol index and freshness together. Failed or
        // cancelled loads leave the last successful generation untouched.
        self.snapshots[index].store(Some(snapshot.clone()));
        Ok(snapshot)
    }
    fn schedule(self: &Arc<Self>, market: Market) {
        if *self.retry_after[market as usize].lock() > Instant::now() {
            return;
        }
        if self.scheduled[market as usize]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let metadata = self.clone();
        tokio::spawn(async move {
            struct Reset(Arc<Metadata>, Market);
            impl Drop for Reset {
                fn drop(&mut self) {
                    self.0.scheduled[self.1 as usize].store(false, Ordering::Release);
                }
            }
            let _reset = Reset(metadata.clone(), market);
            if let Err(error) = metadata.refresh(market).await {
                tracing::warn!(?market, %error, "metadata refresh retained the previous snapshot");
            }
        });
    }
    async fn for_query(
        self: &Arc<Self>,
        market: Market,
        max_age: Duration,
    ) -> Result<Arc<Snapshot>> {
        if let Some(snapshot) = self.snapshots[market as usize].load_full() {
            let age = snapshot.loaded.elapsed();
            if age >= self.refresh_interval() {
                self.schedule(market);
            }
            if age <= max_age {
                return Ok(snapshot);
            }
            return Err(unavailable());
        }
        // Only genuine cold bootstrap needs to wait for upstream data (five seconds from the
        // request's arrival, HTTP admission queueing included).
        tokio::time::timeout_at(
            kline_service::admission::budget_deadline(crate::QUERY_BUDGET),
            self.load(market),
        )
        .await
        .map_err(|_| unavailable())?
    }
    pub async fn get(self: &Arc<Self>, market: Market) -> Result<Arc<Value>> {
        Ok(self.for_query(market, self.max_age).await?.value.clone())
    }
    pub fn snapshot(&self, market: Market) -> Arc<Value> {
        self.snapshots[market as usize]
            .load_full()
            .map_or_else(|| Arc::new(Value::Null), |s| s.value.clone())
    }
    /// Symbol lookup retains its existing two-period grace; exchangeInfo has
    /// the stricter one-period TTL above.
    pub async fn query_snapshot(self: &Arc<Self>, market: Market) -> Result<Arc<Value>> {
        Ok(self
            .for_query(market, self.max_age.saturating_mul(2))
            .await?
            .value
            .clone())
    }
    pub async fn query_validated(
        self: &Arc<Self>,
        market: Market,
        symbols: &[String],
    ) -> Result<Arc<Value>> {
        let snapshot = self
            .for_query(market, self.max_age.saturating_mul(2))
            .await?;
        snapshot.validate(symbols)?;
        Ok(snapshot.value.clone())
    }
    pub fn validate_snapshot(value: &Value, symbols: &[String]) -> Result<()> {
        let valid = value["symbols"]
            .as_array()
            .ok_or_else(|| ApiError::internal("exchangeInfo missing symbols"))?;
        for symbol in symbols {
            if !valid
                .iter()
                .any(|row| row["symbol"].as_str() == Some(symbol.as_str()))
            {
                return Err(ApiError::bad(-1121, "Invalid symbol."));
            }
        }
        Ok(())
    }
    pub async fn body(self: &Arc<Self>, market: Market, now: i64) -> Result<(Bytes, Bytes)> {
        let snapshot = self.for_query(market, self.max_age).await?;
        let separator = if snapshot.encoded.len() == 1 { "" } else { "," };
        Ok((
            Bytes::from(format!("{{\"serverTime\":{now}{separator}")),
            snapshot.encoded.clone(),
        ))
    }
    pub async fn refresh(&self, market: Market) -> Result<Arc<Value>> {
        match self.load(market).await {
            Ok(snapshot) => Ok(snapshot.value.clone()),
            Err(error) => {
                self.failures[market as usize].fetch_add(1, Ordering::Relaxed);
                *self.retry_after[market as usize].lock() = Instant::now() + Duration::from_secs(1);
                Err(error)
            }
        }
    }
    pub async fn symbols(
        self: &Arc<Self>,
        market: Market,
        trading_only: bool,
    ) -> Result<Vec<String>> {
        let snapshot = self.for_query(market, self.max_age).await?;
        if !trading_only {
            return Ok(snapshot.symbols.iter().cloned().collect());
        }
        let mut symbols: Vec<_> = snapshot.value["symbols"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|row| row["status"] == "TRADING")
            .filter_map(|row| row["symbol"].as_str().map(str::to_owned))
            .collect();
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }
    pub async fn validate(self: &Arc<Self>, market: Market, symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }
        // Ordinary funding/premium validation retains its original strict TTL;
        // only ticker query_snapshot uses the two-period lookup grace.
        self.for_query(market, self.max_age)
            .await?
            .validate(symbols)
    }
    pub fn status(&self) -> Value {
        let market = |m: Market| {
            let index = m as usize;
            let snapshot = self.snapshots[index].load();
            serde_json::json!({"age_ms":snapshot.as_ref().map(|s| s.loaded.elapsed().as_millis() as u64),
                "max_body_age_ms":self.max_age.as_millis() as u64,"refresh_after_ms":self.refresh_interval().as_millis() as u64,
                "loads":self.loads[index].load(Ordering::Relaxed),"failures":self.failures[index].load(Ordering::Relaxed),
                "refreshing":self.refresh[index].try_lock().is_err()})
        };
        serde_json::json!({"spot":market(Market::Spot),"future":market(Market::Future)})
    }
}
fn unavailable() -> ApiError {
    ApiError {
        status: 503,
        code: -1008,
        message: "Fresh exchange metadata temporarily unavailable".into(),
        negotiation_fallback: None,
    }
}
