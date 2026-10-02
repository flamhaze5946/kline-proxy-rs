use crate::{
    error::{ApiError, Result},
    metadata::Metadata,
    transport::RestApi,
};
use futures_util::{SinkExt, StreamExt};
use kline_core::{Interval, Market};
use moka::future::Cache;
use parking_lot::RwLock;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
mod fallback;
mod live;
mod prices;
mod wire;
const DECIMALS: &[(&str, &str)] = &[
    ("priceChange", "p"),
    ("priceChangePercent", "P"),
    ("weightedAvgPrice", "w"),
    ("prevClosePrice", "x"),
    ("lastPrice", "c"),
    ("lastQty", "Q"),
    ("bidPrice", "b"),
    ("bidQty", "B"),
    ("askPrice", "a"),
    ("askQty", "A"),
    ("openPrice", "o"),
    ("highPrice", "h"),
    ("lowPrice", "l"),
    ("volume", "v"),
    ("quoteVolume", "q"),
];
const INTEGERS: &[(&str, &str)] = &[
    ("openTime", "O"),
    ("closeTime", "C"),
    ("firstId", "F"),
    ("lastId", "L"),
    ("count", "n"),
];
const MINI: &[&str] = &[
    "symbol",
    "openPrice",
    "highPrice",
    "lowPrice",
    "lastPrice",
    "volume",
    "quoteVolume",
    "openTime",
    "closeTime",
    "firstId",
    "lastId",
    "count",
];
fn ticker_path(market: Market, price: bool) -> &'static str {
    match (market, price) {
        (Market::Future, true) => "/fapi/v2/ticker/price",
        (Market::Future, false) => "/fapi/v1/ticker/24hr",
        (Market::Spot, true) => "/api/v3/ticker/price",
        (Market::Spot, false) => "/api/v3/ticker/24hr",
    }
}
struct CachedTicker {
    value: Arc<Value>,
    observed: tokio::time::Instant,
}
pub struct Tickers {
    api: Arc<RestApi>,
    metadata: Arc<Metadata>,
    engine: Option<Arc<kline_service::Engine>>,
    /// The engine's connect pacer, or one of its own without an engine.
    connects: Arc<kline_service::connect_pacer::ConnectPacer>,
    price_intervals: RwLock<[Vec<Interval>; 2]>,
    rows: [RwLock<BTreeMap<String, CachedTicker>>; 2],
    ticker_order: [RwLock<BTreeMap<String, usize>>; 2],
    baseline_ready: [AtomicBool; 2],
    baseline_refresh: [tokio::sync::Mutex<()>; 2],
    prices: [prices::Prices; 2],
    ticker_access: [AtomicU64; 2],
    baseline_loaded: [AtomicU64; 2],
    rendered: Cache<(Market, bool, bool), bytes::Bytes>,
    single: Cache<(Market, bool, String), Value>,
    pub messages: [AtomicU64; 2],
    next_query: AtomicU64,
    live: Arc<live::Live>,
    live_hits: [AtomicU64; 2],
    kline_hits: [AtomicU64; 2],
    rest_loads: [AtomicU64; 2],
}
impl Tickers {
    pub fn new(api: Arc<RestApi>, metadata: Arc<Metadata>) -> Arc<Self> {
        Self::build(api, metadata, None)
    }
    /// Supply local bars for single prices and Java's empty upstream fallback.
    pub fn with_engine(
        api: Arc<RestApi>,
        metadata: Arc<Metadata>,
        engine: Arc<kline_service::Engine>,
    ) -> Arc<Self> {
        Self::build(api, metadata, Some(engine))
    }
    fn build(
        api: Arc<RestApi>,
        metadata: Arc<Metadata>,
        engine: Option<Arc<kline_service::Engine>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            api,
            metadata,
            connects: engine
                .as_ref()
                .map_or_else(Arc::default, |e| e.connect_pacer().clone()),
            engine,
            price_intervals: RwLock::new(std::array::from_fn(|_| {
                vec![Interval::parse("1h").unwrap()]
            })),
            rows: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            ticker_order: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            baseline_ready: std::array::from_fn(|_| AtomicBool::new(false)),
            baseline_refresh: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
            prices: std::array::from_fn(|_| prices::Prices::new()),
            ticker_access: std::array::from_fn(|_| AtomicU64::new(0)),
            baseline_loaded: std::array::from_fn(|_| AtomicU64::new(0)),
            rendered: Cache::builder()
                .max_capacity(8)
                .time_to_live(Duration::from_millis(500))
                .build(),
            single: Cache::builder()
                .max_capacity(4096)
                .time_to_live(Duration::from_millis(500))
                .build(),
            messages: std::array::from_fn(|_| AtomicU64::new(0)),
            next_query: AtomicU64::new(1),
            live: Arc::new(live::Live::new()),
            live_hits: std::array::from_fn(|_| AtomicU64::new(0)),
            kline_hits: std::array::from_fn(|_| AtomicU64::new(0)),
            rest_loads: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }
    /// Runtime configuration order is independent of the sorted symbol catalog.
    pub fn set_fallback_intervals(&self, intervals: [Interval; 2]) {
        self.set_price_intervals(intervals.map(|interval| vec![interval]));
    }
    /// Single prices try every configured interval; empty-list fallback uses the first.
    pub fn set_price_intervals(&self, intervals: [Vec<Interval>; 2]) {
        *self.price_intervals.write() = intervals.map(|mut intervals| {
            if intervals.is_empty() {
                intervals.push(Interval::parse("1h").unwrap());
            }
            intervals
        });
    }
    pub fn ingest(&self, market: Market, raw: &[u8]) -> Result<()> {
        self.ingest_connected(market, raw, &Arc::new(AtomicBool::new(true)))
    }
    fn ingest_connected(
        &self,
        market: Market,
        raw: &[u8],
        connection: &Arc<AtomicBool>,
    ) -> Result<()> {
        let mut updates = vec![];
        wire::decode(raw, |row| {
            if market == Market::Future && row.kind.as_deref() == Some("aggTrade") {
                self.live.trade(row.trade()?, connection)?;
                return Ok(());
            }
            if row.kind.as_deref() != Some("24hrTicker") {
                return Ok(());
            }
            let value = Arc::new(row.display()?);
            self.live
                .ticker(market, row.event_ms(), value.clone(), connection)?;
            updates.push(value);
            Ok(())
        })?;
        if !updates.is_empty() {
            self.merge(market, updates);
            if self.messages[market as usize].fetch_add(1, Ordering::Relaxed) == 0 {
                tracing::info!(?market, "ticker stream data received");
            }
        }
        self.live.publish(market);
        Ok(())
    }
    pub fn wanted_price_symbols(&self, trading: &[String]) -> Vec<String> {
        self.live.wanted(trading)
    }
    pub async fn price_demand_changed(&self) {
        self.live.demand.notified().await;
    }
    fn merge(&self, market: Market, updates: Vec<Arc<Value>>) {
        let mut rows = self.rows[market as usize].write();
        let observed = tokio::time::Instant::now();
        for row in updates {
            if let Some(s) = row["symbol"].as_str()
                && rows.get(s).is_none_or(|old| {
                    old.value["closeTime"].as_i64().unwrap_or(0)
                        <= row["closeTime"].as_i64().unwrap_or(0)
                })
            {
                if let Some(previous) = rows.get_mut(s) {
                    *previous = CachedTicker {
                        value: row,
                        observed,
                    };
                } else {
                    rows.insert(
                        s.to_owned(),
                        CachedTicker {
                            value: row,
                            observed,
                        },
                    );
                }
            }
        }
    }
    pub async fn refresh(&self, market: Market) -> Result<()> {
        self.refresh_baseline(market, true).await
    }
    async fn refresh_baseline(&self, market: Market, force: bool) -> Result<()> {
        // Hot multi-symbol queries only need the published order; a background
        // REST refresh must not make them wait for its network-held mutex.
        if !force && self.baseline_ready[market as usize].load(Ordering::Acquire) {
            return Ok(());
        }
        let _refresh = self.baseline_refresh[market as usize].lock().await;
        if !force && self.baseline_ready[market as usize].load(Ordering::Acquire) {
            return Ok(());
        }
        let rows: Vec<Value> = self
            .api
            .json_with_priority(
                market,
                ticker_path(market, false),
                &[],
                if market == Market::Future { 40 } else { 80 },
                if force {
                    crate::transport::Priority::Background
                } else {
                    crate::transport::Priority::Foreground
                },
            )
            .await
            .map_err(ApiError::from)?;
        let (updates, order) = self
            .api
            .cpu
            .run(move || {
                let updates = rows
                    .into_iter()
                    .map(|r| display(&r, false).map(Arc::new))
                    .collect::<Result<Vec<_>>>()?;
                let order = updates
                    .iter()
                    .enumerate()
                    .filter_map(|(rank, row)| {
                        row["symbol"]
                            .as_str()
                            .map(|symbol| (symbol.to_owned(), rank))
                    })
                    .collect();
                Ok::<_, ApiError>((updates, order))
            })
            .await
            .map_err(ApiError::from)??;
        self.merge(market, updates);
        *self.ticker_order[market as usize].write() = order;
        let valid = self
            .metadata
            .symbols(market, false)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.rows[market as usize]
            .write()
            .retain(|s, _| valid.contains(s));
        self.live.retain(market, &valid);
        self.baseline_loaded[market as usize]
            .store(crate::funding::now() as u64, Ordering::Release);
        self.baseline_ready[market as usize].store(true, Ordering::Release);
        self.rendered.invalidate_all();
        Ok(())
    }
    /// REST maintenance and price publication have independent, cancellable lifetimes.
    pub async fn run(
        self: Arc<Self>,
        market: Market,
        proactive_baseline: bool,
        stop: tokio::sync::watch::Receiver<bool>,
    ) {
        tokio::join!(
            self.run_ticker_refresh(market, proactive_baseline, stop.clone()),
            self.run_price_refresh(market, stop.clone()),
            self.run_price_publisher(market, stop),
        );
    }
    async fn run_ticker_refresh(
        &self,
        market: Market,
        proactive_baseline: bool,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut baseline_due = tokio::time::Instant::now();
        loop {
            if *stop.borrow() {
                break;
            }
            let now = crate::funding::now() as u64;
            let access = self.ticker_access[market as usize].load(Ordering::Relaxed);
            let active = access != 0 && now.saturating_sub(access) <= 5_000;
            let old_baseline = now
                .saturating_sub(self.baseline_loaded[market as usize].load(Ordering::Acquire))
                >= 500;
            if (proactive_baseline && tokio::time::Instant::now() >= baseline_due)
                || (!proactive_baseline && active && old_baseline)
            {
                tokio::select! { _ = stop.changed() => break, result = self.refresh(market) => {
                    if let Err(e) = result { tracing::warn!(?market,error=%e,"ticker baseline refresh failed"); }
                }}
                baseline_due = tokio::time::Instant::now() + Duration::from_secs(60);
            }
            tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
        }
    }
    pub fn status(&self) -> Value {
        let market = |m: Market| serde_json::json!({"frames":self.messages[m as usize].load(Ordering::Relaxed),"symbols":self.rows[m as usize].read().len(),"live_query_hits":self.live_hits[m as usize].load(Ordering::Relaxed),"kline_price_hits":self.kline_hits[m as usize].load(Ordering::Relaxed),"single_rest_loads":self.rest_loads[m as usize].load(Ordering::Relaxed),"price_snapshot":self.prices[m as usize].status()});
        serde_json::json!({"future":market(Market::Future),"spot":market(Market::Spot),"trade_frames":self.live.trade_frames.load(Ordering::Relaxed)})
    }
    #[tracing::instrument(name = "ticker_query", skip_all, fields(
        query_id = self.next_query.fetch_add(1, Ordering::Relaxed), ?market, price = price,
        symbol = symbol.unwrap_or("*")
    ))]
    pub async fn query(
        &self,
        market: Market,
        price: bool,
        symbol: Option<&str>,
        symbols: Vec<String>,
        mini: bool,
        status: Option<&str>,
    ) -> Result<bytes::Bytes> {
        // Five seconds from the request's arrival, so time queued for HTTP admission counts.
        tokio::time::timeout_at(
            kline_service::admission::budget_deadline(crate::QUERY_BUDGET),
            self.query_inner(market, price, symbol, symbols, mini, status),
        )
        .await
        .map_err(|_| ApiError {
            negotiation_fallback: None,
            status: 503,
            code: -1008,
            message: "Fresh market data temporarily unavailable".into(),
        })?
    }
    async fn query_inner(
        &self,
        market: Market,
        price: bool,
        symbol: Option<&str>,
        symbols: Vec<String>,
        mini: bool,
        status: Option<&str>,
    ) -> Result<bytes::Bytes> {
        let explicit = symbol.is_some() || !symbols.is_empty();
        if !explicit && price {
            return self.all_prices(market).await;
        }
        let metadata_started = tokio::time::Instant::now();
        let meta = if explicit || status.is_some() {
            Some(self.metadata.query_validated(market, &symbols).await?)
        } else {
            None
        };
        let metadata_ms = metadata_started.elapsed().as_secs_f64() * 1000.;
        if metadata_ms >= 250. {
            tracing::info!(metadata_ms, success = true, "ticker metadata completed");
        }
        let statuses: BTreeMap<_, _> = meta
            .as_ref()
            .filter(|_| status.is_some())
            .and_then(|m| m["symbols"].as_array())
            .into_iter()
            .flatten()
            .filter_map(|s| Some((s["symbol"].as_str()?, s["status"].as_str()?)))
            .collect();
        let mut selected: Vec<_> = symbols
            .into_iter()
            .filter(|s| status.is_none_or(|status| statuses.get(s.as_str()) == Some(&status)))
            .collect();
        if symbol.is_some() && selected.is_empty() {
            return Err(ApiError::bad(
                -1220,
                "The symbol's status does not match the requested symbolStatus",
            ));
        }
        if explicit {
            let use_baseline = selected.len() > 1;
            if use_baseline {
                if price {
                    self.order_price_symbols(market, &mut selected).await?;
                    if self.price_baseline_empty(market) {
                        return encode(self.fallback_prices(market, &selected)?);
                    }
                } else {
                    // One shared all-market baseline gives every scattered request
                    // the same upstream order, without per-combination REST calls.
                    self.refresh_baseline(market, false).await?;
                    let order = self.ticker_order[market as usize].read();
                    if order.is_empty() {
                        let rows: Vec<_> = selected
                            .iter()
                            .filter_map(|symbol| self.fallback_ticker(market, symbol))
                            .collect();
                        return encode(if mini {
                            rows.into_iter().map(mini_value).collect::<Vec<_>>()
                        } else {
                            rows
                        });
                    }
                    selected.sort_by_key(|symbol| order.get(symbol).copied().unwrap_or(usize::MAX));
                }
            }
            if price && market == Market::Future {
                self.live.want(&selected);
            }
            let values: Vec<_> = futures_util::stream::iter(selected)
                .map(|s| async move {
                    let mut updates = self.live.changed[market as usize].subscribe();
                    if let Some(value) = self.live.get(market, &s, price) {
                        self.live_hits[market as usize].fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(value));
                    }
                    if price
                        && !use_baseline
                        && let Some(value) = self.cached_single_price(market, &s)?
                    {
                        self.kline_hits[market as usize].fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(value));
                    }
                    let baseline = if !use_baseline {
                        None
                    } else if price {
                        self.recent_baseline_price(market, &s)
                    } else {
                        self.cached_ticker(market, &s, Duration::from_millis(500))
                    };
                    if let Some(value) = baseline {
                        return Ok(Some(value));
                    }
                    let key = (market, price, s.clone());
                    let cache_started = tokio::time::Instant::now();
                    let load_owner = AtomicBool::new(false);
                    let fallback = self.single.try_get_with(key, async {
                        load_owner.store(true, Ordering::Relaxed);
                        self.rest_loads[market as usize].fetch_add(1, Ordering::Relaxed);
                        tracing::info!(symbol = %s, "ticker cache miss loader started");
                        let raw: Value = self
                            .api
                            .json(
                                market,
                                ticker_path(market, price),
                                &[("symbol".into(), s.clone())],
                                if market == Market::Future { 1 } else { 2 },
                            )
                            .await
                            .map_err(ApiError::from)?;
                        if price {
                            let value = price_display(&raw, market)?;
                            self.live.observed_rest_price(market, &value);
                            Ok::<_, ApiError>(value)
                        } else {
                            let value = display(&raw, false)?;
                            self.merge(market, vec![Arc::new(value.clone())]);
                            Ok(value)
                        }
                    });
                    tokio::pin!(fallback);
                    let result = loop {
                        if let Some(value) = self.live.get(market, &s, price) {
                            self.live_hits[market as usize].fetch_add(1, Ordering::Relaxed);
                            return Ok(Some(value));
                        }
                        tokio::select! {
                            result = &mut fallback => break result,
                            _ = updates.changed() => {}
                        }
                    };
                    let cache_ms = cache_started.elapsed().as_secs_f64() * 1000.;
                    let load_owner = load_owner.load(Ordering::Relaxed);
                    if load_owner || cache_ms >= 250. {
                        tracing::info!(
                            metadata_ms,
                            cache_ms,
                            load_owner,
                            success = result.is_ok(),
                            "ticker cache lookup completed"
                        );
                    }
                    let value = result.map_err(|e| (*e).clone())?;
                    Ok(Some(value))
                })
                .buffered(4)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            let values = if mini {
                values.into_iter().map(mini_value).collect()
            } else {
                values
            };
            return encode(if symbol.is_some() {
                values.into_iter().next().unwrap_or(Value::Array(vec![]))
            } else {
                Value::Array(values)
            });
        }
        self.ticker_access[market as usize].store(crate::funding::now() as u64, Ordering::Relaxed);
        let build = async {
            if !self.baseline_ready[market as usize].load(Ordering::Acquire) {
                self.refresh_baseline(market, false).await?
            }
            let mut values: Vec<_> = self.rows[market as usize]
                .read()
                .values()
                .filter(|r| {
                    r.observed.elapsed() < Duration::from_secs(86_400)
                        && status.is_none_or(|s| {
                            statuses.get(r.value["symbol"].as_str().unwrap_or("")) == Some(&s)
                        })
                })
                .map(|row| row.value.clone())
                .collect();
            let order = self.ticker_order[market as usize].read();
            values.sort_by_key(|row| {
                order
                    .get(row["symbol"].as_str().unwrap_or(""))
                    .copied()
                    .unwrap_or(usize::MAX)
            });
            if mini {
                encode(
                    values
                        .into_iter()
                        .map(|row| mini_value((*row).clone()))
                        .collect::<Vec<_>>(),
                )
            } else {
                encode(values)
            }
        };
        if status.is_some() {
            build.await
        } else {
            self.rendered
                .try_get_with((market, price, mini), build)
                .await
                .map_err(|e| (*e).clone())
        }
    }
    pub async fn stream(
        self: Arc<Self>,
        market: Market,
        url: String,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut backoff = 1;
        while !*stop.borrow() {
            tokio::select! {_=stop.changed()=>break,_=self.connects.turn()=>{}}
            let connect = tokio_tungstenite::connect_async_with_config(
                &url,
                Some(
                    WebSocketConfig::default()
                        .max_message_size(Some(2 * 1024 * 1024))
                        .max_frame_size(Some(2 * 1024 * 1024)),
                ),
                true,
            );
            let result = tokio::select! {_=stop.changed()=>break,result=tokio::time::timeout(Duration::from_secs(15),connect)=>result};
            if let Ok(Ok((mut socket, _))) = result {
                let connection = live::Connection::new();
                let started = tokio::time::Instant::now();
                let mut inline_frames = 0;
                loop {
                    let frame = tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep_until(started+Duration::from_secs(23*3600))=>break,frame=tokio::time::timeout(Duration::from_secs(180),socket.next())=>frame};
                    match frame {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Small ticker/trade events take less work than an OS-thread
                            // round trip. Keep large array frames on the bounded CPU pool.
                            let result = if text.len() <= 4096 {
                                inline_frames += 1;
                                self.ingest_connected(market, text.as_bytes(), &connection.0)
                            } else {
                                self.api
                                    .cpu
                                    .run({
                                        let this = self.clone();
                                        let source = connection.0.clone();
                                        move || {
                                            this.ingest_connected(market, text.as_bytes(), &source)
                                        }
                                    })
                                    .await
                                    .map_err(ApiError::from)
                                    .and_then(|r| r)
                            };
                            if let Err(e) = result {
                                tracing::warn!(error=%e,"ticker frame rejected")
                            }
                            if inline_frames >= 32 {
                                inline_frames = 0;
                                tokio::task::yield_now().await;
                            }
                        }
                        Ok(Some(Ok(Message::Ping(_)))) => {
                            if socket.flush().await.is_err() {
                                break;
                            }
                        }
                        Ok(Some(Ok(Message::Pong(_)))) => {}
                        _ => break,
                    }
                }
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = 1;
                }
            }
            tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(backoff))=>{}}
            backoff = (backoff * 2).min(30);
        }
    }
}
fn encode(value: impl serde::Serialize) -> Result<bytes::Bytes> {
    serde_json::to_vec(&value)
        .map(bytes::Bytes::from)
        .map_err(ApiError::internal)
}
fn mini_value(mut value: Value) -> Value {
    if let Some(map) = value.as_object_mut() {
        map.retain(|k, _| MINI.contains(&k.as_str()));
    }
    value
}
pub fn display(raw: &Value, ws: bool) -> Result<Value> {
    if !raw.is_object() {
        return Err(ApiError::upstream_io("invalid ticker object"));
    }
    let mut out = Map::new();
    if let Some(s) = raw
        .get(if ws { "s" } else { "symbol" })
        .filter(|v| !v.is_null())
    {
        out.insert("symbol".into(), crate::metadata_shape::shape("String", s)?);
    }
    for (name, short) in DECIMALS {
        if let Some(v) = raw
            .get(if ws { *short } else { *name })
            .filter(|v| !v.is_null())
        {
            let value = crate::metadata_shape::shape("DecimalString", v)?;
            if !value.is_null() {
                out.insert((*name).into(), value);
            }
        }
    }
    for (name, short) in INTEGERS {
        if let Some(v) = raw
            .get(if ws { *short } else { *name })
            .filter(|v| !v.is_null())
        {
            let value = crate::metadata_shape::shape("Long", v)?;
            if !value.is_null() {
                out.insert((*name).into(), value);
            }
        }
    }
    Ok(out.into())
}
fn price_display(raw: &Value, market: Market) -> Result<Value> {
    if !raw.is_object() {
        return Err(ApiError::upstream_io("invalid ticker object"));
    }
    let price = crate::metadata_shape::shape("DecimalString", &raw["price"])?;
    // Java's DTO permits a null BigDecimal, but ConvertUtil dereferences it.
    // This is a conversion error (500), distinct from a malformed DTO (502).
    if price.is_null() {
        return Err(ApiError::internal("ticker price is null"));
    }
    let mut out = serde_json::json!({"price":price});
    let symbol = crate::metadata_shape::shape("String", &raw["symbol"])?;
    if !symbol.is_null() {
        out["symbol"] = symbol;
    }
    if market == Market::Future {
        let time = crate::metadata_shape::shape("Long", &raw["time"])?;
        out["time"] = if time.is_null() { 0.into() } else { time };
    }
    Ok(out)
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use tokio::sync::watch;
    #[tokio::test]
    async fn a_closed_socket_immediately_invalidates_its_prices() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (close, closed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let now = crate::funding::now();
            socket.send(Message::Text(serde_json::json!({"e":"aggTrade","s":"BTCUSDT","E":now,"T":now-5,"a":9,"p":"123.4000"}).to_string().into())).await.unwrap();
            closed.await.unwrap();
            socket.close(None).await.unwrap();
        });
        let api = RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        let tickers = Tickers::new(api.clone(), Metadata::new(api, 300));
        let (stop, stopped) = watch::channel(false);
        let stream = tokio::spawn(tickers.clone().stream(
            Market::Future,
            format!("ws://{address}"),
            stopped,
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while tickers.live.get(Market::Future, "BTCUSDT", true).is_none() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        close.send(()).unwrap();
        server.await.unwrap();
        tokio::time::timeout(Duration::from_millis(200), async {
            while tickers.live.get(Market::Future, "BTCUSDT", true).is_some() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(200), stream)
            .await
            .unwrap()
            .unwrap();
    }
}
