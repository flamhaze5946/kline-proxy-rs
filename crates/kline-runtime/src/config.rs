use anyhow::{Context, Result, bail};
use kline_core::{Interval, Market};
use kline_service::{Catalog, Instrument};
use serde::Deserialize;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    #[serde(default = "listen_backlog_default")]
    pub listen_backlog: u32,
    #[serde(default = "number_default")]
    pub number_type: String,
    #[serde(default = "yes")]
    pub strict_readiness: bool,
    #[serde(default = "yes")]
    pub closed_bar_latency_enabled: bool,
    #[serde(default = "history_default")]
    pub history_capacity: usize,
    #[serde(default = "wait_default")]
    pub final_wait_ms: u64,
    /// Distinct bulk keys in flight at once; further new keys queue for a slot.
    #[serde(default = "bulk_inflight_default")]
    pub bulk_inflight_limit: usize,
    /// New bulk keys allowed to wait for a slot; beyond it they are answered 503 -1008 at once.
    #[serde(default = "bulk_admission_queue_default")]
    pub bulk_admission_queue: usize,
    /// Longest wait for a slot (absent: final_wait_ms), then 503 -1008.
    #[serde(default)]
    pub bulk_admission_wait_ms: Option<u64>,
    /// A closed_only bulk request arriving at most this long before an interval boundary waits
    /// for it (absent: 250; 0 disables), as Java's kline.bulk.preBoundaryWaitMs.
    #[serde(default)]
    pub bulk_pre_boundary_wait_ms: Option<u64>,
    /// Concurrent requests per HTTP route group; further requests queue for a place.
    #[serde(default = "http_concurrency_default")]
    pub http_concurrency_limit: usize,
    /// Requests allowed to queue per route group; beyond it they are answered 503 -1008 at once.
    #[serde(default = "bulk_admission_queue_default")]
    pub http_admission_queue: usize,
    /// Longest wait for a place (absent: final_wait_ms), then 503 -1008.
    #[serde(default)]
    pub http_admission_wait_ms: Option<u64>,
    #[serde(default)]
    pub clock_offset_ms: i64,
    #[serde(default)]
    pub instruments: Vec<InstrumentConfig>,
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionConfig>,
    #[serde(default)]
    pub market_api: kline_market::config::Config,
    #[serde(default)]
    pub websocket: WebsocketConfig,
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
    pub seed: Option<PathBuf>,
    pub rest: Option<RestConfig>,
    pub persistence: Option<PersistenceConfig>,
}
fn listen_backlog_default() -> u32 {
    1024
}
fn number_default() -> String {
    "double".into()
}
fn history_default() -> usize {
    1000
}
fn wait_default() -> u64 {
    8000
}
fn bulk_inflight_default() -> usize {
    256
}
fn bulk_admission_queue_default() -> usize {
    4096
}
fn http_concurrency_default() -> usize {
    512
}
fn yes() -> bool {
    true
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentConfig {
    pub market: String,
    pub symbol: String,
    pub intervals: Vec<String>,
    #[serde(default = "yes")]
    pub trading: bool,
    pub pair: Option<String>,
    pub contract_type: Option<String>,
    #[serde(default)]
    pub history_capacities: std::collections::BTreeMap<String, usize>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    pub market: String,
    pub url: String,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RestConfig {
    pub spot_url: String,
    pub future_url: String,
    pub weight_per_minute: u32,
    pub workers: usize,
    pub reconcile_seconds: u64,
    pub retry_seconds: u64,
    /// Legacy config fields retained for compatibility; automatic tail REST repair is disabled.
    pub latest_repair_after_ms: u64,
    pub latest_repair_workers: usize,
    pub boundary_guard_before_ms: u64,
    pub boundary_guard_after_ms: u64,
    pub sync_clock: bool,
    pub freshness_grace_ms: u64,
    /// Java rpcRefreshCount, independently configurable per market. Null means retained capacity.
    pub future_refresh_count: Option<usize>,
    pub spot_refresh_count: Option<usize>,
    pub hour_boundary_guard_before_ms: u64,
    pub hour_boundary_guard_after_ms: u64,
}
impl Default for RestConfig {
    fn default() -> Self {
        Self {
            spot_url: "https://api.binance.com".into(),
            future_url: "https://fapi.binance.com".into(),
            weight_per_minute: 1200,
            workers: 4,
            reconcile_seconds: 300,
            retry_seconds: 10,
            latest_repair_after_ms: 500,
            latest_repair_workers: 2,
            boundary_guard_before_ms: 3000,
            boundary_guard_after_ms: 10000,
            sync_clock: true,
            freshness_grace_ms: 15000,
            future_refresh_count: Some(99),
            spot_refresh_count: Some(99),
            hour_boundary_guard_before_ms: 0,
            hour_boundary_guard_after_ms: 0,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub directory: PathBuf,
    pub legacy_directory: Option<PathBuf>,
    #[serde(default = "yes")]
    pub load_on_startup: bool,
    #[serde(default = "yes")]
    pub dump_on_shutdown: bool,
    pub max_store_count: Option<usize>,
    /// None preserves native configs (all periods); Some([]) disables persistence IO.
    pub enabled_intervals: Option<Vec<PersistenceInterval>>,
    #[serde(default)]
    pub retention: Vec<Retention>,
    #[serde(default = "dump_default")]
    pub interval_seconds: u64,
    #[serde(default = "guard_default")]
    pub boundary_guard_before_ms: u64,
    #[serde(default = "guard_default")]
    pub boundary_guard_after_ms: u64,
}
fn dump_default() -> u64 {
    300
}
fn guard_default() -> u64 {
    30000
}
pub fn market(value: &str) -> Result<Market> {
    match value {
        "future" => Ok(Market::Future),
        "spot" => Ok(Market::Spot),
        _ => bail!("invalid market: {value}"),
    }
}
pub fn market_name(value: Market) -> &'static str {
    match value {
        Market::Future => "future",
        Market::Spot => "spot",
    }
}
impl Config {
    /// Preserve the first configured interval for the legacy empty-list fallback.
    pub fn ticker_fallback_intervals(&self) -> Result<[Interval; 2]> {
        Ok(self.ticker_price_intervals()?.map(|intervals| intervals[0]))
    }
    /// Walk all configured periods in insertion order, including partially loaded ones.
    pub fn ticker_price_intervals(&self) -> Result<[Vec<Interval>; 2]> {
        let mut intervals: [Vec<Interval>; 2] = std::array::from_fn(|_| vec![]);
        for sub in &self.subscriptions {
            let index = market(&sub.market)? as usize;
            let interval =
                Interval::parse(&sub.interval).context("invalid subscription interval")?;
            if !intervals[index].contains(&interval) {
                intervals[index].push(interval);
            }
        }
        for instrument in &self.instruments {
            let index = market(&instrument.market)? as usize;
            for interval in &instrument.intervals {
                let interval = Interval::parse(interval).context("invalid interval")?;
                if !intervals[index].contains(&interval) {
                    intervals[index].push(interval);
                }
            }
        }
        Ok(intervals.map(|mut intervals| {
            if intervals.is_empty() {
                intervals.push(Interval::parse("1h").unwrap());
            }
            intervals
        }))
    }
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        let base = path.parent().unwrap_or(Path::new("."));
        if let Some(seed) = &mut config.seed {
            *seed = base.join(&*seed);
        }
        if let Some(p) = &mut config.persistence {
            p.directory = base.join(&p.directory);
            if let Some(legacy) = &mut p.legacy_directory {
                *legacy = base.join(&*legacy);
            }
        }
        Ok(config)
    }
    /// Engine settings from this configuration; admission waits fall back to final_wait_ms.
    pub fn engine_settings(&self) -> Result<kline_service::Settings> {
        Ok(kline_service::Settings {
            final_wait_ms: self.final_wait_ms,
            inflight_limit: self.bulk_inflight_limit,
            admission_queue: self.bulk_admission_queue,
            admission_wait_ms: self.bulk_admission_wait_ms.unwrap_or(self.final_wait_ms),
            pre_boundary_wait_ms: self.bulk_pre_boundary_wait_ms.unwrap_or(250),
            http_concurrency_limit: self.http_concurrency_limit,
            http_admission_queue: self.http_admission_queue,
            http_admission_wait_ms: self.http_admission_wait_ms.unwrap_or(self.final_wait_ms),
            closed_bar_latency_enabled: self.closed_bar_latency_enabled,
            number_type: kline_core::NumberType::parse(&self.number_type)
                .context("invalid number type")?,
            connect_journal: self
                .persistence
                .as_ref()
                .map(|p| p.directory.join("ws-connect-attempts.json")),
            ..kline_service::Settings::default()
        })
    }
    pub fn catalog(&self) -> Result<Catalog> {
        anyhow::ensure!(
            (1..=i32::MAX as u32).contains(&self.listen_backlog),
            "listen_backlog must be 1..2147483647 (also capped by the operating system)"
        );
        anyhow::ensure!(
            kline_core::NumberType::parse(&self.number_type).is_some(),
            "invalid number_type"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.history_capacity),
            "history_capacity must be 1..100000"
        );
        let instruments = self.static_instruments()?;
        for i in &instruments {
            anyhow::ensure!(
                i.capacity.get() <= 100_000 && (self.rest.is_none() || i.capacity.get() >= 2),
                "invalid series history capacity"
            );
        }
        for sub in &self.subscriptions {
            let m = market(&sub.market)?;
            Interval::parse(&sub.interval).context("invalid subscription interval")?;
            anyhow::ensure!(
                m != Market::Future || sub.interval != "1s",
                "futures REST does not support 1s"
            );
            anyhow::ensure!(
                !sub.continuous || m == Market::Future,
                "continuous subscriptions require futures"
            );
            anyhow::ensure!(
                !sub.symbol_patterns.is_empty(),
                "subscription patterns cannot be empty"
            );
            anyhow::ensure!(
                (2..=100_000).contains(&sub.history_capacity),
                "invalid subscription capacity"
            );
            for pattern in &sub.symbol_patterns {
                regex::Regex::new(&format!("^(?:{pattern})$"))?;
            }
            for (symbol, count) in &sub.symbol_capacities {
                validate_symbol(symbol)?;
                anyhow::ensure!((2..=100_000).contains(count), "invalid symbol capacity");
            }
        }
        anyhow::ensure!(
            self.subscriptions.is_empty() || self.rest.is_some(),
            "dynamic subscriptions require REST"
        );
        let minimum = instruments
            .iter()
            .map(|i| i.interval.millis() as u64)
            .chain(
                self.subscriptions
                    .iter()
                    .filter_map(|s| Interval::parse(&s.interval))
                    .map(|i| i.millis() as u64),
            )
            .min()
            .unwrap_or(3_600_000);
        if let Some(r) = &self.rest {
            validate_guard(
                r.hour_boundary_guard_before_ms,
                r.hour_boundary_guard_after_ms,
                3_600_000,
            )?;
            anyhow::ensure!(
                self.history_capacity >= 2,
                "live REST mode needs history_capacity >= 2"
            );
            anyhow::ensure!(
                (1..=32).contains(&r.workers)
                    && (1..=3600).contains(&r.reconcile_seconds)
                    && (1..=3600).contains(&r.retry_seconds)
                    && r.freshness_grace_ms <= 300_000
                    && (100..=30_000).contains(&r.latest_repair_after_ms)
                    && (1..=4).contains(&r.latest_repair_workers),
                "invalid recovery settings"
            );
            validate_guard(
                r.boundary_guard_before_ms,
                r.boundary_guard_after_ms,
                minimum,
            )?;
            for count in [r.future_refresh_count, r.spot_refresh_count]
                .into_iter()
                .flatten()
            {
                anyhow::ensure!(
                    (1..=100_000).contains(&count),
                    "invalid history refresh count"
                );
            }
        }
        if let Some(p) = &self.persistence {
            let mut enabled = std::collections::BTreeSet::new();
            for rule in p.enabled_intervals.iter().flatten() {
                market(&rule.market)?;
                Interval::parse(&rule.interval)
                    .context("invalid persistence interval selection")?;
                anyhow::ensure!(
                    enabled.insert((&rule.market, &rule.interval)),
                    "duplicate persistence interval selection"
                );
            }
            let mut keys = std::collections::BTreeSet::new();
            for rule in &p.retention {
                market(&rule.market)?;
                Interval::parse(&rule.interval).context("invalid retention interval")?;
                anyhow::ensure!(
                    keys.insert((&rule.market, &rule.interval)),
                    "duplicate retention rule"
                );
                for symbol in rule.symbol_counts.keys() {
                    validate_symbol(symbol)?;
                }
            }
            anyhow::ensure!(
                (1..=86400).contains(&p.interval_seconds),
                "invalid persistence interval"
            );
            validate_guard(
                p.boundary_guard_before_ms,
                p.boundary_guard_after_ms,
                minimum,
            )?;
        }
        if let Some(p) = &self.persistence {
            for count in p
                .max_store_count
                .into_iter()
                .chain(p.retention.iter().flat_map(|r| {
                    std::iter::once(r.max_store_count).chain(r.symbol_counts.values().copied())
                }))
            {
                anyhow::ensure!((1..=100_000).contains(&count), "invalid disk retention");
            }
        }
        anyhow::ensure!(
            (30..=86400).contains(&self.websocket.topic_stale_seconds)
                && self.websocket.batch_size > 0
                && self.websocket.batch_size <= 200,
            "invalid WebSocket settings"
        );
        anyhow::ensure!(
            (1..=86400).contains(&self.market_api.metadata_refresh_seconds)
                && self.market_api.funding.publication_grace_ms <= 60_000
                && self.market_api.funding.vision_days <= 365
                && self.market_api.statistics.days > 0
                && self.market_api.statistics.days <= 3650
                && self.market_api.statistics.atr_period >= 2
                && self.market_api.statistics.atr_period <= 100_000
                && self.market_api.statistics.volume_rank > 0
                && self.market_api.statistics.volume_days <= 100_000
                && (1..=32).contains(&self.market_api.funding.vision_workers),
            "invalid market API settings"
        );
        for url in [&self.websocket.future_url, &self.websocket.spot_url] {
            validate_stream(url)?;
        }
        for stream in &self.streams {
            market(&stream.market)?;
            validate_stream(&stream.url)?;
        }
        for url in [
            &self.market_api.cms_url,
            &self.market_api.statistics.altcoin_url,
            &self.market_api.funding.vision_url,
        ] {
            kline_market::transport::endpoint(url)?;
        }
        if let Some(rest) = &self.rest {
            kline_market::transport::endpoint(&rest.spot_url)?;
            kline_market::transport::endpoint(&rest.future_url)?;
            anyhow::ensure!(
                (1..=6000).contains(&rest.weight_per_minute),
                "invalid REST weight budget"
            );
        }
        chrono::NaiveDate::parse_from_str(&self.market_api.statistics.start_date, "%Y-%m-%d")
            .context("invalid statistic start date")?;
        anyhow::ensure!(
            self.market_api
                .statistics
                .timezone_offset_minutes
                .is_none_or(|n| (-1439..=1439).contains(&n)),
            "invalid statistics timezone offset"
        );
        Catalog::new(instruments).map_err(anyhow::Error::msg)
    }
    pub fn static_instruments(&self) -> Result<Vec<Instrument>> {
        let mut instruments = Vec::new();
        for i in &self.instruments {
            validate_symbol(&i.symbol)?;
            anyhow::ensure!(
                i.pair.is_some() == i.contract_type.is_some(),
                "pair and contract_type must be set together"
            );
            anyhow::ensure!(
                !i.intervals.is_empty() && i.symbol.len() <= 128,
                "invalid instrument"
            );
            for code in &i.intervals {
                let interval = Interval::parse(code).context("invalid interval")?;
                let m = market(&i.market)?;
                anyhow::ensure!(
                    self.rest.is_none() || m != Market::Future || code != "1s",
                    "futures REST does not support 1s"
                );
                instruments.push(Instrument {
                    market: m,
                    symbol: i.symbol.clone(),
                    interval,
                    trading: i.trading,
                    continuous: i.pair.clone().zip(i.contract_type.clone()),
                    capacity: NonZeroUsize::new(
                        *i.history_capacities
                            .get(code)
                            .unwrap_or(&self.history_capacity),
                    )
                    .context("zero history capacity")?,
                });
            }
        }
        Ok(instruments)
    }
    pub fn resolved_streams(&self) -> Result<Vec<StreamConfig>> {
        if !self.streams.is_empty() || self.rest.is_none() {
            for s in &self.streams {
                market(&s.market)?;
                anyhow::ensure!(
                    s.url.starts_with("wss://") || s.url.starts_with("ws://127.0.0.1:"),
                    "upstream requires WSS, except loopback tests"
                );
            }
            return Ok(self.streams.clone());
        }
        let mut streams = Vec::new();
        for m in [Market::Future, Market::Spot] {
            let mut topics = Vec::new();
            for i in &self.instruments {
                if market(&i.market)? != m || !i.trading {
                    continue;
                }
                for interval in &i.intervals {
                    topics.push(
                        if let (Some(pair), Some(contract)) = (&i.pair, &i.contract_type) {
                            format!(
                                "{}_{}@continuousKline_{}",
                                pair.to_lowercase(),
                                contract.to_lowercase(),
                                interval
                            )
                        } else {
                            format!("{}@kline_{}", i.symbol.to_lowercase(), interval)
                        },
                    );
                }
            }
            topics.sort();
            topics.dedup();
            for batch in topics.chunks(200) {
                let root = if m == Market::Future {
                    "wss://fstream.binance.com/market/stream"
                } else {
                    "wss://stream.binance.com:9443/stream"
                };
                let mut url = reqwest::Url::parse(root)?;
                url.query_pairs_mut()
                    .append_pair("streams", &batch.join("/"));
                streams.push(StreamConfig {
                    market: market_name(m).into(),
                    url: url.into(),
                });
            }
        }
        Ok(streams)
    }
}
fn validate_guard(before: u64, after: u64, minimum: u64) -> Result<()> {
    anyhow::ensure!(
        before.saturating_add(after) < minimum,
        "boundary guard leaves no safe window; shorten guard for small intervals"
    );
    Ok(())
}
fn validate_symbol(value: &str) -> Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
            && !value.contains(".."),
        "invalid symbol"
    );
    Ok(())
}
fn validate_stream(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    anyhow::ensure!(
        (url.scheme() == "wss" || (url.scheme() == "ws" && local))
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "invalid WebSocket URL; use WSS or loopback WS"
    );
    // Binance serves at most 1024 streams on one connection; a longer list would fail on every
    // reconnect instead of at startup.
    let combined: usize = url
        .query_pairs()
        .filter(|(key, _)| key == "streams")
        .map(|(_, value)| value.split('/').filter(|s| !s.is_empty()).count())
        .sum();
    // Raw streams can also be listed in the path: /ws/<stream>/<stream>/...
    let raw = url.path_segments().map_or(0, |segments| {
        segments
            .skip_while(|s| *s != "ws")
            .skip(1)
            .filter(|s| !s.is_empty())
            .count()
    });
    let streams = combined + raw;
    anyhow::ensure!(
        streams <= 1024,
        "a WebSocket URL may carry at most 1024 streams; this one has {streams}"
    );
    Ok(())
}

pub fn guarded(
    now: i64,
    mut intervals: impl Iterator<Item = Interval>,
    before: u64,
    after: u64,
) -> bool {
    if before == 0 && after == 0 {
        return false;
    }
    intervals.any(|interval| {
        let period = interval.millis() as u64;
        let offset = now.rem_euclid(interval.millis()) as u64;
        offset < after || period - offset <= before
    })
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionConfig {
    pub market: String,
    pub interval: String,
    #[serde(default = "patterns")]
    pub symbol_patterns: Vec<String>,
    #[serde(default = "history_default")]
    pub history_capacity: usize,
    #[serde(default)]
    pub symbol_capacities: std::collections::BTreeMap<String, usize>,
    #[serde(default)]
    pub continuous: bool,
}
fn patterns() -> Vec<String> {
    vec![".*?USDT".into()]
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceInterval {
    pub market: String,
    pub interval: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    pub market: String,
    pub interval: String,
    pub max_store_count: usize,
    #[serde(default)]
    pub symbol_counts: std::collections::BTreeMap<String, usize>,
}
impl PersistenceConfig {
    pub fn includes(&self, slot: &kline_service::Slot) -> bool {
        self.enabled_intervals.as_ref().is_none_or(|rules| {
            rules
                .iter()
                .any(|r| r.market == market_name(slot.market) && r.interval == slot.interval.code())
        })
    }
    pub fn count(&self, slot: &kline_service::Slot) -> usize {
        self.retention
            .iter()
            .find(|r| r.market == market_name(slot.market) && r.interval == slot.interval.code())
            .map(|r| {
                *r.symbol_counts
                    .get(slot.symbol.as_ref())
                    .unwrap_or(&r.max_store_count)
            })
            .or(self.max_store_count)
            .unwrap_or(slot.capacity().saturating_mul(2).min(100_000))
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebsocketConfig {
    pub future_url: String,
    pub spot_url: String,
    pub topic_stale_seconds: u64,
    pub batch_size: usize,
}
impl Default for WebsocketConfig {
    fn default() -> Self {
        Self {
            future_url: "wss://fstream.binance.com/market/stream".into(),
            spot_url: "wss://stream.binance.com:9443/stream".into(),
            topic_stale_seconds: 120,
            batch_size: 200,
        }
    }
}
