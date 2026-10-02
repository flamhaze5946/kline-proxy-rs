//! Bounded final-bar diagnostics; forming updates never enter the boundary locks.
use crate::Engine;
use kline_core::{Interval, Market};
use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::Duration,
};
#[derive(Clone, Serialize)]
pub struct Summary {
    pub detailed_latency_enabled: bool,
    pub market: &'static str,
    pub interval: &'static str,
    pub boundary: i64,
    pub expected: usize,
    pub arrived: usize,
    pub pending: Vec<String>,
    pub first_ms: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub last_symbol: String,
    pub last_receive_ms: f64,
    pub event_to_receive_p99_ms: f64,
    pub receive_p99_ms: f64,
    pub processing_p99_ms: f64,
    pub decode_p99_ms: f64,
    pub cache_p99_ms: f64,
    pub timed_out: bool,
}
struct Sample {
    symbol: String,
    received: f64,
    ready: f64,
    decode: f64,
    cache: f64,
    event_to_receive: f64,
}
struct Boundary {
    detailed_latency_enabled: bool,
    time: i64,
    expected: BTreeSet<String>,
    samples: BTreeMap<String, Sample>,
    reported: bool,
    arrived_expected: usize,
}
#[derive(Default)]
struct State {
    current: Option<Boundary>,
    history: VecDeque<Summary>,
}
struct Queued {
    id: usize,
    open: i64,
    completed_ms: i64,
    observation: Observation,
}
pub struct Diagnostics {
    states: [Mutex<State>; 32],
    queue: Mutex<VecDeque<Queued>>,
    signal: tokio::sync::Notify,
    pub dropped: AtomicU64,
    next_sweep_ms: AtomicI64,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            states: std::array::from_fn(|_| Mutex::new(State::default())),
            queue: Mutex::new(VecDeque::new()),
            signal: tokio::sync::Notify::new(),
            dropped: AtomicU64::new(0),
            next_sweep_ms: AtomicI64::new(0),
        }
    }
}
fn key(m: Market, i: Interval) -> usize {
    m as usize * 16 + i.index()
}
fn market(m: Market) -> &'static str {
    if m == Market::Future {
        "future"
    } else {
        "spot"
    }
}
fn percentile(mut values: Vec<f64>, q: f64) -> f64 {
    if values.is_empty() {
        return 0.;
    }
    values.sort_by(f64::total_cmp);
    values[((values.len() as f64 * q) as usize).min(values.len() - 1)]
}
pub struct Observation {
    pub received_ms: i64,
    pub event_ms: Option<i64>,
    pub decode: Duration,
    pub cache: Duration,
}
impl Diagnostics {
    pub fn record(&self, engine: &Engine, id: usize, open: i64, observation: Observation) {
        let started = std::time::Instant::now();
        let now = engine.now_ms();
        let boundary = open + engine.catalog.slot(id).interval.millis();
        if now - boundary > 30_000 || now < boundary - 1000 {
            return;
        }
        let mut queue = self.queue.lock();
        if queue.len() < 100_000 {
            queue.push_back(Queued {
                id,
                open,
                completed_ms: now,
                observation,
            });
        } else {
            // Only observability samples may be dropped; never price/final updates.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        drop(queue);
        self.signal.notify_one();
        engine.metrics.work.record(0, started.elapsed());
    }
    pub async fn changed(&self) {
        self.signal.notified().await;
    }
    fn drain(&self, engine: &Engine) {
        let queued = std::mem::take(&mut *self.queue.lock());
        for sample in queued {
            self.record_sample(
                engine,
                sample.id,
                sample.open,
                sample.completed_ms,
                sample.observation,
            );
        }
    }
    fn record_sample(
        &self,
        engine: &Engine,
        id: usize,
        open: i64,
        now: i64,
        observation: Observation,
    ) {
        let Observation {
            received_ms,
            event_ms,
            decode,
            cache,
        } = observation;
        let slot = engine.catalog.slot(id);
        let boundary = open + slot.interval.millis();
        let mut state = self.states[key(slot.market, slot.interval)].lock();
        if state.current.as_ref().is_none_or(|b| b.time < boundary) {
            if let Some(old) = state.current.take() {
                finish(&mut state, old, slot.market, slot.interval, true);
            }
            let expected = engine
                .catalog
                .slots()
                .iter()
                .filter(|s| {
                    s.market == slot.market
                        && s.interval == slot.interval
                        && s.is_tracked()
                        && s.is_trading()
                        && s.contains(open)
                })
                .map(|s| s.symbol.to_string())
                .collect();
            state.current = Some(Boundary {
                detailed_latency_enabled: engine.detailed_latency_enabled(),
                time: boundary,
                expected,
                samples: BTreeMap::new(),
                reported: false,
                arrived_expected: 0,
            });
        }
        let Some(current) = &mut state.current else {
            return;
        };
        if current.time != boundary || current.reported {
            return;
        }
        let symbol = slot.symbol.to_string();
        let received = (received_ms - boundary) as f64;
        let processing = (decode + cache).as_secs_f64() * 1000.;
        if current.samples.contains_key(&symbol) {
            return;
        }
        if current.expected.contains(&symbol) {
            current.arrived_expected += 1;
        }
        current.samples.insert(
            symbol.clone(),
            Sample {
                symbol,
                received,
                ready: if engine.detailed_latency_enabled() {
                    received + processing
                } else {
                    (now - boundary) as f64
                },
                decode: decode.as_secs_f64() * 1000.,
                cache: cache.as_secs_f64() * 1000.,
                event_to_receive: event_ms
                    .map(|t| received_ms.saturating_sub(t) as f64)
                    .unwrap_or(0.),
            },
        );
        if current.arrived_expected == current.expected.len() {
            let boundary = state.current.take().unwrap();
            finish(&mut state, boundary, slot.market, slot.interval, false);
        }
    }
    pub fn maintain(&self, engine: &Engine) {
        let started = std::time::Instant::now();
        self.drain(engine);
        let now = engine.now_ms();
        if self
            .next_sweep_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (now >= next || next.saturating_sub(now) > 1000).then_some(now.saturating_add(1000))
            })
            .is_err()
        {
            engine.metrics.work.record(1, started.elapsed());
            return;
        }
        for m in [Market::Future, Market::Spot] {
            for i in Interval::all() {
                let mut state = self.states[key(m, i)].lock();
                if let Some(boundary) = state.current.as_mut().filter(|b| !b.reported) {
                    boundary.expected.retain(|symbol| {
                        engine
                            .catalog
                            .find(m, i, symbol)
                            .is_some_and(|id| engine.catalog.slot(id).is_trading())
                    });
                    boundary
                        .samples
                        .retain(|symbol, _| boundary.expected.contains(symbol));
                    boundary.arrived_expected = boundary.samples.len();
                    let complete = boundary
                        .expected
                        .iter()
                        .all(|s| boundary.samples.contains_key(s));
                    if complete || now - boundary.time >= 8000 {
                        let boundary = state.current.take().unwrap();
                        finish(&mut state, boundary, m, i, !complete);
                    }
                }
            }
        }
        engine.metrics.work.record(1, started.elapsed());
    }
    pub fn summaries(&self) -> Vec<Summary> {
        let mut out: Vec<_> = self
            .states
            .iter()
            .flat_map(|state| state.lock().history.iter().cloned().collect::<Vec<_>>())
            .collect();
        out.sort_by_key(|s| s.boundary);
        out
    }
}
fn finish(
    state: &mut State,
    mut boundary: Boundary,
    market_id: Market,
    interval: Interval,
    timeout: bool,
) {
    if boundary.reported || boundary.samples.is_empty() {
        state.current = Some(boundary);
        return;
    }
    let pending = boundary
        .expected
        .iter()
        .filter(|s| !boundary.samples.contains_key(*s))
        .cloned()
        .collect();
    let samples: Vec<_> = boundary.samples.values().collect();
    let last = samples
        .iter()
        .max_by(|a, b| a.ready.total_cmp(&b.ready))
        .unwrap();
    let summary = Summary {
        detailed_latency_enabled: boundary.detailed_latency_enabled,
        market: market(market_id),
        interval: interval.code(),
        boundary: boundary.time,
        expected: boundary.expected.len(),
        arrived: samples.len(),
        pending,
        first_ms: percentile(samples.iter().map(|s| s.ready).collect(), 0.),
        p50_ms: percentile(samples.iter().map(|s| s.ready).collect(), 0.5),
        p99_ms: percentile(samples.iter().map(|s| s.ready).collect(), 0.99),
        max_ms: last.ready,
        last_symbol: last.symbol.clone(),
        last_receive_ms: percentile(samples.iter().map(|s| s.received).collect(), 1.),
        event_to_receive_p99_ms: percentile(
            samples.iter().map(|s| s.event_to_receive).collect(),
            0.99,
        ),
        receive_p99_ms: percentile(samples.iter().map(|s| s.received).collect(), 0.99),
        processing_p99_ms: percentile(samples.iter().map(|s| s.decode + s.cache).collect(), 0.99),
        decode_p99_ms: percentile(samples.iter().map(|s| s.decode).collect(), 0.99),
        cache_p99_ms: percentile(samples.iter().map(|s| s.cache).collect(), 0.99),
        timed_out: timeout,
    };
    tracing::info!(market=summary.market,interval=summary.interval,boundary=summary.boundary,expected=summary.expected,arrived=summary.arrived,max_ms=summary.max_ms,p99_ms=summary.p99_ms,last_symbol=%summary.last_symbol,pending=?summary.pending,"closed_bar_summary");
    state.history.push_back(summary);
    while state.history.len() > 24 {
        state.history.pop_front();
    }
    boundary.reported = true;
    state.current = Some(boundary);
}
const BOUNDS: &[u64] = &[
    50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000,
    2500000, 5000000, 10000000, 30000000,
];
const WORK_BOUNDS_US: [u64; 17] = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000,
];
struct WorkHistogram {
    bins: [AtomicU64; 18],
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}
pub struct WorkMetrics {
    stages: [WorkHistogram; 5],
}
impl Default for WorkMetrics {
    fn default() -> Self {
        Self {
            stages: std::array::from_fn(|_| WorkHistogram {
                bins: std::array::from_fn(|_| AtomicU64::new(0)),
                sum_ns: AtomicU64::new(0),
                max_ns: AtomicU64::new(0),
            }),
        }
    }
}
impl WorkMetrics {
    pub fn record(&self, stage: usize, elapsed: Duration) {
        let h = &self.stages[stage];
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        let bucket = WORK_BOUNDS_US.partition_point(|us| us.saturating_mul(1000) < ns);
        h.bins[bucket].fetch_add(1, Ordering::Relaxed);
        h.sum_ns.fetch_add(ns, Ordering::Relaxed);
        h.max_ns.fetch_max(ns, Ordering::Relaxed);
    }
    pub fn summary(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        for (name, h) in [
            "diagnostic_enqueue",
            "diagnostic_background",
            "bulk_final_wait",
            "bulk_build_queue",
            "bulk_encode",
        ]
        .into_iter()
        .zip(&self.stages)
        {
            let bins: Vec<_> = h.bins.iter().map(|b| b.load(Ordering::Relaxed)).collect();
            let count: u64 = bins.iter().sum();
            let rank = (count * 99).div_ceil(100);
            let mut sum = 0;
            let bucket = bins.iter().position(|n| {
                sum += n;
                sum >= rank
            });
            let p99 = if count == 0 {
                Some(0.)
            } else {
                bucket
                    .and_then(|i| WORK_BOUNDS_US.get(i))
                    .map(|us| *us as f64 / 1000.)
            };
            out.insert(name.into(), serde_json::json!({"count":count,"total_ms":h.sum_ns.load(Ordering::Relaxed) as f64/1e6,"max_ms":h.max_ns.load(Ordering::Relaxed) as f64/1e6,"p99_upper_ms":p99}));
        }
        out.into()
    }
}
struct Histogram {
    bins: Vec<AtomicU64>,
    sum_us: AtomicU64,
    count: AtomicU64,
    errors: AtomicU64,
}
pub struct HttpMetrics {
    groups: [Histogram; 5],
    bulk_hours: [Mutex<VecDeque<BulkHour>>; 2],
}
/// Response extension: the boundary of the period a bulk request was answered for. A request
/// that arrived before that boundary (it waited for it) is counted with that hour.
#[derive(Clone, Copy, Debug)]
pub struct AnsweredFor(pub i64);
struct BulkHour {
    boundary: i64,
    count: u64,
    errors: u64,
    elapsed: Vec<f64>,
    boundary_elapsed: Vec<f64>,
    boundary_finished: Vec<f64>,
    dropped: u64,
}
#[derive(Serialize)]
pub struct BulkHourSummary {
    pub market: &'static str,
    pub boundary: i64,
    pub requests: u64,
    pub errors: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub boundary_requests: usize,
    pub boundary_p99_ms: f64,
    pub boundary_response_p99_ms: f64,
    pub last_boundary_response_ms: f64,
    pub dropped_samples: u64,
}
impl BulkHour {
    fn summary(&self, market_id: Market) -> BulkHourSummary {
        BulkHourSummary {
            market: market(market_id),
            boundary: self.boundary,
            requests: self.count,
            errors: self.errors,
            p50_ms: percentile(self.elapsed.clone(), 0.5),
            p99_ms: percentile(self.elapsed.clone(), 0.99),
            max_ms: percentile(self.elapsed.clone(), 1.),
            boundary_requests: self.boundary_elapsed.len(),
            boundary_p99_ms: percentile(self.boundary_elapsed.clone(), 0.99),
            boundary_response_p99_ms: percentile(self.boundary_finished.clone(), 0.99),
            last_boundary_response_ms: percentile(self.boundary_finished.clone(), 1.),
            dropped_samples: self.dropped,
        }
    }
}
impl Default for HttpMetrics {
    fn default() -> Self {
        Self {
            groups: std::array::from_fn(|_| Histogram {
                bins: (0..=BOUNDS.len()).map(|_| AtomicU64::new(0)).collect(),
                sum_us: AtomicU64::new(0),
                count: AtomicU64::new(0),
                errors: AtomicU64::new(0),
            }),
            bulk_hours: std::array::from_fn(|_| Mutex::new(VecDeque::new())),
        }
    }
}
impl HttpMetrics {
    pub fn record(&self, path: &str, elapsed: Duration, status: u16) {
        use crate::Clock;
        self.record_at(
            path,
            elapsed,
            status,
            crate::SystemClock { offset_ms: 0 }.now_ms(),
            None,
        );
    }
    /// HTTP application completion, excluding reverse-proxy/TLS/network transmission.
    /// `answered_for`: see [`AnsweredFor`].
    pub fn record_at(
        &self,
        path: &str,
        elapsed: Duration,
        status: u16,
        finished_ms: i64,
        answered_for: Option<i64>,
    ) {
        let group = if path.ends_with("klines/bulk") {
            0
        } else if path.contains("fundingRate") {
            1
        } else if path.contains("ticker") {
            2
        } else if path.starts_with("/statistic/") {
            3
        } else {
            4
        };
        let h = &self.groups[group];
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = BOUNDS.partition_point(|&n| n < us);
        h.bins[bucket].fetch_add(1, Ordering::Relaxed);
        h.count.fetch_add(1, Ordering::Relaxed);
        h.sum_us.fetch_add(us, Ordering::Relaxed);
        if status >= 400 {
            h.errors.fetch_add(1, Ordering::Relaxed);
        }
        if group == 0 {
            let market = if path.starts_with("/fapi/") {
                Market::Future
            } else {
                Market::Spot
            };
            let arrived =
                finished_ms.saturating_sub(elapsed.as_millis().min(i64::MAX as u128) as i64);
            // Counted from when the answered-for period began if the request arrived before it.
            let started = answered_for.map_or(arrived, |b| arrived.max(b));
            let boundary = started.div_euclid(3_600_000) * 3_600_000;
            let mut hours = self.bulk_hours[market as usize].lock();
            if !hours.iter().any(|h| h.boundary == boundary) {
                hours.push_back(BulkHour {
                    boundary,
                    count: 0,
                    errors: 0,
                    elapsed: vec![],
                    boundary_elapsed: vec![],
                    boundary_finished: vec![],
                    dropped: 0,
                });
                while hours.len() > 24 {
                    hours.pop_front();
                }
            }
            let hour = hours.iter_mut().find(|h| h.boundary == boundary).unwrap();
            hour.count += 1;
            if status >= 400 {
                hour.errors += 1;
            }
            if hour.elapsed.len() < 65_536 {
                let ms = elapsed.as_secs_f64() * 1000.;
                hour.elapsed.push(ms);
                if started - boundary < 10_000 && status < 400 {
                    hour.boundary_elapsed.push(ms);
                    hour.boundary_finished.push((finished_ms - boundary) as f64);
                }
            } else {
                hour.dropped += 1;
            }
        }
    }
    pub fn hourly(&self) -> Vec<BulkHourSummary> {
        let mut rows = vec![];
        for m in [Market::Future, Market::Spot] {
            rows.extend(
                self.bulk_hours[m as usize]
                    .lock()
                    .iter()
                    .map(|h| h.summary(m)),
            );
        }
        rows.sort_by_key(|r| r.boundary);
        rows
    }
    pub fn render(&self, text: &mut String) {
        use std::fmt::Write;
        for (name, h) in ["klines_bulk", "funding", "ticker", "statistic", "other"]
            .iter()
            .zip(&self.groups)
        {
            let mut count = 0;
            for (i, bin) in h.bins.iter().enumerate() {
                count += bin.load(Ordering::Relaxed);
                let bound = BOUNDS
                    .get(i)
                    .map(|n| (*n as f64 / 1_000_000.).to_string())
                    .unwrap_or_else(|| "+Inf".into());
                let _ = writeln!(
                    text,
                    "http_server_requests_seconds_bucket{{route=\"{name}\",le=\"{bound}\"}} {count}"
                );
            }
            let _ = writeln!(
                text,
                "http_server_requests_seconds_count{{route=\"{name}\"}} {}\nhttp_server_requests_seconds_sum{{route=\"{name}\"}} {}\nhttp_server_requests_errors_total{{route=\"{name}\"}} {}",
                h.count.load(Ordering::Relaxed),
                h.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.,
                h.errors.load(Ordering::Relaxed)
            );
        }
    }
}
