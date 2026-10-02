use crate::{
    config::{PersistenceConfig, RestConfig, guarded},
    recovery::Recovery,
    rest::{Priority, RestApi},
    storage::Store,
};
use arc_swap::ArcSwap;
use kline_core::Market;
use kline_service::{
    BulkQuery, Clock, Engine, SystemClock, http::Readiness, upstream::StreamStatus,
};
use parking_lot::RwLock;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinSet, time::Instant};
pub struct SyncedClock {
    offset: AtomicI64,
    pub synchronized_at: AtomicI64,
}
impl SyncedClock {
    pub fn new(offset: i64) -> Self {
        Self {
            offset: AtomicI64::new(offset),
            synchronized_at: AtomicI64::new(0),
        }
    }
    pub fn offset_ms(&self) -> i64 {
        self.offset.load(Ordering::Relaxed)
    }
}
impl Clock for SyncedClock {
    fn now_ms(&self) -> i64 {
        SystemClock {
            offset_ms: self.offset_ms(),
        }
        .now_ms()
    }
    fn host_ms(&self) -> i64 {
        SystemClock { offset_ms: 0 }.now_ms()
    }
}
#[derive(Clone)]
pub struct Feed {
    pub market: Market,
    pub status: Arc<StreamStatus>,
    pub ids: Option<Arc<[usize]>>,
}
impl Feed {
    fn token(&self) -> u64 {
        self.status.id.wrapping_mul(1099511628211) ^ self.status.epoch.load(Ordering::Acquire)
    }
}
/// Resolve membership once when subscriptions change, not once per symbol and
/// health poll. Feed order remains the order used by the reconciliation digest.
struct FeedSet {
    feeds: Vec<Feed>,
    membership: std::collections::BTreeMap<usize, Vec<usize>>,
}
impl FeedSet {
    fn new(feeds: Vec<Feed>) -> Self {
        let mut membership = std::collections::BTreeMap::<usize, Vec<usize>>::new();
        for (index, feed) in feeds.iter().enumerate() {
            if let Some(ids) = &feed.ids {
                for &id in ids.iter() {
                    membership.entry(id).or_default().push(index);
                }
            }
        }
        Self { feeds, membership }
    }
}
impl std::ops::Deref for FeedSet {
    type Target = [Feed];
    fn deref(&self) -> &Self::Target {
        &self.feeds
    }
}
pub struct Health {
    ready: AtomicBool,
    stopped: AtomicBool,
    bootstrapped: AtomicBool,
    series: ArcSwap<Vec<SeriesReadiness>>,
    feeds: ArcSwap<FeedSet>,
    acknowledged_feeds: AtomicU64,
    acknowledged_catalog: AtomicU64,
    clock: Arc<SyncedClock>,
    sync_clock: bool,
    engine: Arc<Engine>,
    acknowledged_gap: AtomicU64,
    detail: RwLock<serde_json::Value>,
}
#[derive(Clone, Default)]
struct SeriesReadiness {
    ready: bool,
    established: bool,
    epoch: u64,
    gap: u64,
    capacity: usize,
}
fn digest(tokens: impl Iterator<Item = u64>) -> u64 {
    tokens.fold(14695981039346656037, |sum, n| {
        sum.wrapping_mul(1099511628211) ^ n
    })
}
impl Health {
    pub fn new(
        engine: Arc<Engine>,
        feeds: Vec<Feed>,
        clock: Arc<SyncedClock>,
        sync_clock: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            bootstrapped: AtomicBool::new(false),
            series: ArcSwap::from_pointee(Vec::new()),
            feeds: ArcSwap::from_pointee(FeedSet::new(feeds)),
            acknowledged_feeds: AtomicU64::new(0),
            acknowledged_catalog: AtomicU64::new(0),
            clock,
            sync_clock,
            engine,
            acknowledged_gap: AtomicU64::new(0),
            detail: RwLock::new(serde_json::json!({"ready":false,"phase":"initializing"})),
        })
    }
    pub fn replace_feeds(&self, feeds: Vec<Feed>) {
        self.ready.store(false, Ordering::Release);
        self.feeds.store(Arc::new(FeedSet::new(feeds)));
    }
    fn clock_ready(&self) -> bool {
        !self.sync_clock || {
            let sampled = self.clock.synchronized_at.load(Ordering::Acquire);
            sampled > 0 && SystemClock { offset_ms: 0 }.now_ms() - sampled <= 600_000
        }
    }
    fn connections_ready(&self) -> bool {
        let feeds = self.feeds.load();
        let now = self.clock.now_ms();
        digest(feeds.iter().map(Feed::token)) == self.acknowledged_feeds.load(Ordering::Acquire)
            && feeds.iter().all(|f| {
                let epoch = f.status.epoch.load(Ordering::Acquire);
                f.status.connected.load(Ordering::Acquire)
                    && epoch > 0
                    && f.status.mapped_epoch.load(Ordering::Acquire) == epoch
                    && now - f.status.last_io_ms.load(Ordering::Relaxed) <= 180_000
            })
    }
    fn series_ready(&self, id: usize, state: &SeriesReadiness) -> bool {
        let slot = self.engine.catalog.slot(id);
        if !state.ready || !slot.is_tracked() || slot.capacity() != state.capacity {
            return false;
        }
        if !slot.is_trading() {
            return true;
        }
        if slot.gap_generation() != state.gap {
            return false;
        }
        let now = self.clock.now_ms();
        let feeds = self.feeds.load();
        let members = feeds.membership.get(&id);
        let mut epoch = 0_u64;
        for (_, feed) in feeds.iter().enumerate().filter(|(index, feed)| {
            feed.market == slot.market
                && (feed.ids.is_none()
                    || members.is_some_and(|ids| ids.binary_search(index).is_ok()))
        }) {
            let stream_epoch = feed.status.epoch.load(Ordering::Acquire);
            if !feed.status.connected.load(Ordering::Acquire)
                || stream_epoch == 0
                || feed.status.mapped_epoch.load(Ordering::Acquire) != stream_epoch
                || now - feed.status.last_io_ms.load(Ordering::Relaxed) > 180_000
            {
                return false;
            }
            epoch = epoch.wrapping_mul(1099511628211) ^ feed.token();
        }
        epoch != 0 && epoch == state.epoch
    }
}
impl Readiness for Health {
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && self.clock_ready()
            && self.connections_ready()
            && self.engine.gap_epoch() == self.acknowledged_gap.load(Ordering::Acquire)
            && self.engine.catalog.generation() == self.acknowledged_catalog.load(Ordering::Acquire)
    }
    fn is_serving(&self) -> bool {
        if self.stopped.load(Ordering::Acquire)
            || !self.bootstrapped.load(Ordering::Acquire)
            || !self.clock_ready()
        {
            return false;
        }
        let series = self.series.load();
        let mut established = 0;
        for (id, state) in series.iter().enumerate() {
            if state.established && self.engine.catalog.slot(id).is_tracked() {
                established += 1;
                if !self.series_ready(id, state) {
                    return false;
                }
            }
        }
        established > 0
    }
    fn query_ready(&self, query: &BulkQuery) -> bool {
        if self.stopped.load(Ordering::Acquire)
            || !self.bootstrapped.load(Ordering::Acquire)
            || !self.clock_ready()
        {
            return false;
        }
        let Some(interval) = kline_core::Interval::parse(&query.interval) else {
            return true; // Preserve the engine's invalid-interval response.
        };
        let series = self.series.load();
        let slots = self.engine.catalog.slots();
        let mut selected = 0;
        let mut check = |id: usize| {
            selected += 1;
            series
                .get(id)
                .is_some_and(|state| self.series_ready(id, state))
        };
        let ready = if query.symbols.is_empty() {
            slots
                .iter()
                .enumerate()
                .filter(|(_, slot)| {
                    slot.market == query.market && slot.interval == interval && slot.is_tracked()
                })
                .all(|(id, _)| check(id))
        } else {
            query
                .symbols
                .iter()
                .filter_map(|symbol| {
                    self.engine.catalog.find(
                        query.market,
                        interval,
                        kline_service::http_compat::java_trim(symbol),
                    )
                })
                .filter(|&id| slots[id].is_tracked())
                .all(&mut check)
        };
        ready && (selected > 0 || self.is_serving())
    }
    fn details(&self) -> serde_json::Value {
        let mut detail = self.detail.read().clone();
        let ready = self.is_ready();
        detail["ready"] = ready.into();
        detail["serving"] = self.is_serving().into();
        detail["bootstrapped"] = self.bootstrapped.load(Ordering::Acquire).into();
        if !ready {
            detail["phase"] = if !self.clock_ready() {
                "waiting_for_clock"
            } else if !self.connections_ready() {
                "waiting_for_stream"
            } else {
                "recovering"
            }
            .into();
        }
        let feeds = self.feeds.load();
        detail["clock_ready"] = self.clock_ready().into();
        detail["clock_offset_ms"] = self.clock.offset_ms().into();
        detail["streams_connected"] = feeds
            .iter()
            .filter(|f| f.status.connected.load(Ordering::Acquire))
            .count()
            .into();
        detail["streams_with_data"] = feeds
            .iter()
            .filter(|f| {
                let epoch = f.status.epoch.load(Ordering::Acquire);
                epoch > 0 && f.status.mapped_epoch.load(Ordering::Acquire) == epoch
            })
            .count()
            .into();
        detail
    }
}
struct SeriesState {
    initialized: bool,
    established: bool,
    epoch: u64,
    gap: u64,
    attempt_epoch: u64,
    capacity: usize,
    running: bool,
    due: Instant,
    queued_since: Option<Instant>,
}
impl SeriesState {
    fn new() -> Self {
        Self {
            initialized: false,
            established: false,
            epoch: 0,
            gap: 0,
            attempt_epoch: 0,
            capacity: 0,
            running: false,
            due: Instant::now(),
            queued_since: None,
        }
    }
}
pub async fn reconcile(
    engine: Arc<Engine>,
    api: Arc<RestApi>,
    config: RestConfig,
    health: Arc<Health>,
    mut stop: watch::Receiver<bool>,
) {
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api,
        config: config.clone(),
    });
    let mut series: Vec<SeriesState> = vec![];
    let mut tasks = JoinSet::new();
    let mut cursor = 0;
    let mut scheduled = VecDeque::new();
    let mut urgent = VecDeque::new();
    let mut urgent_running = 0_usize;
    let mut epochs = Vec::new();
    let mut epoch_key = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    'running: while !*stop.borrow() {
        let mut scan_due = false;
        let completed = tokio::select! {
            _ = stop.changed() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => completed,
            _ = tick.tick() => { scan_due = true; None },
        };
        // Drain already completed work together before examining the catalog once.
        for completed in completed
            .into_iter()
            .chain(std::iter::from_fn(|| tasks.try_join_next()))
        {
            match completed {
                Ok((id, epoch, gap, critical, result)) => {
                    urgent_running -= usize::from(critical);
                    let state: &mut SeriesState = &mut series[id];
                    state.running = false;
                    match result {
                        Ok(count) => {
                            state.initialized = true;
                            state.epoch = epoch;
                            state.gap = gap;
                            state.due =
                                Instant::now() + Duration::from_secs(config.reconcile_seconds);
                            tracing::debug!(id, count, "series reconciled");
                        }
                        Err(error) => {
                            state.due = Instant::now() + Duration::from_secs(config.retry_seconds);
                            tracing::warn!(symbol=%engine.catalog.slot(id).symbol,%error,"series recovery will retry");
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "recovery task panicked");
                    break 'running;
                }
            }
        }
        if scan_due {
            let slots = engine.catalog.slots();
            series.resize_with(slots.len(), SeriesState::new);
            let feeds = health.feeds.load();
            let feed_digest = digest(feeds.iter().map(Feed::token));
            let next_key = (slots.generation, feed_digest, Arc::as_ptr(&feeds) as usize);
            if epoch_key != Some(next_key) {
                epochs.clear();
                epochs.resize(series.len(), 0_u64);
                for feed in feeds.iter() {
                    let token = feed.token();
                    if let Some(ids) = &feed.ids {
                        for &id in ids.iter() {
                            if let Some(epoch) = epochs.get_mut(id) {
                                *epoch = epoch.wrapping_mul(1099511628211) ^ token;
                            }
                        }
                    } else {
                        for (id, slot) in slots.iter().enumerate() {
                            if slot.market == feed.market {
                                epochs[id] = epochs[id].wrapping_mul(1099511628211) ^ token;
                            }
                        }
                    }
                }
            }
            epoch_key = Some(next_key);
            let now = engine.now_ms();
            let targets: [(i64, i64); 16] = std::array::from_fn(|index| {
                crate::recovery::latest_target(kline_core::Interval::all().nth(index).unwrap(), now)
            });
            let monotonic_now = Instant::now();
            let clock_ready = health.clock_ready();
            let gap_epoch = engine.gap_epoch();
            let mut ready = 0;
            let mut recovering = 0;
            let mut tracked = 0;
            let (mut initial_pending, mut epoch_pending, mut gap_pending, mut stale_tail) =
                (0, 0, 0, 0);
            let mut stale_tail_examples = Vec::new();
            let mut provisional_current = 0;
            let mut observed_stale_tail = 0;
            scheduled.clear();
            urgent.clear();
            let mut availability = vec![SeriesReadiness::default(); series.len()];
            for step in 0..series.len() {
                let id = (cursor + step) % series.len();
                let state = &mut series[id];
                let slot = &slots[id];
                if !slot.is_tracked() {
                    continue;
                }
                tracked += 1;
                if !slot.is_trading() {
                    ready += 1;
                    state.established = true;
                    availability[id] = SeriesReadiness {
                        ready: true,
                        established: true,
                        capacity: slot.capacity(),
                        ..SeriesReadiness::default()
                    };
                    continue;
                }
                let gap = slot.gap_generation();
                let epoch = epochs[id];
                let (target, _) = targets[slot.interval.index()];
                let (capacity, fresh, pending_final, provisional) =
                    slot.lifecycle_status(now, config.freshness_grace_ms as i64, target);
                provisional_current += usize::from(provisional);
                observed_stale_tail += usize::from(!fresh);
                let available = fresh || provisional;
                if capacity > state.capacity {
                    state.initialized = false;
                    state.due = monotonic_now;
                }
                state.capacity = capacity;
                // Quiet/missing tails, late finals and gap/recheck hints wait for the
                // scheduled reconciliation. Only a connection epoch change recovers early.
                if state.attempt_epoch != epoch {
                    state.due = monotonic_now;
                }
                let current = state.initialized
                    && state.epoch == epoch
                    && state.gap == gap
                    && available
                    && epoch != 0;
                if current {
                    state.established = true;
                    ready += 1
                } else {
                    health.ready.store(false, Ordering::Release);
                    recovering += 1;
                    initial_pending += usize::from(!state.initialized);
                    epoch_pending += usize::from(state.epoch != epoch || epoch == 0);
                    gap_pending += usize::from(state.gap != gap);
                    stale_tail += usize::from(!available);
                    if !available && stale_tail_examples.len() < 16 {
                        let latest = slot.latest();
                        stale_tail_examples.push(serde_json::json!({
                            "market":crate::config::market_name(slot.market),
                            "symbol":slot.symbol.as_ref(), "interval":slot.interval.code(),
                            "latest_open_ms":latest.as_ref().map(|r|r.0.open_time),
                            "latest_close_ms":latest.as_ref().map(|r|r.0.close_time),
                            "last_stream_ms":slot.last_stream_ms.load(Ordering::Relaxed),
                            "pending_latest_final":pending_final,
                        }));
                    }
                }
                availability[id] = SeriesReadiness {
                    ready: current,
                    established: state.established,
                    epoch: state.epoch,
                    gap: state.gap,
                    capacity,
                };
                if !state.running && monotonic_now >= state.due && clock_ready {
                    state.queued_since.get_or_insert(monotonic_now);
                    if !state.initialized || state.epoch != epoch {
                        urgent.push_back(id);
                    } else {
                        scheduled.push_back(id);
                    }
                }
            }

            *health.detail.write() = serde_json::json!({
                "phase": if ready == tracked { "live" } else { "recovering" },
                "configured_series": tracked,
                "ready_series": ready,
                "recovering_series": recovering,
                "rest_jobs": tasks.len(),
                "critical_rest_jobs": urgent_running,
                "critical_queued": urgent.len(),
                "routine_queued": scheduled.len(),
                "latest_repair_jobs": 0,
                "forming_repair_jobs": 0,
                "tail_rest_policy": "scheduled_reconciliation_only",
                "provisional_current": provisional_current,
                "observed_stale_tail": observed_stale_tail,
                "streams": feeds.len(),
                "checks": "REST coverage, stream epochs, usable observed or provisional tail",
                "observed_at_ms": now,
                "freshness_grace_ms": config.freshness_grace_ms,
                "stale_tail_examples": stale_tail_examples,
                "unready_reasons": {
                    "initial_pending": initial_pending,
                    "epoch_pending": epoch_pending,
                    "gap_pending": gap_pending,
                    "stale_tail": stale_tail,
                },
            });
            health.series.store(Arc::new(availability));
            health
                .acknowledged_catalog
                .store(slots.generation, Ordering::Release);
            health.acknowledged_gap.store(gap_epoch, Ordering::Release);
            health
                .acknowledged_feeds
                .store(feed_digest, Ordering::Release);
            health.ready.store(ready == tracked, Ordering::Release);
            if tracked > 0 && health.is_ready() {
                health.bootstrapped.store(true, Ordering::Release);
            }
        }
        // Completed jobs refill from the last bounded scan, without rescanning
        // every series for each REST reply. The next tick rechecks all state.
        // One bounded recovery lane remains available if every ordinary worker
        // is waiting on REST/guards. All jobs still share the same upstream quota.
        while (tasks.len() < config.workers || (!urgent.is_empty() && urgent_running == 0))
            && health.clock_ready()
        {
            let next = urgent.pop_front().map(|id| (id, true)).or_else(|| {
                if tasks.len() < config.workers {
                    scheduled.pop_front().map(|id| (id, false))
                } else {
                    None
                }
            });
            let Some((id, critical)) = next else {
                break;
            };
            let state = &mut series[id];
            let slot = engine.catalog.slot(id);
            if state.running || !slot.is_tracked() || !slot.is_trading() {
                continue;
            }
            let epoch = epochs[id];
            let gap = slot.gap_generation();
            let r = recovery.clone();
            let initialized = state.initialized;
            let queued = state.queued_since.take().unwrap_or_else(Instant::now);
            let queue_ms = queued.elapsed().as_secs_f64() * 1000.;
            let priority = if critical {
                Priority::Urgent
            } else {
                Priority::Background
            };
            tasks.spawn(async move {
                let start = Instant::now();
                let result = r.sync_with_priority(id, initialized, priority).await;
                if critical || result.is_err() {
                    tracing::info!(id, symbol=%slot.symbol, market=?slot.market, interval=slot.interval.code(), initialized, ?priority, queue_ms, work_ms=start.elapsed().as_secs_f64()*1000., success=result.is_ok(), "series recovery completed");
                }
                (id, epoch, gap, critical, result)
            });
            urgent_running += usize::from(critical);
            state.running = true;
            state.attempt_epoch = epoch;
            cursor = (id + 1) % series.len();
        }
    }
    health.ready.store(false, Ordering::Release);
    health.stopped.store(true, Ordering::Release);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
pub async fn sync_clock(
    api: Arc<RestApi>,
    clock: Arc<SyncedClock>,
    market: Market,
    mut stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        let sample = async {
            let mut best = None;
            for _ in 0..3 {
                let sample = api.clock_sample(market).await?;
                if sample.elapsed_ms <= 1000
                    && best.as_ref().is_none_or(|old: &crate::rest::ClockSample| {
                        sample.elapsed_ms < old.elapsed_ms
                    })
                {
                    best = Some(sample);
                }
            }
            let sample = best.ok_or_else(|| anyhow::anyhow!("clock RTT exceeds 1000ms"))?;
            let offset = sample.server_time - (sample.sent_ms + sample.elapsed_ms as i64 / 2);
            anyhow::ensure!(offset.abs() < 300_000, "clock offset exceeds 5 minutes");
            clock.offset.store(offset, Ordering::Relaxed);
            clock
                .synchronized_at
                .store(SystemClock { offset_ms: 0 }.now_ms(), Ordering::Release);
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {_=stop.changed()=>break,result=sample=>if let Err(error)=result {tracing::warn!(%error,"clock sync failed");}}
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(60))=>{}}
    }
}

pub async fn persist(
    mut store: Store,
    engine: Arc<Engine>,
    config: PersistenceConfig,
    mut finish: watch::Receiver<bool>,
) {
    store.configure(config.clone());
    let mut delay = Duration::from_secs(config.interval_seconds);
    loop {
        if !*finish.borrow() {
            tokio::select! {_=finish.changed()=>{},_=tokio::time::sleep(delay)=>{}}
        }
        let final_dump = *finish.borrow();
        if final_dump && !config.dump_on_shutdown {
            break;
        }
        let e = engine.clone();
        let c = config.clone();
        match tokio::task::spawn_blocking(move || {
            let report = store.dump(&e, || {
                final_dump
                    || !guarded(
                        e.now_ms(),
                        e.catalog.slots().iter().map(|s| s.interval),
                        c.boundary_guard_before_ms,
                        c.boundary_guard_after_ms,
                    )
            });
            let dirty = store.is_dirty(&e);
            (store, report, dirty)
        })
        .await
        {
            Ok((returned, report, dirty)) => {
                store = returned;
                delay = Duration::from_secs(if report.failures > 0 {
                    10
                } else if dirty {
                    1
                } else {
                    config.interval_seconds
                });
                if report.written > 0 || report.failures > 0 {
                    tracing::info!(
                        written = report.written,
                        bytes = report.bytes,
                        failures = report.failures,
                        "snapshot pass complete"
                    );
                }
            }
            Err(error) => {
                tracing::error!(%error,"snapshot worker failed");
                break;
            }
        }
        if final_dump {
            break;
        }
    }
}
