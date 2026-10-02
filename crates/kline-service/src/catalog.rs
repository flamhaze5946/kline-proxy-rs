//! Rare directory changes publish an immutable index; series IDs remain stable.
use arc_swap::{ArcSwap, Guard};
use binance_wire::Identity;
use kline_core::{Interval, Market, Series};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instrument {
    pub market: Market,
    pub symbol: String,
    pub interval: Interval,
    pub trading: bool,
    pub continuous: Option<(String, String)>,
    pub capacity: NonZeroUsize,
}
pub struct Slot {
    pub market: Market,
    pub symbol: Arc<str>,
    pub interval: Interval,
    trading: AtomicBool,
    tracked: AtomicBool,
    pub last_stream_ms: AtomicI64,
    pub(crate) state: RwLock<Series>,
    pub(crate) changed: watch::Sender<u64>,
    pub(crate) populated: AtomicBool,
    pub(crate) durable_generation: AtomicU64,
    pub(crate) window_generation: AtomicU64,
    pub(crate) gap_generation: AtomicU64,
    pub(crate) recheck_generation: AtomicU64,
}
impl Slot {
    pub fn is_trading(&self) -> bool {
        self.trading.load(Ordering::Acquire)
    }
    pub fn is_tracked(&self) -> bool {
        self.tracked.load(Ordering::Acquire)
    }
    pub fn snapshot(&self, limit: usize, closed_only: bool, now: i64) -> Vec<kline_core::Bar> {
        let state = self.state.read();
        let placeholder = (!closed_only && limit > 0 && self.is_trading())
            .then(|| state.provisional_bar(self.interval, now))
            .flatten();
        let mut bars = state.snapshot(
            limit.saturating_sub(usize::from(placeholder.is_some())),
            closed_only,
            now,
        );
        if let Some(bar) = placeholder {
            bars.push(bar);
        }
        bars
    }
    pub fn contains(&self, time: i64) -> bool {
        self.state.read().has_bar(time)
    }
    pub fn get(&self, time: i64) -> Option<(kline_core::Bar, bool)> {
        self.state.read().get(time)
    }
    pub fn len(&self) -> usize {
        self.state.read().len()
    }
    pub fn is_empty(&self) -> bool {
        self.state.read().is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.state.read().capacity()
    }
    /// One consistent read replaces separate freshness and capacity lock acquisitions.
    pub fn lifecycle_status(
        &self,
        now: i64,
        grace: i64,
        latest_open: i64,
    ) -> (usize, bool, bool, bool) {
        let state = self.state.read();
        (
            state.capacity(),
            state.fresh_tail(now, grace),
            state.needs_final(latest_open),
            self.is_trading() && state.provisional_tail_available(self.interval, now, grace),
        )
    }
    pub fn reserve_history(&self, records: usize) {
        self.state.write().reserve_history(records);
    }
    pub fn reserve_next(&self) {
        let state = self.state.read();
        let needs_growth =
            state.len() == state.allocated_capacity() && state.len() < state.capacity();
        drop(state);
        if needs_growth {
            self.state.write().reserve_next();
        }
    }
    pub fn records(&self) -> Vec<(kline_core::Bar, bool)> {
        self.state.read().records()
    }
    /// Select bars against one consistent view, including any query-specific fallback.
    pub fn select_bars<T>(&self, select: impl FnOnce(&Series) -> T) -> T {
        select(&self.state.read())
    }
    pub fn latest(&self) -> Option<(kline_core::Bar, bool)> {
        self.state.read().latest()
    }
    pub fn has_deferred_final(&self, now: i64) -> bool {
        self.state.read().has_deferred_final(now)
    }
    pub fn durable_generation(&self) -> u64 {
        self.durable_generation.load(Ordering::Acquire)
    }
    pub fn window_generation(&self) -> u64 {
        self.window_generation.load(Ordering::Acquire)
    }
    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn gap_generation(&self) -> u64 {
        self.gap_generation.load(Ordering::Acquire)
    }
    pub fn recheck_generation(&self) -> u64 {
        self.recheck_generation.load(Ordering::Acquire)
    }
    pub fn fresh_tail(&self, now: i64, grace: i64) -> bool {
        self.state.read().fresh_tail(now, grace)
    }
    pub(crate) fn closed_window(
        &self,
        limit: usize,
        now: i64,
    ) -> (u64, Vec<(kline_core::Bar, bool)>, i64) {
        let state = self.state.read();
        let (rows, until) = state.snapshot_records(limit, now);
        (self.window_generation(), rows, until)
    }
    pub fn needs_final(&self, time: i64) -> bool {
        self.state.read().needs_final(time)
    }
    pub fn durable_snapshot(&self, now: i64) -> (u64, Vec<kline_core::Bar>) {
        let state = self.state.read();
        (self.durable_generation(), state.final_snapshot(now))
    }
    pub(crate) fn pending(&self, time: i64) -> bool {
        // Java only waits for an existing, nonfinal just-closed bar.
        self.state.read().has_non_final(time)
    }
    pub(crate) fn missing_latest(&self, time: i64) -> bool {
        let state = self.state.read();
        // Bulk preserves Java's fixed-duration week/month query boundaries.
        // Do not invent a missing bar on a different upstream opening grid.
        // The data-status extension uses this predicate; Java boundary finality does not.
        if self.interval.code() == "1M"
            || state
                .latest_bar()
                .is_some_and(|bar| (time - bar.open_time).rem_euclid(self.interval.millis()) != 0)
        {
            false
        } else {
            !state.has_bar(time) && state.needs_final(time)
        }
    }
}
type SymbolIndex = HashMap<(Market, Interval), HashMap<String, usize>>;
type ContinuousIndex = HashMap<Interval, HashMap<String, HashMap<String, usize>>>;
pub struct CatalogView {
    slots: Vec<Arc<Slot>>,
    symbols: SymbolIndex,
    continuous: ContinuousIndex,
    definitions: Vec<Instrument>,
    pub generation: u64,
}
impl Deref for CatalogView {
    type Target = [Arc<Slot>];
    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}
pub struct Catalog {
    view: ArcSwap<CatalogView>,
    writer: Mutex<()>,
}
#[derive(Debug, Default)]
pub struct CatalogChange {
    pub added: usize,
    pub removed: usize,
    pub changed: bool,
}
impl Catalog {
    pub fn new(instruments: Vec<Instrument>) -> Result<Self, String> {
        let catalog = Self {
            view: ArcSwap::from_pointee(CatalogView {
                slots: vec![],
                symbols: HashMap::new(),
                continuous: HashMap::new(),
                definitions: vec![],
                generation: 0,
            }),
            writer: Mutex::new(()),
        };
        catalog.sync(instruments)?;
        Ok(catalog)
    }
    pub fn sync(&self, mut instruments: Vec<Instrument>) -> Result<CatalogChange, String> {
        let _writer = self.writer.lock();
        instruments.sort_by(|a, b| {
            (a.market, a.interval, &a.symbol).cmp(&(b.market, b.interval, &b.symbol))
        });
        let old = self.view.load();
        if old.definitions == instruments {
            return Ok(CatalogChange::default());
        }
        let mut keys = HashSet::new();
        let mut routes = HashSet::new();
        for i in &instruments {
            if i.symbol.is_empty()
                || i.symbol.trim() != i.symbol
                || !keys.insert((i.market, i.interval, i.symbol.as_str()))
            {
                return Err("invalid or duplicate instrument".into());
            }
            if let Some((pair, contract)) = &i.continuous {
                if i.market != Market::Future
                    || pair.is_empty()
                    || !matches!(contract.as_str(), "PERPETUAL" | "TRADIFI_PERPETUAL")
                {
                    return Err("invalid continuous metadata".into());
                }
                if i.trading && !routes.insert((i.interval, pair, contract)) {
                    return Err("ambiguous continuous pair/contract mapping".into());
                }
            }
        }
        let mut slots = old.slots.clone();
        let ids: HashMap<_, _> = slots
            .iter()
            .enumerate()
            .map(|(id, s)| ((s.market, s.interval, s.symbol.to_string()), id))
            .collect();
        let mut symbols: SymbolIndex = HashMap::new();
        let mut continuous: ContinuousIndex = HashMap::new();
        let mut wanted = HashSet::new();
        let mut added = 0;
        for i in &instruments {
            let key = (i.market, i.interval, i.symbol.clone());
            let id = if let Some(&id) = ids.get(&key) {
                id
            } else {
                if slots.len() >= 100_000 {
                    return Err("directory exceeds 100000 lifetime series".into());
                }
                let id = slots.len();
                added += 1;
                slots.push(Arc::new(Slot {
                    market: i.market,
                    symbol: i.symbol.clone().into(),
                    interval: i.interval,
                    trading: AtomicBool::new(i.trading),
                    tracked: AtomicBool::new(true),
                    last_stream_ms: AtomicI64::new(0),
                    state: RwLock::new(Series::new(i.capacity)),
                    changed: watch::channel(0).0,
                    populated: AtomicBool::new(false),
                    durable_generation: AtomicU64::new(0),
                    window_generation: AtomicU64::new(0),
                    gap_generation: AtomicU64::new(0),
                    recheck_generation: AtomicU64::new(0),
                }));
                id
            };
            wanted.insert(id);
            symbols
                .entry((i.market, i.interval))
                .or_default()
                .insert(i.symbol.clone(), id);
            if let Some((pair, contract)) = &i.continuous
                && i.trading
            {
                continuous
                    .entry(i.interval)
                    .or_default()
                    .entry(pair.clone())
                    .or_default()
                    .insert(contract.clone(), id);
            }
        }
        // Validate the entire proposed directory before mutating any live series.
        for i in &instruments {
            let id = symbols[&(i.market, i.interval)][&i.symbol];
            let s = &slots[id];
            let mut state = s.state.write();
            if state.capacity() != i.capacity.get() {
                state.resize(i.capacity);
                s.durable_generation.fetch_add(1, Ordering::Release);
                s.window_generation.fetch_add(1, Ordering::Release);
            }
            s.tracked.store(true, Ordering::Release);
            s.trading.store(i.trading, Ordering::Release);
            s.changed.send_modify(|n| *n = n.wrapping_add(1));
        }
        let mut removed = 0;
        for (id, s) in slots.iter().enumerate() {
            if !wanted.contains(&id) && s.tracked.swap(false, Ordering::AcqRel) {
                removed += 1;
                s.trading.store(false, Ordering::Release);
                let mut state = s.state.write();
                state.clear();
                s.durable_generation.fetch_add(1, Ordering::Release);
                s.window_generation.fetch_add(1, Ordering::Release);
                s.populated.store(false, Ordering::Release);
                s.changed.send_modify(|n| *n = n.wrapping_add(1));
            }
        }
        self.view.store(Arc::new(CatalogView {
            slots,
            symbols,
            continuous,
            definitions: instruments,
            generation: old.generation + 1,
        }));
        Ok(CatalogChange {
            added,
            removed,
            changed: true,
        })
    }
    pub fn slots(&self) -> Guard<Arc<CatalogView>> {
        self.view.load()
    }
    pub fn slot(&self, id: usize) -> Arc<Slot> {
        self.view.load().slots[id].clone()
    }
    pub fn generation(&self) -> u64 {
        self.view.load().generation
    }
    pub fn instruments(&self) -> Vec<Instrument> {
        self.view.load().definitions.clone()
    }
    pub fn find(&self, market: Market, interval: Interval, symbol: &str) -> Option<usize> {
        self.view
            .load()
            .symbols
            .get(&(market, interval))?
            .get(symbol)
            .copied()
    }
    pub fn resolve(
        &self,
        market: Market,
        interval: Interval,
        identity: &Identity<'_>,
    ) -> Option<usize> {
        let view = self.view.load();
        match identity {
            Identity::Symbol(symbol) => view
                .symbols
                .get(&(market, interval))?
                .get(symbol.as_ref())
                .copied(),
            Identity::Continuous {
                pair,
                contract_type,
            } if market == Market::Future => view
                .continuous
                .get(&interval)?
                .get(pair.as_ref())?
                .get(contract_type.as_ref())
                .copied(),
            _ => None,
        }
    }
    pub(crate) fn normalize(
        &self,
        market: Market,
        interval: Interval,
        symbols: &[String],
    ) -> Arc<[usize]> {
        let view = self.view.load();
        let index = view.symbols.get(&(market, interval));
        let mut ids: Vec<_> = if symbols.is_empty() {
            index
                .into_iter()
                .flat_map(|m| m.values().copied())
                .collect()
        } else {
            symbols
                .iter()
                .filter_map(|s| index?.get(crate::http_compat::java_trim(s)).copied())
                .collect()
        };
        ids.retain(|&id| {
            view.slots[id].populated.load(Ordering::Acquire) && view.slots[id].is_tracked()
        });
        ids.sort_unstable_by(|&a, &b| view.slots[a].symbol.cmp(&view.slots[b].symbol));
        ids.dedup();
        ids.into()
    }
}
