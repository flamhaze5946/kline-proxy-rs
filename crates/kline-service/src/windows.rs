//! Response-layer cache of encoded closed bars. Domain storage knows no JSON.
use crate::{Engine, Slot};
use binance_wire::DisplayBar;
use bytes::Bytes;
use kline_core::Bar;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub(crate) struct Row {
    pub bar: Bar,
    pub bytes: Bytes,
    pub final_bar: bool,
}
pub(crate) struct Window {
    pub generation: u64,
    pub valid_until: i64,
    pub observed_at: i64,
    pub limit: usize,
    pub rows: Vec<Row>,
    pub all_final: bool,
}
struct Cell {
    cached: Mutex<Option<Arc<Window>>>,
    requested: AtomicUsize,
    queued: AtomicBool,
}
pub(crate) struct Windows {
    cells: RwLock<Vec<Arc<Cell>>>,
    dirty: Mutex<VecDeque<usize>>,
    signal: tokio::sync::Notify,
}
impl Windows {
    pub fn new(size: usize) -> Self {
        let windows = Self {
            cells: RwLock::new(Vec::new()),
            dirty: Mutex::new(VecDeque::new()),
            signal: tokio::sync::Notify::new(),
        };
        windows.resize(size);
        windows
    }
    pub fn resize(&self, size: usize) {
        let mut cells = self.cells.write();
        if cells.len() >= size {
            return;
        }
        cells.resize_with(size, || {
            Arc::new(Cell {
                cached: Mutex::new(None),
                requested: AtomicUsize::new(10),
                queued: AtomicBool::new(false),
            })
        });
    }
    pub fn invalidate(&self, id: usize) {
        self.ensure(id);
        let cells = self.cells.read();
        if let Some(cell) = cells.get(id)
            && !cell.queued.swap(true, Ordering::AcqRel)
        {
            self.dirty.lock().push_back(id);
            self.signal.notify_one();
        }
    }
    fn ensure(&self, id: usize) {
        if self.cells.read().len() <= id {
            self.resize(id + 1);
        }
    }
    pub async fn changed(&self) {
        self.signal.notified().await;
    }
    pub fn get(
        &self,
        id: usize,
        slot: &Slot,
        limit: usize,
        now: i64,
    ) -> Result<Arc<Window>, serde_json::Error> {
        self.ensure(id);
        let cell = self.cells.read()[id].clone();
        cell.requested.fetch_max(limit, Ordering::Relaxed);
        let mut cached = cell.cached.lock();
        if let Some(window) = &*cached
            && window.generation == slot.window_generation()
            && window.observed_at <= now
            && window.valid_until > now
            && window.limit >= limit
            && window.all_final
        {
            return Ok(window.clone());
        }
        let target = cell.requested.load(Ordering::Relaxed).clamp(10, 100);
        let (generation, records, valid_until) = slot.closed_window(target, now);
        let mut rows = Vec::with_capacity(records.len());
        // Both snapshots are ordered by unique open_time. Walk the old window
        // once, but still compare the complete bar: corrections at the same
        // timestamp must never inherit stale encoded values.
        let mut previous = cached
            .as_ref()
            .into_iter()
            .flat_map(|w| w.rows.iter())
            .peekable();
        for (bar, final_bar) in records {
            while previous
                .peek()
                .is_some_and(|row| row.bar.open_time < bar.open_time)
            {
                previous.next();
            }
            let reused = previous.peek().filter(|row| row.bar == bar);
            let bytes = if let Some(row) = reused {
                row.bytes.clone()
            } else {
                serde_json::to_vec(&DisplayBar(bar.clone()))?.into()
            };
            rows.push(Row {
                bar,
                bytes,
                final_bar,
            });
        }
        let all_final = rows.iter().all(|r| r.final_bar);
        let window = Arc::new(Window {
            generation,
            valid_until,
            observed_at: now,
            limit: target,
            rows,
            all_final,
        });
        *cached = Some(window.clone());
        Ok(window)
    }
    pub fn prepare(&self, engine: &Engine) {
        // Bound each maintenance turn; foreground requests always have a correct
        // synchronous fallback if preparation has not reached their series yet.
        for _ in 0..64 {
            let Some(id) = self.dirty.lock().pop_front() else {
                break;
            };
            let cell = self.cells.read()[id].clone();
            cell.queued.store(false, Ordering::Release);
            let slot = engine.catalog.slot(id);
            if !slot.is_tracked() {
                *cell.cached.lock() = None;
                continue;
            }
            slot.reserve_next();
            if let Err(error) = self.get(id, &slot, 10, engine.now_ms()) {
                tracing::warn!(%error, "closed window preparation failed");
            }
        }
        if !self.dirty.lock().is_empty() {
            self.signal.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Catalog, Clock, Instrument, Settings};
    use kline_core::{Interval, Market, NumberType, Source, Update};
    use std::num::NonZeroUsize;

    const H: i64 = 3_600_000;
    struct FixedClock;
    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            200 * H
        }
    }
    fn engine(mode: NumberType) -> Arc<Engine> {
        Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "BTCUSDT".into(),
                interval: Interval::parse("1h").unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(200).unwrap(),
            }])
            .unwrap(),
            Arc::new(FixedClock),
            Settings {
                number_type: mode,
                ..Settings::default()
            },
        )
    }
    fn commit(engine: &Engine, open: i64, value: &str, closed: bool, sequence: u64) {
        let mut bar = Bar {
            open_time: open,
            close_time: open + H - 1,
            trades: 1,
            ..Bar::default()
        };
        bar.set_numbers(engine.settings.number_type, [value; 8])
            .unwrap();
        engine
            .commit(
                0,
                Update {
                    bar,
                    closed,
                    source: Source::Rest,
                    event_time: None,
                    sequence,
                },
            )
            .unwrap();
    }
    fn assert_fresh_encoding(window: &Window) {
        for row in &window.rows {
            assert_eq!(
                row.bytes.as_ref(),
                serde_json::to_vec(&DisplayBar(row.bar.clone())).unwrap()
            );
        }
    }

    #[test]
    fn rolling_windows_reuse_unchanged_bytes_but_reencode_historical_corrections() {
        for mode in [
            NumberType::Double,
            NumberType::Float,
            NumberType::String,
            NumberType::BigDecimal,
        ] {
            let engine = engine(mode);
            let slot = engine.catalog.slot(0);
            for i in 0..100 {
                commit(&engine, i * H, "1.2500", true, 1);
            }
            let before = engine.windows.get(0, &slot, 100, 100 * H).unwrap();
            commit(&engine, 100 * H, "2.5000", true, 2);
            let rolled = engine.windows.get(0, &slot, 100, 101 * H).unwrap();
            assert_eq!(rolled.rows.len(), 100);
            assert_eq!(rolled.rows[0].bar.open_time, H);
            for (old, new) in before.rows[1..].iter().zip(&rolled.rows) {
                assert_eq!(
                    old.bytes.as_ptr(),
                    new.bytes.as_ptr(),
                    "unchanged rows must reuse storage"
                );
            }
            assert_fresh_encoding(&rolled);
            commit(&engine, 50 * H, "3.7500", true, 3);
            let corrected = engine.windows.get(0, &slot, 100, 101 * H).unwrap();
            assert_ne!(corrected.generation, rolled.generation);
            assert_eq!(corrected.valid_until, rolled.valid_until);
            assert!(corrected.all_final);
            for (old, new) in rolled.rows.iter().zip(&corrected.rows) {
                if new.bar.open_time == 50 * H {
                    assert_ne!(old.bytes, new.bytes);
                } else {
                    assert_eq!(old.bytes.as_ptr(), new.bytes.as_ptr());
                }
            }
            assert_fresh_encoding(&corrected);
        }
    }

    #[test]
    fn reuse_keeps_new_finality_and_handles_windows_moving_backwards() {
        let engine = engine(NumberType::Double);
        let slot = engine.catalog.slot(0);
        for i in 0..12 {
            commit(&engine, i * H, "1.25", i != 11, 1);
        }
        let before = engine.windows.get(0, &slot, 10, 12 * H).unwrap();
        assert!(!before.all_final);
        commit(&engine, 11 * H, "1.25", true, 2);
        let finalized = engine.windows.get(0, &slot, 10, 12 * H).unwrap();
        assert!(finalized.all_final);
        assert_eq!(
            before.rows.last().unwrap().bytes.as_ptr(),
            finalized.rows.last().unwrap().bytes.as_ptr()
        );
        let earlier = engine.windows.get(0, &slot, 10, 10 * H).unwrap();
        assert_eq!(earlier.rows.first().unwrap().bar.open_time, 0);
        assert_eq!(earlier.rows.last().unwrap().bar.open_time, 9 * H);
        assert_fresh_encoding(&earlier);
    }
}
