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
        for (bar, final_bar) in records {
            let reused = cached
                .as_ref()
                .and_then(|w| w.rows.iter().find(|r| r.bar == bar));
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
