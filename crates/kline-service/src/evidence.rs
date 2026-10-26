//! Process evidence state. Row evidence itself lives under each Slot's series lock.
use arc_swap::ArcSwap;
use kline_core::evidence::{EvidenceRecord, EvidenceState};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

#[derive(Clone, Debug)]
pub struct EvidenceSettings {
    pub ws_trust_window_ms: u64,
    pub fallback_weight_per_minute: u32,
}
impl Default for EvidenceSettings {
    fn default() -> Self {
        Self {
            ws_trust_window_ms: 1440 * 60_000,
            fallback_weight_per_minute: 120,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BreakerState {
    pub last_conflict_ms: i64,
    pub revised_total: u64,
}
impl BreakerState {
    pub fn ws_final_trusted(self, now: i64, window_ms: u64) -> bool {
        now.saturating_sub(self.last_conflict_ms) >= i64::try_from(window_ms).unwrap_or(i64::MAX)
    }
}
#[derive(Clone, Debug)]
pub struct EvidenceConflict {
    pub id: usize,
    pub observed_ms: i64,
    pub previous: EvidenceRecord,
    pub incoming: EvidenceRecord,
    pub trips_breaker: bool,
}
#[derive(Default)]
pub(crate) struct EvidenceRuntime {
    pub(crate) catalog_writer: Mutex<()>,
    pub(crate) directory_epoch: AtomicU64,
    pub(crate) directory: ArcSwap<EvidenceDirectory>,
    pub(crate) retained: RwLock<BTreeMap<String, crate::evidence_bulk::RetainedRecord>>,
    pub(crate) retained_generation: AtomicU64,
    pub(crate) catalog_loaded: AtomicBool,
    pub(crate) state: Mutex<EvidenceRuntimeState>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EvidenceIdentity {
    pub id: usize,
    pub definition: crate::Instrument,
    pub tracking_generation: u64,
    pub retained: Option<crate::evidence_bulk::RetainedRecord>,
    pub since: Option<i64>,
}
#[derive(Default)]
pub(crate) struct EvidenceDirectory {
    pub generation: u64,
    pub entries: BTreeMap<(kline_core::Market, kline_core::Interval, String), EvidenceIdentity>,
}
pub(crate) struct DirectoryWrite<'a>(&'a AtomicU64);
impl Drop for DirectoryWrite<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Release);
    }
}
impl EvidenceRuntime {
    pub fn begin_directory_write(&self) -> DirectoryWrite<'_> {
        self.directory_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        DirectoryWrite(&self.directory_epoch)
    }
    pub fn publish_directory(&self, catalog: &crate::Catalog) {
        let retained = self.retained.read();
        let mut view = EvidenceDirectory {
            generation: catalog.generation(),
            ..EvidenceDirectory::default()
        };
        for definition in catalog.instruments() {
            let id = catalog
                .find(definition.market, definition.interval, &definition.symbol)
                .unwrap();
            let slot = catalog.slot(id);
            let identity = EvidenceIdentity {
                id,
                tracking_generation: slot.tracking_generation(),
                retained: retained.get(&definition.symbol).cloned(),
                since: slot.not_trading_since(),
                definition,
            };
            view.entries.insert(
                (
                    identity.definition.market,
                    identity.definition.interval,
                    identity.definition.symbol.clone(),
                ),
                identity,
            );
        }
        self.directory.store(Arc::new(view));
    }
}

#[derive(Default)]
pub(crate) struct EvidenceRuntimeState {
    pub breaker: BreakerState,
    pub generation: u64,
    pub conflicts: VecDeque<EvidenceConflict>,
}
impl EvidenceRuntime {
    pub(crate) fn record(&self, conflict: EvidenceConflict) {
        let mut state = self.state.lock();
        state.breaker.revised_total = state.breaker.revised_total.saturating_add(1);
        if conflict.trips_breaker {
            state.breaker.last_conflict_ms = conflict.observed_ms;
        }
        state.generation = state.generation.wrapping_add(1);
        if state.conflicts.len() == 256 {
            state.conflicts.pop_front();
        }
        state.conflicts.push_back(conflict);
    }
}
impl crate::Engine {
    pub fn evidence_enabled(&self) -> bool {
        self.settings.evidence.is_some()
    }
    pub fn breaker_state(&self) -> BreakerState {
        self.evidence.state.lock().breaker
    }
    /// For loading breaker.json before serving. Persistence uses the generation
    /// to avoid clearing dirty after a concurrent conflict.
    pub fn restore_breaker(&self, breaker: BreakerState) {
        let mut state = self.evidence.state.lock();
        state.breaker = breaker;
        state.generation = state.generation.wrapping_add(1);
    }
    pub fn breaker_snapshot(&self) -> (u64, BreakerState) {
        let state = self.evidence.state.lock();
        (state.generation, state.breaker)
    }
    pub fn ws_final_trusted(&self) -> bool {
        self.settings.evidence.as_ref().is_none_or(|settings| {
            self.breaker_state()
                .ws_final_trusted(self.now_ms(), settings.ws_trust_window_ms)
        })
    }
    pub fn evidence_conflicts(&self) -> Vec<EvidenceConflict> {
        self.evidence
            .state
            .lock()
            .conflicts
            .iter()
            .cloned()
            .collect()
    }
    pub fn needs_verdict(&self, id: usize, open: i64) -> bool {
        self.catalog
            .slot(id)
            .select_bars(|s| s.needs_verdict(open, self.ws_final_trusted()))
    }
}

pub(crate) fn incoming_record(input: &kline_core::evidence::EvidenceUpdate) -> EvidenceRecord {
    EvidenceRecord {
        open_time: input.open_time(),
        bar: input.bar().cloned(),
        state: input.state(),
        ws_equal: input.state() == EvidenceState::WsFinal,
        synthetic: matches!(input, kline_core::evidence::EvidenceUpdate::Observation(u) if u.source == kline_core::Source::Synthetic),
    }
}
