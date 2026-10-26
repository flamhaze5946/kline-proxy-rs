use crate::{Catalog, Clock, cache::Cache};
use kline_core::{Commit, Market, Update};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone)]
pub struct Settings {
    pub evidence: Option<crate::evidence::EvidenceSettings>,
    pub number_type: kline_core::NumberType,
    pub closed_bar_latency_enabled: bool,
    pub final_wait_ms: u64,
    /// A closed_only bulk request arriving at most this long before an interval boundary waits
    /// for the boundary (0 disables; capped at 1 s).
    pub pre_boundary_wait_ms: u64,
    /// Bulk decides its interval boundary by the later of this host's clock and the server-time
    /// estimate, so an estimate running behind cannot answer for the period before.
    pub host_clock_floor: bool,
    pub cache_bytes: usize,
    pub concurrent_builds: usize,
    /// Distinct bulk keys that may be in flight (waiting for finals or building) at once.
    pub inflight_limit: usize,
    /// New keys allowed to queue for an in-flight slot; beyond it a request is Busy at once.
    pub admission_queue: usize,
    /// Longest a new key waits for an in-flight slot before it is Busy.
    pub admission_wait_ms: u64,
    /// Concurrent requests per HTTP route group; further requests queue for a place.
    pub http_concurrency_limit: usize,
    /// Requests allowed to queue per route group; beyond it a request is answered 503 at once.
    pub http_admission_queue: usize,
    /// Longest a request waits for a place in its route group before 503.
    pub http_admission_wait_ms: u64,
    /// Where upstream WebSocket connection attempts are recorded across restarts (see
    /// [`crate::connect_pacer`]); none keeps the count to this process.
    pub connect_journal: Option<std::path::PathBuf>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            evidence: None,
            number_type: kline_core::NumberType::Double,
            closed_bar_latency_enabled: true,
            final_wait_ms: 8_000,
            pre_boundary_wait_ms: 250,
            host_clock_floor: true,
            cache_bytes: 64 * 1024 * 1024,
            concurrent_builds: 2,
            inflight_limit: 256,
            admission_queue: 4096,
            admission_wait_ms: 8_000,
            http_concurrency_limit: 512,
            http_admission_queue: 4096,
            http_admission_wait_ms: 8_000,
            connect_journal: None,
        }
    }
}
#[derive(Default)]
pub struct Metrics {
    pub http: crate::diagnostics::HttpMetrics,
    pub work: crate::diagnostics::WorkMetrics,
    pub frames: AtomicU64,
    pub ignored: AtomicU64,
    pub invalid: AtomicU64,
    pub forming: AtomicU64,
    pub finals: AtomicU64,
    pub final_revisions: AtomicU64,
    pub responses_built: AtomicU64,
    pub cache_hits: AtomicU64,
    pub payloads_built: AtomicU64,
    pub payload_reuses: AtomicU64,
    pub admission_queued: AtomicU64,
    pub admission_rejected: AtomicU64,
    pub http_admission_queued: AtomicU64,
    pub http_admission_rejected: AtomicU64,
    pub http_admission_waiting: std::sync::atomic::AtomicUsize,
    pub pre_boundary_waits: AtomicU64,
}
pub struct Engine {
    pub catalog: Catalog,
    pub metrics: Metrics,
    pub(crate) evidence: crate::evidence::EvidenceRuntime,
    pub diagnostics: crate::diagnostics::Diagnostics,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) settings: Settings,
    pub(crate) cache: Cache,
    pub(crate) builds: crate::build_pool::BuildPool,
    pub(crate) windows: crate::windows::Windows,
    connects: Arc<crate::connect_pacer::ConnectPacer>,
    revision: [AtomicU64; 2],
    sequence: AtomicU64,
    gap_epoch: AtomicU64,
}
#[derive(Debug)]
pub struct Ingested {
    pub id: usize,
    pub commit: Commit,
    pub open_time: i64,
    pub closed: bool,
}
impl Engine {
    pub fn new(catalog: Catalog, clock: Arc<dyn Clock>, mut settings: Settings) -> Arc<Self> {
        settings.final_wait_ms = settings.final_wait_ms.min(30_000);
        settings.pre_boundary_wait_ms = settings.pre_boundary_wait_ms.min(1_000);
        settings.concurrent_builds = settings.concurrent_builds.clamp(1, 64);
        settings.inflight_limit = settings.inflight_limit.clamp(1, 65_536);
        settings.admission_queue = settings.admission_queue.min(1 << 20);
        settings.admission_wait_ms = settings.admission_wait_ms.min(30_000);
        settings.http_concurrency_limit = settings.http_concurrency_limit.clamp(1, 65_536);
        settings.http_admission_queue = settings.http_admission_queue.min(1 << 20);
        settings.http_admission_wait_ms = settings.http_admission_wait_ms.min(30_000);
        if settings.evidence.is_some() {
            catalog.enable_evidence();
        }
        let windows = crate::windows::Windows::new(catalog.slots().len());
        let connects = Arc::new(match &settings.connect_journal {
            Some(journal) => {
                crate::connect_pacer::ConnectPacer::default().with_journal(journal.clone())
            }
            None => crate::connect_pacer::ConnectPacer::default(),
        });
        let catalog_loaded = catalog.generation() > 0;
        Arc::new(Self {
            catalog,
            windows,
            clock,
            cache: Cache::new(
                settings.cache_bytes,
                crate::cache::Admission {
                    slots: settings.inflight_limit,
                    queue: settings.admission_queue,
                    wait: std::time::Duration::from_millis(settings.admission_wait_ms),
                },
            ),
            builds: crate::build_pool::BuildPool::new(settings.concurrent_builds),
            settings,
            metrics: Metrics::default(),
            evidence: {
                let state = crate::evidence::EvidenceRuntime::default();
                state
                    .catalog_loaded
                    .store(catalog_loaded, Ordering::Release);
                state
            },
            diagnostics: crate::diagnostics::Diagnostics::default(),
            connects,
            revision: [AtomicU64::new(0), AtomicU64::new(0)],
            sequence: AtomicU64::new(0),
            gap_epoch: AtomicU64::new(0),
        })
    }
    pub fn ingest(
        &self,
        market: Market,
        raw: &[u8],
    ) -> Result<Option<Ingested>, binance_wire::ParseError> {
        let received_ms = self.now_ms();
        let started = self
            .settings
            .closed_bar_latency_enabled
            .then(std::time::Instant::now);
        self.metrics.frames.fetch_add(1, Ordering::Relaxed);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let parsed = match binance_wire::parse_with_type(raw, sequence, self.settings.number_type) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.metrics.invalid.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        let Some(parsed) = parsed else {
            self.metrics.ignored.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        let Some(id) = self
            .catalog
            .resolve(market, parsed.interval, &parsed.identity)
        else {
            self.metrics.ignored.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        self.catalog
            .slot(id)
            .last_stream_ms
            .store(self.now_ms(), Ordering::Relaxed);
        let decoded = started.map(|s| s.elapsed()).unwrap_or_default();
        let committing = self
            .settings
            .closed_bar_latency_enabled
            .then(std::time::Instant::now);
        let closed = parsed.update.closed;
        let open_time = parsed.update.bar.open_time;
        let event_time = parsed.update.event_time;
        let commit = self.commit(id, parsed.update)?;
        if closed {
            self.diagnostics.record(
                self,
                id,
                open_time,
                crate::diagnostics::Observation {
                    received_ms,
                    event_ms: event_time,
                    decode: decoded,
                    cache: committing.map(|s| s.elapsed()).unwrap_or_default(),
                },
            );
        }
        (if closed {
            &self.metrics.finals
        } else {
            &self.metrics.forming
        })
        .fetch_add(1, Ordering::Relaxed);
        Ok(Some(Ingested {
            id,
            commit,
            open_time,
            closed,
        }))
    }
    pub fn commit(&self, id: usize, update: Update) -> Result<Commit, kline_core::InvalidBar> {
        if self.evidence_enabled() {
            return self.commit_evidence(
                id,
                kline_core::evidence::EvidenceUpdate::Observation(update),
            );
        }
        if update.bar.number_type != self.settings.number_type {
            return Err(kline_core::InvalidBar::Number);
        }
        let source = update.source;
        let slot = self.catalog.slot(id);
        if !slot.is_tracked() {
            return Ok(Commit::default());
        }
        let (result, window_changed) = {
            let mut state = slot.state.write();
            if !slot.is_tracked() {
                return Ok(Commit::default());
            }
            let previous = state
                .latest_bar()
                .filter(|bar| {
                    source == kline_core::Source::Stream
                        && update.bar.open_time > bar.close_time.saturating_add(1)
                })
                .cloned();
            let gap = previous.is_some();
            let mut filled_trimmed = false;
            if let Some(previous) = previous
                && slot.interval.code() != "1M"
            {
                // Reject the incoming record before any gap fill mutates state.
                update.bar.validate()?;
                let period = slot.interval.millis();
                let missing = (update.bar.open_time - previous.open_time - 1) / period;
                let first = missing
                    .saturating_sub(state.capacity().saturating_sub(1) as i64)
                    .max(0)
                    + 1;
                for step in first..=missing {
                    let filled = state.commit(Update {
                        bar: previous.synthetic_after(previous.open_time + step * period, period),
                        closed: false,
                        source: kline_core::Source::Synthetic,
                        event_time: None,
                        sequence: 0,
                    })?;
                    filled_trimmed |= filled.trimmed;
                }
            }
            let mut result = state.commit(update)?;
            result.trimmed |= filled_trimmed;
            if gap && result.updated {
                slot.gap_generation.fetch_add(1, Ordering::Release);
                self.gap_epoch.fetch_add(1, Ordering::Release);
            }
            if result.updated && state.len() == 1 {
                slot.populated.store(true, Ordering::Release);
            }
            if result.became_final || result.final_revised || result.trimmed {
                slot.durable_generation.fetch_add(1, Ordering::Release);
            }
            let window_changed = result.inserted
                || result.close_time_changed
                || result.became_final
                || result.final_revised
                || result.stream_final_observed
                || result.trimmed;
            if window_changed {
                slot.window_generation.fetch_add(1, Ordering::Release);
            }
            (result, window_changed)
        };
        if result.final_revised || (result.became_final && source != kline_core::Source::Stream) {
            self.revision[slot.market as usize].fetch_add(1, Ordering::Release);
        }
        if result.final_revised {
            self.metrics.final_revisions.fetch_add(1, Ordering::Relaxed);
        }
        // Notification follows atomic state publication. Subscribers always recheck.
        if window_changed {
            slot.changed.send_modify(|n| *n = n.wrapping_add(1));
            self.windows.invalidate(id);
        }
        Ok(result)
    }
    pub fn commit_evidence(
        &self,
        id: usize,
        input: kline_core::evidence::EvidenceUpdate,
    ) -> Result<Commit, kline_core::InvalidBar> {
        self.commit_evidence_in_generation(id, input, None)
    }
    /// Recovery binds every row publication to the same tracked lifetime, so an
    /// old response cannot resurrect an identity removed during its HTTP await.
    pub fn commit_evidence_in_generation(
        &self,
        id: usize,
        input: kline_core::evidence::EvidenceUpdate,
        tracking_generation: Option<u64>,
    ) -> Result<Commit, kline_core::InvalidBar> {
        use kline_core::evidence::EvidenceUpdate;
        if input
            .bar()
            .is_some_and(|bar| bar.number_type != self.settings.number_type)
        {
            return Err(kline_core::InvalidBar::Number);
        }
        if let Some(bar) = input.bar() {
            kline_core::evidence::validate_bar(bar)?;
        } else if input.open_time() < 0 || input.open_time() % 60_000 != 0 {
            return Err(kline_core::InvalidBar::Time);
        }
        let slot = self.catalog.slot(id);
        let result = {
            let mut series = slot.state.write();
            if !slot.is_tracked()
                || tracking_generation
                    .is_some_and(|generation| generation != slot.tracking_generation())
            {
                return Ok(Commit::default());
            }
            let previous = series.evidence_record(input.open_time());
            let incoming = crate::evidence::incoming_record(&input);
            let mut trimmed = false;
            let mut filled = false;
            if let EvidenceUpdate::Observation(update) = &input
                && update.source == kline_core::Source::Stream
                && let Some(previous) = series
                    .latest_bar()
                    .filter(|bar| update.bar.open_time > bar.close_time.saturating_add(1))
                    .cloned()
            {
                update.bar.validate()?;
                kline_core::evidence::decimal_fields(&update.bar)?;
                let period = slot.interval.millis();
                let missing = (update.bar.open_time - previous.open_time - 1) / period;
                let first = missing
                    .saturating_sub(series.capacity().saturating_sub(1) as i64)
                    .max(0)
                    + 1;
                for step in first..=missing {
                    let result = series.commit_evidence(
                        EvidenceUpdate::Observation(Update {
                            bar: previous
                                .synthetic_after(previous.open_time + step * period, period),
                            closed: false,
                            source: kline_core::Source::Synthetic,
                            event_time: None,
                            sequence: 0,
                        }),
                        slot.is_trading(),
                    )?;
                    trimmed |= result.trimmed;
                    filled |= result.evidence_changed;
                }
                if filled {
                    slot.gap_generation.fetch_add(1, Ordering::Release);
                    self.gap_epoch.fetch_add(1, Ordering::Release);
                }
            }
            let mut result = series.commit_evidence(input, slot.is_trading())?;
            result.trimmed |= trimmed;
            result.evidence_changed |= filled || trimmed;
            if series.covered_from().is_some() {
                slot.populated.store(true, Ordering::Release);
            }
            // Conflict evaluation precedes replacement under this same series lock.
            if result.evidence_conflict {
                self.evidence.record(crate::evidence::EvidenceConflict {
                    id,
                    observed_ms: self.now_ms(),
                    previous,
                    incoming,
                    trips_breaker: result.breaker_conflict,
                });
                self.metrics.final_revisions.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(id, symbol=%slot.symbol, trips_breaker=result.breaker_conflict, "evidence conclusion revised");
            }
            if result.evidence_changed {
                slot.durable_generation.fetch_add(1, Ordering::Release);
            }
            if result.evidence_changed || result.stream_final_observed {
                slot.window_generation.fetch_add(1, Ordering::Release);
            }
            result
        };
        if result.evidence_changed {
            self.revision[slot.market as usize].fetch_add(1, Ordering::Release);
        }
        if result.evidence_changed || result.stream_final_observed {
            slot.changed.send_modify(|n| *n = n.wrapping_add(1));
            self.windows.invalidate(id);
        }
        Ok(result)
    }

    pub fn refresh_catalog(
        &self,
        instruments: Vec<crate::Instrument>,
    ) -> Result<crate::catalog::CatalogChange, String> {
        self.refresh_catalog_inner(instruments, None)
    }
    pub fn refresh_catalog_with_retained(
        &self,
        instruments: Vec<crate::Instrument>,
        records: Vec<crate::evidence_bulk::RetainedRecord>,
    ) -> Result<crate::catalog::CatalogChange, String> {
        self.refresh_catalog_inner(instruments, Some(records))
    }
    fn refresh_catalog_inner(
        &self,
        instruments: Vec<crate::Instrument>,
        retained: Option<Vec<crate::evidence_bulk::RetainedRecord>>,
    ) -> Result<crate::catalog::CatalogChange, String> {
        let _writer = self
            .evidence_enabled()
            .then(|| self.evidence.catalog_writer.lock());
        let removed: Vec<_> = if self.evidence_enabled() {
            let wanted: std::collections::HashSet<_> = instruments
                .iter()
                .map(|i| (i.market, i.interval, i.symbol.as_str()))
                .collect();
            self.catalog
                .slots()
                .iter()
                .filter(|slot| {
                    slot.is_tracked()
                        && !wanted.contains(&(slot.market, slot.interval, slot.symbol.as_ref()))
                })
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let change = self.catalog.sync(instruments)?;
        if let Some(records) = retained {
            self.install_retained(records);
        }
        self.evidence.catalog_loaded.store(true, Ordering::Release);
        for slot in removed {
            slot.tracking_generation.fetch_add(1, Ordering::Release);
        }
        if change.changed {
            self.windows.resize(self.catalog.slots().len());
            for id in 0..self.catalog.slots().len() {
                self.windows.invalidate(id);
            }
            for revision in &self.revision {
                revision.fetch_add(1, Ordering::Release);
            }
        }
        Ok(change)
    }
    pub fn revision(&self, market: Market) -> u64 {
        self.revision[market as usize].load(Ordering::Acquire)
    }
    /// Record a recheck hint without claiming an observed data gap.
    /// Runtime reconciliation does not advance its REST deadline for this hint.
    pub fn request_recheck(&self, id: usize) {
        let slot = self.catalog.slot(id);
        if slot.is_tracked() {
            slot.recheck_generation.fetch_add(1, Ordering::Release);
        }
    }
    pub fn gap_epoch(&self) -> u64 {
        self.gap_epoch.load(Ordering::Acquire)
    }
    pub fn cache_sizes(&self) -> (usize, usize, usize) {
        self.cache.sizes()
    }
    pub fn admission_waiting(&self) -> usize {
        self.cache.waiting()
    }
    /// Live registrations on in-flight bulk keys (leaders plus followers).
    pub fn inflight_holders(&self) -> usize {
        self.cache.holders()
    }
    /// Every upstream WebSocket connection attempt of this process takes a turn here first.
    pub fn connect_pacer(&self) -> &Arc<crate::connect_pacer::ConnectPacer> {
        &self.connects
    }
    /// The settings in effect after the clamps in [`Engine::new`].
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
    /// A new admission gate for one HTTP route group, sized by these settings.
    pub fn http_gate(&self) -> crate::admission::Gate {
        crate::admission::Gate::new(
            self.settings.http_concurrency_limit,
            self.settings.http_admission_queue,
            std::time::Duration::from_millis(self.settings.http_admission_wait_ms),
        )
    }
    pub async fn windows_changed(&self) {
        self.windows.changed().await;
    }
    pub fn prepare_closed_windows(&self) {
        self.windows.prepare(self);
    }
    pub fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }
    pub fn number_type(&self) -> kline_core::NumberType {
        self.settings.number_type
    }
    pub fn detailed_latency_enabled(&self) -> bool {
        self.settings.closed_bar_latency_enabled
    }
}
