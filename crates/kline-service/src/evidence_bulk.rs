//! kp-evidence-v1 wire contract. All symbol maps are deliberately dense.
use crate::{BulkReply, Engine, ServiceError, cache::Key};
use kline_core::evidence::EvidenceState;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

pub type EvidenceKline = [serde_json::Value; 12];
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedRecord {
    pub symbol: String,
    pub pair: String,
    pub contract_type: String,
    pub since: i64,
    pub expires_ms: i64,
    pub delivery_ms: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SymbolStatus {
    Trading,
    NotTrading {
        since: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        delivery_ms: Option<i64>,
    },
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowEvidence {
    Final,
    Pending,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceWindow {
    pub first_open: i64,
    pub last_open: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceResponse {
    pub interval: String,
    pub ts_ms: i64,
    pub contract: String,
    pub number_type: String,
    pub limit_effective: usize,
    pub window: EvidenceWindow,
    pub ws_final_trusted: bool,
    pub revised_total: u64,
    pub status: BTreeMap<String, SymbolStatus>,
    pub klines: BTreeMap<String, Vec<EvidenceKline>>,
    pub evidence: BTreeMap<String, Vec<RowEvidence>>,
    pub absent: BTreeMap<String, Vec<i64>>,
    pub missing: BTreeMap<String, Vec<i64>>,
    pub synthetic: BTreeMap<String, Vec<i64>>,
    pub covered_from: BTreeMap<String, Option<i64>>,
}
impl EvidenceResponse {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.interval == "1m"
                && self.contract == "kp-evidence-v1"
                && self.number_type == "string",
            "invalid evidence contract"
        );
        anyhow::ensure!(
            (1..=100).contains(&self.limit_effective)
                && self.window.first_open % 60_000 == 0
                && self.window.last_open % 60_000 == 0
                && self.window.last_open - self.window.first_open
                    == (self.limit_effective as i64 - 1) * 60_000,
            "invalid evidence window"
        );
        anyhow::ensure!(
            self.window.last_open == self.ts_ms.div_euclid(60_000) * 60_000 - 60_000,
            "timestamp/window mismatch"
        );
        let keys: BTreeSet<_> = self.status.keys().collect();
        for map in [
            self.klines.keys().collect::<BTreeSet<_>>(),
            self.evidence.keys().collect(),
            self.absent.keys().collect(),
            self.missing.keys().collect(),
            self.synthetic.keys().collect(),
            self.covered_from.keys().collect(),
        ] {
            anyhow::ensure!(map == keys, "incomplete evidence symbol map");
        }
        for (symbol, status) in &self.status {
            anyhow::ensure!(
                self.covered_from[symbol].is_none_or(|open| open >= 0 && open % 60_000 == 0),
                "invalid covered_from"
            );
            let bars = &self.klines[symbol];
            let evidence = &self.evidence[symbol];
            anyhow::ensure!(bars.len() == evidence.len(), "unaligned evidence");
            let mut covered = BTreeSet::new();
            let mut pending = BTreeSet::new();
            let mut previous = None;
            for (row, kind) in bars.iter().zip(evidence) {
                let open = row[0]
                    .as_i64()
                    .ok_or_else(|| anyhow::anyhow!("invalid open_time"))?;
                anyhow::ensure!(
                    [1, 2, 3, 4, 5, 7, 9, 10]
                        .iter()
                        .all(|&i| row[i].is_string())
                        && row[6].as_i64().is_some_and(|close| close >= open)
                        && row[8]
                            .as_u64()
                            .is_some_and(|trades| trades <= i32::MAX as u64)
                        && row[11] == "0",
                    "invalid string kline wire shape"
                );
                anyhow::ensure!(previous.is_none_or(|p| p < open), "unordered evidence rows");
                previous = Some(open);
                anyhow::ensure!(covered.insert(open), "duplicate evidence minute");
                if *kind == RowEvidence::Pending {
                    pending.insert(open);
                }
            }
            for times in [&self.absent[symbol], &self.missing[symbol]] {
                anyhow::ensure!(times.windows(2).all(|w| w[0] < w[1]), "unordered gap list");
                for &open in times {
                    anyhow::ensure!(covered.insert(open), "overlapping evidence minute");
                }
            }
            anyhow::ensure!(
                self.synthetic[symbol].windows(2).all(|w| w[0] < w[1])
                    && self.synthetic[symbol].iter().all(|t| pending.contains(t)),
                "synthetic must be pending"
            );
            let expected: BTreeSet<i64> =
                self.covered_from[symbol].map_or_else(BTreeSet::new, |first| {
                    (self.window.first_open.max(first)..=self.window.last_open)
                        .step_by(60_000)
                        .collect()
                });
            anyhow::ensure!(covered == expected, "evidence window partition violated");
            if matches!(status, SymbolStatus::Unknown) {
                anyhow::ensure!(
                    self.covered_from[symbol].is_none() && covered.is_empty(),
                    "unknown has coverage"
                );
            }
        }
        Ok(())
    }
}
type DirectoryVersion = (std::sync::Arc<std::sync::atomic::AtomicU64>, u64);
#[derive(Clone, Debug)]
pub(crate) struct EvidenceSelection {
    pub key: Key,
    pub symbols: std::sync::Arc<[String]>,
    pub identities: Vec<Option<crate::evidence::EvidenceIdentity>>,
    pub directory_versions: Vec<DirectoryVersion>,
}
#[derive(Clone, Debug)]
pub(crate) struct EvidenceStamp {
    pub trusted: bool,
    pub breaker_generation: u64,
    pub generations: Vec<EvidenceSeriesVersion>,
    pub selection: EvidenceSelection,
    pub observed_at: i64,
    pub valid_until: i64,
}
#[derive(Clone)]
pub(crate) struct EvidenceSeriesVersion {
    slot: std::sync::Arc<crate::catalog::Slot>,
    closed: u64,
    tracking: u64,
}
impl std::fmt::Debug for EvidenceSeriesVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EvidenceSeriesVersion")
            .field(&self.closed)
            .field(&self.tracking)
            .finish()
    }
}
#[derive(Debug, thiserror::Error)]
#[error("evidence symbol count exceeds directory capacity 100000")]
struct EvidenceSymbolLimit;
pub(crate) fn is_symbol_limit(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Worker(e) if e.is::<EvidenceSymbolLimit>())
}
#[derive(Debug, thiserror::Error)]
#[error("evidence selection or boundary expired")]
struct StaleEvidence;
pub(crate) fn stale_evidence() -> ServiceError {
    ServiceError::Worker(StaleEvidence.into())
}
pub(crate) fn is_stale_evidence(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Worker(e) if e.is::<StaleEvidence>())
}
impl Engine {
    pub(crate) fn expose_evidence_boundary(&self, boundary: i64) {
        self.evidence
            .exposed_boundary
            .fetch_max(boundary, Ordering::Release);
    }
    #[cfg(test)]
    pub(crate) fn evidence_key(
        &self,
        query: &crate::BulkQuery,
        interval: kline_core::Interval,
        now: i64,
    ) -> (Key, std::sync::Arc<[String]>) {
        self.evidence_key_snapshot(query, interval, now, || {})
    }
    fn evidence_key_snapshot(
        &self,
        query: &crate::BulkQuery,
        interval: kline_core::Interval,
        now: i64,
        after_ids: impl FnOnce(),
    ) -> (Key, std::sync::Arc<[String]>) {
        let _writer = self.evidence.catalog_writer.lock();
        let mut symbols = if query.symbols.is_empty() {
            self.catalog
                .instruments()
                .into_iter()
                .filter(|i| i.market == query.market && i.interval == interval)
                .map(|i| i.symbol)
                .collect::<Vec<_>>()
        } else {
            query
                .symbols
                .iter()
                .map(|s| crate::http_compat::java_trim(s).to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        };
        symbols.sort();
        symbols.dedup();
        let ids = symbols
            .iter()
            .filter_map(|s| self.catalog.find(query.market, interval, s))
            .filter(|&id| self.catalog.slot(id).is_tracked())
            .collect::<Vec<_>>()
            .into();
        after_ids();
        let boundary = interval.boundary(self.bulk_now_ms().max(now));
        self.expose_evidence_boundary(boundary);
        (
            Key {
                market: query.market,
                interval,
                limit: query.limit.unwrap_or(5).clamp(1, 100) as usize,
                closed_only: true,
                ids,
                boundary,
                revision: self.catalog.generation(),
            },
            symbols.into(),
        )
    }
    pub(crate) fn select_evidence(
        &self,
        query: &crate::BulkQuery,
        interval: kline_core::Interval,
        now: i64,
    ) -> Result<EvidenceSelection, ServiceError> {
        let mut unique = BTreeSet::new();
        for symbol in &query.symbols {
            let symbol = crate::http_compat::java_trim(symbol);
            if !symbol.is_empty() {
                unique.insert(symbol);
            }
            if unique.len() > crate::catalog::MAX_SERIES {
                return Err(ServiceError::Worker(EvidenceSymbolLimit.into()));
            }
        }
        let mut view = None;
        let (key, symbols) = self.evidence_key_snapshot(query, interval, now, || {
            view = Some(self.evidence.directory.load_full());
        });
        let view = view.unwrap();
        if view.generation != key.revision {
            return Err(stale_evidence());
        }
        let identities = symbols
            .iter()
            .map(|symbol| {
                view.entries
                    .get(&(key.market, key.interval, symbol.clone()))
                    .cloned()
            })
            .collect();
        let directory_versions =
            self.evidence_directory_versions(&symbols, key.market, key.interval)?;
        let selection = EvidenceSelection {
            key,
            symbols,
            identities,
            directory_versions,
        };
        if !self.evidence_selection_current(&selection) {
            return Err(stale_evidence());
        }
        Ok(selection)
    }
    fn evidence_directory_versions(
        &self,
        symbols: &[String],
        market: kline_core::Market,
        interval: kline_core::Interval,
    ) -> Result<Vec<DirectoryVersion>, ServiceError> {
        // Classify and clone keys outside the writer. Selection validation detects
        // a publication between this view and the version capture below.
        let view = self.evidence.directory.load_full();
        let known = symbols
            .iter()
            .map(|symbol| (market, interval, symbol.clone()))
            .filter(|key| view.entries.contains_key(key))
            .collect::<Vec<_>>();
        let has_unknown = known.len() != symbols.len();
        let mut result = Vec::with_capacity(known.len() + usize::from(has_unknown));
        let mut versions = self.evidence.catalog_writer.lock();
        if self.catalog.published_generation.load(Ordering::Acquire) != view.generation {
            return Err(stale_evidence());
        }
        for key in known {
            if !versions.contains_key(&key) && versions.len() >= crate::catalog::MAX_SERIES {
                return Err(stale_evidence());
            }
            let weak = versions.entry(key).or_default();
            let version = weak.upgrade().unwrap_or_else(|| {
                let version = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                *weak = std::sync::Arc::downgrade(&version);
                version
            });
            let value = version.load(Ordering::Acquire);
            result.push((version, value));
        }
        if has_unknown {
            let version = self.evidence.membership_version.clone();
            let value = version.load(Ordering::Acquire);
            result.push((version, value));
        }
        Ok(result)
    }
    pub(crate) fn evidence_selection_current(&self, selection: &EvidenceSelection) -> bool {
        let epoch = self.evidence.directory_epoch.load(Ordering::Acquire);
        if !epoch.is_multiple_of(2)
            || selection
                .directory_versions
                .iter()
                .any(|(v, expected)| v.load(Ordering::Acquire) != *expected)
        {
            return false;
        }
        let view = self.evidence.directory.load_full();
        let same = self.catalog.generation() == view.generation
            && selection
                .symbols
                .iter()
                .zip(&selection.identities)
                .all(|(symbol, expected)| {
                    let actual = view.entries.get(&(
                        selection.key.market,
                        selection.key.interval,
                        symbol.clone(),
                    ));
                    actual == expected.as_ref()
                        && actual.is_none_or(|identity| {
                            let slot = self.catalog.slot(identity.id);
                            slot.is_tracked()
                                && slot.tracking_generation() == identity.tracking_generation
                                && slot.is_trading() == identity.definition.trading
                        })
                });
        same && self.evidence.directory_epoch.load(Ordering::Acquire) == epoch
    }
    fn evidence_status(
        identity: Option<&crate::evidence::EvidenceIdentity>,
        now: i64,
    ) -> SymbolStatus {
        match identity {
            None => SymbolStatus::Unknown,
            Some(i) if i.definition.trading => SymbolStatus::Trading,
            Some(i) if i.retained.as_ref().is_some_and(|r| r.expires_ms <= now) => {
                SymbolStatus::Unknown
            }
            Some(i) => SymbolStatus::NotTrading {
                since: i
                    .retained
                    .as_ref()
                    .map_or_else(|| i.since.unwrap_or(now), |r| r.since),
                delivery_ms: i.retained.as_ref().and_then(|r| r.delivery_ms),
            },
        }
    }
    pub fn retained_records(&self) -> Vec<RetainedRecord> {
        self.evidence.retained.read().values().cloned().collect()
    }
    pub(crate) fn install_retained(&self, records: Vec<RetainedRecord>) {
        let records: BTreeMap<_, _> = records.into_iter().map(|r| (r.symbol.clone(), r)).collect();
        let mut old = self.evidence.retained.write();
        if *old != records {
            *old = records;
            self.evidence
                .retained_generation
                .fetch_add(1, Ordering::Release);
        }
        for slot in self.catalog.slots().iter() {
            slot.set_not_trading_since(
                (!slot.is_trading())
                    .then(|| old.get(slot.symbol.as_ref()).map(|r| r.since))
                    .flatten(),
            );
        }
    }
    pub fn restore_retained(&self, records: Vec<RetainedRecord>) {
        let mut writer = self.evidence.catalog_writer.lock();
        let proposed: BTreeMap<_, _> = records
            .iter()
            .map(|r| (r.symbol.clone(), r.clone()))
            .collect();
        if *self.evidence.retained.read() == proposed
            && self.catalog.published_generation.load(Ordering::Acquire)
                == self.evidence.catalog_generation.load(Ordering::Acquire)
        {
            return;
        }
        let _write = self.evidence.begin_directory_write();
        self.install_retained(records);
        self.evidence.publish_directory(&self.catalog, &mut writer);
    }
    pub fn evidence_catalog_loaded(&self) -> bool {
        self.evidence.catalog_loaded.load(Ordering::Acquire)
    }
    pub(crate) fn evidence_reply_current(
        &self,
        key: &Key,
        stamp: &EvidenceStamp,
        now: i64,
    ) -> bool {
        // The stamp owns the series and symbol-epoch atomics. Do not look up rows,
        // directory views, or any lock here, even with a nonblocking try operation.
        let _ = now;
        let epoch = self.evidence.directory_epoch.load(Ordering::Acquire);
        if !epoch.is_multiple_of(2)
            || self.catalog.published_generation.load(Ordering::Acquire)
                != self.evidence.catalog_generation.load(Ordering::Acquire)
            || self.evidence.breaker_generation.load(Ordering::Acquire) != stamp.breaker_generation
            || stamp
                .selection
                .directory_versions
                .iter()
                .any(|(version, expected)| version.load(Ordering::Acquire) != *expected)
            || key.ids.len() != stamp.generations.len()
            || stamp.generations.iter().any(|version| {
                version.slot.closed_version.load(Ordering::Acquire) != version.closed
                    || version.slot.tracking_generation.load(Ordering::Acquire) != version.tracking
            })
        {
            return false;
        }
        let last_conflict_ms = self.evidence.last_conflict_ms.load(Ordering::Acquire);
        if self.evidence.directory_epoch.load(Ordering::Acquire) != epoch
            || self.evidence.breaker_generation.load(Ordering::Acquire) != stamp.breaker_generation
        {
            return false;
        }
        let now = self.bulk_now_ms().max(key.boundary);
        now >= stamp.observed_at
            && now < stamp.valid_until
            && crate::evidence::BreakerState {
                last_conflict_ms,
                revised_total: 0,
            }
            .ws_final_trusted(
                self.now_ms(),
                self.settings.evidence.as_ref().unwrap().ws_trust_window_ms,
            ) == stamp.trusted
    }
    #[cfg(test)]
    pub(crate) fn build_evidence_body(
        &self,
        key: &Key,
        symbols: &[String],
        waited_ms: u64,
    ) -> Result<BulkReply, ServiceError> {
        self.build_evidence_snapshot(key, symbols, waited_ms, || {})
    }
    #[cfg(test)]
    fn build_evidence_snapshot(
        &self,
        key: &Key,
        symbols: &[String],
        waited_ms: u64,
        after_stamps: impl FnOnce(),
    ) -> Result<BulkReply, ServiceError> {
        let view = self.evidence.directory.load_full();
        let selection = EvidenceSelection {
            key: key.clone(),
            symbols: symbols.to_vec().into(),
            directory_versions: self.evidence_directory_versions(
                symbols,
                key.market,
                key.interval,
            )?,
            identities: symbols
                .iter()
                .map(|symbol| {
                    view.entries
                        .get(&(key.market, key.interval, symbol.clone()))
                        .cloned()
                })
                .collect(),
        };
        if selection
            .identities
            .iter()
            .flatten()
            .map(|i| i.id)
            .collect::<Vec<_>>()
            .as_slice()
            != key.ids.as_ref()
        {
            return Err(stale_evidence());
        }
        self.build_evidence_selected_snapshot(&selection, waited_ms, after_stamps)
    }
    pub(crate) fn build_evidence_selected(
        &self,
        selection: &EvidenceSelection,
        waited_ms: u64,
    ) -> Result<BulkReply, ServiceError> {
        self.build_evidence_selected_snapshot(selection, waited_ms, || {})
    }
    fn build_evidence_selected_snapshot(
        &self,
        selection: &EvidenceSelection,
        waited_ms: u64,
        after_stamps: impl FnOnce(),
    ) -> Result<BulkReply, ServiceError> {
        let key = &selection.key;
        let symbols = &selection.symbols;
        if !self.evidence_selection_current(selection) {
            return Err(stale_evidence());
        }
        let now = self.bulk_now_ms().max(key.boundary);
        if key.interval.boundary(now) != key.boundary {
            return Err(stale_evidence());
        }
        self.expose_evidence_boundary(key.boundary);
        let (snapshot_generation, breaker) = self.breaker_snapshot();
        let breaker_generation = self.evidence.breaker_generation.load(Ordering::Acquire);
        if snapshot_generation != breaker_generation {
            return Err(stale_evidence());
        }
        let trusted = breaker.ws_final_trusted(
            self.now_ms(),
            self.settings.evidence.as_ref().unwrap().ws_trust_window_ms,
        );
        let mut response = EvidenceResponse {
            interval: "1m".into(),
            ts_ms: now,
            contract: "kp-evidence-v1".into(),
            number_type: "string".into(),
            limit_effective: key.limit,
            window: EvidenceWindow {
                first_open: key.boundary - key.limit as i64 * 60_000,
                last_open: key.boundary - 60_000,
            },
            ws_final_trusted: trusted,
            revised_total: breaker.revised_total,
            status: BTreeMap::new(),
            klines: BTreeMap::new(),
            evidence: BTreeMap::new(),
            absent: BTreeMap::new(),
            missing: BTreeMap::new(),
            synthetic: BTreeMap::new(),
            covered_from: BTreeMap::new(),
        };
        let mut finalized = true;
        let generations = key
            .ids
            .iter()
            .map(|&id| {
                let slot = self.catalog.slot(id);
                let closed = slot.closed_version.load(Ordering::Acquire);
                let tracking = slot.tracking_generation.load(Ordering::Acquire);
                EvidenceSeriesVersion {
                    slot,
                    closed,
                    tracking,
                }
            })
            .collect();
        after_stamps();
        for (symbol, identity) in symbols.iter().zip(&selection.identities) {
            let mut rows = Vec::new();
            let mut kinds = Vec::new();
            let mut absent = Vec::new();
            let mut missing = Vec::new();
            let mut synthetic = Vec::new();
            let mut covered_from = None;
            let status = Self::evidence_status(identity.as_ref(), now);
            if let Some(identity) = identity
                .as_ref()
                .filter(|_| !matches!(status, SymbolStatus::Unknown))
            {
                let id = identity.id;
                let slot = self.catalog.slot(id);
                let encoded = self.windows.get(id, &slot, 100, now)?;
                slot.select_bars(|series| -> Result<(), serde_json::Error> {
                    covered_from = series.covered_from();
                    if let Some(first) = covered_from {
                        for open in (first.max(response.window.first_open)
                            ..=response.window.last_open)
                            .step_by(60_000)
                        {
                            let record = series.evidence_record(open);
                            match record.state {
                                EvidenceState::Empty => missing.push(open),
                                EvidenceState::Absent => absent.push(open),
                                state => {
                                    let kind = if state.needs_verdict(trusted) {
                                        RowEvidence::Pending
                                    } else {
                                        RowEvidence::Final
                                    };
                                    let bar = record.bar.unwrap();
                                    let value = if let Some(row) =
                                        encoded.rows.iter().find(|r| r.bar == bar)
                                    {
                                        serde_json::from_slice(&row.bytes)?
                                    } else {
                                        serde_json::to_value(binance_wire::DisplayBar(bar))?
                                    };
                                    rows.push(serde_json::from_value(value)?);
                                    kinds.push(kind);
                                    if record.synthetic {
                                        synthetic.push(open);
                                    }
                                }
                            }
                        }
                    }
                    if identity.definition.trading
                        && series.needs_verdict(response.window.last_open, trusted)
                    {
                        finalized = false;
                    }
                    Ok(())
                })?;
            }
            response.status.insert(symbol.clone(), status);
            response.klines.insert(symbol.clone(), rows);
            response.evidence.insert(symbol.clone(), kinds);
            response.absent.insert(symbol.clone(), absent);
            response.missing.insert(symbol.clone(), missing);
            response.synthetic.insert(symbol.clone(), synthetic);
            response.covered_from.insert(symbol.clone(), covered_from);
        }
        if response.klines.values().map(Vec::len).sum::<usize>() > 75_000 {
            return Err(ServiceError::Evidence);
        }
        response.validate().map_err(ServiceError::Worker)?;
        let body: bytes::Bytes = serde_json::to_vec(&response)?.into();
        if !self.evidence_selection_current(selection) {
            return Err(stale_evidence());
        }
        let stamp = EvidenceStamp {
            trusted,
            breaker_generation,
            generations,
            selection: selection.clone(),
            observed_at: now,
            valid_until: selection
                .identities
                .iter()
                .flatten()
                .filter_map(|i| i.retained.as_ref())
                .filter(|r| r.expires_ms > now)
                .map(|r| r.expires_ms)
                .min()
                .unwrap_or(i64::MAX)
                .min(key.boundary + 60_000),
        };
        // Acquire the same published versions again after every row and encoding step.
        if !self.evidence_reply_current(key, &stamp, self.bulk_now_ms()) {
            return Err(stale_evidence());
        }
        self.metrics.responses_built.fetch_add(1, Ordering::Relaxed);
        Ok(BulkReply::evidence(
            body,
            finalized,
            waited_ms,
            key.boundary,
            stamp,
        ))
    }
}

#[cfg(test)]
mod flight_tests {
    use super::*;
    use crate::{
        Catalog, Clock, Instrument, Settings,
        cache::Lookup,
        evidence::{BreakerState, EvidenceSettings},
    };
    use kline_core::{Bar, Interval, Market, NumberType, Source, Update};
    use std::{
        num::NonZeroUsize,
        sync::{Arc, atomic::AtomicI64},
    };
    struct TestClock(AtomicI64);
    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }
    fn setup() -> (Arc<Engine>, Arc<TestClock>, Key, Arc<[String]>) {
        let clock = Arc::new(TestClock(AtomicI64::new(300500)));
        let e = Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "BTCUSDT".into(),
                interval: Interval::parse("1m").unwrap(),
                trading: true,
                continuous: Some(("BTCUSDT".into(), "PERPETUAL".into())),
                capacity: NonZeroUsize::new(241).unwrap(),
            }])
            .unwrap(),
            clock.clone(),
            Settings {
                number_type: NumberType::String,
                evidence: Some(EvidenceSettings {
                    ws_trust_window_ms: 60000,
                    ..EvidenceSettings::default()
                }),
                ..Settings::default()
            },
        );
        let mut b = Bar {
            open_time: 240000,
            close_time: 299999,
            ..Bar::default()
        };
        b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
        e.commit(
            0,
            Update {
                bar: b,
                closed: true,
                source: Source::Stream,
                event_time: Some(1),
                sequence: 1,
            },
        )
        .unwrap();
        let key = Key {
            market: Market::Future,
            interval: Interval::parse("1m").unwrap(),
            limit: 5,
            closed_only: true,
            ids: Arc::from([0]),
            boundary: 300000,
            revision: e.catalog.generation(),
        };
        (e, clock, key, Arc::from(["BTCUSDT".into()]))
    }
    #[tokio::test]
    async fn completed_payload_none_flight_is_replaced_on_trip_and_on_trust_expiry() {
        let (e, clock, key, symbols) = setup();
        for expiry in [false, true] {
            if expiry {
                e.restore_breaker(BreakerState {
                    last_conflict_ms: 240600,
                    revised_total: 1,
                });
            }
            let deadline = tokio::time::Instant::now();
            let registration = match e
                .cache
                .get_evidence(
                    key.clone(),
                    symbols.clone(),
                    |r| e.reply_current(&key, r, e.bulk_now_ms()),
                    None,
                    deadline,
                )
                .unwrap()
            {
                Lookup::Flight(r) => r,
                _ => panic!("expected flight"),
            };
            let reply = Arc::new(e.build_evidence_body(&key, &symbols, 0).unwrap());
            assert!(reply.payload.is_none());
            registration.cell.set(reply).unwrap();
            if expiry {
                clock.0.store(300600, Ordering::Relaxed);
            } else {
                e.restore_breaker(BreakerState {
                    last_conflict_ms: 300500,
                    revised_total: 1,
                });
            }
            let next = match e
                .cache
                .get_evidence(
                    key.clone(),
                    symbols.clone(),
                    |r| e.reply_current(&key, r, e.bulk_now_ms()),
                    None,
                    deadline,
                )
                .unwrap()
            {
                Lookup::Flight(r) => r,
                _ => panic!("expected replacement flight"),
            };
            assert!(!Arc::ptr_eq(&registration.cell, &next.cell));
            drop(next);
            drop(registration);
        }
    }
    #[tokio::test]
    async fn evidence_and_complete_unknown_sets_have_distinct_flight_identity() {
        let (e, _, key, symbols) = setup();
        let deadline = tokio::time::Instant::now();
        let a = match e
            .cache
            .get_evidence(key.clone(), symbols, |_| true, None, deadline)
            .unwrap()
        {
            Lookup::Flight(r) => r,
            _ => panic!(),
        };
        let b = match e
            .cache
            .get_evidence(
                key.clone(),
                Arc::from(["BTCUSDT".into(), "UNKNOWN".into()]),
                |_| true,
                None,
                deadline,
            )
            .unwrap()
        {
            Lookup::Flight(r) => r,
            _ => panic!(),
        };
        let c = match e.cache.get(key, |_| true, None, deadline).unwrap() {
            Lookup::Flight(r) => r,
            _ => panic!(),
        };
        assert!(!Arc::ptr_eq(&a.cell, &b.cell));
        assert!(!Arc::ptr_eq(&a.cell, &c.cell));
    }
    fn query(symbols: &[&str], limit: i32) -> crate::BulkQuery {
        crate::BulkQuery {
            market: Market::Future,
            interval: "1m".into(),
            limit: Some(limit),
            closed_only: true,
            symbols: symbols.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
    fn update(open: i64, value: &str, source: Source) -> Update {
        let mut bar = Bar {
            open_time: open,
            close_time: open + 59999,
            trades: 1,
            ..Bar::default()
        };
        bar.set_numbers(NumberType::String, [value; 8]).unwrap();
        Update {
            bar,
            closed: false,
            source,
            event_time: None,
            sequence: 0,
        }
    }
    #[tokio::test]
    async fn fix1_forming_updates_during_build_preserve_window_but_inside_updates_invalidate() {
        let (e, _, key, symbols) = setup();
        e.commit(0, update(180000, "1", Source::Rest)).unwrap();
        for n in 0..8 {
            let reply = e
                .build_evidence_snapshot(&key, &symbols, 0, || {
                    for j in 0..20 {
                        e.commit(
                            0,
                            update(300000, &(n * 20 + j + 2).to_string(), Source::Stream),
                        )
                        .unwrap();
                    }
                })
                .unwrap();
            assert!(e.reply_current(&key, &reply, e.bulk_now_ms()));
            e.cache
                .put_evidence(key.clone(), symbols.clone(), Arc::new(reply));
            assert!(e.bulk_with_evidence(query(&["BTCUSDT"], 5)).await.is_ok());
        }
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let breaker = e.breaker_snapshot();
        e.commit(0, update(180000, "999", Source::Rest)).unwrap();
        assert_eq!(e.breaker_snapshot(), breaker);
        assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
    }
    #[test]
    fn fix1_ids_and_revision_are_selected_under_one_directory_lock() {
        let (e, _, _, _) = setup();
        let before = e.catalog.generation();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut writer = None;
        let (key, symbols) = e.evidence_key_snapshot(
            &query(&["NEWUSDT"], 1),
            Interval::parse("1m").unwrap(),
            e.bulk_now_ms(),
            || {
                let e = e.clone();
                writer = Some(std::thread::spawn(move || {
                    tx.send(e.evidence.catalog_writer.try_lock().is_none())
                        .unwrap();
                    let mut definitions = e.catalog.instruments();
                    let mut new = definitions[0].clone();
                    new.symbol = "NEWUSDT".into();
                    new.continuous = Some(("NEWUSDT".into(), "PERPETUAL".into()));
                    definitions.push(new);
                    e.refresh_catalog(definitions).unwrap();
                }));
                assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap());
            },
        );
        writer.unwrap().join().unwrap();
        assert!(key.ids.is_empty());
        assert_eq!(key.revision, before);
        assert!(is_stale_evidence(
            &e.build_evidence_body(&key, &symbols, 0).unwrap_err()
        ));
        let (new, _) = e.evidence_key(&query(&["NEWUSDT"], 1), key.interval, e.bulk_now_ms());
        assert_eq!(new.ids.len(), 1);
        assert!(new.revision > key.revision);
    }
    #[test]
    fn fix1_unrelated_catalog_and_retained_changes_do_not_invalidate_requested_symbols() {
        let (e, _, key, symbols) = setup();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let mut definitions = e.catalog.instruments();
        let mut other = definitions[0].clone();
        other.symbol = "OTHERUSDT".into();
        other.continuous = Some(("OTHERUSDT".into(), "PERPETUAL".into()));
        definitions.push(other);
        e.refresh_catalog(definitions.clone()).unwrap();
        e.restore_retained(vec![RetainedRecord {
            symbol: "OTHERUSDT".into(),
            pair: "OTHERUSDT".into(),
            contract_type: "PERPETUAL".into(),
            since: 300000,
            expires_ms: 400000,
            delivery_ms: None,
        }]);
        assert!(e.reply_current(&key, &reply, e.bulk_now_ms()));
        definitions[0].continuous = Some(("BTCUSDT".into(), "TRADIFI_PERPETUAL".into()));
        e.refresh_catalog(definitions.clone()).unwrap();
        assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        definitions[0].trading = false;
        e.refresh_catalog(definitions).unwrap();
        assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
    }
    #[tokio::test]
    async fn fix1_build_queued_across_boundary_reselects_window_instead_of_failing() {
        let (e, clock, _, _) = setup();
        clock.0.store(359700, Ordering::Relaxed);
        let (entered, mut ready) = tokio::sync::mpsc::unbounded_channel();
        let mut releases = Vec::new();
        let mut blockers = Vec::new();
        for _ in 0..2 {
            let (release, rx) = std::sync::mpsc::channel();
            releases.push(release);
            let e = e.clone();
            let entered = entered.clone();
            blockers.push(tokio::spawn(async move {
                e.builds
                    .run(false, move || {
                        entered.send(()).unwrap();
                        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                    })
                    .await
                    .unwrap();
            }));
        }
        ready.recv().await.unwrap();
        ready.recv().await.unwrap();
        let request = {
            let e = e.clone();
            tokio::spawn(async move { e.bulk_with_evidence(query(&["BTCUSDT"], 1)).await })
        };
        for _ in 0..1000 {
            if e.inflight_holders() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(e.inflight_holders(), 1);
        e.commit_evidence(
            0,
            kline_core::evidence::EvidenceUpdate::Rest(update(300000, "1", Source::Rest).bar),
        )
        .unwrap();
        clock.0.store(360100, Ordering::Relaxed);
        for release in releases {
            release.send(()).unwrap();
        }
        let reply = request.await.unwrap().unwrap();
        assert_eq!(reply.boundary, 360000);
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body["window"]["last_open"], 300000);
        for blocker in blockers {
            blocker.await.unwrap();
        }
    }
    #[tokio::test]
    async fn fix1_actual_row_budget_allows_747_trading_plus_four_empty_retained_and_unknowns() {
        let (source, clock, _, _) = setup();
        let boundary = 500 * 60_000;
        clock.0.store(boundary + 10000, Ordering::Relaxed);
        let template = source.catalog.instruments().remove(0);
        let definitions = (0..751)
            .map(|i| {
                let mut d = template.clone();
                d.symbol = format!("COIN{i}");
                d.continuous = Some((d.symbol.clone(), "PERPETUAL".into()));
                d.trading = i < 747;
                d
            })
            .collect();
        let e = Engine::new(
            Catalog::new(definitions).unwrap(),
            clock,
            source.settings().clone(),
        );
        e.restore_retained(
            e.catalog
                .instruments()
                .into_iter()
                .filter(|i| !i.trading)
                .map(|i| RetainedRecord {
                    pair: i.symbol.clone(),
                    symbol: i.symbol,
                    contract_type: "PERPETUAL".into(),
                    since: boundary,
                    expires_ms: boundary + 241 * 60000,
                    delivery_ms: None,
                })
                .collect(),
        );
        for (id, _) in e
            .catalog
            .slots()
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.is_trading())
        {
            for open in (boundary - 100 * 60000..boundary).step_by(60000) {
                e.commit_evidence(
                    id,
                    kline_core::evidence::EvidenceUpdate::Rest(update(open, "1", Source::Rest).bar),
                )
                .unwrap();
            }
        }
        let reply = e.bulk_with_evidence(query(&[], 100)).await.unwrap();
        let body: EvidenceResponse = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body.status.len(), 751);
        assert_eq!(body.klines.values().map(Vec::len).sum::<usize>(), 74700);
        let mut all: Vec<_> = e
            .catalog
            .instruments()
            .into_iter()
            .map(|i| i.symbol)
            .collect();
        all.push("UNKNOWN".into());
        let request = crate::BulkQuery {
            symbols: all,
            ..query(&[], 100)
        };
        assert!(e.bulk_with_evidence(request).await.is_ok());
        for (id, _) in e
            .catalog
            .slots()
            .iter()
            .enumerate()
            .filter(|(_, slot)| !slot.is_trading())
        {
            for open in (boundary - 100 * 60000..boundary).step_by(60000) {
                e.commit_evidence(
                    id,
                    kline_core::evidence::EvidenceUpdate::Rest(update(open, "1", Source::Rest).bar),
                )
                .unwrap();
            }
        }
        assert!(matches!(
            e.bulk_with_evidence(query(&[], 100)).await,
            Err(ServiceError::Evidence)
        ));
    }
    #[test]
    fn fix1_same_bytes_replacing_a_synthetic_flag_invalidates_the_window() {
        let (e, _, key, symbols) = setup();
        e.commit(0, update(180000, "1", Source::Synthetic)).unwrap();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let before = e.catalog.slot(0).window_generation();
        e.commit(0, update(180000, "1", Source::Rest)).unwrap();
        assert!(!e.catalog.slot(0).evidence_record(180000).synthetic);
        assert!(e.catalog.slot(0).window_generation() > before);
        assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
    }
    #[test]
    fn fix2_page_and_revocation_are_mutually_exclusive_after_the_first_row() {
        let (e, _, _, _) = setup();
        let mut first = update(180000, "1", Source::Stream);
        first.closed = true;
        e.commit(0, first).unwrap();
        let updates = vec![
            kline_core::evidence::EvidenceUpdate::Rest(update(180000, "2", Source::Rest).bar),
            kline_core::evidence::EvidenceUpdate::Rest(update(240000, "2", Source::Rest).bar),
        ];
        let (tx, rx) = std::sync::mpsc::channel();
        let mut withdrawal = None;
        let result = e.commit_evidence_page_with(0, 0, updates.clone(), |i| {
            if i == 0 {
                let e = e.clone();
                let tx = tx.clone();
                withdrawal = Some(std::thread::spawn(move || {
                    tx.send(e.catalog.slot(0).evidence_lifecycle.try_lock().is_none())
                        .unwrap();
                    e.refresh_catalog(vec![]).unwrap();
                }));
                assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap());
            }
        });
        assert!(result.unwrap());
        withdrawal.unwrap().join().unwrap();
        assert_eq!(e.breaker_state().revised_total, 2);
        assert!(!e.catalog.slot(0).is_tracked());
        assert!(e.catalog.slot(0).records().is_empty());
        let before = (
            e.breaker_snapshot(),
            e.catalog.slot(0).window_generation(),
            e.catalog.slot(0).durable_generation(),
            e.cache_sizes(),
        );
        assert!(!e.commit_evidence_page(0, 0, updates).unwrap());
        assert_eq!(
            (
                e.breaker_snapshot(),
                e.catalog.slot(0).window_generation(),
                e.catalog.slot(0).durable_generation(),
                e.cache_sizes()
            ),
            before
        );
    }
    #[test]
    fn fix2_recheck_ignores_writer_lock_and_resamples_boundary_and_retention_time() {
        for retained in [false, true] {
            let (e, clock, key, symbols) = setup();
            if retained {
                let mut defs = e.catalog.instruments();
                defs[0].trading = false;
                e.refresh_catalog_with_retained(
                    defs,
                    vec![RetainedRecord {
                        symbol: "BTCUSDT".into(),
                        pair: "BTCUSDT".into(),
                        contract_type: "PERPETUAL".into(),
                        since: 300000,
                        expires_ms: 300600,
                        delivery_ms: None,
                    }],
                )
                .unwrap();
            }
            let reply = Arc::new(e.build_evidence_body(&key, &symbols, 0).unwrap());
            let writer = e.evidence.catalog_writer.lock();
            clock
                .0
                .store(if retained { 300600 } else { 360001 }, Ordering::Relaxed);
            let (tx, rx) = std::sync::mpsc::channel();
            let check = e.clone();
            let thread = std::thread::spawn(move || {
                tx.send(check.reply_current(&key, &reply, 300500)).unwrap();
            });
            let checked = rx.recv_timeout(std::time::Duration::from_secs(1));
            drop(writer);
            thread.join().unwrap();
            assert!(!checked.expect("recheck waited for catalog_writer"));
        }
    }
    #[tokio::test]
    async fn fix2_same_id_relisting_or_recreation_between_wait_and_build_restarts_wait() {
        for recreate in [false, true] {
            let (e, clock, _, _) = setup();
            clock.0.store(360500, Ordering::Relaxed);
            let mut defs = e.catalog.instruments();
            defs[0].trading = false;
            e.refresh_catalog_with_retained(
                defs.clone(),
                vec![RetainedRecord {
                    symbol: "BTCUSDT".into(),
                    pair: "BTCUSDT".into(),
                    contract_type: "PERPETUAL".into(),
                    since: 300000,
                    expires_ms: 600000,
                    delivery_ms: None,
                }],
            )
            .unwrap();
            let (entered, mut ready) = tokio::sync::mpsc::unbounded_channel();
            let mut releases = Vec::new();
            let mut blockers = Vec::new();
            for _ in 0..2 {
                let (tx, rx) = std::sync::mpsc::channel();
                releases.push(tx);
                let e = e.clone();
                let entered = entered.clone();
                blockers.push(tokio::spawn(async move {
                    e.builds
                        .run(false, move || {
                            entered.send(()).unwrap();
                            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                        })
                        .await
                        .unwrap();
                }));
            }
            ready.recv().await.unwrap();
            ready.recv().await.unwrap();
            let mut request = {
                let e = e.clone();
                tokio::spawn(async move { e.bulk_with_evidence(query(&["BTCUSDT"], 1)).await })
            };
            for _ in 0..1000 {
                if e.inflight_holders() > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(e.inflight_holders(), 1);
            if recreate {
                e.refresh_catalog(vec![]).unwrap();
            }
            defs[0].trading = true;
            e.refresh_catalog_with_retained(defs, vec![]).unwrap();
            for tx in releases {
                tx.send(()).unwrap();
            }
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), &mut request)
                    .await
                    .is_err(),
                "old non-trading wait escaped"
            );
            e.commit_evidence(
                0,
                kline_core::evidence::EvidenceUpdate::Rest(update(300000, "1", Source::Rest).bar),
            )
            .unwrap();
            let reply = tokio::time::timeout(std::time::Duration::from_secs(1), request)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let packet: EvidenceResponse = serde_json::from_slice(&reply.body).unwrap();
            assert_eq!(packet.status["BTCUSDT"], SymbolStatus::Trading);
            assert_eq!(packet.evidence["BTCUSDT"], vec![RowEvidence::Final]);
            for blocker in blockers {
                blocker.await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn fix2_all_market_build_keeps_cache_hits_and_ws_ingest_unblocked() {
        let (source, clock, _, _) = setup();
        let template = source.catalog.instruments().remove(0);
        let mut definitions = vec![template.clone()];
        for i in 0..750 {
            let mut d = template.clone();
            d.symbol = format!("ZZ{i}");
            d.continuous = Some((d.symbol.clone(), "PERPETUAL".into()));
            definitions.push(d);
        }
        let e = Engine::new(
            Catalog::new(definitions).unwrap(),
            clock,
            source.settings().clone(),
        );
        let mut closed = update(240000, "1", Source::Stream);
        closed.closed = true;
        e.commit(0, closed).unwrap();
        let cached = e.bulk_with_evidence(query(&["BTCUSDT"], 1)).await.unwrap();
        let selection = e
            .select_evidence(
                &query(&[], 100),
                Interval::parse("1m").unwrap(),
                e.bulk_now_ms(),
            )
            .unwrap();
        assert_eq!(selection.symbols.len(), 751);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let builder = {
            let e = e.clone();
            std::thread::spawn(move || {
                e.build_evidence_selected_snapshot(&selection, 0, || {
                    // A full-market build no longer acquires catalog_writer at all.
                    assert!(e.evidence.catalog_writer.try_lock().is_some());
                    tx.send(()).unwrap();
                    resume
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                })
            })
        };
        rx.await.unwrap();
        let hit = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            e.bulk_with_evidence(query(&["BTCUSDT"], 1)),
        )
        .await;
        let frame = serde_json::json!({"e":"continuous_kline","E":300500,"ps":"BTCUSDT","ct":"PERPETUAL","k":{"t":300000,"T":359999,"i":"1m","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","n":1,"V":"1","Q":"1","x":false}});
        let ingested = e
            .ingest(Market::Future, &serde_json::to_vec(&frame).unwrap())
            .unwrap();
        release.send(()).unwrap();
        assert!(builder.join().unwrap().is_ok());
        assert!(Arc::ptr_eq(
            &cached,
            &hit.expect("cache hit blocked behind full market build")
                .unwrap()
        ));
        assert!(ingested.is_some());
    }
    #[test]
    fn fix2_symbol_cap_is_normalized_and_rejects_before_publication() {
        let (e, _, _, _) = setup();
        let mut request = query(&[], 100);
        request.symbols = (0..=crate::catalog::MAX_SERIES)
            .map(|i| format!("UNKNOWN{i}"))
            .collect();
        let before = (
            e.cache_sizes(),
            e.breaker_snapshot(),
            e.metrics.responses_built.load(Ordering::Relaxed),
        );
        assert!(is_symbol_limit(
            &e.select_evidence(&request, Interval::parse("1m").unwrap(), e.bulk_now_ms())
                .unwrap_err()
        ));
        assert_eq!(
            (
                e.cache_sizes(),
                e.breaker_snapshot(),
                e.metrics.responses_built.load(Ordering::Relaxed)
            ),
            before
        );
        request.symbols.pop();
        assert!(
            e.select_evidence(&request, Interval::parse("1m").unwrap(), e.bulk_now_ms())
                .is_ok()
        );
        request.symbols = vec![" BTCUSDT ".into(); crate::catalog::MAX_SERIES + 1];
        assert_eq!(
            e.select_evidence(&request, Interval::parse("1m").unwrap(), e.bulk_now_ms())
                .unwrap()
                .symbols
                .len(),
            1
        );
    }
    #[test]
    fn fix3_forming_writer_holds_all_locks_but_atomic_recheck_still_passes() {
        let (e, _, key, symbols) = setup();
        e.commit(0, update(300000, "1", Source::Stream)).unwrap();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let slot = e.catalog.slot(0);
        let before = slot.closed_version.load(Ordering::Acquire);
        e.commit(0, update(300000, "2", Source::Stream)).unwrap();
        assert_eq!(slot.closed_version.load(Ordering::Acquire), before);
        let writer = e.evidence.catalog_writer.lock();
        let lifecycle = slot.evidence_lifecycle.lock();
        let retained = e.evidence.retained.write();
        let breaker = e.evidence.state.lock();
        let series = slot.state.write();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let e = e.clone();
            let key = key.clone();
            std::thread::spawn(move || {
                tx.send(e.reply_current(&key, &reply, e.bulk_now_ms()))
                    .unwrap()
            })
        };
        let result = rx.recv_timeout(std::time::Duration::from_secs(1));
        drop(series);
        drop(breaker);
        drop(retained);
        drop(lifecycle);
        drop(writer);
        worker.join().unwrap();
        assert!(result.expect("recheck acquired a lock"));
    }
    #[test]
    fn fix3_recheck_source_contains_only_atomic_reads_and_clock_reads() {
        let source = include_str!("evidence_bulk.rs");
        let body = source
            .split("pub(crate) fn evidence_reply_current(")
            .nth(1)
            .unwrap()
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        for forbidden in [
            ".lock(",
            ".read(",
            ".write(",
            "try_lock",
            "try_read",
            "try_write",
            "self.catalog.slot(",
            "self.catalog.generation(",
            "evidence_selection_current",
            "breaker_snapshot",
            "breaker_state",
            "ws_final_trusted()",
        ] {
            assert!(
                !body.contains(forbidden),
                "recheck must remain purely atomic: {forbidden}"
            );
        }
    }
    #[test]
    fn fix3_closed_state_values_coverage_and_old_history_invalidate_versions() {
        use kline_core::evidence::EvidenceUpdate;
        for open in (0..300000).step_by(60000) {
            let (e, _, key, symbols) = setup();
            e.commit(0, update(open, "1", Source::Rest)).unwrap();
            let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
            let slot = e.catalog.slot(0);
            let before = slot.closed_version.load(Ordering::Acquire);
            e.commit_evidence(0, EvidenceUpdate::Rest(update(open, "2", Source::Rest).bar))
                .unwrap();
            assert!(
                slot.closed_version.load(Ordering::Acquire) > before,
                "closed row {open}"
            );
            assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        }
        for operation in ["P-bytes", "A", "W-to-P", "coverage", "forming-trim"] {
            let (e, _, key, symbols) = setup();
            e.commit(0, update(180000, "1", Source::Rest)).unwrap();
            let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
            let slot = e.catalog.slot(0);
            let before = slot.closed_version.load(Ordering::Acquire);
            match operation {
                "P-bytes" => {
                    e.commit(0, update(180000, "2", Source::Rest)).unwrap();
                }
                "A" => {
                    e.commit_evidence(0, EvidenceUpdate::Absent(180000))
                        .unwrap();
                }
                "W-to-P" => {
                    let mut u = update(240000, "2", Source::Stream);
                    u.closed = true;
                    e.commit(0, u).unwrap();
                }
                "coverage" => {
                    e.commit(0, update(120000, "1", Source::Rest)).unwrap();
                }
                "forming-trim" => {
                    let mut u = update(300000 + 241 * 60000, "1", Source::Stream);
                    u.closed = false;
                    e.commit(0, u).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                slot.closed_version.load(Ordering::Acquire) > before,
                "{operation}"
            );
            assert!(
                !e.reply_current(&key, &reply, e.bulk_now_ms()),
                "{operation}"
            );
        }
        let (e, _, mut key, symbols) = setup();
        key.limit = 1;
        e.commit(0, update(0, "1", Source::Rest)).unwrap();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        e.commit(0, update(0, "2", Source::Rest)).unwrap();
        assert!(
            !e.reply_current(&key, &reply, e.bulk_now_ms()),
            "old history invalidation is permitted"
        );
    }
    #[test]
    fn fix3_resize_and_clear_publish_closed_epoch_versions_under_series_lock() {
        for clear in [false, true] {
            let (e, _, key, symbols) = setup();
            let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
            let slot = e.catalog.slot(0);
            let before = slot.closed_version.load(Ordering::Acquire);
            let mut definitions = e.catalog.instruments();
            if clear {
                definitions.clear();
            } else {
                definitions[0].capacity = NonZeroUsize::new(240).unwrap();
            }
            e.refresh_catalog(definitions).unwrap();
            assert!(
                slot.closed_version.load(Ordering::Acquire) > before,
                "clear={clear}"
            );
            assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        }
    }
    #[test]
    fn fix3_builder_rechecks_atomic_versions_after_reading_rows() {
        let (e, _, key, symbols) = setup();
        let result = e.build_evidence_snapshot(&key, &symbols, 0, || {
            e.commit_evidence(
                0,
                kline_core::evidence::EvidenceUpdate::Rest(update(240000, "1", Source::Rest).bar),
            )
            .unwrap();
        });
        assert!(is_stale_evidence(&result.unwrap_err()));
    }
    #[test]
    fn fix3_directory_trust_breaker_and_unknown_identity_changes_invalidate() {
        for change in ["directory", "trust", "breaker"] {
            let (e, _, key, symbols) = setup();
            let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
            match change {
                "directory" => {
                    let mut defs = e.catalog.instruments();
                    defs[0].trading = false;
                    e.refresh_catalog(defs).unwrap();
                }
                "trust" => e.restore_breaker(BreakerState {
                    last_conflict_ms: 300500,
                    revised_total: 0,
                }),
                "breaker" => e.restore_breaker(BreakerState {
                    last_conflict_ms: 0,
                    revised_total: 1,
                }),
                _ => unreachable!(),
            }
            assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()), "{change}");
        }
        let (e, _, key, symbols) = setup();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let mut definitions = e.catalog.instruments();
        definitions[0].trading = false;
        e.catalog.sync(definitions).unwrap();
        assert!(
            !e.reply_current(&key, &reply, e.bulk_now_ms()),
            "unpublished direct Catalog::sync"
        );
        let (e, _, key, _) = setup();
        let selection = e
            .select_evidence(&query(&["NEWUSDT"], 1), key.interval, e.bulk_now_ms())
            .unwrap();
        let reply = e.build_evidence_selected(&selection, 0).unwrap();
        let mut defs = e.catalog.instruments();
        let mut new = defs[0].clone();
        new.symbol = "NEWUSDT".into();
        new.continuous = None;
        defs.push(new);
        e.refresh_catalog(defs).unwrap();
        assert!(!e.reply_current(&selection.key, &reply, e.bulk_now_ms()));
    }
    #[test]
    fn fix3_unchanged_refresh_preserves_directory_epoch_and_published_view() {
        let (e, _, key, symbols) = setup();
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let epoch = e.evidence.directory_epoch.load(Ordering::Acquire);
        let view = e.evidence.directory.load_full();
        let records = e.retained_records();
        for _ in 0..4 {
            let mut definitions = e.catalog.instruments();
            definitions.reverse();
            assert!(
                !e.refresh_catalog_with_retained(definitions, records.clone())
                    .unwrap()
                    .changed
            );
            assert_eq!(e.evidence.directory_epoch.load(Ordering::Acquire), epoch);
            assert!(Arc::ptr_eq(&view, &e.evidence.directory.load_full()));
            assert!(e.reply_current(&key, &reply, e.bulk_now_ms()));
        }
    }
    #[test]
    fn fix3_actual_withdrawal_lock_contends_after_first_page_row() {
        let (e, _, _, _) = setup();
        let mut first = update(180000, "1", Source::Stream);
        first.closed = true;
        e.commit(0, first).unwrap();
        let updates = vec![
            kline_core::evidence::EvidenceUpdate::Rest(update(180000, "2", Source::Rest).bar),
            kline_core::evidence::EvidenceUpdate::Rest(update(240000, "2", Source::Rest).bar),
        ];
        let (tx, rx) = std::sync::mpsc::channel();
        *e.catalog.withdrawal_lock_probe.lock() = Some(Arc::new(move |contended| {
            let _ = tx.send(contended);
        }));
        let mut withdrawal = None;
        let result = e.commit_evidence_page_with(0, 0, updates, |index| {
            if index == 0 {
                withdrawal = Some({
                    let e = e.clone();
                    std::thread::spawn(move || e.refresh_catalog(vec![]).unwrap())
                });
                let contended = rx.recv_timeout(std::time::Duration::from_secs(1));
                assert!(
                    contended.unwrap(),
                    "actual withdrawal must compete for the page lock"
                );
                assert!(e.catalog.slot(0).is_tracked());
            }
        });
        assert!(result.unwrap());
        withdrawal.unwrap().join().unwrap();
        assert_eq!(e.breaker_state().revised_total, 2);
        assert!(!e.catalog.slot(0).is_tracked());
    }
    #[test]
    fn fix3_late_ws_challenges_publish_breaker_version_without_closed_row_changes() {
        use kline_core::evidence::EvidenceUpdate;
        for absent in [false, true] {
            let (e, _, key, symbols) = setup();
            e.commit_evidence(
                0,
                if absent {
                    EvidenceUpdate::Absent(240000)
                } else {
                    EvidenceUpdate::Rest(update(240000, "1", Source::Rest).bar)
                },
            )
            .unwrap();
            let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
            let slot = e.catalog.slot(0);
            let closed = slot.closed_version.load(Ordering::Acquire);
            let generation = e.evidence.breaker_generation.load(Ordering::Acquire);
            let mut late = update(240000, "2", Source::Stream);
            late.closed = true;
            e.commit(0, late).unwrap();
            assert_eq!(slot.closed_version.load(Ordering::Acquire), closed);
            assert_eq!(
                e.evidence.breaker_generation.load(Ordering::Acquire),
                generation + 1
            );
            assert_eq!(
                e.evidence.breaker_generation.load(Ordering::Acquire),
                e.breaker_snapshot().0
            );
            assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        }
    }
    #[test]
    fn fix3_closed_version_uses_the_same_host_clock_floor_as_response_windows() {
        struct SkewClock;
        impl Clock for SkewClock {
            fn now_ms(&self) -> i64 {
                299500
            }
            fn host_ms(&self) -> i64 {
                300500
            }
        }
        let (source, _, _, _) = setup();
        let e = Engine::new(
            Catalog::new(source.catalog.instruments()).unwrap(),
            Arc::new(SkewClock),
            source.settings().clone(),
        );
        e.commit(0, update(240000, "1", Source::Stream)).unwrap();
        let selection = e
            .select_evidence(
                &query(&["BTCUSDT"], 1),
                Interval::parse("1m").unwrap(),
                e.bulk_now_ms(),
            )
            .unwrap();
        assert_eq!(selection.key.boundary, 300000);
        let reply = e.build_evidence_selected(&selection, 0).unwrap();
        let before = e.catalog.slot(0).closed_version.load(Ordering::Acquire);
        e.commit(0, update(240000, "2", Source::Stream)).unwrap();
        assert!(e.catalog.slot(0).closed_version.load(Ordering::Acquire) > before);
        assert!(!e.reply_current(&selection.key, &reply, e.bulk_now_ms()));
    }
    fn fix4_pending_setup(now: i64) -> (Arc<Engine>, Arc<TestClock>, Key, Arc<[String]>) {
        let (source, clock, key, symbols) = setup();
        clock.0.store(now, Ordering::Release);
        let e = Engine::new(
            Catalog::new(source.catalog.instruments()).unwrap(),
            clock.clone(),
            source.settings().clone(),
        );
        e.commit(0, update(240000, "1", Source::Stream)).unwrap();
        (e, clock, key, symbols)
    }
    #[test]
    fn fix4_pending_rewrite_after_clock_rollback_invalidates_exposed_window() {
        let (e, clock, key, symbols) = fix4_pending_setup(300005);
        let reply = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body["klines"]["BTCUSDT"][0][1], "1");
        assert_eq!(body["evidence"]["BTCUSDT"][0], "pending");
        let slot = e.catalog.slot(0);
        let before = slot.closed_version.load(Ordering::Acquire);
        // Cross backwards into the row's own minute, outside the pre-wait margin.
        clock.0.store(299700, Ordering::Release);
        e.commit(0, update(240000, "2", Source::Stream)).unwrap();
        assert!(slot.closed_version.load(Ordering::Acquire) > before);
        clock.0.store(300006, Ordering::Release);
        assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        let fresh = e.build_evidence_body(&key, &symbols, 0).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&fresh.body).unwrap();
        assert_eq!(body["klines"]["BTCUSDT"][0][1], "2");
        assert_eq!(body["evidence"]["BTCUSDT"][0], "pending");
    }
    #[test]
    fn fix4_selection_and_build_publish_monotonic_not_before_boundaries() {
        for selecting in [false, true] {
            let (e, clock, mut key, symbols) = fix4_pending_setup(299000);
            assert_eq!(e.evidence.exposed_boundary.load(Ordering::Acquire), 0);
            key.boundary = 360000;
            let reply = if selecting {
                let selection = e
                    .select_evidence(&query(&["BTCUSDT"], 5), key.interval, 360000)
                    .unwrap();
                assert_eq!(selection.key.boundary, 360000);
                assert_eq!(e.evidence.exposed_boundary.load(Ordering::Acquire), 360000);
                e.build_evidence_selected(&selection, 0).unwrap()
            } else {
                e.build_evidence_body(&key, &symbols, 0).unwrap()
            };
            assert_eq!(e.evidence.exposed_boundary.load(Ordering::Acquire), 360000);
            let earlier = e
                .select_evidence(&query(&["BTCUSDT"], 5), key.interval, 300000)
                .unwrap();
            assert_eq!(earlier.key.boundary, 300000);
            e.build_evidence_selected(&earlier, 0).unwrap();
            assert_eq!(e.evidence.exposed_boundary.load(Ordering::Acquire), 360000);
            clock.0.store(239700, Ordering::Release);
            let before = e.catalog.slot(0).closed_version.load(Ordering::Acquire);
            e.commit(0, update(240000, "2", Source::Stream)).unwrap();
            assert!(e.catalog.slot(0).closed_version.load(Ordering::Acquire) > before);
            clock.0.store(360006, Ordering::Release);
            assert!(!e.reply_current(&key, &reply, e.bulk_now_ms()));
        }
    }
    #[test]
    fn fix4_pre_boundary_margin_classifies_pending_updates_before_any_exposure() {
        let (e, _, _, _) = fix4_pending_setup(299800);
        assert_eq!(e.evidence.exposed_boundary.load(Ordering::Acquire), 0);
        let before = e.catalog.slot(0).closed_version.load(Ordering::Acquire);
        e.commit(0, update(240000, "2", Source::Stream)).unwrap();
        assert!(e.catalog.slot(0).closed_version.load(Ordering::Acquire) > before);
    }
    #[test]
    fn fix4_unknown_symbols_share_membership_version_without_registering_names() {
        let (e, _, key, _) = setup();
        let mut request = query(&[], 1);
        request.symbols = (0..crate::catalog::MAX_SERIES)
            .map(|i| format!("UNKNOWN-{i}"))
            .collect();
        let many = e
            .select_evidence(&request, key.interval, e.bulk_now_ms())
            .unwrap();
        assert_eq!(many.identities.len(), crate::catalog::MAX_SERIES);
        assert!(many.identities.iter().all(Option::is_none));
        assert_eq!(many.directory_versions.len(), 1);
        assert!(Arc::ptr_eq(
            &many.directory_versions[0].0,
            &e.evidence.membership_version
        ));
        assert!(e.evidence.catalog_writer.lock().is_empty());
        let other = e
            .select_evidence(
                &query(&["ANOTHER-UNKNOWN"], 1),
                key.interval,
                e.bulk_now_ms(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(
            &many.directory_versions[0].0,
            &other.directory_versions[0].0
        ));
        assert!(e.evidence.catalog_writer.lock().is_empty());
    }
    #[test]
    fn fix4_dead_symbol_versions_are_cleaned_only_when_directory_is_published() {
        let (e, _, key, _) = setup();
        let known = e
            .select_evidence(&query(&["BTCUSDT"], 1), key.interval, e.bulk_now_ms())
            .unwrap();
        assert_eq!(e.evidence.catalog_writer.lock().len(), 1);
        drop(known);
        let unknown = e
            .select_evidence(&query(&["UNKNOWN"], 1), key.interval, e.bulk_now_ms())
            .unwrap();
        assert_eq!(
            e.evidence.catalog_writer.lock().len(),
            1,
            "selection must not scan or collect dead versions"
        );
        assert_eq!(
            e.evidence
                .catalog_writer
                .lock()
                .values()
                .next()
                .unwrap()
                .strong_count(),
            0
        );
        let reply = e.build_evidence_selected(&unknown, 0).unwrap();
        assert_eq!(e.evidence.catalog_writer.lock().len(), 1);
        let membership = e.evidence.membership_version.load(Ordering::Acquire);
        let mut definitions = e.catalog.instruments();
        definitions[0].trading = false;
        e.refresh_catalog(definitions.clone()).unwrap();
        assert!(e.evidence.catalog_writer.lock().is_empty());
        assert_eq!(
            e.evidence.membership_version.load(Ordering::Acquire),
            membership
        );
        assert!(e.reply_current(&unknown.key, &reply, e.bulk_now_ms()));
        let mut new = definitions[0].clone();
        new.symbol = "NEWUSDT".into();
        new.continuous = None;
        definitions.push(new);
        e.refresh_catalog(definitions.clone()).unwrap();
        assert!(e.evidence.membership_version.load(Ordering::Acquire) > membership);
        assert!(!e.reply_current(&unknown.key, &reply, e.bulk_now_ms()));
        let unknown = e
            .select_evidence(&query(&["UNKNOWN"], 1), key.interval, e.bulk_now_ms())
            .unwrap();
        let reply = e.build_evidence_selected(&unknown, 0).unwrap();
        definitions.pop();
        e.refresh_catalog(definitions).unwrap();
        assert!(
            !e.reply_current(&unknown.key, &reply, e.bulk_now_ms()),
            "removal is also a membership change"
        );
    }
    #[test]
    fn fix4_symbol_version_table_has_a_hard_directory_capacity_bound() {
        let (e, _, key, _) = setup();
        {
            let mut versions = e.evidence.catalog_writer.lock();
            for i in 0..crate::catalog::MAX_SERIES {
                versions.insert(
                    (key.market, key.interval, format!("expired-fixture-{i}")),
                    std::sync::Weak::new(),
                );
            }
        }
        let before = (
            e.cache_sizes(),
            e.metrics.responses_built.load(Ordering::Acquire),
        );
        let error = e
            .select_evidence(&query(&["BTCUSDT"], 1), key.interval, e.bulk_now_ms())
            .unwrap_err();
        assert!(is_stale_evidence(&error));
        assert_eq!(
            e.evidence.catalog_writer.lock().len(),
            crate::catalog::MAX_SERIES
        );
        assert_eq!(
            (
                e.cache_sizes(),
                e.metrics.responses_built.load(Ordering::Acquire)
            ),
            before
        );
        let mut definitions = e.catalog.instruments();
        definitions[0].trading = false;
        e.refresh_catalog(definitions).unwrap();
        assert!(e.evidence.catalog_writer.lock().is_empty());
        assert!(
            e.select_evidence(&query(&["BTCUSDT"], 1), key.interval, e.bulk_now_ms())
                .is_ok()
        );
        assert_eq!(e.evidence.catalog_writer.lock().len(), 1);
    }
}
