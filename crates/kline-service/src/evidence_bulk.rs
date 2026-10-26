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
#[derive(Clone, Debug)]
pub(crate) struct EvidenceSelection {
    pub key: Key,
    pub symbols: std::sync::Arc<[String]>,
    pub identities: Vec<Option<crate::evidence::EvidenceIdentity>>,
}
#[derive(Clone, Debug)]
pub(crate) struct EvidenceStamp {
    pub trusted: bool,
    pub breaker_generation: u64,
    pub generations: Vec<(Option<i64>, u64, u64)>,
    pub selection: EvidenceSelection,
    pub observed_at: i64,
    pub valid_until: i64,
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
        (
            Key {
                market: query.market,
                interval,
                limit: query.limit.unwrap_or(5).clamp(1, 100) as usize,
                closed_only: true,
                ids,
                boundary: interval.boundary(self.bulk_now_ms().max(now)),
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
        let selection = EvidenceSelection {
            key,
            symbols,
            identities,
        };
        if !self.evidence_selection_current(&selection) {
            return Err(stale_evidence());
        }
        Ok(selection)
    }
    pub(crate) fn evidence_selection_current(&self, selection: &EvidenceSelection) -> bool {
        let epoch = self.evidence.directory_epoch.load(Ordering::Acquire);
        if !epoch.is_multiple_of(2) {
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
        let _writer = self.evidence.catalog_writer.lock();
        let _write = self.evidence.begin_directory_write();
        self.install_retained(records);
        self.evidence.publish_directory(&self.catalog);
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
        // This path never waits for a directory writer, series writer or breaker writer.
        let _ = now;
        let Some(state) = self.evidence.state.try_lock() else {
            return false;
        };
        let generation = state.generation;
        let breaker = state.breaker;
        drop(state);
        if generation != stamp.breaker_generation
            || !self.evidence_selection_current(&stamp.selection)
        {
            return false;
        }
        if key.ids.len() != stamp.generations.len() {
            return false;
        }
        for (&id, &expected) in key.ids.iter().zip(&stamp.generations) {
            let slot = self.catalog.slot(id);
            let Some(series) = slot.state.try_read() else {
                return false;
            };
            if series.evidence_window_stamp(
                key.boundary - key.limit as i64 * 60_000,
                key.boundary - 60_000,
            ) != expected
            {
                return false;
            }
        }
        if !self.evidence_selection_current(&stamp.selection) {
            return false;
        }
        let Some(state) = self.evidence.state.try_lock() else {
            return false;
        };
        if state.generation != generation {
            return false;
        }
        drop(state);
        // Re-sample after every potentially contended read and all comparisons.
        let now = self.bulk_now_ms().max(key.boundary);
        now >= stamp.observed_at
            && now < stamp.valid_until
            && breaker.ws_final_trusted(
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
        let (breaker_generation, breaker) = self.breaker_snapshot();
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
        let mut generations = Vec::new();
        for &id in key.ids.iter() {
            generations.push(self.catalog.slot(id).select_bars(|series| {
                series.evidence_window_stamp(response.window.first_open, response.window.last_open)
            }));
        }
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
        if self.bulk_now_ms().max(key.boundary) >= stamp.valid_until {
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
}
