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
pub(crate) struct EvidenceStamp {
    pub trusted: bool,
    pub breaker_generation: u64,
    pub retained_generation: u64,
    pub catalog_generation: u64,
    pub generations: Vec<u64>,
    pub observed_at: i64,
    pub valid_until: i64,
}
impl Engine {
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
        self.install_retained(records);
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
        let (generation, breaker) = self.breaker_snapshot();
        let trusted = breaker.ws_final_trusted(
            self.now_ms(),
            self.settings.evidence.as_ref().unwrap().ws_trust_window_ms,
        );
        now >= stamp.observed_at
            && now < stamp.valid_until
            && trusted == stamp.trusted
            && generation == stamp.breaker_generation
            && self.evidence.retained_generation.load(Ordering::Acquire)
                == stamp.retained_generation
            && self.catalog.generation() == stamp.catalog_generation
            && key.revision == stamp.catalog_generation
            && key.ids.len() == stamp.generations.len()
            && key
                .ids
                .iter()
                .zip(&stamp.generations)
                .all(|(&id, &g)| self.catalog.slot(id).window_generation() == g)
    }
    pub(crate) fn build_evidence_body(
        &self,
        key: &Key,
        symbols: &[String],
        waited_ms: u64,
    ) -> Result<BulkReply, ServiceError> {
        let _catalog = self.evidence.catalog_writer.lock();
        let now = self.bulk_now_ms().max(key.boundary);
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
        let retained = self.evidence.retained.read();
        let mut finalized = true;
        let mut generations = Vec::new();
        for &id in key.ids.iter() {
            generations.push(self.catalog.slot(id).window_generation());
        }
        for symbol in symbols {
            let mut rows = Vec::new();
            let mut kinds = Vec::new();
            let mut absent = Vec::new();
            let mut missing = Vec::new();
            let mut synthetic = Vec::new();
            let mut covered_from = None;
            let status = if let Some(id) = self
                .catalog
                .find(key.market, key.interval, symbol)
                .filter(|&id| {
                    self.catalog.slot(id).is_tracked()
                        && (self.catalog.slot(id).is_trading()
                            || retained.get(symbol).is_none_or(|r| r.expires_ms > now))
                }) {
                let slot = self.catalog.slot(id);
                let status = if slot.is_trading() {
                    SymbolStatus::Trading
                } else {
                    let record = retained.get(symbol);
                    SymbolStatus::NotTrading {
                        since: record
                            .map_or_else(|| slot.not_trading_since().unwrap_or(now), |r| r.since),
                        delivery_ms: record.and_then(|r| r.delivery_ms),
                    }
                };
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
                    if slot.is_trading() && series.needs_verdict(response.window.last_open, trusted)
                    {
                        finalized = false;
                    }
                    Ok(())
                })?;
                status
            } else {
                SymbolStatus::Unknown
            };
            response.status.insert(symbol.clone(), status);
            response.klines.insert(symbol.clone(), rows);
            response.evidence.insert(symbol.clone(), kinds);
            response.absent.insert(symbol.clone(), absent);
            response.missing.insert(symbol.clone(), missing);
            response.synthetic.insert(symbol.clone(), synthetic);
            response.covered_from.insert(symbol.clone(), covered_from);
        }
        response.validate().map_err(ServiceError::Worker)?;
        let body: bytes::Bytes = serde_json::to_vec(&response)?.into();
        let stamp = EvidenceStamp {
            trusted,
            breaker_generation,
            retained_generation: self.evidence.retained_generation.load(Ordering::Acquire),
            catalog_generation: key.revision,
            generations: generations.clone(),
            observed_at: now,
            valid_until: retained
                .values()
                .filter(|r| r.expires_ms > now && symbols.contains(&r.symbol))
                .map(|r| r.expires_ms)
                .min()
                .unwrap_or(i64::MAX)
                .min(key.boundary + 60_000),
        };
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
}
