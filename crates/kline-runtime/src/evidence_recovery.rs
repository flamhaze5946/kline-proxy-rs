//! Evidence-only REST request planning and publication. Legacy Recovery stays intact.
use crate::{recovery::Recovery, rest::Priority};
use anyhow::Result;
use kline_core::{
    Bar, Source, Update,
    evidence::{EvidenceState, EvidenceUpdate, MINUTE_MS as MINUTE, rest_verdict, validate_bar},
};
use tokio::sync::OwnedMutexGuard;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    Startup,
    Routine,
    Fallback,
    History,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerdictRange {
    pub first: i64,
    pub last: i64,
}
#[derive(Clone, Debug)]
pub struct EvidenceRequest {
    pub kind: TaskKind,
    pub range: VerdictRange,
    pub start: Option<i64>,
    pub end: i64,
    pub limit: usize,
    pub priority: Priority,
}
impl EvidenceRequest {
    pub fn startup(now: i64) -> Self {
        let boundary = now.div_euclid(MINUTE) * MINUTE;
        Self {
            kind: TaskKind::Startup,
            range: VerdictRange {
                first: boundary.saturating_sub(240 * MINUTE).max(0),
                last: boundary - MINUTE,
            },
            start: None,
            end: now,
            limit: 241,
            priority: Priority::Background,
        }
    }
    pub fn fallback(earliest: i64, latest: i64, now: i64) -> Self {
        let current = earliest == latest;
        Self {
            kind: TaskKind::Fallback,
            range: VerdictRange {
                first: earliest.saturating_sub(MINUTE).max(0),
                last: latest,
            },
            start: (!current).then_some(earliest.saturating_sub(MINUTE).max(0)),
            end: now,
            limit: if current {
                3
            } else {
                ((latest - earliest) / MINUTE + 3).min(99) as usize
            },
            priority: Priority::Urgent,
        }
    }
}
impl Recovery {
    pub fn evidence_sync_request(&self, id: usize, initialized: bool, now: i64) -> EvidenceRequest {
        let slot = self.engine.catalog.slot(id);
        if !initialized {
            let mut request = EvidenceRequest::startup(now);
            if !slot.is_trading()
                && let Some(since) = slot.not_trading_since()
            {
                request.range.last = request
                    .range
                    .last
                    .min((since - 1).div_euclid(MINUTE) * MINUTE);
            }
            return request;
        }
        let boundary = slot.interval.boundary(now);
        let last = slot
            .not_trading_since()
            .filter(|_| !slot.is_trading())
            .map_or(boundary - MINUTE, |since| {
                ((since - 1).div_euclid(MINUTE) * MINUTE).min(boundary - MINUTE)
            });
        if slot.is_trading() {
            // 97 closed target minutes, one left anchor and the forming successor.
            // This is independent of which rows have already arrived over WS.
            return EvidenceRequest {
                kind: TaskKind::Routine,
                range: VerdictRange {
                    first: (boundary - 97 * MINUTE).max(0),
                    last,
                },
                start: Some((boundary - 98 * MINUTE).max(0)),
                end: now,
                limit: 99,
                priority: Priority::Background,
            };
        }
        let trusted = self.engine.ws_final_trusted();
        let first = slot.select_bars(|series| {
            let records = series.records();
            let floor = series.covered_from().unwrap_or(last).max(0);
            // J57: 98 rows plus the left anchor keeps the normal request at 99.
            let lookback = records
                .get(records.len().saturating_sub(98))
                .map_or(last, |r| r.0.open_time)
                .min(last);
            let pending = series.earliest_needs_verdict(floor, last, trusted);
            pending
                .map_or(lookback, |open| open.min(lookback))
                .max(floor)
        });
        let start = first.saturating_sub(MINUTE).max(0);
        // since caps the verdict, never the response's right anchor.
        let end = now.min(start.saturating_add(499 * MINUTE - 1));
        EvidenceRequest {
            kind: if slot.is_trading() {
                TaskKind::Routine
            } else {
                TaskKind::History
            },
            range: VerdictRange { first, last },
            start: Some(start),
            end,
            limit: ((end - start) / MINUTE + 1).clamp(1, 499) as usize,
            priority: Priority::Background,
        }
    }
    pub(crate) fn evidence_history_request(&self, id: usize, now: i64) -> Option<EvidenceRequest> {
        let slot = self.engine.catalog.slot(id);
        let last = now.div_euclid(MINUTE) * MINUTE - 98 * MINUTE;
        let first = slot.select_bars(|series| {
            series.covered_from().and_then(|first| {
                (first <= last)
                    .then(|| {
                        series.earliest_needs_verdict(first, last, self.engine.ws_final_trusted())
                    })
                    .flatten()
            })
        })?;
        let start = first.saturating_sub(MINUTE).max(0);
        let end = now.min(start.saturating_add(499 * MINUTE - 1));
        Some(EvidenceRequest {
            kind: TaskKind::History,
            range: VerdictRange { first, last },
            start: Some(start),
            end,
            limit: ((end - start) / MINUTE + 1).clamp(1, 499) as usize,
            priority: Priority::Background,
        })
    }
    /// The caller acquires the per-slot gate before this enters the limiter.
    /// The owned guard remains live through decoding, adjudication and commit.
    pub(crate) async fn execute_evidence(
        &self,
        id: usize,
        request: EvidenceRequest,
        _gate: OwnedMutexGuard<()>,
    ) -> Result<usize> {
        let slot = self.engine.catalog.slot(id);
        if !slot.is_tracked() {
            return Ok(0);
        }
        let tracking_generation = slot.tracking_generation();
        slot.mark_rest_request(self.engine.now_ms());
        let bars = self
            .api
            .klines_priority(
                slot.market,
                &slot.symbol,
                slot.interval,
                request.start,
                request.end,
                request.limit,
                request.priority,
            )
            .await?;
        anyhow::ensure!(
            slot.is_tracked() && slot.tracking_generation() == tracking_generation,
            "evidence request belongs to a removed series"
        );
        self.submit_evidence(id, &request, bars, tracking_generation)
    }
    fn submit_evidence(
        &self,
        id: usize,
        request: &EvidenceRequest,
        bars: Vec<Bar>,
        tracking_generation: u64,
    ) -> Result<usize> {
        anyhow::ensure!(
            !bars.is_empty(),
            "REST returned no bars; series remains unready"
        );
        // Validate the whole response before publishing any of it, including the
        // decimal lexemes that legacy string mode deliberately leaves opaque.
        anyhow::ensure!(bars.len() <= request.limit, "REST page exceeds limit");
        for (i, bar) in bars.iter().enumerate() {
            validate_bar(bar)?;
            anyhow::ensure!(
                bar.number_type == self.engine.number_type(),
                "REST number type mismatch"
            );
            anyhow::ensure!(
                bar.open_time <= request.end
                    && request.start.is_none_or(|start| bar.open_time >= start),
                "REST row outside request"
            );
            anyhow::ensure!(
                i == 0 || bars[i - 1].open_time < bar.open_time,
                "REST rows not strictly increasing"
            );
        }
        let count = bars.len();
        let slot = self.engine.catalog.slot(id);
        let trading = slot.is_trading();
        let retained_latest = bars
            .iter()
            .rev()
            .find(|bar| trading || bar.open_time <= request.range.last)
            .map(|bar| bar.open_time);
        // Cold requests ask for the latest 241 rows, even for a stale listing.
        // Their retained window follows returned observations, not wall time.
        let first = if request.kind == TaskKind::Startup {
            bars[0].open_time.max(
                retained_latest
                    .unwrap_or(bars[0].open_time)
                    .saturating_sub((slot.capacity().saturating_sub(1) as i64) * MINUTE),
            )
        } else {
            request.range.first
        };
        for bar in &bars {
            let verdict = bar.open_time >= first
                && bar.open_time <= request.range.last
                && rest_verdict(&bars, bar.open_time, request.end) == EvidenceState::RestFinal;
            // Left/right anchors can be read beyond a nontrading target, but do
            // not create a new listing segment out of those evidence-only rows.
            if bar.open_time < first {
                continue;
            }
            if bar.open_time > request.range.last && !trading {
                continue;
            }
            self.engine.commit_evidence_in_generation(
                id,
                if verdict {
                    EvidenceUpdate::Rest(bar.clone())
                } else {
                    EvidenceUpdate::Observation(Update {
                        bar: bar.clone(),
                        closed: false,
                        source: Source::Rest,
                        event_time: None,
                        sequence: 0,
                    })
                },
                Some(tracking_generation),
            )?;
        }
        // No gap outside this response can be absent. Bound the scan both by
        // its anchors and by minute retention, including very sparse cold pages.
        let gap_first = first.max(
            retained_latest
                .unwrap_or(first)
                .saturating_sub((slot.capacity().saturating_sub(1) as i64) * MINUTE),
        );
        let gap_last = request.range.last.min(bars.last().unwrap().open_time);
        for open in (gap_first..=gap_last).step_by(MINUTE as usize) {
            if rest_verdict(&bars, open, request.end) == EvidenceState::Absent {
                self.engine.commit_evidence_in_generation(
                    id,
                    EvidenceUpdate::Absent(open),
                    Some(tracking_generation),
                )?;
            }
        }
        let slot = self.engine.catalog.slot(id);
        anyhow::ensure!(
            slot.is_tracked() && slot.tracking_generation() == tracking_generation,
            "evidence series removed during publication"
        );
        // Gaps, stale tails and pending rows are successful transport/commit;
        // only startup's own successful whole-window request initializes it.
        Ok(count)
    }
    pub(crate) fn evidence_guarded(&self, now: i64) -> bool {
        crate::config::guarded(
            now,
            self.engine
                .catalog
                .slots()
                .iter()
                .filter(|s| s.is_tracked())
                .map(|s| s.interval),
            self.config.boundary_guard_before_ms,
            self.config.boundary_guard_after_ms,
        ) || crate::config::guarded(
            now,
            [kline_core::Interval::parse("1h").unwrap()].into_iter(),
            self.config.hour_boundary_guard_before_ms,
            self.config.hour_boundary_guard_after_ms,
        )
    }
    pub(crate) async fn sync_evidence(&self, id: usize, initialized: bool) -> Result<usize> {
        if initialized {
            self.boundary_guard(false).await;
        }
        let slot = self.engine.catalog.slot(id);
        let gate = slot.rest_gate().lock_owned().await;
        let request = self.evidence_sync_request(id, initialized, self.engine.now_ms());
        self.execute_evidence(id, request, gate).await
    }
    pub(crate) async fn repair_evidence(&self, id: usize, open: i64) -> Result<usize> {
        let slot = self.engine.catalog.slot(id);
        // This precheck never cancels a response once it enters the limiter.
        if !slot.is_tracked() || !slot.is_trading() || !self.engine.needs_verdict(id, open) {
            return Ok(0);
        }
        let gate = slot.rest_gate().lock_owned().await;
        let now = self.engine.now_ms();
        let latest = slot.interval.boundary(now) - MINUTE;
        self.execute_evidence(id, EvidenceRequest::fallback(open, latest, now), gate)
            .await
    }
}

#[cfg(test)]
mod fix1_history_tests {
    use super::*;
    use kline_core::{Interval, Market, NumberType};
    use kline_service::{Catalog, Clock, Engine, Instrument, Settings, evidence::EvidenceSettings};
    use std::{num::NonZeroUsize, sync::Arc};
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0
        }
    }
    #[test]
    fn older_pending_is_a_separate_history_plan_without_widening_routine() {
        let boundary = 6_000_000_000;
        let e = Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                interval: Interval::parse("1m").unwrap(),
                symbol: "BTCUSDT".into(),
                trading: true,
                continuous: Some(("BTCUSDT".into(), "PERPETUAL".into())),
                capacity: NonZeroUsize::new(241).unwrap(),
            }])
            .unwrap(),
            Arc::new(FixedClock(boundary + 1500)),
            Settings {
                number_type: NumberType::String,
                evidence: Some(EvidenceSettings::default()),
                ..Settings::default()
            },
        );
        for open in (boundary - 240 * MINUTE..=boundary).step_by(MINUTE as usize) {
            let mut bar = Bar {
                open_time: open,
                close_time: open + MINUTE - 1,
                ..Bar::default()
            };
            bar.set_numbers(NumberType::String, ["1"; 8]).unwrap();
            let input = if open == boundary - 200 * MINUTE {
                EvidenceUpdate::Observation(Update {
                    bar,
                    closed: false,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                })
            } else {
                EvidenceUpdate::Rest(bar)
            };
            e.commit_evidence(0, input).unwrap();
        }
        let r = Recovery {
            engine: e,
            api: crate::rest::RestApi::new_with_type(
                "http://127.0.0.1:1",
                "http://127.0.0.1:1",
                6000,
                NumberType::String,
            )
            .unwrap(),
            config: crate::config::RestConfig::default(),
        };
        let routine = r.evidence_sync_request(0, true, boundary + 1500);
        assert_eq!(routine.limit, 99);
        assert_eq!(routine.start, Some(boundary - 98 * MINUTE));
        let history = r.evidence_history_request(0, boundary + 1500).unwrap();
        assert_eq!(history.kind, TaskKind::History);
        assert_eq!(history.range.first, boundary - 200 * MINUTE);
        assert_eq!(history.range.last, boundary - 98 * MINUTE);
        assert_eq!(history.start, Some(boundary - 201 * MINUTE));
        assert_eq!(history.end, boundary + 1500);
        assert_eq!(history.limit, 202);
    }
}
