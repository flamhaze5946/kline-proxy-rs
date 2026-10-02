//! Bounded REST reconciliation. Existing stream precedence remains in kline-core.
use crate::{
    config::{RestConfig, guarded},
    rest::{Priority, RestApi},
};
use anyhow::Result;
use chrono::{Datelike, TimeZone, Utc};
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::Engine;
use std::{sync::Arc, time::Duration};

pub struct Recovery {
    pub engine: Arc<Engine>,
    pub api: Arc<RestApi>,
    pub config: RestConfig,
}
#[derive(Clone, Copy)]
enum TailGoal {
    Final,
    Fresh,
}
impl Recovery {
    /// Small, cancellable tail repair shares the same transport budget as history.
    /// It deliberately does not mark the complete retained history as reconciled.
    pub async fn repair_latest(&self, id: usize, open: i64) -> Result<usize> {
        self.repair_tail(id, open, TailGoal::Final).await
    }
    /// A final can arrive without the next forming bar. Refresh only that tail,
    /// independently of guarded history work, while keeping closed repair urgent.
    pub async fn repair_forming(&self, id: usize) -> Result<usize> {
        let slot = self.engine.catalog.slot(id);
        let (open, _) = latest_target(slot.interval, self.engine.now_ms());
        self.repair_tail(id, open, TailGoal::Fresh).await
    }
    async fn repair_tail(&self, id: usize, open: i64, goal: TailGoal) -> Result<usize> {
        let slot = self.engine.catalog.slot(id);
        let mut changes = slot.subscribe_changes();
        let complete = || {
            !slot.is_tracked()
                || !slot.is_trading()
                || match goal {
                    TailGoal::Final => !slot.needs_final(open),
                    TailGoal::Fresh => {
                        slot.fresh_tail(self.engine.now_ms(), self.config.freshness_grace_ms as i64)
                    }
                }
        };
        if complete() {
            return Ok(0);
        }
        let snapshot_time = self.engine.now_ms();
        let request = self.api.klines_priority(
            slot.market,
            &slot.symbol,
            slot.interval,
            Some(open),
            snapshot_time,
            2,
            match goal {
                TailGoal::Final => Priority::Urgent,
                TailGoal::Fresh => Priority::Foreground,
            },
        );
        tokio::pin!(request);
        let bars = loop {
            tokio::select! {
                // Stream delivery cancels both queued and in-flight tail work.
                biased;
                _ = changes.changed() => {
                    if complete() {
                        return Ok(0);
                    }
                }
                result = &mut request => break result?,
            }
        };
        // A final can race transport completion; the stream remains authoritative.
        if complete() {
            return Ok(0);
        }
        let mut count = 0;
        for bar in bars {
            anyhow::ensure!(
                bar.open_time >= open && bar.open_time <= snapshot_time,
                "priority repair returned an out-of-range bar"
            );
            let committed = self.engine.commit(
                id,
                Update {
                    closed: bar.close_time <= snapshot_time,
                    bar,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )?;
            count += usize::from(committed.updated || committed.became_final);
        }
        match goal {
            TailGoal::Final => anyhow::ensure!(
                !slot.needs_final(open),
                "priority repair did not return the required final"
            ),
            TailGoal::Fresh => anyhow::ensure!(
                slot.fresh_tail(self.engine.now_ms(), self.config.freshness_grace_ms as i64),
                "tail repair did not return a fresh bar"
            ),
        }
        Ok(count)
    }
    async fn boundary_guard(&self, cold: bool) {
        if cold {
            return;
        }
        let mut intervals = [false; 16];
        for slot in self.engine.catalog.slots().iter() {
            intervals[slot.interval.index()] = true;
        }
        while guarded(
            self.engine.now_ms(),
            kline_core::Interval::all().filter(|i| intervals[i.index()]),
            self.config.boundary_guard_before_ms,
            self.config.boundary_guard_after_ms,
        ) || guarded(
            self.engine.now_ms(),
            [kline_core::Interval::parse("1h").unwrap()].into_iter(),
            self.config.hour_boundary_guard_before_ms,
            self.config.hour_boundary_guard_after_ms,
        ) {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    /// Call once after each connection epoch; then periodically to repair missed frames.
    pub async fn sync(&self, id: usize, initialized: bool) -> Result<usize> {
        self.sync_with_priority(id, initialized, Priority::Background)
            .await
    }
    pub async fn sync_with_priority(
        &self,
        id: usize,
        initialized: bool,
        priority: Priority,
    ) -> Result<usize> {
        let slot = self.engine.catalog.slot(id);
        if !slot.is_trading() {
            return Ok(0);
        }
        let records = slot.records();
        let capacity = slot.capacity();
        let cold = !initialized
            && records.iter().filter(|r| r.1).count() < capacity.saturating_sub(1).max(1);
        let page_limit = if slot.market == Market::Spot {
            1000
        } else {
            499
        };
        let mut fetched: Vec<Bar> = Vec::new();
        let calendar_month = slot.interval.code() == "1M";
        // Historical trading suspensions can close a real fixed-interval bar early.
        // Coverage follows consecutive opening slots, while its upstream close time
        // must stay unchanged. Calendar months still follow their actual boundaries.
        let next_open = |bar: &Bar| {
            if calendar_month {
                bar.close_time.saturating_add(1)
            } else {
                bar.open_time.saturating_add(slot.interval.millis())
            }
        };
        let maximum_period = if calendar_month {
            31 * 86_400_000
        } else {
            slot.interval.millis()
        };
        let minimum_period = if calendar_month {
            28 * 86_400_000
        } else {
            slot.interval.millis()
        };
        let recovery_limit =
            ((capacity as i64 + 2) * maximum_period / minimum_period) as usize + page_limit;
        // A REST request started before close cannot retrospectively finalize that bar.
        self.boundary_guard(cold).await;
        let snapshot_time = self.engine.now_ms();
        if cold {
            let mut end = snapshot_time;
            while fetched.len() < capacity {
                self.boundary_guard(true).await;
                let limit = page_limit.min(capacity - fetched.len());
                let page = self
                    .api
                    .klines_priority(
                        slot.market,
                        &slot.symbol,
                        slot.interval,
                        None,
                        end,
                        limit,
                        priority,
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let earliest = page[0].open_time;
                let short = page.len() < limit;
                fetched.extend(page);
                if short || earliest == 0 {
                    break;
                }
                anyhow::ensure!(earliest <= end, "REST pagination did not move backwards");
                end = earliest - 1;
            }
        } else {
            let configured_refresh = match slot.market {
                Market::Future => self.config.future_refresh_count,
                Market::Spot => self.config.spot_refresh_count,
            }
            .unwrap_or(capacity)
            .min(capacity)
            .max(1);
            let mut start = records
                .iter()
                .rev()
                .find(|r| r.1)
                .or_else(|| records.last())
                .map(|r| r.0.open_time)
                .unwrap_or(0);
            // Finals and synthetic fills can be revised upstream. Revisit Java's bounded
            // refresh window even when the stream is healthy and the history has no holes.
            if let Some((bar, _)) = records.get(records.len().saturating_sub(configured_refresh)) {
                start = start.min(bar.open_time);
            }
            // Revisit the earliest gap/nonfinal, including holes before the newest bar.
            for (i, (bar, closed)) in records.iter().enumerate() {
                if !closed {
                    start = start.min(bar.open_time);
                }
                if i > 0 && next_open(&records[i - 1].0) < bar.open_time {
                    start = start.min(next_open(&records[i - 1].0));
                }
            }
            // A long outage only needs the retained window, not unbounded historical work.
            start = start.max(
                snapshot_time
                    .saturating_sub((capacity as i64 + 2) * maximum_period)
                    .max(0),
            );
            while start <= snapshot_time {
                self.boundary_guard(false).await;
                // Bound both ends, so an endpoint selecting its latest LIMIT rows
                // cannot silently skip the front of a long outage.
                let page_end = start
                    .saturating_add(page_limit as i64 * minimum_period - 1)
                    .min(snapshot_time);
                let requested_limit =
                    (((page_end - start) / minimum_period + 1) as usize).min(page_limit);
                let page = self
                    .api
                    .klines_priority(
                        slot.market,
                        &slot.symbol,
                        slot.interval,
                        Some(start),
                        page_end,
                        requested_limit,
                        priority,
                    )
                    .await?;
                fetched.extend(page);
                anyhow::ensure!(
                    fetched.len() <= recovery_limit,
                    "REST recovery exceeds retained window"
                );
                if page_end == snapshot_time {
                    break;
                }
                start = page_end
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("REST cursor overflow"))?;
            }
        }
        anyhow::ensure!(
            !fetched.is_empty(),
            "REST returned no bars; series remains unready"
        );
        fetched.sort_unstable_by_key(|b| b.open_time);
        anyhow::ensure!(
            fetched.windows(2).all(|w| w[0].open_time < w[1].open_time),
            "overlapping REST pages"
        );
        if cold {
            slot.reserve_history(fetched.len());
        }
        let count = fetched.len();
        // Bounded Java-compatible zero-volume gap fill, anchored by actual observations.
        // Never fabricate a tail beyond the newest returned real bar.
        let mut previous = records
            .iter()
            .rev()
            .find(|r| r.0.open_time < fetched[0].open_time)
            .map(|r| r.0.clone());
        let mut committed = 0_usize;
        for bar in fetched {
            if let Some(prev) = &previous
                && next_open(prev) < bar.open_time
            {
                anyhow::ensure!(
                    !calendar_month,
                    "calendar-month gap requires real REST bars"
                );
                let period = slot.interval.millis();
                let steps = (bar.open_time - prev.open_time - 1) / period;
                let first = steps.saturating_sub(capacity as i64).max(0) + 1;
                for step in first..=steps {
                    let open = prev.open_time + step * period;
                    let synthetic = prev.synthetic_after(open, period);
                    self.engine.commit(
                        id,
                        Update {
                            closed: synthetic.close_time <= snapshot_time,
                            bar: synthetic,
                            source: Source::Synthetic,
                            event_time: None,
                            sequence: 0,
                        },
                    )?;
                    committed += 1;
                    if committed.is_multiple_of(256) {
                        tokio::task::yield_now().await;
                    }
                }
            }
            self.engine.commit(
                id,
                Update {
                    closed: bar.close_time <= snapshot_time,
                    bar: bar.clone(),
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )?;
            committed += 1;
            if committed.is_multiple_of(256) {
                tokio::task::yield_now().await;
            }
            previous = Some(bar);
        }
        let records = slot.records();
        anyhow::ensure!(
            records.windows(2).all(|w| {
                next_open(&w[0].0) == w[1].0.open_time && w[0].0.close_time < w[1].0.open_time
            }),
            "REST recovery left a retained history gap or overlap"
        );
        anyhow::ensure!(
            records
                .iter()
                .all(|(bar, closed)| *closed || bar.close_time > snapshot_time),
            "REST recovery left an expired nonfinal bar"
        );
        // A short/new listing is valid, but old-only responses cannot claim fresh readiness.
        let latest = slot
            .latest()
            .ok_or_else(|| anyhow::anyhow!("empty series after recovery"))?
            .0;
        anyhow::ensure!(
            latest.close_time >= snapshot_time - self.config.freshness_grace_ms as i64,
            "REST tail is stale"
        );
        Ok(count)
    }
}

/// Recovery follows upstream calendar boundaries; bulk retains its existing Java
/// boundary contract. Return the latest completed opening and its closing boundary.
pub(crate) fn latest_target(interval: Interval, now: i64) -> (i64, i64) {
    if matches!(interval.code(), "1M" | "1w")
        && let Some(date) = Utc.timestamp_millis_opt(now).single()
    {
        let day = date.date_naive();
        let current = if interval.code() == "1M" {
            day.with_day(1).unwrap()
        } else {
            day - chrono::Days::new(u64::from(day.weekday().num_days_from_monday()))
        };
        let previous = if interval.code() == "1M" {
            (current - chrono::Days::new(1)).with_day(1).unwrap()
        } else {
            current - chrono::Days::new(7)
        };
        return (
            previous
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis(),
            current
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis(),
        );
    }
    let boundary = interval.boundary(now);
    (boundary - interval.millis(), boundary)
}
