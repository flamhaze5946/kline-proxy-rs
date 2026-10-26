//! Opt-in reconciliation. The 0.4.24 loop is intentionally left on its own path.
use super::*;
use crate::{
    evidence_recovery::{EvidenceRequest, TaskKind},
    evidence_schedule::{FallbackBudget, TaskRecord},
};
use kline_core::evidence::MINUTE_MS as MINUTE;

pub(super) async fn reconcile(
    engine: Arc<Engine>,
    api: Arc<RestApi>,
    config: RestConfig,
    health: Arc<Health>,
    mut stop: watch::Receiver<bool>,
) {
    let recovery = Arc::new(Recovery {
        engine: engine.clone(),
        api,
        config: config.clone(),
    });
    let mut records: Vec<Option<TaskRecord>> = Vec::new();
    let mut tasks: JoinSet<(
        usize,
        crate::evidence_schedule::Attempt,
        anyhow::Result<usize>,
    )> = JoinSet::new();
    let mut fallback_running = 0;
    let mut sync_running = 0;
    let settings = engine.settings().evidence.as_ref().unwrap();
    let mut budget = FallbackBudget::new(settings.fallback_weight_per_minute);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let interval = (config.reconcile_seconds * 1000) as i64;
    'running: while !*stop.borrow() {
        let completed = tokio::select! {
            _ = stop.changed() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => completed,
            _ = tick.tick() => None,
        };
        for result in completed
            .into_iter()
            .chain(std::iter::from_fn(|| tasks.try_join_next()))
        {
            match result {
                Ok((id, attempt, result)) => {
                    let attempt: crate::evidence_schedule::Attempt = attempt;
                    if attempt.kind == TaskKind::Fallback {
                        fallback_running -= 1;
                    } else {
                        sync_running -= 1;
                    }
                    if let Some(record) = records.get_mut(id).and_then(Option::as_mut) {
                        record.finish(attempt, engine.now_ms(), result.is_ok(), interval);
                    }
                    if let Err(error) = result {
                        tracing::warn!(id, kind=?attempt.kind, %error, "evidence recovery will retry");
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "evidence recovery task panicked");
                    break 'running;
                }
            }
        }
        let slots = engine.catalog.slots();
        records.resize_with(slots.len(), || None);
        let feeds = health.feeds.load();
        let feed_digest = digest(feeds.iter().map(Feed::token));
        let mut epochs = vec![0_u64; slots.len()];
        for feed in feeds.iter() {
            if let Some(ids) = &feed.ids {
                for &id in ids.iter() {
                    if let Some(epoch) = epochs.get_mut(id) {
                        *epoch = epoch.wrapping_mul(1099511628211) ^ feed.token();
                    }
                }
            } else {
                for (id, _) in slots
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| slot.market == feed.market)
                {
                    epochs[id] = epochs[id].wrapping_mul(1099511628211) ^ feed.token();
                }
            }
        }
        let now = engine.now_ms();
        let latest = now.div_euclid(MINUTE) * MINUTE - MINUTE;
        let trusted = engine.ws_final_trusted();
        let mut available = vec![SeriesReadiness::default(); slots.len()];
        let (mut tracked, mut ready) = (0, 0);
        let mut fallback = Vec::new();
        let mut sync = Vec::new();
        let mut in_flight_blocked = 0;
        for (id, slot) in slots.iter().enumerate() {
            if !slot.is_tracked() {
                records[id] = None;
                continue;
            }
            tracked += 1;
            if records[id]
                .as_ref()
                .is_none_or(|record| record.tracking_generation != slot.tracking_generation())
            {
                let mut record = TaskRecord::new(now);
                record.tracking_generation = slot.tracking_generation();
                records[id] = Some(record);
            }
            let record = records[id].as_mut().unwrap();
            record.observe(now, epochs[id], slot.capacity());
            // J57: retain all legacy readiness accounting for nontrading slots,
            // while also allowing their startup and historical work to be queued.
            if !slot.is_trading() {
                ready += 1;
                record.established = true;
                available[id] = SeriesReadiness {
                    ready: true,
                    established: true,
                    capacity: slot.capacity(),
                    ..SeriesReadiness::default()
                };
            } else {
                let (_, fresh, _, provisional) =
                    slot.lifecycle_status(now, config.freshness_grace_ms as i64, latest);
                let current = record.initialized
                    && record.epoch == epochs[id]
                    && epochs[id] != 0
                    && record.gap == slot.gap_generation()
                    && (fresh || provisional);
                if current {
                    ready += 1;
                    record.established = true;
                }
                available[id] = SeriesReadiness {
                    ready: current,
                    established: record.established,
                    epoch: record.epoch,
                    gap: record.gap,
                    capacity: slot.capacity(),
                };
            }
            if slot.is_trading() {
                record.observe_latest(latest, engine.needs_verdict(id, latest));
                record.resolve_specified(|open| engine.needs_verdict(id, open));
                let earliest = slot.select_bars(|s| {
                    s.earliest_needs_verdict((latest - 96 * MINUTE).max(0), latest, trusted)
                });
                if let Some(earliest) = earliest
                    && let Some(due) =
                        record.fallback_due(now, earliest, latest, slot.last_rest_request_ms())
                {
                    if record.running {
                        in_flight_blocked += 1;
                    } else {
                        fallback.push((earliest, id, due));
                    }
                }
            }
            if !record.running
                && let Some(kind) = record.sync_due(now, slot.is_trading())
                && (kind == TaskKind::Startup || !recovery.evidence_guarded(now))
            {
                sync.push((kind, id));
            }
        }
        fallback.sort_by_key(|&(earliest, id, _)| (earliest, id));
        let queued = fallback.len();
        let sync_queued = sync.len();
        let mut concurrency_blocked = 0;
        let mut budget_blocked = 0;
        if health.clock_ready() {
            // These counters are independent: background work never occupies a
            // latest_repair_workers permit, including during boundary guards.
            for (earliest, id, due) in fallback {
                if fallback_running >= config.latest_repair_workers {
                    concurrency_blocked += 1;
                    continue;
                }
                if !budget.available(now) {
                    budget_blocked += 1;
                    continue;
                }
                let Ok(gate) = slots[id].rest_gate().try_lock_owned() else {
                    in_flight_blocked += 1;
                    continue;
                };
                let record = records[id].as_mut().unwrap();
                let request = EvidenceRequest::fallback(earliest, latest, now);
                let attempt = record.begin(
                    TaskKind::Fallback,
                    request.range,
                    Some(due),
                    epochs[id],
                    slots[id].gap_generation(),
                );
                budget.consume(now);
                fallback_running += 1;
                let r = recovery.clone();
                tasks.spawn(
                    async move { (id, attempt, r.execute_evidence(id, request, gate).await) },
                );
            }
            // Startup first; subsequent sync retries remain Background and use
            // their own exponentially backed-off demand deadlines.
            sync.sort_by_key(|&(kind, id)| (kind != TaskKind::Startup, id));
            for (kind, id) in sync {
                if sync_running >= config.workers {
                    break;
                }
                let record = records[id].as_mut().unwrap();
                if record.running {
                    continue;
                }
                let Ok(gate) = slots[id].rest_gate().try_lock_owned() else {
                    continue;
                };
                let request = recovery.evidence_sync_request(id, kind != TaskKind::Startup, now);
                let attempt = record.begin(
                    kind,
                    request.range,
                    None,
                    epochs[id],
                    slots[id].gap_generation(),
                );
                sync_running += 1;
                let r = recovery.clone();
                tasks.spawn(
                    async move { (id, attempt, r.execute_evidence(id, request, gate).await) },
                );
            }
        }
        health.series.store(Arc::new(available));
        health
            .acknowledged_catalog
            .store(slots.generation, Ordering::Release);
        health
            .acknowledged_gap
            .store(engine.gap_epoch(), Ordering::Release);
        health
            .acknowledged_feeds
            .store(feed_digest, Ordering::Release);
        health.ready.store(ready == tracked, Ordering::Release);
        if tracked > 0 && health.is_ready() {
            health.bootstrapped.store(true, Ordering::Release);
        }
        *health.detail.write() = serde_json::json!({
            "phase": if ready == tracked { "live" } else { "recovering" },
            "configured_series": tracked, "ready_series": ready, "recovering_series": tracked-ready,
            "rest_jobs": tasks.len(), "latest_repair_jobs": fallback_running,
            "initial_pending": records.iter().flatten().filter(|r| !r.initialized).count(),
            "routine_rest_jobs": sync_running, "routine_queued": sync_queued,
            "fallback_queued": queued, "fallback_in_flight_blocked": in_flight_blocked,
            "fallback_concurrency_blocked": concurrency_blocked, "fallback_budget_blocked": budget_blocked,
            "ws_final_trusted": trusted, "revised_total": engine.breaker_state().revised_total,
            "tail_rest_policy": "evidence", "observed_at_ms": now, "streams": feeds.len(),
        });
    }
    health.ready.store(false, Ordering::Release);
    health.stopped.store(true, Ordering::Release);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
