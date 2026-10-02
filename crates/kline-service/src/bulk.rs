use crate::{
    Engine, Slot,
    cache::{Key, Lookup},
};
use binance_wire::DisplayBar;
use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use kline_core::{Interval, Market};
use serde::Serialize;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::time::Instant;

pub struct BulkQuery {
    pub market: Market,
    pub interval: String,
    pub limit: Option<i32>,
    pub closed_only: bool,
    pub symbols: Vec<String>,
}
#[derive(Debug)]
pub struct BulkReply {
    pub body: Bytes,
    pub finalized: bool,
    pub waited_ms: u64,
    /// The boundary of the period this reply was built for (the key's).
    pub boundary: i64,
    pub(crate) payload: Option<Arc<Payload>>,
    // A current bar arriving (or the preceding bar becoming final) must replace
    // a cached provisional response immediately, without waiting for its TTL.
    forming_generations: Vec<u64>,
    forming_valid_until: i64,
}
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Invalid interval.")]
    Interval,
    #[error("response builder at capacity")]
    Busy,
    #[error("response encoding failed: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("response task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("response worker failed: {0}")]
    Worker(#[from] anyhow::Error),
}

#[derive(Debug)]
pub(crate) struct Payload {
    bytes: Bytes,
    generations: Vec<u64>,
    valid_until: i64,
    observed_at: i64,
    data_status: Option<DataStatus>,
}
impl BulkReply {
    pub(crate) fn cache_bytes(&self) -> usize {
        self.body.len()
            + self.payload.as_ref().map_or(0, |p| p.bytes.len())
            + self.forming_generations.len() * std::mem::size_of::<u64>()
    }
    /// A finished reply without a payload, for cache tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            body: Bytes::new(),
            finalized: true,
            waited_ms: 0,
            boundary: 0,
            payload: None,
            forming_generations: vec![],
            forming_valid_until: i64::MAX,
        }
    }
}
#[derive(Serialize)]
struct Prefix {
    interval: &'static str,
    ts_ms: i64,
}
#[derive(Serialize)]
struct Finality<'a> {
    finalized: bool,
    pending: Vec<Arc<str>>,
    waited_ms: u64,
    not_trading: Vec<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_status: Option<&'a DataStatus>,
}
/// Additional diagnostics, independent from the Java-compatible boundary finality.
#[derive(Debug, Serialize)]
struct DataStatus {
    window_finalized: bool,
    nonfinal_symbols: Vec<Arc<str>>,
    missing_latest: Vec<Arc<str>>,
}
struct Wait {
    pending: Vec<Arc<str>>,
    not_trading: Vec<Arc<str>>,
    waited_ms: u64,
}

impl Engine {
    fn payload_current(&self, key: &Key, payload: &Payload, now: i64) -> bool {
        let slots = self.catalog.slots();
        now < payload.valid_until
            && now >= payload.observed_at
            && key.revision == slots.generation
            && payload.generations.len() == key.ids.len()
            && key
                .ids
                .iter()
                .zip(&payload.generations)
                .all(|(&id, &generation)| slots[id].window_generation() == generation)
    }
    fn reply_current(&self, key: &Key, reply: &BulkReply, now: i64) -> bool {
        if !key.closed_only {
            let slots = self.catalog.slots();
            return key.boundary == key.interval.boundary(now)
                && now < reply.forming_valid_until
                && key.revision == self.revision(key.market)
                && key.ids.len() == reply.forming_generations.len()
                && key
                    .ids
                    .iter()
                    .zip(&reply.forming_generations)
                    .all(|(&id, &generation)| slots[id].window_generation() == generation);
        }
        reply
            .payload
            .as_ref()
            .map_or(!key.closed_only, |p| self.payload_current(key, p, now))
    }
    pub async fn bulk(self: &Arc<Self>, query: BulkQuery) -> Result<Arc<BulkReply>, ServiceError> {
        let interval = Interval::parse(&query.interval).ok_or(ServiceError::Interval)?;
        // A closed_only request arriving just before an interval boundary (its sender's clock a
        // little ahead of ours) wants the bars that close at that boundary: wait for it, and from
        // then on never treat this request as earlier than the boundary.
        let mut not_before = if query.closed_only {
            self.await_pre_boundary(interval, &query).await
        } else {
            None
        };
        // A slot granted after queueing is carried into the next lookup; waiting for it does not
        // use up one of the attempts that guard against replies going stale under races. One
        // deadline bounds all of this request's admission waits, however many lookups it takes.
        let mut slot = None;
        let mut admission_deadline = None;
        let mut boundary_deadline: Option<(i64, Instant)> = None;
        let mut attempts = 0;
        while attempts < 4 {
            let now =
                not_before.map_or_else(|| self.clock.now_ms(), |b| self.clock.now_ms().max(b));
            let key = Key {
                market: query.market,
                interval,
                limit: query.limit.unwrap_or(5).clamp(1, 100) as usize,
                closed_only: query.closed_only,
                ids: self
                    .catalog
                    .normalize(query.market, interval, &query.symbols),
                boundary: interval.boundary(now),
                revision: if query.closed_only {
                    self.catalog.generation()
                } else {
                    self.revision(query.market)
                },
            };
            if query.closed_only {
                // The selected boundary only moves forward: a retry or a wait for a slot after
                // the clock has been stepped back must not fall back to the previous period.
                not_before = Some(not_before.map_or(key.boundary, |b| b.max(key.boundary)));
            }
            // The final-wait deadline for the selected boundary: the end of the budget counted
            // from that boundary, fixed by the monotonic clock the first time this request
            // selects it and kept across slot waits and retries. A flight this lookup starts
            // waits until it; a flight it joins keeps its own.
            let final_deadline = match boundary_deadline {
                Some((boundary, deadline)) if boundary == key.boundary => deadline,
                _ => {
                    let elapsed = (now - key.boundary).max(0) as u64;
                    let deadline = Instant::now()
                        + Duration::from_millis(
                            self.settings.final_wait_ms.saturating_sub(elapsed),
                        );
                    boundary_deadline = Some((key.boundary, deadline));
                    deadline
                }
            };
            let reply = match self.cache.get(
                key.clone(),
                |r| self.reply_current(&key, r, now),
                slot.take(),
                final_deadline,
            )? {
                Lookup::Full => {
                    // Measured from the request's arrival, so HTTP admission queueing counts.
                    let deadline = *admission_deadline.get_or_insert_with(|| {
                        crate::admission::budget_deadline(self.cache.admission_wait())
                    });
                    slot = Some(
                        self.cache
                            .admit_until(deadline, &self.metrics.admission_queued)
                            .await
                            .inspect_err(|_| {
                                self.metrics
                                    .admission_rejected
                                    .fetch_add(1, Ordering::Relaxed);
                            })?,
                    );
                    continue;
                }
                Lookup::Ready(reply) => {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    reply
                }
                Lookup::Flight(registration) => {
                    let candidate = registration.payload.clone();
                    let final_deadline = registration.final_deadline;
                    registration
                        .cell
                        .get_or_try_init(|| async {
                            let waiting = std::time::Instant::now();
                            // The flight's own deadline: a follower that takes over from a
                            // cancelled leader neither restarts nor extends the final wait,
                            // even if the clock has been stepped back meanwhile.
                            let wait = self.wait_final(&key, final_deadline).await;
                            self.metrics.work.record(2, waiting.elapsed());
                            let queued = std::time::Instant::now();
                            let engine = self.clone();
                            let build_key = key.clone();
                            let large = key.ids.len().saturating_mul(key.limit) > 1024;
                            let reply = self
                                .builds
                                .run(large, move || {
                                    engine.metrics.work.record(3, queued.elapsed());
                                    let started = std::time::Instant::now();
                                    let reply = engine.build_body(&build_key, wait, candidate);
                                    engine.metrics.work.record(4, started.elapsed());
                                    reply
                                })
                                .await??;
                            let reply = Arc::new(reply);
                            self.cache.put(key.clone(), reply.clone());
                            Ok::<_, ServiceError>(reply)
                        })
                        .await?
                        .clone()
                }
            };
            attempts += 1;
            // A correction may race an in-flight build. Never let a later caller
            // inherit the completed old generation through the single-flight cell.
            if (!key.closed_only && !self.reply_current(&key, &reply, self.now_ms()))
                || reply.payload.as_ref().is_some_and(|p| {
                    !self.payload_current(&key, p, self.now_ms().max(key.boundary))
                })
            {
                continue;
            }
            return Ok(reply);
        }
        Err(ServiceError::Busy)
    }
    /// Waits until the next `interval` boundary when it is at most `pre_boundary_wait_ms` away
    /// and returns that boundary; `None` when there is nothing to wait for.
    async fn await_pre_boundary(&self, interval: Interval, query: &BulkQuery) -> Option<i64> {
        let limit = self.settings.pre_boundary_wait_ms;
        if limit == 0
            || self.settings.final_wait_ms == 0
            || self
                .catalog
                .normalize(query.market, interval, &query.symbols)
                .is_empty()
        {
            return None;
        }
        let now = self.clock.now_ms();
        let next = interval.boundary(now) + interval.millis();
        let early = (next - now) as u64;
        if early > limit {
            return None;
        }
        let started = Instant::now();
        tokio::time::sleep(Duration::from_millis(early)).await;
        self.metrics
            .pre_boundary_waits
            .fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            interval = interval.code(),
            boundary = next,
            early_ms = early,
            slept_ms = started.elapsed().as_millis() as u64,
            "BULK_PRE_BOUNDARY_WAIT"
        );
        Some(next)
    }
    async fn wait_final(&self, key: &Key, deadline: Instant) -> Wait {
        if !key.closed_only {
            return Wait {
                pending: vec![],
                not_trading: vec![],
                waited_ms: 0,
            };
        }
        let open = key.boundary - key.interval.millis();
        let pending = || {
            key.ids
                .iter()
                .filter_map(|&id| {
                    let slot = self.catalog.slot(id);
                    (slot.is_trading() && slot.pending(open)).then(|| slot.symbol.clone())
                })
                .collect::<Vec<_>>()
        };
        let not_trading = key
            .ids
            .iter()
            .filter_map(|&id| {
                let slot = self.catalog.slot(id);
                (!slot.is_trading() && slot.pending(open)).then(|| slot.symbol.clone())
            })
            .collect();
        let initial = pending();
        let start = Instant::now();
        if initial.is_empty() || start >= deadline {
            return Wait {
                pending: initial,
                not_trading,
                waited_ms: 0,
            };
        }
        let mut waits: FuturesUnordered<_> = key
            .ids
            .iter()
            .filter_map(|&id| {
                let slot = self.catalog.slot(id);
                (slot.is_trading() && slot.pending(open))
                    .then(|| wait_slot(slot.clone(), open, deadline))
            })
            .collect();
        while waits.next().await.is_some() {}
        Wait {
            pending: pending(),
            not_trading,
            waited_ms: start.elapsed().as_millis() as u64,
        }
    }
    fn build_body(
        &self,
        key: &Key,
        mut wait: Wait,
        candidate: Option<Arc<Payload>>,
    ) -> Result<BulkReply, ServiceError> {
        // Never before the key's boundary: after a pre-boundary wait the clock may still read a
        // moment short of it, and the bars that closed at the boundary must count as closed.
        let now = self.clock.now_ms().max(key.boundary);
        let mut stable = key.closed_only;
        let mut forming_generations = Vec::new();
        let mut forming_valid_until = i64::MAX;
        let payload = if let Some(payload) = candidate.filter(|p| self.payload_current(key, p, now))
        {
            self.metrics.payload_reuses.fetch_add(1, Ordering::Relaxed);
            payload
        } else {
            let mut bytes =
                Vec::with_capacity(key.ids.len().saturating_mul(key.limit).saturating_mul(128));
            bytes.push(b'{');
            let mut first = true;
            let mut generations = Vec::with_capacity(key.ids.len());
            let mut valid_until = key.boundary.saturating_add(key.interval.millis());
            let mut data_status = DataStatus {
                window_finalized: true,
                nonfinal_symbols: vec![],
                missing_latest: vec![],
            };
            if key.closed_only {
                wait.pending.clear();
                wait.not_trading.clear();
            }
            for &id in key.ids.iter() {
                let slot = self.catalog.slot(id);
                if !key.closed_only {
                    // Capture before the locked read: a racing commit can cause
                    // a retry, but can never bless old rows with a new generation.
                    forming_generations.push(slot.window_generation());
                }
                let window = if key.closed_only {
                    Some(self.windows.get(id, &slot, key.limit, now)?)
                } else {
                    None
                };
                let rows = window
                    .as_ref()
                    .map(|w| &w.rows[w.rows.len().saturating_sub(key.limit)..]);
                let forming = if window.is_none() {
                    slot.snapshot(key.limit, false, now)
                } else {
                    vec![]
                };
                if let Some(last) = forming.last().filter(|bar| bar.close_time >= now) {
                    // Calendar month / Monday week rollover can differ from the legacy cache key.
                    forming_valid_until =
                        forming_valid_until.min(last.close_time.saturating_add(1));
                }
                if let Some(window) = &window {
                    generations.push(window.generation);
                    valid_until = valid_until.min(window.valid_until);
                    let all_final = rows.unwrap().iter().all(|r| r.final_bar);
                    stable &= all_final;
                    if !all_final {
                        data_status.window_finalized = false;
                        data_status.nonfinal_symbols.push(slot.symbol.clone());
                    }
                    if slot.missing_latest(key.boundary - key.interval.millis()) {
                        data_status.missing_latest.push(slot.symbol.clone());
                    }
                    if slot.pending(key.boundary - key.interval.millis()) {
                        if slot.is_trading() {
                            wait.pending.push(slot.symbol.clone());
                        } else {
                            wait.not_trading.push(slot.symbol.clone());
                        }
                    }
                }
                if rows.is_some_and(|r| r.is_empty()) || (rows.is_none() && forming.is_empty()) {
                    continue;
                }
                if !first {
                    bytes.push(b',');
                }
                first = false;
                serde_json::to_writer(&mut bytes, &slot.symbol)?;
                bytes.push(b':');
                if let Some(rows) = rows {
                    bytes.push(b'[');
                    for (index, row) in rows.iter().enumerate() {
                        if index > 0 {
                            bytes.push(b',');
                        }
                        bytes.extend_from_slice(&row.bytes);
                    }
                    bytes.push(b']');
                } else {
                    serde_json::to_writer(
                        &mut bytes,
                        &forming.into_iter().map(DisplayBar).collect::<Vec<_>>(),
                    )?;
                }
            }
            bytes.push(b'}');
            self.metrics.payloads_built.fetch_add(1, Ordering::Relaxed);
            Arc::new(Payload {
                bytes: bytes.into(),
                generations,
                valid_until,
                observed_at: now,
                data_status: key.closed_only.then_some(data_status),
            })
        };
        let finalized = wait.pending.is_empty();
        let suffix = serde_json::to_vec(&Finality {
            finalized,
            pending: wait.pending,
            waited_ms: wait.waited_ms,
            not_trading: wait.not_trading,
            data_status: payload.data_status.as_ref(),
        })?;
        let mut body = Vec::with_capacity(payload.bytes.len() + suffix.len() + 80);
        serde_json::to_writer(
            &mut body,
            &Prefix {
                interval: key.interval.code(),
                ts_ms: now,
            },
        )?;
        body.pop();
        body.extend_from_slice(b",\"klines\":");
        body.extend_from_slice(&payload.bytes);
        body.push(b',');
        body.extend_from_slice(&suffix[1..]);
        self.metrics.responses_built.fetch_add(1, Ordering::Relaxed);
        Ok(BulkReply {
            body: body.into(),
            finalized,
            waited_ms: wait.waited_ms,
            boundary: key.boundary,
            payload: (stable && finalized).then_some(payload),
            forming_generations,
            forming_valid_until,
        })
    }
}
async fn wait_slot(slot: Arc<Slot>, open: i64, deadline: Instant) {
    // Subscribe before state inspection; watch retains a close even if it races
    // with the check. No global broadcast and no periodic 25ms polling.
    let mut change = slot.changed.subscribe();
    while slot.is_trading() && slot.pending(open) {
        if tokio::time::timeout_at(deadline, change.changed())
            .await
            .is_err()
        {
            break;
        }
    }
}
