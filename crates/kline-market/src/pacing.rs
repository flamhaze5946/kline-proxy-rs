//! One weighted budget per upstream market, with cancellable priority admission.
//! Urgent repair can spend a small reserved burst, never bypass shared cooldown
//! or the rolling minute budget. Ordinary work keeps the existing smooth pacing.
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Urgent,
    /// Exchange metadata has its own quota; bootstrap is also Foreground and
    /// must not consume the headroom required by an indivisible weight-20 load.
    Metadata,
    /// Small complete price snapshots have a one-second queue deadline. They
    /// can borrow bounded pacing headroom, but never the shared minute quota.
    PriceRefresh,
    Foreground,
    Background,
}
struct Waiting {
    priority: Priority,
    weight: u32,
    since: Instant,
}
struct Grant {
    at: Instant,
    weight: u32,
    priority: Priority,
    price_burst: u32,
    metadata_burst: u32,
    foreground_burst: u32,
}
struct State {
    next: Instant,
    blocked_until: Instant,
    sequence: u64,
    used: u32,
    background_used: u32,
    urgent_used: u32,
    metadata_used: u32,
    metadata_burst_used: u32,
    foreground_burst_used: u32,
    price_burst_used: u32,
    price_used: u32,
    waiting: BTreeMap<u64, Waiting>,
    grants: VecDeque<Grant>,
}
pub(crate) struct Pacer {
    state: Mutex<State>,
    signal: Notify,
    budget: u32,
    reserve: u32,
    price_reserve: u32,
    metadata_reserve: u32,
    foreground_burst_limit: u32,
}
pub(crate) struct PaceWait {
    pub elapsed: Duration,
    pub rounds: u32,
    pub cooldown_rounds: u32,
}
struct Ticket {
    pacer: Arc<Pacer>,
    id: u64,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.pacer.state.lock().waiting.remove(&self.id);
        self.pacer.signal.notify_waiters();
    }
}
impl Pacer {
    pub fn new(budget: u32) -> Arc<Self> {
        Self::with_metadata_reserve(budget, (budget / 20).min(60))
    }
    pub fn funding(budget: u32) -> Arc<Self> {
        Self::with_metadata_reserve(budget, 0)
    }
    fn with_metadata_reserve(budget: u32, metadata_reserve: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                next: Instant::now(),
                blocked_until: Instant::now(),
                sequence: 0,
                used: 0,
                background_used: 0,
                urgent_used: 0,
                metadata_used: 0,
                metadata_burst_used: 0,
                foreground_burst_used: 0,
                price_burst_used: 0,
                price_used: 0,
                waiting: BTreeMap::new(),
                grants: VecDeque::new(),
            }),
            signal: Notify::new(),
            budget,
            reserve: if metadata_reserve == 0 {
                0
            } else {
                (budget / 20).min(20)
            },
            price_reserve: if metadata_reserve == 0 {
                0
            } else {
                (budget / 10).min(120)
            },
            metadata_reserve,
            foreground_burst_limit: (budget / 20).min(60),
        })
    }
    pub fn cooldown(&self, until: Instant) {
        let mut state = self.state.lock();
        state.blocked_until = state.blocked_until.max(until);
        drop(state);
        self.signal.notify_waiters();
    }
    fn due(&self, state: &State, request: &Waiting, now: Instant) -> Instant {
        let total = state.used;
        let background = state.background_used;
        let urgent = state.urgent_used;
        let can_borrow = request.priority == Priority::Urgent
            && urgent.saturating_add(request.weight) <= self.reserve;
        let paced = if can_borrow {
            now
        } else if (request.priority == Priority::PriceRefresh
            && state.price_burst_used.saturating_add(request.weight) <= self.price_reserve)
            || (request.priority == Priority::Metadata
                && state.metadata_burst_used.saturating_add(request.weight)
                    <= self.metadata_reserve)
            || (request.priority == Priority::Foreground
                && state.foreground_burst_used.saturating_add(request.weight)
                    <= self.foreground_burst_limit)
        {
            state.next.min(request.since + Duration::from_secs(1))
        } else {
            state.next
        };
        let mut due = state.blocked_until.max(paced);
        // A configured budget smaller than a single indivisible request retains
        // conservative debt pacing; normal production requests fit the budget.
        // Keep room for weight-20 exchangeInfo independently of foreground
        // bootstrap. Otherwise a stream of eligible weight-2 jobs can spend
        // each released token before that older, larger request ever fits.
        // Reserve every latency-sensitive lane independently. A long bootstrap
        // is tagged Urgent too, but cannot consume the price/metadata budgets.
        // Keep one indivisible price/metadata request available after bursts
        // are spent, so a stream of weight-1 recovery work cannot starve it.
        let metadata_room = self
            .metadata_reserve
            .saturating_sub(state.metadata_used)
            .max(self.metadata_reserve.min(20));
        let price_room = self
            .price_reserve
            .saturating_sub(state.price_used)
            .max(self.price_reserve.min(4));
        let urgent_room = self.reserve.saturating_sub(state.urgent_used);
        let reserved = if request.priority == Priority::Metadata {
            0
        } else {
            metadata_room
        } + if request.priority == Priority::PriceRefresh {
            0
        } else {
            price_room
        } + if request.priority == Priority::Urgent {
            0
        } else {
            urgent_room
        };
        let cap = self.budget.saturating_sub(reserved).max(request.weight);
        let background_cap = self
            .budget
            .saturating_sub(self.reserve + self.price_reserve + self.metadata_reserve)
            .max(request.weight);
        let mut total = total.saturating_add(request.weight);
        let mut background =
            background.saturating_add(if request.priority == Priority::Background {
                request.weight
            } else {
                0
            });
        for grant in &state.grants {
            if total <= cap
                && (request.priority != Priority::Background || background <= background_cap)
            {
                break;
            }
            due = due.max(grant.at + Duration::from_secs(60));
            total = total.saturating_sub(grant.weight);
            if grant.priority == Priority::Background {
                background = background.saturating_sub(grant.weight);
            }
        }
        due
    }
    pub async fn acquire(self: &Arc<Self>, weight: u32, priority: Priority) -> PaceWait {
        let started = Instant::now();
        let ticket = {
            let mut state = self.state.lock();
            state.sequence += 1;
            let id = state.sequence;
            state.waiting.insert(
                id,
                Waiting {
                    priority,
                    weight,
                    since: started,
                },
            );
            Ticket {
                pacer: self.clone(),
                id,
            }
        };
        self.signal.notify_waiters();
        let mut rounds = 0;
        let mut cooldown_rounds = 0;
        loop {
            let notified = self.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let wait_until = {
                let mut state = self.state.lock();
                let now = Instant::now();
                while state
                    .grants
                    .front()
                    .is_some_and(|g| g.at + Duration::from_secs(60) <= now)
                {
                    let old = state.grants.pop_front().unwrap();
                    state.used -= old.weight;
                    if old.priority == Priority::Background {
                        state.background_used -= old.weight;
                    }
                    if old.priority == Priority::Urgent {
                        state.urgent_used -= old.weight;
                    }
                    if old.priority == Priority::Metadata {
                        state.metadata_used -= old.weight;
                        state.metadata_burst_used -= old.metadata_burst;
                    }
                    state.price_burst_used -= old.price_burst;
                    if old.priority == Priority::PriceRefresh {
                        state.price_used -= old.weight;
                    }
                    state.foreground_burst_used -= old.foreground_burst;
                }
                if state.blocked_until > now {
                    cooldown_rounds += 1;
                }
                let mut earliest = now + Duration::from_secs(60);
                let mut chosen = None;
                for (&id, request) in &state.waiting {
                    let due = self.due(&state, request, now);
                    earliest = earliest.min(due.max(now));
                    if due <= now {
                        // Aging applies only to eligible work, so an old sleeping
                        // background request cannot hold the urgent reserve hostage.
                        let rank = if (request.priority == Priority::Background
                            && now.duration_since(request.since) >= Duration::from_secs(2))
                            || (request.priority == Priority::Urgent
                                && state.urgent_used.saturating_add(request.weight) > self.reserve)
                        {
                            Priority::Foreground as u8
                        } else {
                            request.priority as u8
                        };
                        let candidate = (rank, id);
                        if chosen.is_none_or(|current| candidate < current) {
                            chosen = Some(candidate);
                        }
                    }
                }
                if chosen.is_some_and(|(_, id)| id == ticket.id) {
                    state.waiting.remove(&ticket.id);
                    state.used += weight;
                    if priority == Priority::Background {
                        state.background_used += weight;
                    }
                    if priority == Priority::Urgent {
                        state.urgent_used += weight;
                    }
                    let metadata_burst = if priority == Priority::Metadata && now < state.next {
                        weight
                    } else {
                        0
                    };
                    if priority == Priority::Metadata {
                        state.metadata_used += weight;
                        state.metadata_burst_used += metadata_burst;
                    }
                    let price_burst = if priority == Priority::PriceRefresh && now < state.next {
                        weight
                    } else {
                        0
                    };
                    state.price_burst_used += price_burst;
                    if priority == Priority::PriceRefresh {
                        state.price_used += weight;
                    }
                    let foreground_burst = if priority == Priority::Foreground && now < state.next {
                        weight
                    } else {
                        0
                    };
                    state.foreground_burst_used += foreground_burst;
                    state.grants.push_back(Grant {
                        at: now,
                        weight,
                        priority,
                        price_burst,
                        metadata_burst,
                        foreground_burst,
                    });
                    state.next = state.next.max(now)
                        + Duration::from_secs_f64(60. * weight as f64 / self.budget as f64);
                    return PaceWait {
                        elapsed: started.elapsed(),
                        rounds,
                        cooldown_rounds,
                    };
                }
                // Eligible peers will signal after admission/cancellation; do not
                // spin on a deadline in the past while their task is scheduled.
                if earliest <= now {
                    now + Duration::from_secs(60)
                } else {
                    earliest
                }
            };
            rounds += 1;
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep_until(wait_until) => {} }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bootstrap_foreground_cannot_spend_the_metadata_reserve() {
        let p = Pacer::new(1200);
        let now = Instant::now();
        let mut state = p.state.lock();
        state.used = 1140;
        state.background_used = 0;
        state.price_burst_used = 120;
        state.price_used = 140;
        state.next = now + Duration::from_secs(57);
        state.grants.push_back(Grant {
            at: now,
            weight: 1000,
            priority: Priority::Foreground,
            price_burst: 0,
            metadata_burst: 0,
            foreground_burst: 0,
        });
        state.grants.push_back(Grant {
            at: now,
            weight: 140,
            priority: Priority::PriceRefresh,
            price_burst: 120,
            metadata_burst: 0,
            foreground_burst: 0,
        });
        let metadata = Waiting {
            priority: Priority::Metadata,
            weight: 20,
            since: now,
        };
        let price = Waiting {
            priority: Priority::PriceRefresh,
            weight: 2,
            since: now,
        };
        assert_eq!(p.due(&state, &metadata, now), now + Duration::from_secs(1));
        assert!(p.due(&state, &price, now) >= now + Duration::from_secs(60));
        let bootstrap = Waiting {
            priority: Priority::Foreground,
            weight: 2,
            since: now,
        };
        assert!(p.due(&state, &bootstrap, now) >= now + Duration::from_secs(60));
        // Lifecycle bootstrap uses Urgent. After its small reserved burst is
        // spent, ordinary paced urgent work must preserve metadata headroom too.
        state.urgent_used = 1000;
        state.grants.front_mut().unwrap().priority = Priority::Urgent;
        let recovery = Waiting {
            priority: Priority::Urgent,
            weight: 2,
            since: now,
        };
        assert!(p.due(&state, &recovery, now) >= now + Duration::from_secs(60));
        assert_eq!(p.due(&state, &metadata, now), now + Duration::from_secs(1));
    }
    #[tokio::test(start_paused = true)]
    async fn metadata_weight_fits_reserved_headroom_during_price_and_history_pressure() {
        let p = Pacer::new(1200);
        p.acquire(1000, Priority::Background).await;
        for _ in 0..35 {
            p.acquire(4, Priority::PriceRefresh).await;
        }
        let queued_price = tokio::spawn({
            let p = p.clone();
            async move { p.acquire(4, Priority::PriceRefresh).await }
        });
        tokio::task::yield_now().await;
        let metadata = p.acquire(20, Priority::Metadata).await;
        assert!(
            metadata.elapsed <= Duration::from_secs(1),
            "small requests cannot spend the foreground reserve"
        );
        assert!(p.state.lock().used <= 1200);
        assert!(p.state.lock().metadata_burst_used <= 60);
        p.cooldown(Instant::now() + Duration::from_secs(5));
        assert!(p.acquire(20, Priority::Metadata).await.elapsed >= Duration::from_secs(5));
        queued_price.await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn sustained_bootstrap_cannot_starve_prices_or_metadata() {
        let p = Pacer::new(1200);
        let mut recovery = Vec::new();
        for _ in 0..8 {
            let p = p.clone();
            recovery.push(tokio::spawn(async move {
                loop {
                    p.acquire(2, Priority::Urgent).await;
                }
            }));
        }
        for step in 0..120 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let price = p.acquire(4, Priority::PriceRefresh).await;
            assert!(
                price.elapsed <= Duration::from_secs(2),
                "price queued {:?} at {step}",
                price.elapsed
            );
            if step % 40 == 10 {
                let metadata = p.acquire(20, Priority::Metadata).await;
                assert!(
                    metadata.elapsed <= Duration::from_secs(2),
                    "metadata queued {:?}",
                    metadata.elapsed
                );
            }
            assert!(p.state.lock().used <= 1200);
        }
        for job in recovery {
            job.abort();
            let _ = job.await;
        }
        assert!(p.state.lock().waiting.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn price_refresh_deadline_preempts_old_history_debt_but_honors_cooldown() {
        let p = Pacer::new(1200);
        p.acquire(80, Priority::Background).await; // Four seconds of smooth debt.
        let history = tokio::spawn({
            let p = p.clone();
            async move { p.acquire(80, Priority::Background).await }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        let price = p.acquire(4, Priority::PriceRefresh).await;
        assert_eq!(price.elapsed, Duration::from_secs(1));
        assert!(
            !history.is_finished(),
            "aged history cannot consume the price reserve"
        );
        p.cooldown(Instant::now() + Duration::from_secs(5));
        assert!(p.acquire(4, Priority::PriceRefresh).await.elapsed >= Duration::from_secs(5));
        history.await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn price_reserve_never_bypasses_the_rolling_minute_budget() {
        let p = Pacer::new(1200);
        p.acquire(1200, Priority::Foreground).await;
        assert!(p.acquire(4, Priority::PriceRefresh).await.elapsed >= Duration::from_secs(60));
        let state = p.state.lock();
        assert!(state.used <= 1200);
        assert!(state.price_burst_used <= p.price_reserve);
    }
    #[tokio::test(start_paused = true)]
    async fn urgent_preempts_sleeping_background_but_not_cooldown() {
        let p = Pacer::new(1200);
        p.acquire(40, Priority::Background).await;
        let background = tokio::spawn({
            let p = p.clone();
            async move { p.acquire(1, Priority::Background).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(p.acquire(1, Priority::Urgent).await.elapsed, Duration::ZERO);
        assert!(!background.is_finished());
        p.cooldown(Instant::now() + Duration::from_secs(3));
        let urgent = p.acquire(1, Priority::Urgent).await;
        assert!(urgent.elapsed >= Duration::from_secs(3));
        background.await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn cancelled_priority_waiter_releases_queue_and_rolling_budget_is_shared() {
        let p = Pacer::new(1200);
        p.acquire(1200, Priority::Foreground).await;
        let task = tokio::spawn({
            let p = p.clone();
            async move { p.acquire(1, Priority::Urgent).await }
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        task.abort();
        let _ = task.await;
        assert!(p.state.lock().waiting.is_empty());
        let start = Instant::now();
        p.acquire(1, Priority::Foreground).await;
        assert!(start.elapsed() >= Duration::from_secs(60));
    }
}
