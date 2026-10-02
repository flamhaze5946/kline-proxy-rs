//! Request admission for an HTTP route group: a fixed number of concurrent requests, and a
//! bounded FIFO queue in front of it instead of an immediate 503 when the group is full.
//!
//! At an interval boundary every bulk request of the fleet waits for the just-closed bars, so the
//! group's concurrency can fill for a few hundred milliseconds although nothing is wrong; those
//! requests should wait for a free place, not fail. The queue length and the wait stay bounded so
//! that a real overload still answers 503 -1008 quickly.
//!
//! The moment a request reaches its route group is kept in [`REQUEST_ENTRY`] for the rest of the
//! request, so a handler's own latency budget (for example the market query's five seconds) is
//! measured from arrival and includes the time spent queueing here. A route with such a budget
//! passes it to [`Gate::enter`], so the request never queues past it: once the budget is gone the
//! handler could only fail.
use crate::Metrics;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

tokio::task_local! {
    /// When the current HTTP request reached its route group (before any admission queueing).
    pub static REQUEST_ENTRY: Instant;
}

/// The end of a `budget` that started when the current request arrived; outside a request
/// (background work, tests) the budget starts now.
pub fn budget_deadline(budget: Duration) -> Instant {
    REQUEST_ENTRY
        .try_with(|entry| *entry)
        .unwrap_or_else(|_| Instant::now())
        + budget
}

/// Health, readiness, metrics and actuator routes bypass admission: they are cheap, and probes
/// and scrapers must not queue behind a boundary burst of bulk requests.
pub fn is_management(path: &str) -> bool {
    path.starts_with("/health") || path.starts_with("/actuator") || path == "/metrics"
}

pub struct Gate {
    permits: Arc<Semaphore>,
    waiting: AtomicUsize,
    queue: usize,
    wait: Duration,
}
impl Gate {
    pub fn new(limit: usize, queue: usize, wait: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            waiting: AtomicUsize::new(0),
            queue,
            wait,
        }
    }
    /// A place in the group for a request that arrived at `entry`, or `None` when the queue is
    /// full or the wait passes first. The wait is the gate's own, shortened to `budget` when the
    /// route has a smaller total budget of its own. The deadline is strict: a place granted after
    /// it is handed back. Counters and the waiting gauge go to `metrics`, shared by every route
    /// group.
    pub async fn enter(
        &self,
        metrics: &Metrics,
        entry: Instant,
        budget: Option<Duration>,
    ) -> Option<OwnedSemaphorePermit> {
        if let Ok(permit) = self.permits.clone().try_acquire_owned() {
            return Some(permit);
        }
        let reject = || {
            metrics
                .http_admission_rejected
                .fetch_add(1, Ordering::Relaxed);
            None
        };
        let deadline = entry + budget.map_or(self.wait, |budget| budget.min(self.wait));
        if Instant::now() >= deadline {
            return reject();
        }
        struct Waiting<'a>(&'a AtomicUsize, &'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
                self.1.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let reserved = self.waiting.fetch_add(1, Ordering::AcqRel);
        metrics
            .http_admission_waiting
            .fetch_add(1, Ordering::AcqRel);
        let waiting = Waiting(&self.waiting, &metrics.http_admission_waiting);
        if reserved >= self.queue {
            drop(waiting);
            return reject();
        }
        metrics
            .http_admission_queued
            .fetch_add(1, Ordering::Relaxed);
        let permit = tokio::time::timeout_at(deadline, self.permits.clone().acquire_owned()).await;
        drop(waiting);
        match permit {
            // Tokio polls the acquisition before the timer, so a place freed while this task was
            // not being polled can be granted after the deadline; it goes back to the next waiter.
            Ok(Ok(permit)) if Instant::now() <= deadline => Some(permit),
            _ => reject(),
        }
    }
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    fn counts(m: &Metrics) -> (u64, u64, usize) {
        (
            m.http_admission_queued.load(Relaxed),
            m.http_admission_rejected.load(Relaxed),
            m.http_admission_waiting.load(Relaxed),
        )
    }
    async fn until(mut done: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not reached");
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_group_queues_in_order_and_admits_as_places_free() {
        let metrics = Arc::new(Metrics::default());
        let gate = Arc::new(Gate::new(1, 8, Duration::from_secs(5)));
        let held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut waiters = Vec::new();
        for i in 0..3 {
            let (g, m, tx) = (gate.clone(), metrics.clone(), order_tx.clone());
            waiters.push(tokio::spawn(async move {
                let permit = g.enter(&m, Instant::now(), None).await.unwrap();
                tx.send(i).unwrap();
                drop(permit);
            }));
            until(|| gate.waiting() == i + 1).await;
        }
        assert_eq!(counts(&metrics), (3, 0, 3));
        // A newcomer must not overtake registered waiters when a place frees.
        drop(held);
        let newcomer = gate.enter(&metrics, Instant::now(), None).await;
        for waiter in waiters {
            waiter.await.unwrap();
        }
        drop(newcomer);
        let order: Vec<_> = std::iter::from_fn(|| order_rx.try_recv().ok()).collect();
        assert_eq!(order, vec![0, 1, 2]);
        assert_eq!(counts(&metrics).2, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_queue_rejects_at_once_and_an_expired_wait_rejects() {
        let metrics = Metrics::default();
        let gate = Gate::new(1, 1, Duration::from_millis(30));
        let _held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let queued = gate.enter(&metrics, Instant::now(), None);
        tokio::pin!(queued);
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert_eq!(gate.waiting(), 1);
        let started = Instant::now();
        assert!(gate.enter(&metrics, Instant::now(), None).await.is_none());
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "a full queue rejects at once"
        );
        assert!(queued.await.is_none());
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert_eq!(counts(&metrics), (1, 2, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_place_granted_after_the_deadline_is_handed_back() {
        let metrics = Arc::new(Metrics::default());
        let gate = Arc::new(Gate::new(1, 8, Duration::from_millis(50)));
        let held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let waiter = tokio::spawn({
            let (gate, metrics) = (gate.clone(), metrics.clone());
            async move { gate.enter(&metrics, Instant::now(), None).await.is_some() }
        });
        until(|| gate.waiting() == 1).await;
        // Free the place and move past the deadline before the waiter is polled again: the
        // semaphore has already assigned the place, but it arrives too late to be used.
        drop(held);
        tokio::time::advance(Duration::from_millis(60)).await;
        assert!(!waiter.await.unwrap());
        assert_eq!(counts(&metrics), (1, 1, 0));
        assert!(gate.enter(&metrics, Instant::now(), None).await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_already_past_its_wait_is_rejected_without_queueing() {
        let metrics = Metrics::default();
        let gate = Gate::new(1, 8, Duration::from_millis(50));
        let _held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let entry = Instant::now();
        tokio::time::advance(Duration::from_millis(80)).await;
        assert!(gate.enter(&metrics, entry, None).await.is_none());
        assert_eq!(counts(&metrics), (0, 1, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_waiter_leaves_the_queue() {
        let metrics = Arc::new(Metrics::default());
        let gate = Arc::new(Gate::new(1, 1, Duration::from_secs(5)));
        let held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let waiter = tokio::spawn({
            let (gate, metrics) = (gate.clone(), metrics.clone());
            async move { gate.enter(&metrics, Instant::now(), None).await.is_some() }
        });
        until(|| gate.waiting() == 1).await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(gate.waiting(), 0);
        assert_eq!(counts(&metrics).2, 0);
        drop(held);
        assert!(gate.enter(&metrics, Instant::now(), None).await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn two_groups_share_counters_and_the_waiting_gauge() {
        let metrics = Arc::new(Metrics::default());
        let a = Arc::new(Gate::new(1, 2, Duration::from_millis(40)));
        let b = Arc::new(Gate::new(1, 1, Duration::from_secs(5)));
        let held_a = a.enter(&metrics, Instant::now(), None).await.unwrap();
        let held_b = b.enter(&metrics, Instant::now(), None).await.unwrap();
        let wait = |gate: &Arc<Gate>| {
            let (gate, metrics) = (gate.clone(), metrics.clone());
            tokio::spawn(async move {
                let place = gate.enter(&metrics, Instant::now(), None).await;
                let admitted = place.is_some();
                // Keep the place well past every other deadline in this test.
                tokio::time::sleep(Duration::from_millis(500)).await;
                drop(place);
                admitted
            })
        };
        let a_first = wait(&a);
        until(|| a.waiting() == 1).await;
        let a_second = wait(&a);
        let b_cancelled = wait(&b);
        until(|| a.waiting() == 2 && b.waiting() == 1).await;
        assert_eq!(counts(&metrics).2, 3);
        assert!(
            b.enter(&metrics, Instant::now(), None).await.is_none(),
            "b's queue is full"
        );
        b_cancelled.abort();
        assert!(b_cancelled.await.unwrap_err().is_cancelled());
        // At 30 ms `a` frees its place: the first waiter gets it, the second times out at 40 ms.
        tokio::time::advance(Duration::from_millis(30)).await;
        drop(held_a);
        assert!(a_first.await.unwrap());
        assert!(!a_second.await.unwrap());
        drop(held_b);
        assert_eq!(counts(&metrics), (3, 2, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_route_budget_shorter_than_the_wait_ends_the_queueing_early() {
        let metrics = Metrics::default();
        let gate = Gate::new(1, 8, Duration::from_millis(800));
        let _held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let started = Instant::now();
        let budget = Some(Duration::from_millis(100));
        assert!(gate.enter(&metrics, started, budget).await.is_none());
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        // A budget longer than the gate's wait does not extend it.
        let started = Instant::now();
        let budget = Some(Duration::from_secs(5));
        assert!(gate.enter(&metrics, started, budget).await.is_none());
        assert_eq!(started.elapsed(), Duration::from_millis(800));
        // A request that has already used up its budget elsewhere is refused without queueing.
        let entry = Instant::now() - Duration::from_millis(100);
        let budget = Some(Duration::from_millis(100));
        assert!(gate.enter(&metrics, entry, budget).await.is_none());
        assert_eq!(counts(&metrics), (2, 3, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_cancelled_after_its_place_was_assigned_hands_the_place_on() {
        let metrics = Arc::new(Metrics::default());
        let gate = Arc::new(Gate::new(1, 8, Duration::from_secs(5)));
        let held = gate.enter(&metrics, Instant::now(), None).await.unwrap();
        let wait = || {
            let (gate, metrics) = (gate.clone(), metrics.clone());
            tokio::spawn(async move { gate.enter(&metrics, Instant::now(), None).await.is_some() })
        };
        let first = wait();
        until(|| gate.waiting() == 1).await;
        let second = wait();
        until(|| gate.waiting() == 2).await;
        // Releasing assigns the place to the first waiter, which is cancelled before it runs.
        drop(held);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            second.await.unwrap(),
            "the assigned place must pass to the next waiter"
        );
        assert_eq!(counts(&metrics), (2, 0, 0));
        assert_eq!(gate.permits.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_budget_runs_from_the_request_entry_inside_a_request_and_from_now_outside() {
        let entry = Instant::now();
        tokio::time::advance(Duration::from_millis(4_990)).await;
        let inside = REQUEST_ENTRY
            .scope(entry, async { budget_deadline(Duration::from_secs(5)) })
            .await;
        assert_eq!(inside, entry + Duration::from_secs(5));
        assert_eq!(
            budget_deadline(Duration::from_secs(5)),
            Instant::now() + Duration::from_secs(5)
        );
    }
}
