use crate::{BulkReply, ServiceError};
use kline_core::{Interval, Market};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct Key {
    pub market: Market,
    pub interval: Interval,
    pub limit: usize,
    pub closed_only: bool,
    pub ids: Arc<[usize]>,
    pub boundary: i64,
    pub revision: u64,
}
struct Ready {
    at: Instant,
    reply: Arc<BulkReply>,
}
type Flight = OnceCell<Arc<BulkReply>>;
/// An in-flight key holds one admission slot until its entry leaves the map. `holders` counts
/// the live registrations and changes only under the state mutex, so the last one to drop is
/// the one that removes the entry (an `Arc::strong_count` check could not be: the count falls
/// after the mutex is released, and two concurrent drops could each see the other alive).
struct FlightEntry {
    cell: Arc<Flight>,
    holders: usize,
    /// When waiting for the key's finals ends, fixed when the flight starts: a holder that takes
    /// over the build from a cancelled one keeps it, whatever the clock reads by then.
    final_deadline: tokio::time::Instant,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct State {
    ready: HashMap<Key, Ready>,
    flights: HashMap<Key, FlightEntry>,
    bytes: usize,
}
/// Bounds on distinct in-flight keys. A new key that finds every slot taken waits in a
/// bounded FIFO queue instead of failing at once: at an interval boundary every closed_only
/// key holds its slot until the just-closed bars are final, so a burst larger than the slot
/// count would otherwise be rejected although the slots free within the final wait.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Admission {
    pub slots: usize,
    pub queue: usize,
    pub wait: Duration,
}
pub(crate) struct Cache {
    state: Mutex<State>,
    max_bytes: usize,
    slots: Arc<Semaphore>,
    waiting: AtomicUsize,
    admission: Admission,
}
pub(crate) enum Lookup<'a> {
    Ready(Arc<BulkReply>),
    Flight(Registration<'a>),
    /// Every admission slot is taken; acquire one with [`Cache::admit_until`] and look up again.
    Full,
}
pub(crate) struct Registration<'a> {
    cache: &'a Cache,
    key: Key,
    pub cell: Arc<Flight>,
    pub payload: Option<Arc<crate::bulk::Payload>>,
    pub final_deadline: tokio::time::Instant,
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut state = self.cache.state.lock();
        let last = match state.flights.get_mut(&self.key) {
            Some(entry) if Arc::ptr_eq(&entry.cell, &self.cell) => {
                entry.holders -= 1;
                entry.holders == 0
            }
            // The entry was replaced (stale reply) or removed: this registration no longer counts.
            _ => false,
        };
        if last {
            state.flights.remove(&self.key);
        }
    }
}
impl Cache {
    pub fn new(max_bytes: usize, admission: Admission) -> Self {
        Self {
            state: Mutex::default(),
            max_bytes,
            slots: Arc::new(Semaphore::new(admission.slots)),
            waiting: AtomicUsize::new(0),
            admission,
        }
    }
    /// The longest a request may wait for a slot, across all its lookups.
    pub fn admission_wait(&self) -> Duration {
        self.admission.wait
    }
    /// Wait in FIFO order, until `deadline`, for a slot to register a new in-flight key.
    /// `queued` counts requests that entered the queue. Busy when the queue is full or the
    /// deadline passes; the deadline is strict, a slot granted after it is handed back.
    pub async fn admit_until(
        &self,
        deadline: tokio::time::Instant,
        queued: &AtomicU64,
    ) -> Result<OwnedSemaphorePermit, ServiceError> {
        struct Queued<'a>(&'a AtomicUsize);
        impl Drop for Queued<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ServiceError::Busy);
        }
        let reservation = Queued(&self.waiting);
        if self.waiting.fetch_add(1, Ordering::AcqRel) >= self.admission.queue {
            return Err(ServiceError::Busy);
        }
        queued.fetch_add(1, Ordering::Relaxed);
        let permit = tokio::time::timeout_at(deadline, self.slots.clone().acquire_owned()).await;
        drop(reservation);
        match permit {
            // Tokio polls the acquisition before the timer, so a slot freed while this task was
            // not being polled can be granted after the deadline; it goes back to the next waiter.
            Ok(Ok(permit)) if tokio::time::Instant::now() <= deadline => Ok(permit),
            _ => Err(ServiceError::Busy),
        }
    }
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }
    /// Live registrations over all in-flight keys (leaders and followers).
    pub fn holders(&self) -> usize {
        self.state.lock().flights.values().map(|e| e.holders).sum()
    }
    /// `slot` is a permit from [`Cache::admit_until`]; it is used only if this call registers a
    /// new in-flight key, otherwise it is released on return. `final_deadline` becomes the new
    /// flight's final-wait deadline; joining an existing flight keeps that flight's.
    pub fn get(
        &self,
        key: Key,
        current: impl Fn(&BulkReply) -> bool,
        slot: Option<OwnedSemaphorePermit>,
        final_deadline: tokio::time::Instant,
    ) -> Result<Lookup<'_>, ServiceError> {
        let mut state = self.state.lock();
        if let Some(cached) = state.ready.get(&key)
            && cached.at.elapsed() < Duration::from_secs(1)
            && current(&cached.reply)
        {
            return Ok(Lookup::Ready(cached.reply.clone()));
        }
        let mut payload = None;
        if let Some(old) = state.ready.remove(&key) {
            state.bytes -= old.reply.cache_bytes();
            if current(&old.reply) {
                payload = old.reply.payload.clone();
            }
        }
        if state
            .flights
            .get(&key)
            .and_then(|entry| entry.cell.get())
            .is_some_and(|reply| !current(reply))
        {
            state.flights.remove(&key);
        }
        if let Some(entry) = state.flights.get_mut(&key) {
            entry.holders += 1;
            let (cell, final_deadline) = (entry.cell.clone(), entry.final_deadline);
            return Ok(Lookup::Flight(Registration {
                cache: self,
                key,
                cell,
                payload,
                final_deadline,
            }));
        }
        let slot = match slot {
            Some(slot) => slot,
            None => match self.slots.clone().try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => return Ok(Lookup::Full),
            },
        };
        let cell = Arc::new(OnceCell::new());
        state.flights.insert(
            key.clone(),
            FlightEntry {
                cell: cell.clone(),
                holders: 1,
                final_deadline,
                _slot: slot,
            },
        );
        Ok(Lookup::Flight(Registration {
            cache: self,
            key,
            cell,
            payload,
            final_deadline,
        }))
    }
    pub fn put(&self, key: Key, reply: Arc<BulkReply>) {
        if !reply.finalized
            || (key.closed_only && reply.payload.is_none())
            || reply.cache_bytes() > self.max_bytes
        {
            return;
        }
        let mut state = self.state.lock();
        if let Some(old) = state.ready.remove(&key) {
            state.bytes -= old.reply.cache_bytes();
        }
        while state.ready.len() >= 64 || state.bytes + reply.cache_bytes() > self.max_bytes {
            let oldest = state
                .ready
                .iter()
                .min_by_key(|(_, v)| v.at)
                .map(|(k, _)| k.clone());
            let Some(oldest) = oldest else {
                break;
            };
            let removed = state.ready.remove(&oldest).expect("entry under cache lock");
            state.bytes -= removed.reply.cache_bytes();
        }
        state.bytes += reply.cache_bytes();
        state.ready.insert(
            key,
            Ready {
                at: Instant::now(),
                reply,
            },
        );
    }
    pub fn sizes(&self) -> (usize, usize, usize) {
        let s = self.state.lock();
        (s.ready.len(), s.flights.len(), s.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BulkReply;

    fn cache(slots: usize) -> Arc<Cache> {
        Arc::new(Cache::new(
            1 << 20,
            Admission {
                slots,
                queue: 8,
                wait: Duration::from_millis(50),
            },
        ))
    }
    fn key() -> Key {
        Key {
            market: Market::Future,
            interval: Interval::parse("1h").unwrap(),
            limit: 1,
            closed_only: true,
            ids: Arc::from([0usize]),
            boundary: 3_600_000,
            revision: 0,
        }
    }
    fn flight(lookup: Result<Lookup<'_>, ServiceError>) -> Registration<'_> {
        match lookup {
            Ok(Lookup::Flight(registration)) => registration,
            _ => panic!("expected an in-flight registration"),
        }
    }
    /// Until `cache` has `n` queued waiters; fails instead of hanging when a waiter ends first.
    async fn until_queued(cache: &Cache, n: usize, waiters: &[&tokio::task::JoinHandle<bool>]) {
        for _ in 0..10_000 {
            if cache.waiting() == n {
                return;
            }
            assert!(
                waiters.iter().all(|w| !w.is_finished()),
                "a waiter ended before it queued"
            );
            tokio::task::yield_now().await;
        }
        panic!("the waiters never queued");
    }

    #[tokio::test(start_paused = true)]
    async fn a_slot_granted_after_the_admission_deadline_is_handed_back() {
        let cache = cache(1);
        let held = cache.slots.clone().try_acquire_owned().unwrap();
        let queued = Arc::new(AtomicU64::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        let waiter = tokio::spawn({
            let (cache, queued) = (cache.clone(), queued.clone());
            async move { cache.admit_until(deadline, &queued).await.is_ok() }
        });
        until_queued(&cache, 1, &[&waiter]).await;
        // The slot frees, then time passes the deadline before the waiter runs again.
        drop(held);
        tokio::time::advance(Duration::from_millis(60)).await;
        assert!(!waiter.await.unwrap());
        assert_eq!(queued.load(Ordering::Relaxed), 1);
        assert_eq!(cache.waiting(), 0);
        assert_eq!(
            cache.slots.available_permits(),
            1,
            "the late slot went back"
        );
        let expired = tokio::time::Instant::now();
        assert!(cache.admit_until(expired, &queued).await.is_err());
        assert_eq!(
            queued.load(Ordering::Relaxed),
            1,
            "an expired deadline never queues"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_cancelled_after_its_slot_was_assigned_hands_the_slot_on() {
        let cache = cache(1);
        let held = cache.slots.clone().try_acquire_owned().unwrap();
        let queued = Arc::new(AtomicU64::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let wait = || {
            let (cache, queued) = (cache.clone(), queued.clone());
            tokio::spawn(async move { cache.admit_until(deadline, &queued).await.is_ok() })
        };
        let first = wait();
        until_queued(&cache, 1, &[&first]).await;
        let second = wait();
        until_queued(&cache, 2, &[&first, &second]).await;
        // Releasing assigns the slot to the first waiter, which is cancelled before it runs.
        drop(held);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            second.await.unwrap(),
            "the assigned slot must pass to the next waiter"
        );
        assert_eq!(cache.waiting(), 0);
        assert_eq!(cache.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn a_granted_slot_goes_back_when_the_key_is_already_in_flight() {
        let cache = cache(2);
        let leader = flight(cache.get(key(), |_| true, None, tokio::time::Instant::now()));
        // As if granted by admit_until while the leader registered the same key.
        let granted = cache.slots.clone().try_acquire_owned().unwrap();
        assert_eq!(cache.slots.available_permits(), 0);
        let follower =
            flight(cache.get(key(), |_| true, Some(granted), tokio::time::Instant::now()));
        assert!(Arc::ptr_eq(&leader.cell, &follower.cell));
        assert_eq!(
            cache.slots.available_permits(),
            1,
            "a follower needs no slot"
        );
        assert_eq!((cache.sizes().1, cache.holders()), (1, 2));
        drop(leader);
        drop(follower);
        assert_eq!((cache.sizes().1, cache.holders()), (0, 0));
        assert_eq!(cache.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn a_granted_slot_goes_back_when_the_key_is_ready() {
        let cache = cache(1);
        let key = Key {
            closed_only: false,
            ..key()
        };
        cache.put(key.clone(), Arc::new(BulkReply::empty()));
        // As if granted by admit_until while another request built the key.
        let granted = cache.slots.clone().try_acquire_owned().unwrap();
        assert!(matches!(
            cache.get(key, |_| true, Some(granted), tokio::time::Instant::now()),
            Ok(Lookup::Ready(_))
        ));
        assert_eq!(
            cache.slots.available_permits(),
            1,
            "a cache hit needs no slot"
        );
        assert_eq!(cache.sizes().1, 0);
    }

    #[tokio::test]
    async fn a_replaced_flight_is_not_removed_by_its_old_holders() {
        let cache = cache(2);
        let old = flight(cache.get(key(), |_| true, None, tokio::time::Instant::now()));
        old.cell.set(Arc::new(BulkReply::empty())).unwrap();
        // A later lookup finds the finished reply stale and starts a new flight for the key; the
        // replaced entry's slot goes back although its registration is still alive.
        let new = flight(cache.get(key(), |_| false, None, tokio::time::Instant::now()));
        assert!(!Arc::ptr_eq(&old.cell, &new.cell));
        assert_eq!(cache.slots.available_permits(), 1);
        drop(old);
        assert_eq!(
            (cache.sizes().1, cache.holders()),
            (1, 1),
            "the old holder must leave the new flight alone"
        );
        drop(new);
        assert_eq!((cache.sizes().1, cache.holders()), (0, 0));
        assert_eq!(cache.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn a_holder_that_panics_while_building_leaves_the_flight_to_the_others() {
        let cache = cache(1);
        let other = flight(cache.get(key(), |_| true, None, tokio::time::Instant::now()));
        let builder = tokio::spawn({
            let cache = cache.clone();
            async move {
                let registration =
                    flight(cache.get(key(), |_| true, None, tokio::time::Instant::now()));
                let _built: Result<_, ServiceError> = registration
                    .cell
                    .get_or_try_init(|| async { panic!("build failed") })
                    .await;
            }
        });
        assert!(builder.await.unwrap_err().is_panic());
        // Unwinding dropped the builder's registration: the other holder still holds the key and
        // can initialise the flight itself.
        assert_eq!((cache.sizes().1, cache.holders()), (1, 1));
        assert!(other.cell.get().is_none());
        drop(other);
        assert_eq!((cache.sizes().1, cache.holders()), (0, 0));
        assert_eq!(cache.slots.available_permits(), 1);
    }
}
