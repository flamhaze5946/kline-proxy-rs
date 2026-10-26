//! Evidence scheduler bookkeeping; due demands are distinct from actual attempts.
use crate::evidence_recovery::{TaskKind, VerdictRange};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT_RECORD: AtomicU64 = AtomicU64::new(1);
use kline_core::evidence::MINUTE_MS as MINUTE;
pub const OFFSETS: [i64; 3] = [1500, 3000, 6000];

#[derive(Clone, Debug)]
pub(crate) struct Demand {
    pub due: i64,
    pub range: Option<VerdictRange>,
    pub attempts: u32,
    revision: u64,
}
impl Demand {
    fn new(now: i64) -> Self {
        Self {
            due: now,
            range: None,
            attempts: 0,
            revision: 0,
        }
    }
    fn require(&mut self, now: i64) {
        self.due = self.due.min(now);
        self.revision = self.revision.wrapping_add(1);
    }
    fn failed(&mut self, now: i64) {
        self.due = now.saturating_add(
            10_000_i64
                .saturating_mul(1_i64 << self.attempts.saturating_sub(1).min(5))
                .min(300_000),
        );
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Attempt {
    pub kind: TaskKind,
    incarnation: u64,
    revision: u64,
    pub epoch: u64,
    pub gap: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FallbackDue {
    Specified { minute: i64, index: usize },
    Target,
}

pub(crate) struct TaskRecord {
    pub tracking_generation: u64,
    incarnation: u64,
    unresolved: BTreeSet<i64>,
    pub initialized: bool,
    pub established: bool,
    pub epoch: u64,
    pub gap: u64,
    observed_epoch: u64,
    pub capacity: usize,
    pub startup: Demand,
    pub routine: Demand,
    pub fallback: Demand,
    pub history: Demand,
    pub running: bool,
    specified: BTreeMap<i64, [bool; 3]>,
}
impl TaskRecord {
    pub fn new(now: i64) -> Self {
        Self {
            tracking_generation: 0,
            incarnation: NEXT_RECORD.fetch_add(1, Ordering::Relaxed),
            unresolved: BTreeSet::new(),
            initialized: false,
            established: false,
            epoch: 0,
            gap: 0,
            observed_epoch: 0,
            capacity: 0,
            startup: Demand::new(now),
            routine: Demand::new(now),
            fallback: Demand::new(now),
            history: Demand::new(now),
            running: false,
            specified: BTreeMap::new(),
        }
    }
    pub fn observe(&mut self, now: i64, epoch: u64, capacity: usize) {
        if self.capacity > 0 && capacity > self.capacity {
            self.initialized = false;
            self.startup.require(now);
        }
        self.capacity = capacity;
        if self.observed_epoch != epoch {
            self.observed_epoch = epoch;
            // A new epoch during a request remains a new demand after it completes.
            self.routine.require(now);
        }
        self.specified
            .retain(|open, _| *open >= now.div_euclid(MINUTE) * MINUTE - 97 * MINUTE);
        self.unresolved
            .retain(|open| self.specified.contains_key(open));
    }
    pub fn observe_latest(&mut self, latest: i64, needs_verdict: bool) {
        if needs_verdict {
            self.specified.entry(latest).or_insert([false; 3]);
            self.unresolved.insert(latest);
        } else {
            self.unresolved.remove(&latest);
        }
    }
    pub fn resolve_specified(&mut self, mut needs: impl FnMut(i64) -> bool) {
        self.unresolved.retain(|open| needs(*open));
    }
    pub fn fallback_due(
        &self,
        now: i64,
        earliest: i64,
        latest: i64,
        last_request: i64,
    ) -> Option<FallbackDue> {
        for (&minute, consumed) in &self.specified {
            if !self.unresolved.contains(&minute) {
                continue;
            }
            for (index, &offset) in OFFSETS.iter().enumerate() {
                if !consumed[index] && now >= minute + MINUTE + offset {
                    return Some(FallbackDue::Specified { minute, index });
                }
            }
        }
        let latest_done = self
            .specified
            .get(&latest)
            .is_none_or(|attempts| attempts.iter().all(|v| *v));
        if (earliest < latest || (latest_done && now >= latest + MINUTE + 6000))
            && now >= self.fallback.due
            && now.saturating_sub(last_request) >= 5000
        {
            Some(FallbackDue::Target)
        } else {
            None
        }
    }
    pub fn sync_due(&self, now: i64, trading: bool) -> Option<TaskKind> {
        if !self.initialized {
            return (now >= self.startup.due).then_some(TaskKind::Startup);
        }
        if trading {
            (now >= self.routine.due).then_some(TaskKind::Routine)
        } else {
            (now >= self.history.due).then_some(TaskKind::History)
        }
    }
    fn demand(&mut self, kind: TaskKind) -> &mut Demand {
        match kind {
            TaskKind::Startup => &mut self.startup,
            TaskKind::Routine => &mut self.routine,
            TaskKind::Fallback => &mut self.fallback,
            TaskKind::History => &mut self.history,
        }
    }
    pub fn begin(
        &mut self,
        kind: TaskKind,
        range: VerdictRange,
        fallback: Option<FallbackDue>,
        epoch: u64,
        gap: u64,
    ) -> Attempt {
        if let Some(FallbackDue::Specified { minute, index }) = fallback {
            self.specified.get_mut(&minute).unwrap()[index] = true;
        }
        let demand = self.demand(kind);
        demand.attempts = demand.attempts.saturating_add(1);
        demand.range = Some(range);
        let revision = demand.revision;
        self.running = true;
        Attempt {
            kind,
            incarnation: self.incarnation,
            revision,
            epoch,
            gap,
        }
    }
    pub fn finish(&mut self, attempt: Attempt, now: i64, success: bool, interval_ms: i64) {
        if self.incarnation != attempt.incarnation {
            return;
        }
        self.running = false;
        // Unmet target-set work moves forward after completion, rather than
        // resending in place after a slow response. Untried designated points
        // remain independently due and may run immediately.
        self.fallback.due = now.saturating_add(5000);
        let unchanged = self.demand(attempt.kind).revision == attempt.revision;
        if success {
            // No short fallback, routine success or history success can initialize.
            if attempt.kind == TaskKind::Startup && unchanged {
                self.initialized = true;
                self.routine.due = if self.observed_epoch != attempt.epoch {
                    now
                } else {
                    now.saturating_add(interval_ms)
                };
                self.history.due = now.saturating_add(interval_ms);
            }
            if attempt.kind != TaskKind::Fallback {
                self.epoch = attempt.epoch;
                self.gap = attempt.gap;
            }
            let demand = self.demand(attempt.kind);
            demand.attempts = 0;
            if unchanged {
                if attempt.kind != TaskKind::Fallback {
                    demand.due = now.saturating_add(interval_ms);
                }
                demand.range = None;
            }
        } else if attempt.kind != TaskKind::Fallback {
            self.demand(attempt.kind).failed(now);
        }
    }
}

pub(crate) struct FallbackBudget {
    grants: VecDeque<i64>,
    limit: u32,
}
impl FallbackBudget {
    pub fn new(limit: u32) -> Self {
        Self {
            grants: VecDeque::new(),
            limit,
        }
    }
    pub fn available(&mut self, now: i64) -> bool {
        while self
            .grants
            .front()
            .is_some_and(|time| now.saturating_sub(*time) >= MINUTE)
        {
            self.grants.pop_front();
        }
        self.grants.len() < self.limit as usize
    }
    pub fn consume(&mut self, now: i64) {
        self.grants.push_back(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn range() -> VerdictRange {
        VerdictRange { first: 0, last: 0 }
    }
    #[test]
    fn designated_attempts_are_consumed_once_and_exempt_from_target_spacing() {
        let mut r = TaskRecord::new(MINUTE);
        r.observe_latest(0, true);
        for (index, offset) in OFFSETS.into_iter().enumerate() {
            let now = MINUTE + offset;
            let due = r.fallback_due(now, 0, 0, now - 1).unwrap();
            assert_eq!(due, FallbackDue::Specified { minute: 0, index });
            let attempt = r.begin(TaskKind::Fallback, range(), Some(due), 1, 0);
            r.finish(attempt, now, false, 300_000);
            assert_eq!(r.fallback_due(now, 0, 0, now), None);
        }
        assert_eq!(r.fallback_due(71_000, 0, 0, 66_001), None);
        assert_eq!(
            r.fallback_due(71_001, 0, 0, 66_001),
            Some(FallbackDue::Target)
        );
        // Becoming ready and then pending in the same minute does not replenish.
        r.observe_latest(0, false);
        r.resolve_specified(|_| false);
        r.observe_latest(0, true);
        assert_eq!(r.fallback_due(72_000, 0, 0, 71_999), None);
    }
    #[test]
    fn points_due_in_flight_are_not_consumed_by_the_running_request() {
        let mut r = TaskRecord::new(MINUTE);
        r.observe_latest(0, true);
        let startup = r.begin(TaskKind::Startup, range(), None, 1, 0);
        r.finish(startup, 67_000, false, 300_000);
        for index in 0..3 {
            let due = r.fallback_due(67_000, 0, 0, 67_000).unwrap();
            assert_eq!(due, FallbackDue::Specified { minute: 0, index });
            let a = r.begin(TaskKind::Fallback, range(), Some(due), 1, 0);
            r.finish(a, 67_000, false, 300_000);
        }
        assert!(r.fallback_due(67_000, 0, 0, 67_000).is_none());
    }
    #[test]
    fn only_startup_success_initializes_and_each_class_has_its_own_backoff() {
        let mut r = TaskRecord::new(0);
        let startup = r.begin(TaskKind::Startup, range(), None, 1, 0);
        r.finish(startup, 100, false, 300_000);
        assert_eq!(r.startup.due, 10_100);
        let fallback = r.begin(TaskKind::Fallback, range(), None, 1, 0);
        r.finish(fallback, 200, true, 300_000);
        assert!(!r.initialized);
        assert_eq!(r.startup.attempts, 1);
        assert_eq!(r.startup.due, 10_100);
        let startup = r.begin(TaskKind::Startup, range(), None, 1, 0);
        r.finish(startup, 10_200, true, 300_000);
        assert!(r.initialized);
        let mut delays = Vec::new();
        for _ in 0..8 {
            let a = r.begin(TaskKind::Routine, range(), None, 1, 0);
            r.finish(a, 1000, false, 300_000);
            delays.push(r.routine.due - 1000);
        }
        assert_eq!(
            delays,
            [
                10_000, 20_000, 40_000, 80_000, 160_000, 300_000, 300_000, 300_000
            ]
        );
        assert_eq!(r.history.attempts, 0);
    }
    #[test]
    fn new_epoch_and_new_incarnation_survive_completion_of_old_work() {
        let mut r = TaskRecord::new(0);
        r.observe(0, 1, 241);
        let a = r.begin(TaskKind::Startup, range(), None, 1, 0);
        r.observe(1000, 2, 241);
        r.finish(a, 2000, true, 300_000);
        assert!(r.initialized);
        assert_eq!(r.sync_due(2000, true), Some(TaskKind::Routine));
        let a = r.begin(TaskKind::Routine, range(), None, 2, 0);
        r.observe(3000, 3, 241);
        r.finish(a, 4000, true, 300_000);
        assert_eq!(r.sync_due(4000, true), Some(TaskKind::Routine));
        let mut recreated = TaskRecord::new(4000);
        recreated.finish(a, 5000, true, 300_000);
        assert!(!recreated.initialized);
        assert_eq!(recreated.routine.due, 4000);
    }
    #[test]
    fn rolling_fallback_budget_does_not_apply_to_sync() {
        let mut b = FallbackBudget::new(2);
        assert!(b.available(0));
        b.consume(0);
        assert!(b.available(1));
        b.consume(1);
        assert!(!b.available(59_999));
        assert!(b.available(60_000));
        b.consume(60_000);
        assert!(!b.available(60_000));
        assert!(b.available(60_001));
    }
}

#[cfg(test)]
mod slow_completion_tests {
    use super::*;
    #[test]
    fn exhausted_designated_points_do_not_resend_in_place_after_a_slow_reply() {
        let mut r = TaskRecord::new(MINUTE);
        r.observe_latest(0, true);
        let range = VerdictRange { first: 0, last: 0 };
        for index in 0..3 {
            let due = r.fallback_due(100_000, 0, 0, 100_000).unwrap();
            assert_eq!(due, FallbackDue::Specified { minute: 0, index });
            let a = r.begin(TaskKind::Fallback, range, Some(due), 1, 0);
            r.finish(a, 100_000, false, 300_000);
        }
        assert!(r.fallback_due(100_000, 0, 0, 60_000).is_none());
        assert!(r.fallback_due(104_999, 0, 0, 60_000).is_none());
        assert_eq!(
            r.fallback_due(105_000, 0, 0, 60_000),
            Some(FallbackDue::Target)
        );
    }
}
