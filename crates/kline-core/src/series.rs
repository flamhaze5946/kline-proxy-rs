use crate::evidence::{EvidenceRecord, EvidenceState, EvidenceUpdate, decimal_equal, validate_bar};
use crate::{Bar, Interval, InvalidBar};
use chrono::{Months, TimeZone, Utc};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Stream,
    Rest,
    Restore,
    Synthetic,
}

#[derive(Debug, Clone)]
pub struct Update {
    pub bar: Bar,
    pub closed: bool,
    pub source: Source,
    pub event_time: Option<i64>,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Commit {
    pub updated: bool,
    pub inserted: bool,
    pub close_time_changed: bool,
    pub became_final: bool,
    pub final_revised: bool,
    pub stream_final_observed: bool,
    pub trimmed: bool,
    /// Evidence state/bytes changed, including deletion into an explicit gap.
    pub evidence_changed: bool,
    pub evidence_conflict: bool,
    pub breaker_conflict: bool,
}

#[derive(Debug, Clone, Copy)]
struct Version {
    event_time: Option<i64>,
    sequence: u64,
}
impl Version {
    fn newer_than(self, update: &Update) -> bool {
        if let (Some(previous), Some(candidate)) = (self.event_time, update.event_time)
            && previous != candidate
        {
            return previous > candidate;
        }
        self.sequence > 0 && update.sequence > 0 && self.sequence > update.sequence
    }
}

#[derive(Debug, Clone)]
struct Entry {
    bar: Bar,
    final_bar: bool,
    version: Option<Version>,
    evidence: EvidenceState,
    ws_equal: bool,
    synthetic: bool,
}

/// A bounded sorted ring. Updating/appending the latest bar is O(1); corrections
/// use binary search. The value, final flag, and version share one retained entry.
/// The caller supplies the per-series synchronization boundary.
pub struct Series {
    entries: VecDeque<Entry>,
    capacity: usize,
    evidence_enabled: bool,
    absent: BTreeSet<i64>,
    evidence_changes: BTreeMap<i64, u64>,
    evidence_revision: u64,
    evidence_epoch: u64,
    // Process-local evidence of x=true; REST/restore finality cannot authorize extrapolation.
    latest_stream_final_open: Option<i64>,
}
impl Series {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            // Young listings rarely have a full configured history. Keep the retention
            // limit independent from allocated storage, and cap every growth step.
            entries: VecDeque::new(),
            capacity: capacity.get(),
            evidence_enabled: false,
            absent: BTreeSet::new(),
            evidence_changes: BTreeMap::new(),
            evidence_revision: 0,
            evidence_epoch: 0,
            latest_stream_final_open: None,
        }
    }
    pub fn resize(&mut self, capacity: NonZeroUsize) {
        if self.evidence_enabled {
            self.evidence_revision += 1;
            self.evidence_epoch = self.evidence_revision;
        }
        self.capacity = capacity.get();
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
        self.entries.shrink_to_fit();
        self.trim_evidence();
    }
    pub fn clear(&mut self) {
        self.entries = VecDeque::new();
        self.latest_stream_final_open = None;
        self.absent.clear();
        self.evidence_changes.clear();
        if self.evidence_enabled {
            self.evidence_revision += 1;
            self.evidence_epoch = self.evidence_revision;
        }
    }
    fn locate(&self, time: i64) -> Result<usize, usize> {
        if let Some(last) = self.entries.back() {
            if last.bar.open_time == time {
                return Ok(self.entries.len() - 1);
            }
            if last.bar.open_time < time {
                return Err(self.entries.len());
            }
        }
        if self.entries.len() >= 2 {
            let index = self.entries.len() - 2;
            if self.entries[index].bar.open_time == time {
                return Ok(index);
            }
        }
        self.entries
            .binary_search_by_key(&time, |e| e.bar.open_time)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn allocated_capacity(&self) -> usize {
        self.entries.capacity()
    }
    /// Validated batch loads know their size before publication. Reserve once,
    /// with a small tail allowance, avoiding repeated allocator copies/fragments.
    pub fn reserve_history(&mut self, records: usize) {
        let target = records
            .max(self.entries.len())
            .saturating_add(32)
            .min(self.capacity);
        if target > self.entries.capacity() {
            self.entries.reserve_exact(target - self.entries.len());
        }
    }
    /// Reserve the next bounded growth step before inserting, or from maintenance
    /// ahead of a close. `reserve_exact` prevents VecDeque doubling past retention.
    pub fn reserve_next(&mut self) {
        if self.entries.len() == self.entries.capacity() && self.entries.len() < self.capacity {
            let target = self
                .entries
                .capacity()
                .max(32)
                .saturating_mul(2)
                .min(self.capacity);
            self.entries.reserve_exact(target - self.entries.len());
        }
    }
    /// Borrow retained records for compact summaries without cloning every OHLCV.
    pub fn iter_records(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&Bar, bool)> + ExactSizeIterator {
        self.entries
            .iter()
            .map(|entry| (&entry.bar, entry.final_bar))
    }
    pub fn records(&self) -> Vec<(Bar, bool)> {
        self.entries
            .iter()
            .map(|e| (e.bar.clone(), e.final_bar))
            .collect()
    }
    /// Inclusive time range, including forming bars and preserving any internal gaps.
    /// Binary search avoids cloning retained history outside the requested window.
    pub fn range_snapshot(&self, first: i64, last: i64) -> Vec<Bar> {
        if first > last {
            return vec![];
        }
        let start = self.entries.partition_point(|e| e.bar.open_time < first);
        let end = self.entries.partition_point(|e| e.bar.open_time <= last);
        self.entries
            .range(start..end)
            .map(|e| e.bar.clone())
            .collect()
    }
    pub fn has_deferred_final(&self, now: i64) -> bool {
        self.entries
            .iter()
            .any(|e| e.final_bar && e.bar.close_time > now)
    }
    pub fn final_snapshot(&self, now: i64) -> Vec<Bar> {
        self.entries
            .iter()
            .filter(|e| e.final_bar && e.bar.close_time <= now)
            .map(|e| e.bar.clone())
            .collect()
    }
    pub fn latest(&self) -> Option<(Bar, bool)> {
        self.entries.back().map(|e| (e.bar.clone(), e.final_bar))
    }
    pub fn latest_bar(&self) -> Option<&Bar> {
        self.entries.back().map(|e| &e.bar)
    }
    /// A read-only current-period placeholder, anchored on the immediately
    /// preceding WebSocket x=true close. Never stored, persisted, or chained over a gap.
    pub fn provisional_bar(&self, interval: Interval, now: i64) -> Option<Bar> {
        let (open, end) = self.provisional_period(interval, now)?;
        Some(self.entries.back()?.bar.synthetic_after(open, end - open))
    }
    fn provisional_period(&self, interval: Interval, now: i64) -> Option<(i64, i64)> {
        let last = self.entries.back()?;
        if !last.final_bar || self.latest_stream_final_open != Some(last.bar.open_time) {
            return None;
        }
        let open = last.bar.close_time.checked_add(1)?;
        let end = if interval.code() == "1M" {
            Utc.timestamp_millis_opt(open)
                .single()?
                .checked_add_months(Months::new(1))?
                .timestamp_millis()
        } else {
            open.checked_add(interval.millis())?
        };
        (open <= now && now < end).then_some((open, end))
    }
    /// Query availability is separate from observed freshness used by REST repair.
    /// A placeholder does not excuse an older unconfirmed bar in the recent tail.
    pub fn provisional_tail_available(&self, interval: Interval, now: i64, grace: i64) -> bool {
        self.provisional_period(interval, now).is_some()
            && self
                .entries
                .iter()
                .rev()
                .take(2)
                .all(|e| e.final_bar || e.bar.close_time >= now.saturating_sub(grace))
    }
    pub fn fresh_tail(&self, now: i64, grace: i64) -> bool {
        let Some(last) = self.entries.back() else {
            return false;
        };
        if last.bar.close_time < now.saturating_sub(grace)
            || last.bar.open_time > now.saturating_add(grace)
        {
            return false;
        }
        self.entries
            .iter()
            .rev()
            .take(2)
            .all(|e| e.final_bar || e.bar.close_time >= now.saturating_sub(grace))
    }
    pub fn get(&self, time: i64) -> Option<(Bar, bool)> {
        self.locate(time).ok().map(|i| {
            let e = &self.entries[i];
            (e.bar.clone(), e.final_bar)
        })
    }
    pub fn has_non_final(&self, time: i64) -> bool {
        self.locate(time).is_ok_and(|i| !self.entries[i].final_bar)
    }
    pub fn has_bar(&self, time: i64) -> bool {
        self.locate(time).is_ok()
    }
    /// A newly listed series need not have a bar before its first observation.
    pub fn needs_final(&self, time: i64) -> bool {
        match self.locate(time) {
            Ok(i) => !self.entries[i].final_bar,
            Err(_) => self
                .entries
                .front()
                .is_some_and(|e| e.bar.open_time <= time),
        }
    }
    pub fn snapshot_records(&self, limit: usize, now: i64) -> (Vec<(Bar, bool)>, i64) {
        let mut selected = Vec::with_capacity(limit.min(self.len()));
        let mut next_change = i64::MAX;
        for entry in self.entries.iter().rev() {
            if selected.len() == limit {
                break;
            }
            if entry.bar.close_time > now {
                next_change = next_change.min(entry.bar.close_time);
            } else if selected.len() < limit {
                selected.push((entry.bar.clone(), entry.final_bar));
            }
        }
        selected.reverse();
        (selected, next_change)
    }
    pub fn snapshot(&self, limit: usize, closed_only: bool, now: i64) -> Vec<Bar> {
        let mut bars: Vec<_> = self
            .entries
            .iter()
            .rev()
            .filter(|e| !closed_only || e.bar.close_time <= now)
            .take(limit)
            .map(|e| e.bar.clone())
            .collect();
        bars.reverse();
        bars
    }
    pub fn commit(&mut self, update: Update) -> Result<Commit, InvalidBar> {
        if self.evidence_enabled {
            return self.commit_evidence(EvidenceUpdate::Observation(update), false);
        }
        self.commit_legacy(update)
    }
    fn commit_legacy(&mut self, update: Update) -> Result<Commit, InvalidBar> {
        update.bar.validate()?;
        let stream_final =
            (update.source == Source::Stream && update.closed).then_some(update.bar.open_time);
        let mut result = Commit::default();
        let pos = match self.locate(update.bar.open_time) {
            Ok(i) => {
                let old = &self.entries[i];
                if !update.closed && old.final_bar {
                    return Ok(result);
                }
                if (!update.closed || old.final_bar || update.source != Source::Stream)
                    && update.bar.trades < old.bar.trades
                {
                    return Ok(result);
                }
                if update.bar.trades == old.bar.trades && (!update.closed || old.final_bar) {
                    if matches!(update.source, Source::Synthetic | Source::Restore) {
                        return Ok(result);
                    }
                    if old
                        .version
                        .is_some_and(|v| update.source != Source::Stream || v.newer_than(&update))
                    {
                        return Ok(result);
                    }
                }
                result.updated = old.bar != update.bar;
                result.close_time_changed = old.bar.close_time != update.bar.close_time;
                result.became_final = update.closed && !old.final_bar;
                result.final_revised = result.updated && old.final_bar;
                i
            }
            Err(mut i) => {
                // Do not allocate or resurrect metadata for an already evicted old bar.
                if self.entries.len() == self.capacity {
                    if i == 0 {
                        return Ok(result);
                    }
                    self.entries.pop_front();
                    i -= 1;
                    result.trimmed = true;
                }
                self.reserve_next();
                self.entries.insert(
                    i,
                    Entry {
                        bar: update.bar,
                        final_bar: update.closed,
                        evidence: EvidenceState::Pending,
                        ws_equal: false,
                        synthetic: update.source == Source::Synthetic,
                        version: (update.source == Source::Stream).then_some(Version {
                            event_time: update.event_time,
                            sequence: update.sequence,
                        }),
                    },
                );
                result.updated = true;
                result.inserted = true;
                result.became_final = update.closed;
                result.stream_final_observed = self.observe_stream_final(stream_final);
                return Ok(result);
            }
        };
        let entry = &mut self.entries[pos];
        entry.bar = update.bar;
        entry.synthetic = update.source == Source::Synthetic;
        entry.final_bar |= update.closed;
        if update.source == Source::Stream {
            entry.version = Some(Version {
                event_time: update.event_time,
                sequence: update.sequence,
            });
        } else if result.updated {
            entry.version = None;
        }
        result.stream_final_observed = self.observe_stream_final(stream_final);
        Ok(result)
    }

    /// Enabled before publication. Snapshot rows remain unconfirmed.
    pub fn enable_evidence(&mut self) {
        if self.evidence_enabled {
            return;
        }
        self.evidence_enabled = true;
        for entry in &mut self.entries {
            entry.final_bar = false;
            entry.evidence = EvidenceState::Pending;
            entry.ws_equal = false;
        }
    }
    pub fn evidence_record(&self, open: i64) -> EvidenceRecord {
        if let Ok(i) = self.locate(open) {
            let e = &self.entries[i];
            EvidenceRecord {
                open_time: open,
                bar: Some(e.bar.clone()),
                state: e.evidence,
                ws_equal: e.ws_equal,
                synthetic: e.synthetic,
            }
        } else {
            EvidenceRecord {
                open_time: open,
                bar: None,
                state: if self.absent.contains(&open) {
                    EvidenceState::Absent
                } else {
                    EvidenceState::Empty
                },
                ws_equal: false,
                synthetic: false,
            }
        }
    }
    pub fn covered_from(&self) -> Option<i64> {
        self.entries
            .front()
            .map(|e| e.bar.open_time)
            .into_iter()
            .chain(self.absent.first().copied())
            .min()
    }
    pub fn needs_verdict(&self, open: i64, trusted: bool) -> bool {
        if self.covered_from().is_some_and(|first| open < first) {
            return false;
        }
        self.evidence_state(open).needs_verdict(trusted)
    }
    pub fn evidence_state(&self, open: i64) -> EvidenceState {
        match self.locate(open) {
            Ok(i) => self.entries[i].evidence,
            Err(_) if self.absent.contains(&open) => EvidenceState::Absent,
            Err(_) => EvidenceState::Empty,
        }
    }
    pub fn earliest_needs_verdict(&self, first: i64, last: i64, trusted: bool) -> Option<i64> {
        (first..=last)
            .step_by(60_000)
            .find(|&open| self.needs_verdict(open, trusted))
    }
    /// Rows and gap markers share the series lock and the same minute retention.
    fn trim_evidence(&mut self) -> bool {
        if !self.evidence_enabled {
            return false;
        }
        let latest = self
            .entries
            .back()
            .map(|e| e.bar.open_time)
            .into_iter()
            .chain(self.absent.last().copied())
            .max();
        let Some(latest) = latest else {
            return false;
        };
        let first =
            latest.saturating_sub((self.capacity.saturating_sub(1) as i64).saturating_mul(60_000));
        let before = self.entries.len() + self.absent.len();
        while self
            .entries
            .front()
            .is_some_and(|e| e.bar.open_time < first)
        {
            self.entries.pop_front();
        }
        self.absent = self.absent.split_off(&first);
        before != self.entries.len() + self.absent.len()
    }
    /// A fixed-window token excludes ordinary updates outside that window.
    pub fn evidence_window_stamp(&self, first: i64, last: i64) -> (Option<i64>, u64, u64) {
        (
            self.covered_from(),
            self.evidence_epoch,
            self.evidence_changes
                .range(first..=last)
                .map(|(_, v)| *v)
                .max()
                .unwrap_or(0),
        )
    }
    pub fn commit_evidence(
        &mut self,
        input: EvidenceUpdate,
        trading: bool,
    ) -> Result<Commit, InvalidBar> {
        let open = input.open_time();
        let result = self.commit_evidence_inner(input, trading)?;
        if result.evidence_changed {
            self.evidence_revision += 1;
            self.evidence_changes.insert(open, self.evidence_revision);
            let latest = self
                .entries
                .back()
                .map(|e| e.bar.open_time)
                .into_iter()
                .chain(self.absent.last().copied())
                .max()
                .unwrap_or(open);
            let floor = latest.saturating_sub((self.capacity.saturating_sub(1) as i64) * 60_000);
            self.evidence_changes = self.evidence_changes.split_off(&floor);
        }
        Ok(result)
    }
    fn commit_evidence_inner(
        &mut self,
        input: EvidenceUpdate,
        trading: bool,
    ) -> Result<Commit, InvalidBar> {
        use EvidenceState::*;
        let open = input.open_time();
        if open < 0 || open % 60_000 != 0 {
            return Err(InvalidBar::Time);
        }
        if let Some(bar) = input.bar() {
            validate_bar(bar)?;
        }
        let latest = self
            .entries
            .back()
            .map(|e| e.bar.open_time)
            .into_iter()
            .chain(self.absent.last().copied())
            .max();
        if latest.is_some_and(|latest| {
            open < latest
                .saturating_sub((self.capacity.saturating_sub(1) as i64).saturating_mul(60_000))
        }) {
            return Ok(Commit::default());
        }
        let previous = self.evidence_record(open);
        let incoming = input.state();
        let equal = previous
            .bar
            .as_ref()
            .zip(input.bar())
            .is_some_and(|(a, b)| decimal_equal(a, b))
            || (previous.state == Absent && incoming == Absent);
        let terminal = |s| matches!(s, WsFinal | RestFinal | Absent);
        let conflict = terminal(previous.state) && terminal(incoming) && !equal;
        let mut result = Commit {
            evidence_conflict: conflict,
            breaker_conflict: crate::evidence::trips_breaker(
                trading,
                conflict,
                incoming,
                previous.state,
                previous.ws_equal,
            ),
            final_revised: conflict,
            ..Commit::default()
        };
        match incoming {
            Pending if terminal(previous.state) => return Ok(result),
            Pending => {
                let EvidenceUpdate::Observation(mut update) = input else {
                    unreachable!()
                };
                // Synthetic never overwrites actual bytes, irrespective of trade count.
                if update.source == Source::Synthetic
                    && previous.bar.is_some()
                    && !previous.synthetic
                {
                    return Ok(result);
                }
                update.closed = false;
                if let Some(previous) = previous
                    .bar
                    .as_ref()
                    .filter(|bar| decimal_equal(bar, &update.bar))
                {
                    update.bar = previous.clone();
                }
                result = self.commit_legacy(update)?;
                if let Ok(i) = self.locate(open) {
                    let e = &mut self.entries[i];
                    e.evidence = Pending;
                    result.evidence_changed |= e.synthetic != previous.synthetic;
                    if result.updated {
                        e.ws_equal = false;
                    }
                }
            }
            WsFinal if previous.state == RestFinal || previous.state == Absent => {
                if equal && let Ok(i) = self.locate(open) {
                    self.entries[i].ws_equal = true;
                    result.stream_final_observed = self.observe_stream_final(Some(open));
                }
                return Ok(result);
            }
            Absent => {
                if previous.state == Absent {
                    return Ok(result);
                }
                if let Ok(i) = self.locate(open) {
                    self.entries.remove(i);
                }
                self.absent.insert(open);
                result.updated = previous.bar.is_some();
                result.evidence_changed = true;
            }
            WsFinal | RestFinal => {
                let (bar, version) = match input {
                    EvidenceUpdate::Observation(u) => {
                        let version = Version {
                            event_time: u.event_time,
                            sequence: u.sequence,
                        };
                        let newer = self
                            .locate(open)
                            .ok()
                            .and_then(|i| self.entries[i].version)
                            .is_none_or(|v| !v.newer_than(&u));
                        if previous.state == WsFinal && !equal && !newer {
                            let i = self.locate(open).unwrap();
                            self.entries[i].evidence = Pending;
                            self.entries[i].final_bar = false;
                            result.evidence_changed = true;
                            return Ok(result);
                        }
                        if equal
                            && newer
                            && let Ok(i) = self.locate(open)
                        {
                            // No state/byte change, but remember the newest WS
                            // version for a subsequent conflicting WS frame.
                            self.entries[i].version = Some(version);
                        }
                        (u.bar, Some(version))
                    }
                    EvidenceUpdate::Rest(bar) => (bar, None),
                    EvidenceUpdate::Absent(_) => unreachable!(),
                };
                let next = if previous.state == WsFinal && incoming == WsFinal && !equal {
                    Pending
                } else {
                    incoming
                };
                match self.locate(open) {
                    Ok(i) => {
                        let e = &mut self.entries[i];
                        if !equal {
                            result.close_time_changed = e.bar.close_time != bar.close_time;
                            e.bar = bar;
                            e.version = version;
                            e.ws_equal = false;
                            e.synthetic = false;
                            result.updated = true;
                        }
                        if incoming == WsFinal || (previous.state == WsFinal && equal) {
                            e.ws_equal = true;
                        }
                        e.evidence = next;
                        e.synthetic = false;
                        e.final_bar = matches!(next, WsFinal | RestFinal);
                    }
                    Err(mut i) => {
                        if self.entries.len() == self.capacity {
                            if i == 0 {
                                return Ok(Commit::default());
                            }
                            self.entries.pop_front();
                            i -= 1;
                            result.trimmed = true;
                        }
                        self.reserve_next();
                        self.entries.insert(
                            i,
                            Entry {
                                bar,
                                final_bar: true,
                                version,
                                evidence: next,
                                ws_equal: incoming == WsFinal,
                                synthetic: false,
                            },
                        );
                        result.updated = true;
                        result.inserted = true;
                    }
                }
                self.absent.remove(&open);
                result.evidence_changed = previous.state != next || result.updated;
                result.became_final = matches!(next, WsFinal | RestFinal)
                    && !matches!(previous.state, WsFinal | RestFinal);
                if incoming == WsFinal {
                    result.stream_final_observed = self.observe_stream_final(Some(open));
                }
            }
            Empty => unreachable!(),
        }
        result.evidence_changed |= result.updated || result.inserted;
        result.trimmed |= self.trim_evidence();
        result.evidence_changed |= result.trimmed;
        Ok(result)
    }

    fn observe_stream_final(&mut self, open: Option<i64>) -> bool {
        if let Some(open) = open
            && self
                .latest_stream_final_open
                .is_none_or(|previous| open > previous)
        {
            self.latest_stream_final_open = Some(open);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn series(n: usize) -> Series {
        Series::new(NonZeroUsize::new(n).unwrap())
    }
    #[test]
    fn allocation_grows_with_history_without_changing_retention() {
        let mut s = series(1001);
        assert_eq!(s.capacity(), 1001);
        assert_eq!(s.allocated_capacity(), 0);
        for t in 0..3000 {
            s.commit(update(t * 1000, 1, true, None, 1, 1.)).unwrap();
            assert!(s.allocated_capacity() <= s.capacity());
            assert_eq!(s.len(), ((t + 1) as usize).min(1001));
        }
        assert_eq!(s.allocated_capacity(), 1001);
        assert_eq!(s.records()[0].0.open_time, 1_999_000);
        s.resize(NonZeroUsize::new(7).unwrap());
        assert_eq!(s.len(), 7);
        assert_eq!(s.allocated_capacity(), 7);
        s.resize(NonZeroUsize::new(2001).unwrap());
        assert_eq!(s.allocated_capacity(), 7);
        s.commit(update(3_000_000, 1, true, None, 1, 1.)).unwrap();
        assert!(s.allocated_capacity() <= 64);
    }
    fn update(time: i64, n: u32, closed: bool, event: Option<i64>, seq: u64, price: f64) -> Update {
        Update {
            bar: Bar {
                open_time: time,
                close_time: time + 999,
                trades: n,
                values: [price; 8],

                ..Bar::default()
            },
            closed,
            event_time: event,
            sequence: seq,
            source: Source::Stream,
        }
    }
    #[test]
    fn inclusive_ranges_match_retained_records_after_wrap_resize_and_corrections() {
        let mut series = series(17);
        for i in 0..80 {
            if i % 5 != 0 {
                series
                    .commit(update(i * 1000, 1, i % 3 == 0, None, 0, i as f64))
                    .unwrap();
            }
        }
        series
            .commit(update(73500, 2, true, None, 0, 123.))
            .unwrap();
        for capacity in [17, 9, 30] {
            series.resize(NonZeroUsize::new(capacity).unwrap());
            let records = series.records();
            for first in [
                -1,
                0,
                60999,
                61000,
                62001,
                73500,
                73999,
                79000,
                80000,
                i64::MAX,
            ] {
                for last in [-1, 0, 61000, 73500, 74000, 79000, 80000, i64::MAX] {
                    let expected: Vec<_> = records
                        .iter()
                        .filter(|(bar, _)| bar.open_time >= first && bar.open_time <= last)
                        .map(|(bar, _)| bar.clone())
                        .collect();
                    assert_eq!(
                        series.range_snapshot(first, last),
                        expected,
                        "{first}..={last}, capacity {capacity}"
                    );
                }
            }
        }
        series.clear();
        assert!(series.range_snapshot(0, i64::MAX).is_empty());
    }
    #[test]
    fn first_stream_final_is_authoritative_and_never_regresses() {
        let mut s = series(3);
        s.commit(update(0, 20, false, Some(30), 1, 100.)).unwrap();
        assert!(
            s.commit(update(0, 10, true, Some(20), 2, 109.))
                .unwrap()
                .became_final
        );
        assert!(
            !s.commit(update(0, 30, false, Some(40), 3, 110.))
                .unwrap()
                .updated
        );
        assert_eq!(s.get(0).unwrap().0.values[3], 109.);
    }
    #[test]
    fn equal_trade_revisions_obey_event_then_sequence_and_source() {
        let mut s = series(3);
        s.commit(update(0, 10, true, Some(20), 2, 100.)).unwrap();
        for u in [
            update(0, 9, true, Some(40), 4, 90.),
            update(0, 10, true, Some(19), 4, 90.),
            update(0, 10, true, Some(20), 1, 90.),
        ] {
            assert!(!s.commit(u).unwrap().updated);
        }
        let mut rest = update(0, 10, true, Some(40), 8, 110.);
        rest.source = Source::Rest;
        assert!(!s.commit(rest).unwrap().updated);
        assert!(
            s.commit(update(0, 10, true, Some(21), 1, 101.))
                .unwrap()
                .final_revised
        );
        assert!(
            !s.commit(update(0, 10, true, None, 0, 101.))
                .unwrap()
                .updated
        );
        assert!(
            s.commit(update(0, 10, true, None, 5, 102.))
                .unwrap()
                .final_revised
        );
    }
    #[test]
    fn restore_and_synthetic_cannot_overwrite_equal_trades() {
        for source in [Source::Restore, Source::Synthetic] {
            let mut s = series(2);
            let mut first = update(0, 10, true, None, 0, 100.);
            first.source = Source::Rest;
            s.commit(first).unwrap();
            let mut second = update(0, 10, true, None, 0, 110.);
            second.source = source;
            assert!(!s.commit(second.clone()).unwrap().updated);
            second.bar.trades = 11;
            assert!(s.commit(second).unwrap().updated);
        }
    }
    #[test]
    fn retention_and_out_of_order_insert_keep_all_state_bounded() {
        let mut s = series(3);
        for t in [0, 3000, 1000, 2000, 4000] {
            s.commit(update(t, 1, true, Some(t), 1, 1.)).unwrap();
        }
        assert_eq!(s.len(), 3);
        assert_eq!(
            s.snapshot(100, true, i64::MAX)
                .iter()
                .map(|b| b.open_time)
                .collect::<Vec<_>>(),
            [2000, 3000, 4000]
        );
        assert!(
            !s.commit(update(0, 100, true, Some(10), 2, 10.))
                .unwrap()
                .updated
        );
        assert!(s.get(0).is_none());
        for t in 5..10_000 {
            s.commit(update(t * 1000, 1, t % 2 == 0, None, 1, 1.))
                .unwrap();
        }
        assert_eq!(s.len(), 3);
        assert_eq!(s.entries.capacity(), 3);
    }
    #[test]
    fn time_closed_filter_does_not_claim_finality() {
        let mut s = series(3);
        s.commit(update(0, 1, false, None, 1, 1.)).unwrap();
        s.commit(update(1000, 1, false, None, 2, 2.)).unwrap();
        assert_eq!(s.snapshot(3, true, 1000).len(), 1);
        assert!(s.has_non_final(0));
    }
}
