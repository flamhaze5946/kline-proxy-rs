//! Evidence rules for the opt-in one-minute service. No clocks or I/O live here.
use crate::{Bar, InvalidBar, Source, Update};
use bigdecimal::BigDecimal;
use std::str::FromStr;

pub const MINUTE_MS: i64 = 60_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvidenceState {
    #[default]
    Empty,
    Pending,
    WsFinal,
    RestFinal,
    Absent,
}
impl EvidenceState {
    pub fn needs_verdict(self, ws_final_trusted: bool) -> bool {
        matches!(self, Self::Empty | Self::Pending) || (self == Self::WsFinal && !ws_final_trusted)
    }
}

#[derive(Clone, Debug)]
pub struct EvidenceRecord {
    pub open_time: i64,
    pub bar: Option<Bar>,
    pub state: EvidenceState,
    pub ws_equal: bool,
    pub synthetic: bool,
}

#[derive(Clone, Debug)]
pub enum EvidenceUpdate {
    Observation(Update),
    Rest(Bar),
    Absent(i64),
}
impl EvidenceUpdate {
    pub fn open_time(&self) -> i64 {
        match self {
            Self::Observation(u) => u.bar.open_time,
            Self::Rest(bar) => bar.open_time,
            Self::Absent(open) => *open,
        }
    }
    pub fn bar(&self) -> Option<&Bar> {
        match self {
            Self::Observation(u) => Some(&u.bar),
            Self::Rest(bar) => Some(bar),
            Self::Absent(_) => None,
        }
    }
    pub fn state(&self) -> EvidenceState {
        match self {
            Self::Observation(u) if u.source == Source::Stream && u.closed => {
                EvidenceState::WsFinal
            }
            Self::Observation(_) => EvidenceState::Pending,
            Self::Rest(_) => EvidenceState::RestFinal,
            Self::Absent(_) => EvidenceState::Absent,
        }
    }
}

/// The complete row check shared by REST preflight, WS preflight and commit.
pub fn validate_bar(bar: &Bar) -> Result<(), InvalidBar> {
    if bar.open_time < 0 || bar.open_time % MINUTE_MS != 0 {
        return Err(InvalidBar::Time);
    }
    bar.validate()?;
    decimal_fields(bar)?;
    Ok(())
}

/// Branch A: one mature REST response is a verdict. Branch B would change only
/// this decision boundary, rather than adding a runtime policy switch.
pub fn mature_rest_is_verdict(_response: &[Bar], _open: i64, _end_time: i64) -> bool {
    true
}

/// All anchors belong to this one validated response. The final returned row is
/// never mature, even if its nominal close is in the past.
pub fn rest_verdict(bars: &[Bar], open: i64, end_time: i64) -> EvidenceState {
    let row = bars.binary_search_by_key(&open, |bar| bar.open_time);
    let boundary = row.ok().map_or(open.saturating_add(MINUTE_MS), |i| {
        bars[i].close_time.saturating_add(1)
    });
    if end_time < boundary.saturating_add(300) || !mature_rest_is_verdict(bars, open, end_time) {
        return if row.is_ok() {
            EvidenceState::Pending
        } else {
            EvidenceState::Empty
        };
    }
    match row {
        Ok(i) if i + 1 < bars.len() => EvidenceState::RestFinal,
        Ok(_) => EvidenceState::Pending,
        Err(i) if i > 0 && i < bars.len() => EvidenceState::Absent,
        Err(_) => EvidenceState::Empty,
    }
}

/// String lexemes stay unchanged in storage. Evidence compares exact decimal
/// values (including timestamps and trades), never their binary float shadows.
pub fn decimal_fields(bar: &Bar) -> Result<[BigDecimal; 8], InvalidBar> {
    use crate::ExactNumbers;
    let fields = match bar.exact.as_deref() {
        Some(ExactNumbers::Decimals(values)) => return Ok(values.clone()),
        Some(ExactNumbers::Strings(values)) => values.clone(),
        None => bar.values.map(|value| value.to_string()),
    };
    let mut parsed = Vec::with_capacity(8);
    for field in fields {
        parsed.push(BigDecimal::from_str(&field).map_err(|_| InvalidBar::Number)?);
    }
    Ok(parsed.try_into().unwrap())
}
pub fn decimal_equal(left: &Bar, right: &Bar) -> bool {
    left.open_time == right.open_time
        && left.close_time == right.close_time
        && left.trades == right.trades
        && decimal_fields(left)
            .ok()
            .zip(decimal_fields(right).ok())
            .is_some_and(|(a, b)| a == b)
}

/// Evaluate before changing bytes or ws_equal; late WS challenges to R/A count.
pub fn trips_breaker(
    trading: bool,
    conflict: bool,
    input: EvidenceState,
    previous: EvidenceState,
    ws_equal: bool,
) -> bool {
    trading
        && conflict
        && (input == EvidenceState::WsFinal || previous == EvidenceState::WsFinal || ws_equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NumberType, Series};
    use std::num::NonZeroUsize;
    fn bar(value: &str) -> Bar {
        let mut bar = Bar {
            open_time: MINUTE_MS,
            close_time: 2 * MINUTE_MS - 1,
            trades: 1,
            ..Bar::default()
        };
        bar.set_numbers(NumberType::String, [value; 8]).unwrap();
        bar
    }
    fn update(value: &str, closed: bool, source: Source) -> EvidenceUpdate {
        EvidenceUpdate::Observation(Update {
            bar: bar(value),
            closed,
            source,
            event_time: Some(2),
            sequence: 2,
        })
    }
    fn series(state: EvidenceState) -> Series {
        let mut s = Series::new(NonZeroUsize::new(241).unwrap());
        s.enable_evidence();
        let input = match state {
            EvidenceState::Empty => return s,
            EvidenceState::Pending => update("1", false, Source::Stream),
            EvidenceState::WsFinal => update("1", true, Source::Stream),
            EvidenceState::RestFinal => EvidenceUpdate::Rest(bar("1")),
            EvidenceState::Absent => EvidenceUpdate::Absent(MINUTE_MS),
        };
        s.commit_evidence(input, true).unwrap();
        s
    }
    #[test]
    fn every_transition_cell_and_equal_terminal_claims() {
        use EvidenceState::*;
        let states = [Empty, Pending, WsFinal, RestFinal, Absent];
        let expected = [
            [WsFinal, RestFinal, Absent, Pending],
            [WsFinal, RestFinal, Absent, Pending],
            [Pending, RestFinal, Absent, WsFinal],
            [RestFinal, RestFinal, Absent, RestFinal],
            [Absent, RestFinal, Absent, Absent],
        ];
        for (row, state) in states.into_iter().enumerate() {
            for (column, &target) in expected[row].iter().enumerate() {
                // The unconfirmed column has four distinct input sources.
                for source in [
                    Source::Stream,
                    Source::Rest,
                    Source::Synthetic,
                    Source::Restore,
                ] {
                    let mut s = series(state);
                    let input = match column {
                        0 => update("2", true, Source::Stream),
                        1 => EvidenceUpdate::Rest(bar("2")),
                        2 => EvidenceUpdate::Absent(MINUTE_MS),
                        _ => update("2", false, source),
                    };
                    let c = s.commit_evidence(input, true).unwrap();
                    assert_eq!(
                        s.evidence_state(MINUTE_MS),
                        target,
                        "{state:?}, column {column}, {source:?}"
                    );
                    assert_eq!(
                        c.evidence_conflict,
                        matches!(state, WsFinal | RestFinal | Absent)
                            && column < 3
                            && !(state == Absent && column == 2)
                    );
                    if state == Absent && column != 1 {
                        assert!(s.get(MINUTE_MS).is_none());
                    }
                }
            }
        }
        for state in [WsFinal, RestFinal] {
            for input in [
                update("1.000", true, Source::Stream),
                EvidenceUpdate::Rest(bar("1.000")),
            ] {
                let is_ws = input.state() == WsFinal;
                let mut s = series(state);
                let c = s.commit_evidence(input, true).unwrap();
                assert!(!c.evidence_conflict);
                assert!(!c.updated);
                assert_eq!(
                    s.evidence_record(MINUTE_MS).ws_equal,
                    state == WsFinal || is_ws
                );
                if state == RestFinal {
                    assert!(!c.evidence_changed);
                }
                // Retain original lexemes when numerically equal.
                assert!(
                    matches!(s.get(MINUTE_MS).unwrap().0.exact.as_deref(), Some(crate::ExactNumbers::Strings(a)) if a[0]=="1")
                );
            }
        }
        let mut s = series(Absent);
        assert!(
            !s.commit_evidence(EvidenceUpdate::Absent(MINUTE_MS), true)
                .unwrap()
                .evidence_changed
        );
    }
    #[test]
    fn conflict_uses_previous_ws_backing_and_includes_late_ws_challenges() {
        use EvidenceState::*;
        for trading in [false, true] {
            for conflict in [false, true] {
                for ws_equal in [false, true] {
                    for previous in [Empty, Pending, WsFinal, RestFinal, Absent] {
                        for incoming in [Pending, WsFinal, RestFinal, Absent] {
                            assert_eq!(
                                trips_breaker(trading, conflict, incoming, previous, ws_equal),
                                trading
                                    && conflict
                                    && (incoming == WsFinal || previous == WsFinal || ws_equal)
                            );
                        }
                    }
                }
            }
        }
        let mut s = series(RestFinal);
        assert!(
            !s.commit_evidence(EvidenceUpdate::Rest(bar("2")), true)
                .unwrap()
                .breaker_conflict
        );
        assert!(
            s.commit_evidence(update("3", true, Source::Stream), true)
                .unwrap()
                .breaker_conflict
        );
        assert_eq!(s.get(MINUTE_MS).unwrap().0, bar("2"));
        let mut s = series(RestFinal);
        s.commit_evidence(update("1.0", true, Source::Stream), true)
            .unwrap();
        assert!(
            s.commit_evidence(EvidenceUpdate::Rest(bar("2")), true)
                .unwrap()
                .breaker_conflict
        );
        assert!(!s.evidence_record(MINUTE_MS).ws_equal);
        let mut s = series(Absent);
        assert!(
            s.commit_evidence(update("1", true, Source::Stream), true)
                .unwrap()
                .breaker_conflict
        );
        assert_eq!(s.evidence_state(MINUTE_MS), Absent);
    }
    #[test]
    fn maturity_requires_request_end_and_exact_same_response_anchors() {
        let mut left = bar("1");
        left.open_time = 0;
        left.close_time = MINUTE_MS - 1;
        let middle = bar("2");
        let mut right = bar("3");
        right.open_time = 3 * MINUTE_MS;
        right.close_time = 4 * MINUTE_MS - 1;
        let rows = [left.clone(), middle.clone(), right.clone()];
        assert_eq!(
            rest_verdict(&rows, MINUTE_MS, 2 * MINUTE_MS + 299),
            EvidenceState::Pending
        );
        assert_eq!(
            rest_verdict(&rows, MINUTE_MS, 2 * MINUTE_MS + 300),
            EvidenceState::RestFinal
        );
        assert_eq!(
            rest_verdict(&rows, 2 * MINUTE_MS, 3 * MINUTE_MS + 299),
            EvidenceState::Empty
        );
        assert_eq!(
            rest_verdict(&rows, 2 * MINUTE_MS, 3 * MINUTE_MS + 300),
            EvidenceState::Absent
        );
        assert_eq!(
            rest_verdict(&rows, 3 * MINUTE_MS, 10 * MINUTE_MS),
            EvidenceState::Pending
        );
        assert_eq!(
            rest_verdict(&[right.clone()], 2 * MINUTE_MS, 10 * MINUTE_MS),
            EvidenceState::Empty
        );
        assert_eq!(
            rest_verdict(&[left], 2 * MINUTE_MS, 10 * MINUTE_MS),
            EvidenceState::Empty
        );
        assert_eq!(
            rest_verdict(&[middle, right], 0, 10 * MINUTE_MS),
            EvidenceState::Empty
        );
    }
    #[test]
    fn ws_conflict_chooses_version_and_drops_finality_even_for_older_input() {
        for event in [1, 3] {
            let mut s = series(EvidenceState::WsFinal);
            let EvidenceUpdate::Observation(mut u) = update("2", true, Source::Stream) else {
                unreachable!()
            };
            u.event_time = Some(event);
            let c = s
                .commit_evidence(EvidenceUpdate::Observation(u), true)
                .unwrap();
            assert!(c.evidence_conflict && c.evidence_changed);
            assert_eq!(s.evidence_state(MINUTE_MS), EvidenceState::Pending);
            assert_eq!(
                s.get(MINUTE_MS).unwrap().0,
                bar(if event == 1 { "1" } else { "2" })
            );
            assert!(!s.get(MINUTE_MS).unwrap().1);
        }
    }
    #[test]
    fn gap_markers_delete_bytes_and_retention_is_in_minutes() {
        let mut s = series(EvidenceState::WsFinal);
        s.commit_evidence(EvidenceUpdate::Absent(MINUTE_MS), true)
            .unwrap();
        assert!(s.records().is_empty());
        assert!(s.final_snapshot(i64::MAX).is_empty());
        assert_eq!(s.covered_from(), Some(MINUTE_MS));
        assert!(!s.needs_verdict(MINUTE_MS - 60_000, false));
        assert!(!s.needs_verdict(MINUTE_MS, false));
        assert!(s.needs_verdict(2 * MINUTE_MS, false));
        s.commit_evidence(EvidenceUpdate::Absent(242 * MINUTE_MS), true)
            .unwrap();
        assert_eq!(s.evidence_state(MINUTE_MS), EvidenceState::Empty);
        assert_eq!(s.covered_from(), Some(242 * MINUTE_MS));
        assert!(
            !s.commit_evidence(EvidenceUpdate::Rest(bar("1")), true)
                .unwrap()
                .evidence_changed
        );
    }
    #[test]
    fn unconfirmed_synthetic_and_restore_cannot_promote_or_overwrite_real_bytes() {
        let mut s = series(EvidenceState::Pending);
        s.commit_evidence(update("9", false, Source::Synthetic), true)
            .unwrap();
        assert_eq!(s.get(MINUTE_MS).unwrap().0, bar("1"));
        let mut s = series(EvidenceState::Empty);
        s.commit_evidence(update("1", true, Source::Restore), true)
            .unwrap();
        assert_eq!(s.evidence_state(MINUTE_MS), EvidenceState::Pending);
        assert!(!s.get(MINUTE_MS).unwrap().1);
        let mut s = series(EvidenceState::Empty);
        s.commit_evidence(update("1", false, Source::Synthetic), true)
            .unwrap();
        assert!(s.evidence_record(MINUTE_MS).synthetic);
        s.commit_evidence(EvidenceUpdate::Rest(bar("1")), true)
            .unwrap();
        assert!(!s.evidence_record(MINUTE_MS).synthetic);
    }
    #[test]
    fn decimal_comparison_is_not_float_or_lexeme_equality() {
        assert!(decimal_equal(&bar("0"), &bar("0.000")));
        assert!(!decimal_equal(
            &bar("1.0000000000000000001"),
            &bar("1.0000000000000000002")
        ));
        assert!(decimal_equal(&bar("1E3"), &bar("1000.0")));
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;
    use crate::{NumberType, Series};
    use std::num::NonZeroUsize;
    #[test]
    fn matching_ws_updates_version_without_replacing_lexemes_or_advancing_state() {
        let mut series = Series::new(NonZeroUsize::new(241).unwrap());
        series.enable_evidence();
        let mut b = Bar {
            open_time: 0,
            close_time: MINUTE_MS - 1,
            trades: 1,
            ..Bar::default()
        };
        b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
        let ws = |bar: Bar, event| {
            EvidenceUpdate::Observation(Update {
                bar,
                closed: true,
                source: Source::Stream,
                event_time: Some(event),
                sequence: event as u64,
            })
        };
        series.commit_evidence(ws(b.clone(), 1), true).unwrap();
        let mut equal = b.clone();
        equal.set_numbers(NumberType::String, ["1.00"; 8]).unwrap();
        assert!(
            !series
                .commit_evidence(ws(equal, 9), true)
                .unwrap()
                .evidence_changed
        );
        let mut different = b.clone();
        different.set_numbers(NumberType::String, ["2"; 8]).unwrap();
        series.commit_evidence(ws(different, 5), true).unwrap();
        assert_eq!(series.get(0).unwrap().0, b);
        assert_eq!(series.evidence_state(0), EvidenceState::Pending);
    }
}

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use crate::{NumberType, Series};
    use std::num::NonZeroUsize;
    #[test]
    fn evidence_retention_and_allocation_stay_bounded_by_minutes() {
        let mut s = Series::new(NonZeroUsize::new(241).unwrap());
        s.enable_evidence();
        for step in 0..1000 {
            let open = step * MINUTE_MS;
            let mut b = Bar {
                open_time: open,
                close_time: open + MINUTE_MS - 1,
                ..Bar::default()
            };
            b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
            s.commit_evidence(EvidenceUpdate::Rest(b), true).unwrap();
            assert!(s.allocated_capacity() <= 241);
            if step >= 500 && step % 3 == 0 {
                s.commit_evidence(EvidenceUpdate::Absent(open), true)
                    .unwrap();
            }
            assert!(s.len() <= 241);
            assert_eq!(
                s.evidence_state(open),
                if step >= 500 && step % 3 == 0 {
                    EvidenceState::Absent
                } else {
                    EvidenceState::RestFinal
                }
            );
            if step >= 241 {
                assert_eq!(
                    s.evidence_state(open - 241 * MINUTE_MS),
                    EvidenceState::Empty
                );
            }
        }
    }
    #[test]
    fn target_predicate_includes_frame_free_minutes_but_excludes_before_first_observation() {
        let mut s = Series::new(NonZeroUsize::new(241).unwrap());
        s.enable_evidence();
        assert!(s.needs_verdict(0, true));
        let mut b = Bar {
            open_time: MINUTE_MS,
            close_time: 2 * MINUTE_MS - 1,
            ..Bar::default()
        };
        b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
        s.commit_evidence(
            EvidenceUpdate::Observation(Update {
                bar: b,
                closed: true,
                source: Source::Stream,
                event_time: Some(1),
                sequence: 1,
            }),
            true,
        )
        .unwrap();
        assert!(!s.needs_verdict(0, false));
        assert!(!s.needs_verdict(MINUTE_MS, true));
        assert!(s.needs_verdict(MINUTE_MS, false));
        assert!(s.needs_verdict(2 * MINUTE_MS, true));
        assert_eq!(
            s.earliest_needs_verdict(0, 3 * MINUTE_MS, true),
            Some(2 * MINUTE_MS)
        );
    }
}
