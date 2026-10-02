//! Domain rules only: no JSON, network, clock, async runtime, or shared locks.
mod series;
pub use series::{Commit, Series, Source, Update};
mod numbers;
pub use numbers::{ExactNumbers, NumberType, decimal_to_plain_string};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Market {
    Future,
    Spot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Interval(u8);
const INTERVALS: [(&str, i64); 16] = [
    ("1s", 1_000),
    ("1m", 60_000),
    ("3m", 180_000),
    ("5m", 300_000),
    ("15m", 900_000),
    ("30m", 1_800_000),
    ("1h", 3_600_000),
    ("2h", 7_200_000),
    ("4h", 14_400_000),
    ("6h", 21_600_000),
    ("8h", 28_800_000),
    ("12h", 43_200_000),
    ("1d", 86_400_000),
    ("3d", 259_200_000),
    ("1w", 604_800_000),
    ("1M", 2_592_000_000),
];
impl Interval {
    pub fn index(self) -> usize {
        self.0 as usize
    }
    pub fn all() -> impl Iterator<Item = Self> {
        (0..INTERVALS.len()).map(|i| Self(i as u8))
    }
    pub fn parse(code: &str) -> Option<Self> {
        INTERVALS
            .iter()
            .position(|(s, _)| *s == code)
            .map(|i| Self(i as u8))
    }
    pub fn code(self) -> &'static str {
        INTERVALS[self.0 as usize].0
    }
    /// Java compatibility: 1w and 1M use the original fixed duration boundaries.
    /// Calendar-aware month/week alignment is a separate migration decision.
    pub fn millis(self) -> i64 {
        INTERVALS[self.0 as usize].1
    }
    pub fn boundary(self, now: i64) -> i64 {
        now.div_euclid(self.millis()) * self.millis()
    }
}

/// Double/float retain inline numeric storage. Exact modes share immutable fields.
/// Order: open, high, low, close, volume, quote volume, taker base, taker quote.
#[derive(Debug, Clone, Default)]
pub struct Bar {
    pub open_time: i64,
    pub close_time: i64,
    pub trades: u32,
    pub values: [f64; 8],
    pub number_type: NumberType,
    pub exact: Option<std::sync::Arc<ExactNumbers>>,
}
impl Bar {
    pub fn validate(&self) -> Result<(), InvalidBar> {
        if self.open_time < 0 || self.close_time < self.open_time {
            return Err(InvalidBar::Time);
        }
        if self.trades > i32::MAX as u32 {
            return Err(InvalidBar::Trades);
        }
        if !self.valid_numbers() {
            return Err(InvalidBar::Number);
        }
        Ok(())
    }
}
impl PartialEq for Bar {
    fn eq(&self, other: &Self) -> bool {
        self.open_time == other.open_time
            && self.close_time == other.close_time
            && self.trades == other.trades
            && self.numbers_equal(other)
    }
}
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidBar {
    #[error("invalid bar timestamps")]
    Time,
    #[error("trade count exceeds the Java int range")]
    Trades,
    #[error("non-finite numeric field")]
    Number,
}
