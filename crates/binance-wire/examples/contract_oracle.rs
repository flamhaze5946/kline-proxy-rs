use binance_wire::{DisplayBar, display_number};
use kline_core::{Bar, Series, Source, Update};
use serde::Deserialize;
use std::num::NonZeroUsize;
#[derive(Deserialize)]
struct Input {
    numbers: Vec<String>,
    actions: Vec<Action>,
}
#[derive(Deserialize)]
struct Action {
    open: i64,
    trades: u32,
    price: f64,
    closed: bool,
    source: String,
    event: Option<i64>,
    sequence: u64,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let input: Input = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let numbers: Vec<_> = input
        .numbers
        .iter()
        .map(|s| {
            let n: f64 = s.parse().unwrap();
            (
                format!("{:x}", n.to_bits()),
                display_number(n).unwrap().to_string(),
            )
        })
        .collect();
    let mut series = Series::new(NonZeroUsize::new(1000).unwrap());
    let mut commits = Vec::new();
    for a in input.actions {
        let source = match a.source.as_str() {
            "STREAM" => Source::Stream,
            "REST" => Source::Rest,
            "RESTORE" => Source::Restore,
            "SYNTHETIC" => Source::Synthetic,
            _ => panic!("invalid source"),
        };
        let update = Update {
            bar: Bar {
                open_time: a.open,
                close_time: a.open + 3599999,
                trades: a.trades,
                values: [100., 110., 90., a.price, 1000., 100000., 500., 50000.],

                ..Bar::default()
            },
            closed: a.closed,
            source,
            event_time: a.event,
            sequence: a.sequence,
        };
        let commit = series.commit(update)?;
        let (bar, closed) = series.get(a.open).unwrap();
        commits.push((
            (commit.updated, commit.became_final, commit.final_revised),
            DisplayBar(bar),
            closed,
        ));
    }
    let output = serde_json::json!({"numbers":numbers,"commits":commits});
    std::fs::write(&args[2], serde_json::to_vec(&output)?)?;
    Ok(())
}
