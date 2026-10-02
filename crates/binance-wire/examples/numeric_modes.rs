use binance_wire::{DisplayBar, parse_rest_bars_with_type, parse_with_type};
use kline_core::{Bar, NumberType, Series, Source, Update};
use serde::Deserialize;
use serde_json::json;
use std::num::NonZeroUsize;
#[derive(Deserialize)]
struct Case {
    mode: String,
    value: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let cases: Vec<Case> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let mut output = vec![];
    for case in cases {
        let mode = NumberType::parse(&case.mode).unwrap();
        let s = &case.value;
        let wire = serde_json::to_vec(&json!([[0, s, s, s, s, s, 3599999, s, 1, s, s, "0"]]))?;
        let bar = parse_rest_bars_with_type(&wire, mode)?.remove(0);
        let frame = serde_json::to_vec(
            &json!({"e":"kline","s":"BTCUSDT","E":123,"k":{"t":0,"T":3599999,"i":"1h","o":s,"h":s,"l":s,"c":s,"v":s,"q":s,"V":s,"Q":s,"n":1,"x":true}}),
        )?;
        let parsed = parse_with_type(&frame, 1, mode)?.unwrap().update.bar;
        let fill = bar.synthetic_after(3_600_000, 3_600_000);
        let mut series = Series::new(NonZeroUsize::new(10).unwrap());
        let mut commits = vec![];
        for price in [s.as_str(), "1E2", "100.00", "100", "1.00E2"] {
            let mut revision = Bar {
                close_time: 3_599_999,
                trades: 1,
                ..Bar::default()
            };
            revision.set_numbers(mode, [price; 8])?;
            let c = series.commit(Update {
                bar: revision,
                closed: true,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            })?;
            commits.push((c.updated, c.became_final, c.final_revised));
        }
        output.push(json!({"rest":DisplayBar(bar),"ws":DisplayBar(parsed),"fill":DisplayBar(fill),"commits":commits}));
    }
    std::fs::write(&args[2], serde_json::to_vec(&output)?)?;
    Ok(())
}
