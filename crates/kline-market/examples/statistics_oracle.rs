use kline_core::Bar;
use serde::Deserialize;
use std::collections::BTreeMap;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Sample {
    mode: String,
    rows: Vec<Rows>,
    days: usize,
    volume_days: usize,
    rank: usize,
}
#[derive(Deserialize)]
struct Rows {
    symbol: String,
    bars: Vec<Row>,
}
#[derive(Deserialize)]
struct Row {
    open: i64,
    values: [serde_json::Value; 8],
}
fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let input: Vec<Sample> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let mut output = vec![];
    for s in input {
        let mode = kline_core::NumberType::parse(&s.mode).unwrap();
        let rows: Vec<_> = s
            .rows
            .into_iter()
            .map(|r| {
                (
                    r.symbol,
                    r.bars
                        .into_iter()
                        .map(|b| {
                            let mut bar = Bar {
                                open_time: b.open,
                                close_time: b.open + 86_399_999,
                                trades: 1,
                                ..Bar::default()
                            };
                            let values = b.values.map(|v| {
                                v.as_str()
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| v.to_string())
                            });
                            bar.set_numbers(mode, std::array::from_fn(|i| values[i].as_str()))
                                .unwrap();
                            bar
                        })
                        .collect(),
                )
            })
            .collect();
        let (a, b) = kline_market::statistics::calculate(&rows, s.days, s.volume_days, s.rank)?;
        output.push(BTreeMap::from([("a", a), ("b", b)]));
    }
    std::fs::write(&args[2], serde_json::to_vec(&output)?)?;
    Ok(())
}
