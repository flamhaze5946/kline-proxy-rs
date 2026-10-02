//! Read-only import of Java's version-1 daily JSON shards into the Rust snapshot format.
use anyhow::{Context, Result};
use kline_core::Bar;
use kline_service::Slot;
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path};
#[derive(Deserialize)]
struct Shard {
    version: u32,
    service: String,
    interval: String,
    symbol: String,
    rows: Vec<serde_json::Value>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Row {
    open_time: i64,
    close_time: i64,
    trade_num: u32,
    open_price: String,
    high_price: String,
    low_price: String,
    close_price: String,
    volume: String,
    quote_volume: String,
    active_buy_volume: String,
    active_buy_quote_volume: String,
}
pub fn load(
    root: &Path,
    slot: &Slot,
    capacity: usize,
    now: i64,
    mode: kline_core::NumberType,
) -> Result<Vec<Bar>> {
    anyhow::ensure!(
        slot.symbol
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
            && !slot.symbol.contains(".."),
        "unsafe legacy symbol path"
    );
    let directory = root
        .join(crate::config::market_name(slot.market))
        .join(slot.interval.code())
        .join(slot.symbol.as_ref());
    if !directory.is_dir() {
        return Ok(vec![]);
    }
    let mut files: Vec<_> = std::fs::read_dir(&directory)?
        .filter_map(|p| p.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.ends_with(".json") && !s.starts_with('_'))
        })
        .collect();
    files.sort();
    files.reverse();
    let mut bars = BTreeMap::new();
    for path in files {
        if bars.len() >= capacity {
            break;
        }
        // Validate the shard identity before publishing. Like Java, a malformed
        // numeric row is skipped without discarding the other rows in that day.
        let parsed = (|| {
            let mut validated = Vec::new();
            anyhow::ensure!(
                std::fs::metadata(&path)?.len() <= 32 * 1024 * 1024,
                "legacy shard too large"
            );
            let shard: Shard = serde_json::from_slice(&std::fs::read(&path)?)
                .with_context(|| format!("invalid legacy shard {}", path.display()))?;
            anyhow::ensure!(
                shard.version == 1
                    && shard.service == crate::config::market_name(slot.market)
                    && shard.interval == slot.interval.code()
                    && shard.symbol == slot.symbol.as_ref(),
                "legacy shard identity mismatch"
            );
            for raw in shard.rows {
                let parsed = (|| {
                    let row: Row = serde_json::from_value(raw)?;
                    let mut bar = Bar {
                        open_time: row.open_time,
                        close_time: row.close_time,
                        trades: row.trade_num,
                        ..Bar::default()
                    };
                    bar.set_numbers(
                        mode,
                        [
                            &row.open_price,
                            &row.high_price,
                            &row.low_price,
                            &row.close_price,
                            &row.volume,
                            &row.quote_volume,
                            &row.active_buy_volume,
                            &row.active_buy_quote_volume,
                        ],
                    )?;
                    bar.validate()?;
                    Ok::<_, anyhow::Error>(bar)
                })();
                match parsed {
                    Ok(bar) if bar.close_time <= now => validated.push(bar),
                    Ok(_) => {}
                    Err(error) => tracing::warn!(path=%path.display(),%error,"legacy row skipped"),
                }
            }
            Ok::<_, anyhow::Error>(validated)
        })();
        match parsed {
            Ok(rows) => {
                for bar in rows {
                    bars.entry(bar.open_time).or_insert(bar);
                }
            }
            Err(error) => tracing::warn!(path=%path.display(),%error,"legacy shard skipped"),
        }
    }
    let mut bars: Vec<_> = bars.into_values().rev().take(capacity).collect();
    bars.reverse();
    Ok(bars)
}
