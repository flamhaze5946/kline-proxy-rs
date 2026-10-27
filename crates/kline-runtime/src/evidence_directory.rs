//! Shared startup/refresh retention merge. A catalog key is (future, 1m, symbol).
use crate::{config::Config, evidence_state};
use anyhow::{Context, Result};
use kline_core::{Interval, Market};
use kline_service::{Instrument, evidence_bulk::RetainedRecord};
use serde_json::Value;
use std::{collections::BTreeMap, num::NonZeroUsize};

pub fn merge(config: &Config, exchange: &Value, now: i64) -> Result<Vec<Instrument>> {
    let evidence = config.evidence.as_ref().context("evidence disabled")?;
    let metadata: BTreeMap<_, _> = exchange["symbols"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v["symbol"].as_str().map(|s| (s, v)))
        .collect();
    let mut instruments = BTreeMap::new();
    for (&symbol, row) in &metadata {
        if row["status"] != "TRADING" {
            continue;
        }
        let Some(capacity) = capacity(config, symbol)? else {
            continue;
        };
        let pair = row["pair"].as_str();
        let contract = row["contractType"].as_str();
        if pair != Some(symbol) || !matches!(contract, Some("PERPETUAL" | "TRADIFI_PERPETUAL")) {
            tracing::warn!(symbol, "ineligible trading evidence instrument excluded");
            continue;
        }
        instruments.insert(
            symbol.to_owned(),
            Instrument {
                market: Market::Future,
                symbol: symbol.into(),
                interval: Interval::parse("1m").unwrap(),
                trading: true,
                continuous: Some((symbol.into(), contract.unwrap().into())),
                capacity,
            },
        );
    }
    let mut runtime = evidence.runtime.lock();
    let old = runtime.records.clone();
    let mut records = old.clone();
    // Cancel even an ineligible return to TRADING, before considering expiry.
    records.retain(|symbol, _| {
        metadata
            .get(symbol.as_str())
            .is_none_or(|row| row["status"] != "TRADING")
    });
    let previous = runtime.previous.clone();
    for instrument in previous.iter().filter(|i| i.trading) {
        if instruments.contains_key(&instrument.symbol)
            || metadata
                .get(instrument.symbol.as_str())
                .is_some_and(|row| row["status"] == "TRADING")
        {
            continue;
        }
        if let Some((pair, contract)) = &instrument.continuous {
            records
                .entry(instrument.symbol.clone())
                .or_insert_with(|| RetainedRecord {
                    symbol: instrument.symbol.clone(),
                    pair: pair.clone(),
                    contract_type: contract.clone(),
                    since: now,
                    expires_ms: now
                        .saturating_add((evidence.not_trading_retention_min * 60_000) as i64),
                    delivery_ms: metadata
                        .get(instrument.symbol.as_str())
                        .and_then(|row| row["deliveryDate"].as_i64())
                        .or_else(|| runtime.delivery.get(&instrument.symbol).copied()),
                });
        }
    }
    for (symbol, record) in &records {
        if record.expires_ms <= now {
            continue;
        }
        let capacity =
            capacity(config, symbol)?.context("retained identity has no matching capacity rule")?;
        instruments.entry(symbol.clone()).or_insert(Instrument {
            market: Market::Future,
            symbol: symbol.clone(),
            interval: Interval::parse("1m").unwrap(),
            trading: false,
            continuous: Some((record.pair.clone(), record.contract_type.clone())),
            capacity,
        });
    }
    // Revocations above must not erase the retry basis for an expired orphan.
    // Cleanup is based on old records and the fully merged directory.
    for (symbol, record) in old.iter().chain(records.iter()) {
        if record.expires_ms <= now
            && !instruments.contains_key(symbol)
            && let Some(p) = &config.persistence
        {
            crate::storage::remove_snapshot_identity(
                &p.directory,
                Market::Future,
                Interval::parse("1m").unwrap(),
                symbol,
            )?;
        }
    }
    records.retain(|_, record| record.expires_ms > now);
    if old != records {
        runtime.generation = runtime.generation.wrapping_add(1);
    }
    runtime.records = records;
    runtime
        .delivery
        .retain(|symbol, _| instruments.contains_key(symbol));
    for (symbol, row) in metadata {
        if let Some(delivery) = row["deliveryDate"].as_i64()
            && instruments.contains_key(symbol)
        {
            runtime.delivery.insert(symbol.to_owned(), delivery);
        }
    }
    let result: Vec<_> = instruments.into_values().collect();
    runtime.previous = result.clone();
    drop(runtime);
    if let Err(error) = evidence_state::persist_retained(config) {
        tracing::warn!(%error,"retained write failed; remains dirty");
    }
    Ok(result)
}
fn capacity(config: &Config, symbol: &str) -> Result<Option<NonZeroUsize>> {
    if let Some(instrument) = config
        .static_instruments()?
        .into_iter()
        .find(|i| i.symbol == symbol)
    {
        return Ok(Some(instrument.capacity));
    }
    for sub in &config.subscriptions {
        for pattern in &sub.symbol_patterns {
            if regex::Regex::new(&format!("^(?:{pattern})$"))?.is_match(symbol) {
                return Ok(NonZeroUsize::new(
                    *sub.symbol_capacities
                        .get(symbol)
                        .unwrap_or(&sub.history_capacity),
                ));
            }
        }
    }
    Ok(None)
}
