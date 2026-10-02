use crate::{
    config::{Config, market},
    lifecycle::{Feed, Health},
};
use anyhow::{Context, Result};
use kline_core::{Interval, Market};
use kline_service::{
    Engine, Instrument,
    upstream::{self, StreamStatus},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::watch;

pub async fn discover(
    config: &Config,
    metadata: &Arc<kline_market::metadata::Metadata>,
) -> Result<Vec<Instrument>> {
    let mut instruments: BTreeMap<_, _> = config
        .static_instruments()?
        .into_iter()
        .map(|i| ((i.market, i.interval, i.symbol.clone()), i))
        .collect();
    for sub in &config.subscriptions {
        let m = market(&sub.market)?;
        let exchange = metadata.get(m).await?;
        let interval = Interval::parse(&sub.interval).context("interval")?;
        let patterns: Vec<_> = sub
            .symbol_patterns
            .iter()
            .map(|p| regex::Regex::new(&format!("^(?:{p})$")))
            .collect::<std::result::Result<_, _>>()?;
        for symbol in exchange["symbols"].as_array().into_iter().flatten() {
            let Some(name) = symbol["symbol"].as_str() else {
                continue;
            };
            if symbol["status"] != "TRADING" || !patterns.iter().any(|p| p.is_match(name)) {
                continue;
            }
            let continuous = if sub.continuous {
                let pair = symbol["pair"]
                    .as_str()
                    .context("missing pair for continuous series")?;
                let contract = symbol["contractType"]
                    .as_str()
                    .context("missing contractType")?;
                if !matches!(contract, "PERPETUAL" | "TRADIFI_PERPETUAL") {
                    continue;
                }
                Some((pair.into(), contract.into()))
            } else {
                None
            };
            let capacity = *sub
                .symbol_capacities
                .get(name)
                .unwrap_or(&sub.history_capacity);
            anyhow::ensure!((2..=100_000).contains(&capacity), "invalid symbol capacity");
            instruments
                .entry((m, interval, name.to_owned()))
                .or_insert(Instrument {
                    market: m,
                    symbol: name.into(),
                    interval,
                    trading: true,
                    continuous,
                    capacity: NonZeroUsize::new(capacity).unwrap(),
                });
        }
    }
    Ok(instruments.into_values().collect())
}
#[derive(Clone)]
struct Spec {
    market: Market,
    url: String,
    ids: Arc<[usize]>,
    topics: Arc<[(String, usize)]>,
}
fn specs(engine: &Engine, config: &Config) -> Result<BTreeMap<String, Spec>> {
    let mut result = BTreeMap::new();
    if !config.streams.is_empty() {
        for (n, s) in config.streams.iter().enumerate() {
            let market = market(&s.market)?;
            let ids = engine
                .catalog
                .slots()
                .iter()
                .enumerate()
                .filter(|(_, s)| s.market == market && s.is_trading() && s.is_tracked())
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
                .into();
            result.insert(
                format!("explicit-{n}"),
                Spec {
                    market,
                    url: s.url.clone(),
                    ids,
                    topics: Arc::from([]),
                },
            );
        }
        return Ok(result);
    }
    let mut topics: [Vec<(String, usize)>; 2] = std::array::from_fn(|_| vec![]);
    for i in engine.catalog.instruments() {
        if !i.trading {
            continue;
        }
        let id = engine
            .catalog
            .find(i.market, i.interval, &i.symbol)
            .unwrap();
        let topic = if let Some((pair, contract)) = i.continuous {
            format!(
                "{}_{}@continuousKline_{}",
                pair.to_lowercase(),
                contract.to_lowercase(),
                i.interval.code()
            )
        } else {
            format!("{}@kline_{}", i.symbol.to_lowercase(), i.interval.code())
        };
        topics[i.market as usize].push((topic, id));
    }
    for market in [Market::Future, Market::Spot] {
        // Stable numeric ID buckets avoid restarting every connection when a symbol is added.
        let mut groups: BTreeMap<usize, Vec<(String, usize)>> = BTreeMap::new();
        for (topic, id) in &topics[market as usize] {
            groups
                .entry(id / config.websocket.batch_size)
                .or_default()
                .push((topic.clone(), *id));
        }
        for (group, batch) in groups {
            let root = if market == Market::Future {
                &config.websocket.future_url
            } else {
                &config.websocket.spot_url
            };
            let mut url = reqwest::Url::parse(root)?;
            url.query_pairs_mut().append_pair(
                "streams",
                &batch
                    .iter()
                    .map(|(t, _)| t.as_str())
                    .collect::<Vec<_>>()
                    .join("/"),
            );
            let ids = batch.iter().map(|(_, id)| *id).collect::<Vec<_>>().into();
            result.insert(
                format!("{market:?}-{group}"),
                Spec {
                    market,
                    url: url.into(),
                    ids,
                    topics: batch.into(),
                },
            );
        }
    }
    Ok(result)
}
struct Running {
    spec: Spec,
    status: Arc<StreamStatus>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    commands: tokio::sync::mpsc::Sender<Vec<String>>,
    resubscribed: BTreeMap<usize, i64>,
}
pub async fn supervise(
    engine: Arc<Engine>,
    config: Arc<Config>,
    health: Arc<Health>,
    mut stop: watch::Receiver<bool>,
) {
    let mut running: BTreeMap<String, Running> = BTreeMap::new();
    let mut desired = BTreeMap::new();
    let mut generation = None;
    loop {
        let current = engine.catalog.generation();
        if generation != Some(current) {
            desired = match specs(&engine, &config) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error=%e,"invalid subscription set");
                    break;
                }
            };
            generation = Some(current);
        }
        let now = engine.now_ms();
        let obsolete: Vec<_> = running
            .iter()
            .filter(|(key, r)| {
                let changed = desired
                    .get(*key)
                    .is_none_or(|s| s.url != r.spec.url || s.ids != r.spec.ids);
                changed || r.task.is_finished()
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut changed = !obsolete.is_empty();
        for key in obsolete {
            if let Some(r) = running.remove(&key) {
                r.status.connected.store(false, Ordering::Release);
                let _ = r.stop.send(true);
                let _ = r.task.await;
                tracing::info!(key, "subscription directory changed; reconnecting");
            }
        }
        for (key, spec) in &desired {
            if running.contains_key(key) {
                continue;
            }
            changed = true;
            let status = Arc::new(StreamStatus::default());
            let (tx, rx) = watch::channel(false);
            let (commands, receiver) = tokio::sync::mpsc::channel(2);
            let task = tokio::spawn(upstream::run_controlled(
                engine.clone(),
                spec.market,
                spec.url.clone(),
                rx,
                status.clone(),
                Some(receiver),
            ));
            running.insert(
                key.clone(),
                Running {
                    spec: spec.clone(),
                    status,
                    stop: tx,
                    task,
                    commands,
                    resubscribed: BTreeMap::new(),
                },
            );
        }
        // A quiet symbol is not a dead connection. Resubscribe only its topic and
        // request REST reconciliation; preserve every healthy stream epoch.
        for (key, r) in &mut running {
            if !r.status.connected.load(Ordering::Acquire) {
                continue;
            }
            let started = r.status.connected_since_ms.load(Ordering::Relaxed);
            let stale: Vec<_> = r
                .spec
                .topics
                .iter()
                .filter(|(_, id)| {
                    let last = engine
                        .catalog
                        .slot(*id)
                        .last_stream_ms
                        .load(Ordering::Relaxed)
                        .max(started)
                        .max(r.resubscribed.get(id).copied().unwrap_or(0));
                    now - last > config.websocket.topic_stale_seconds as i64 * 1000
                })
                .cloned()
                .collect();
            if !stale.is_empty()
                && r.commands
                    .try_send(stale.iter().map(|(topic, _)| topic.clone()).collect())
                    .is_ok()
            {
                for (_, id) in &stale {
                    r.resubscribed.insert(*id, now);
                }
                tracing::info!(
                    key,
                    topics = stale.len(),
                    "stalled topics resubscribed without reconnecting healthy streams"
                );
            }
        }
        if changed {
            health.replace_feeds(
                running
                    .values()
                    .map(|r| Feed {
                        market: r.spec.market,
                        status: r.status.clone(),
                        ids: Some(r.spec.ids.clone()),
                    })
                    .collect(),
            );
        }
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
    }
    for r in running.values() {
        let _ = r.stop.send(true);
    }
    for (_, r) in running {
        let _ = r.task.await;
    }
    health.replace_feeds(vec![]);
}
pub async fn refresh(
    engine: Arc<Engine>,
    config: Arc<Config>,
    metadata: Arc<kline_market::metadata::Metadata>,
    mut stop: watch::Receiver<bool>,
) {
    let markets: BTreeSet<_> = config
        .subscriptions
        .iter()
        .filter_map(|s| market(&s.market).ok())
        .collect();
    loop {
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(metadata.refresh_interval())=>{}}
        let update = async {
            for &market in &markets {
                metadata.refresh(market).await?;
            }
            let instruments = discover(&config, &metadata).await?;
            let change = engine
                .refresh_catalog(instruments)
                .map_err(anyhow::Error::msg)?;
            if change.changed {
                tracing::info!(
                    added = change.added,
                    removed = change.removed,
                    "trading directory updated"
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {_=stop.changed()=>break,result=update=>if let Err(e)=result{tracing::warn!(error=%e,"directory refresh failed; keeping last valid directory")}}
    }
}
