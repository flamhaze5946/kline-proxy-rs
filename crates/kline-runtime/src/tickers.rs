//! Spot retired !ticker@arr; use current per-symbol 24h streams in bounded groups.
use kline_core::Market;
use kline_market::{metadata::Metadata, ticker::Tickers};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::watch;
struct Running {
    url: String,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}
pub async fn supervise(
    tickers: Arc<Tickers>,
    metadata: Arc<Metadata>,
    root: String,
    market: Market,
    mut stop: watch::Receiver<bool>,
) {
    let mut running: BTreeMap<(bool, usize), Running> = BTreeMap::new();
    loop {
        let desired = async {
            let topics = if market == Market::Future {
                vec!["!ticker@arr".to_owned()]
            } else {
                metadata
                    .symbols(market, true)
                    .await?
                    .into_iter()
                    .map(|s| format!("{}@ticker", s.to_lowercase()))
                    .collect()
            };
            let mut desired = BTreeMap::new();
            for (key, batch) in topics.chunks(200).enumerate() {
                let mut url = reqwest::Url::parse(&root)?;
                url.query_pairs_mut()
                    .append_pair("streams", &batch.join("/"));
                desired.insert((false, key), url.to_string());
            }
            if market == Market::Future {
                let trading = metadata.symbols(market, true).await?;
                let prices: Vec<_> = tickers
                    .wanted_price_symbols(&trading)
                    .into_iter()
                    .map(|s| format!("{}@aggTrade", s.to_lowercase()))
                    .collect();
                for (key, batch) in prices.chunks(200).enumerate() {
                    let mut url = reqwest::Url::parse(&root)?;
                    url.query_pairs_mut()
                        .append_pair("streams", &batch.join("/"));
                    desired.insert((true, key), url.to_string());
                }
            }
            Ok::<_, anyhow::Error>(desired)
        };
        let result = tokio::select! {_=stop.changed()=>break,result=desired=>result};
        match result {
            Ok(desired) => {
                let obsolete: Vec<_> = running
                    .iter()
                    .filter(|(key, r)| desired.get(key) != Some(&r.url) || r.task.is_finished())
                    .map(|(key, _)| *key)
                    .collect();
                for key in obsolete {
                    let r = running.remove(&key).unwrap();
                    let _ = r.stop.send(true);
                    let _ = r.task.await;
                }
                for (key, url) in desired {
                    if running.contains_key(&key) {
                        continue;
                    }
                    let (tx, rx) = watch::channel(false);
                    let task = tokio::spawn(tickers.clone().stream(market, url.clone(), rx));
                    running.insert(
                        key,
                        Running {
                            url,
                            stop: tx,
                            task,
                        },
                    );
                }
            }
            Err(error) => {
                tracing::warn!(?market,%error,"ticker subscriptions refresh failed; retaining current streams")
            }
        }
        tokio::select! {
            _=stop.changed()=>break,
            _=tickers.price_demand_changed(), if market == Market::Future => {},
            _=tokio::time::sleep(Duration::from_secs(60))=>{}
        }
    }
    for r in running.values() {
        let _ = r.stop.send(true);
    }
    for (_, r) in running {
        let _ = r.task.await;
    }
}
