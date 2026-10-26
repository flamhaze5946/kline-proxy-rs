//! Funding work uses async single-flight caches and its own upstream pacing lane.
use crate::{
    config::FundingConfig,
    error::{ApiError, Result},
    metadata::Metadata,
    transport::RestApi,
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use kline_core::Market;
use kline_service::{Clock, SystemClock};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
pub const H: i64 = 3_600_000;
const MAX_CHUNK_WINDOW: i128 = 1000;
pub fn now() -> i64 {
    SystemClock { offset_ms: 0 }.now_ms()
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Rate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding_time: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_type: Option<String>,
}
pub type Chunk = BTreeMap<String, Vec<Rate>>;
#[derive(Clone, Default, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct Query {
    #[serde(
        default,
        deserialize_with = "kline_service::http_compat::optional_strings"
    )]
    pub symbols: Option<Vec<Option<String>>>,
    #[serde(default, deserialize_with = "kline_service::http_compat::optional_i64")]
    pub since_ms: Option<i64>,
    #[serde(default, deserialize_with = "kline_service::http_compat::optional_i64")]
    pub until_ms: Option<i64>,
    #[serde(default, deserialize_with = "kline_service::http_compat::optional_i32")]
    pub limit: Option<i32>,
}
impl kline_service::http_compat::JsonBody for Query {
    const RECORD: bool = true;
    const STRING_LIST_FIELDS: &'static [&'static str] = &["symbols"];
    const INTEGER_FIELDS: &'static [&'static str] = &["since_ms", "until_ms", "limit"];
}
#[derive(Clone, Hash, PartialEq, Eq)]
struct Recent {
    limit: usize,
    boundary: i64,
    revision: u64,
}
#[derive(Clone, Hash, PartialEq, Eq)]
struct RecentSymbol {
    symbol: String,
    limit: usize,
    boundary: i64,
    revision: u64,
}
pub struct Funding {
    pub api: Arc<RestApi>,
    pub metadata: Arc<Metadata>,
    pub config: FundingConfig,
    pub chunks: Cache<i64, Arc<Chunk>>,
    recent: Cache<Recent, Bytes>,
    recent_symbols: Cache<RecentSymbol, Arc<Vec<Rate>>>,
    revision: AtomicU64,
    pub upstream_loads: AtomicU64,
}
impl Funding {
    pub fn new(api: Arc<RestApi>, metadata: Arc<Metadata>, config: FundingConfig) -> Arc<Self> {
        Arc::new(Self {
            api,
            metadata,
            config,
            chunks: Cache::builder().max_capacity(720).build(),
            recent: Cache::builder()
                .max_capacity(64)
                .time_to_live(Duration::from_secs(60))
                .build(),
            recent_symbols: Cache::builder()
                .max_capacity(2048)
                .time_to_live(Duration::from_secs(60))
                .build(),
            revision: AtomicU64::new(0),
            upstream_loads: AtomicU64::new(0),
        })
    }
    pub async fn raw(
        &self,
        symbol: Option<&str>,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<i32>,
    ) -> Result<Vec<Rate>> {
        let mut params = Vec::new();
        if let Some(s) = symbol {
            params.push(("symbol".into(), s.into()))
        }
        if let Some(t) = start {
            params.push(("startTime".into(), t.to_string()))
        }
        if let Some(t) = end {
            params.push(("endTime".into(), t.to_string()))
        }
        if let Some(n) = limit {
            params.push(("limit".into(), n.to_string()))
        }
        self.upstream_loads.fetch_add(1, Ordering::Relaxed);
        let mut rates: Vec<Rate> = self
            .api
            .json::<Vec<Rate>>(Market::Future, "/fapi/v1/fundingRate", &params, 1)
            .await
            .map_err(ApiError::from)?;
        for r in &mut rates {
            for n in [&mut r.funding_rate, &mut r.mark_price]
                .into_iter()
                .flatten()
            {
                *n = crate::decimal(n)?;
            }
        }
        Ok(rates)
    }
    async fn load_chunk(&self, start: i64) -> Result<Chunk> {
        let wait = start
            .saturating_add(self.config.publication_grace_ms as i64)
            .saturating_sub(now());
        if wait > 0 && wait <= self.config.publication_grace_ms as i64 {
            tokio::time::sleep(Duration::from_millis(wait as u64)).await;
        }
        let end = start.wrapping_add(H);
        let rows = self.raw(None, Some(start), Some(end), Some(1000)).await?;
        if rows.len() >= 1000 {
            tracing::warn!(
                start,
                "funding hour reached 1000 rows; upstream may have truncated it"
            );
        }
        let mut chunk: Chunk = BTreeMap::new();
        for r in rows {
            if let (Some(t), Some(s)) = (r.funding_time, r.symbol.as_ref())
                && t >= start
                && t < end
                && !kline_service::http_compat::is_blank(s)
            {
                chunk.entry(s.clone()).or_default().push(r);
            }
        }
        for rows in chunk.values_mut() {
            rows.sort_by_key(|r| r.funding_time);
        }
        Ok(chunk)
    }
    pub async fn chunk(&self, start: i64) -> Result<Arc<Chunk>> {
        self.chunks
            .try_get_with(start, async { self.load_chunk(start).await.map(Arc::new) })
            .await
            .map_err(|e| (*e).clone())
    }
    pub async fn bulk(&self, q: Query) -> Result<Bytes> {
        let symbols: Vec<String> = q
            .symbols
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .map(|s| kline_service::http_compat::java_trim(&s).to_owned())
            .filter(|s| !kline_service::http_compat::is_blank(s))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let ranged = q.since_ms.is_some() || q.until_ms.is_some();
        let limit = q
            .limit
            .unwrap_or(1)
            .clamp(1, if ranged { 1000 } else { 100 }) as usize;
        if let (Some(start), Some(end)) = (q.since_ms, q.until_ms) {
            if start >= end {
                return self.encode(BTreeMap::new()).await;
            }
            let span = end.wrapping_sub(start);
            if symbols.is_empty() || (span > 0 && span <= 8 * H) {
                return self
                    .encode(self.window(&symbols, start, end, limit).await?)
                    .await;
            }
        }
        if !ranged {
            if !symbols.is_empty() {
                // Cache by symbol, not the client's combination. Do not add a
                // second body TTL on top of the existing 60-second data TTL.
                return self
                    .encode(self.per_symbol(symbols, None, None, limit).await?)
                    .await;
            }
            let request_now = now();
            let boundary = request_now.div_euclid(H) * H;
            let key = Recent {
                limit,
                boundary,
                revision: self.revision.load(Ordering::Acquire),
            };
            let load = async {
                self.encode(self.window(&[], now() - 8 * H, now(), limit).await?)
                    .await
            };
            if request_now - boundary < (self.config.publication_grace_ms as i64) {
                return load.await;
            }
            return self
                .recent
                .try_get_with(key, load)
                .await
                .map_err(|e| (*e).clone());
        }
        let symbols = if symbols.is_empty() {
            self.metadata.symbols(Market::Future, true).await?
        } else {
            symbols
        };
        self.encode(
            self.per_symbol(symbols, q.since_ms, q.until_ms, limit)
                .await?,
        )
        .await
    }
    async fn encode(&self, chunk: Chunk) -> Result<Bytes> {
        let large = chunk.len() > 128 || chunk.values().map(Vec::len).sum::<usize>() > 512;
        let work = move || encode(chunk);
        if large {
            self.api.cpu.run_large(work).await.map_err(ApiError::from)?
        } else {
            self.api.cpu.run(work).await.map_err(ApiError::from)?
        }
    }
    async fn window(
        &self,
        symbols: &[String],
        start: i64,
        end: i64,
        limit: usize,
    ) -> Result<Chunk> {
        // Match Java long arithmetic without a Rust overflow panic. An extreme
        // negative floor can wrap into the future and is then skipped below.
        let first = start.div_euclid(H).wrapping_mul(H);
        let last = end.wrapping_sub(1).div_euclid(H).wrapping_mul(H);
        let current = now();
        let chunks = (i128::from(last.min(current)) - i128::from(first)) / i128::from(H) + 1;
        if chunks > MAX_CHUNK_WINDOW {
            return Err(ApiError::bad(
                -1130,
                "Funding window exceeds 1000 hourly chunks; narrow the window or specify symbols for a bounded REST query.",
            ));
        }
        // Stream chunks into newest-N buckets: memory is bounded independently of the range.
        let mut out: Chunk = symbols.iter().map(|s| (s.clone(), Vec::new())).collect();
        let append = |bucket: &mut Vec<Rate>, rows: &[Rate]| {
            bucket.extend(
                rows.iter()
                    .filter(|r| r.funding_time.is_some_and(|v| v >= start && v < end))
                    .cloned(),
            );
            if bucket.len() > limit {
                bucket.drain(..bucket.len() - limit);
            }
        };
        let mut t = first;
        while t <= last && t <= current {
            let chunk = self.chunk(t).await?;
            if symbols.is_empty() {
                for (symbol, rows) in chunk.iter() {
                    append(out.entry(symbol.clone()).or_default(), rows);
                }
            } else {
                // Requested buckets are preseeded, including symbols with no
                // records. Look them up directly instead of scanning the market.
                for (symbol, bucket) in &mut out {
                    if let Some(rows) = chunk.get(symbol) {
                        append(bucket, rows);
                    }
                }
            }
            let Some(next) = t.checked_add(H) else { break };
            t = next;
        }
        Ok(out)
    }
    async fn per_symbol(
        &self,
        symbols: Vec<String>,
        start: Option<i64>,
        end: Option<i64>,
        limit: usize,
    ) -> Result<Chunk> {
        let ranged = start.is_some() || end.is_some();
        let page = if ranged { 1000 } else { limit };
        let rows:Vec<_>=stream::iter(symbols).map(|symbol|async move{
            let mut rows = if ranged {
                self.raw(Some(&symbol), start, end, Some(page as i32)).await?
            } else {
                let key = RecentSymbol {
                    symbol: symbol.clone(), limit,
                    boundary: now().div_euclid(H) * H,
                    revision: self.revision.load(Ordering::Acquire),
                };
                self.recent_symbols.try_get_with(key, async {
                    self.raw(Some(&symbol), None, None, Some(page as i32)).await.map(Arc::new)
                }).await.map_err(|e| (*e).clone())?.as_ref().clone()
            };
            let max_events=match(start,end){(Some(a),Some(b))if a<b=>(b-1).div_euclid(H)-a.div_euclid(H)+1,_=>i64::MAX};
            if ranged&&rows.len()>=page&&max_events>page as i64{return Err(ApiError::bad(-1130,format!("funding range [{start:?}, {end:?}) for {symbol} filled the {page}-row page; narrow the window or query Binance directly.")))}
            rows.retain(|r|r.funding_time.is_some_and(|t|start.is_none_or(|s|t>=s)&&end.is_none_or(|e|t<e)));rows.sort_by_key(|r|r.funding_time);if rows.len()>limit{rows.drain(..rows.len()-limit);}for r in &mut rows{if r.symbol.as_ref().is_none_or(|s|kline_service::http_compat::is_blank(s)){r.symbol=Some(symbol.clone());}}
            Ok((symbol,rows))
        }).buffer_unordered(4).try_collect().await?;
        Ok(rows.into_iter().collect())
    }
    /// Refresh publication data, invalidating recent caches only when it changes.
    pub async fn retry_boundary(&self, boundary: i64) -> Result<()> {
        self.refresh_boundary(boundary, false).await
    }
    /// The existing :05 refresh also renews independent per-symbol recent caches.
    pub async fn force_retry_boundary(&self, boundary: i64) -> Result<()> {
        self.refresh_boundary(boundary, true).await
    }
    async fn refresh_boundary(&self, boundary: i64, force_recent: bool) -> Result<()> {
        // Fetch before replacing: readers keep the last good snapshot while the
        // background retry is slow or fails. A nonempty snapshot may still be
        // partial, so every scheduled retry runs even after the first rows arrive.
        let changed = if let Some(previous) = self.chunks.get(&boundary).await {
            let mut fresh = self.load_chunk(boundary).await?;
            // Publication is incremental. Retain already observed records if a
            // later successful response is empty/partial; newer values for the
            // same symbol, timestamp and rate type replace earlier ones.
            for (symbol, rows) in previous.iter() {
                let bucket = fresh.entry(symbol.clone()).or_default();
                for row in rows {
                    if !bucket.iter().any(|new| {
                        new.funding_time == row.funding_time && new.rate_type == row.rate_type
                    }) {
                        bucket.push(row.clone());
                    }
                }
                bucket.sort_by_key(|row| row.funding_time);
            }
            let changed = previous.as_ref() != &fresh;
            if changed {
                self.chunks.insert(boundary, Arc::new(fresh)).await;
            }
            changed
        } else {
            // Join an in-flight cold load rather than racing it with a second
            // request that could publish in the opposite order.
            self.chunk(boundary).await?;
            true
        };
        if changed || force_recent {
            self.revision.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        let mut current = now();
        let mut boundary = current.div_euclid(H) * H;
        let mut warmed = false;
        let mut retried = current - boundary >= 5 * 60_000;
        loop {
            let warm_at = boundary + self.config.publication_grace_ms as i64 + 1;
            let final_retry_at = boundary + 5 * 60_000;
            let due = if !warmed {
                warm_at.max(current)
            } else if !retried {
                next_publication_retry(boundary, self.config.publication_grace_ms, current)
                    .unwrap_or(final_retry_at)
            } else {
                boundary + H + self.config.publication_grace_ms as i64 + 1
            };
            tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_millis(due.saturating_sub(now()).max(0) as u64))=>{}}
            current = now();
            let next = current.div_euclid(H) * H;
            if next != boundary {
                boundary = next;
                warmed = false;
                retried = current - boundary >= 5 * 60_000;
            }
            let retry = warmed && !retried;
            let work = async {
                if retry && current >= final_retry_at {
                    self.force_retry_boundary(boundary).await
                } else if retry {
                    self.retry_boundary(boundary).await
                } else {
                    self.bulk(Query::default()).await.map(|_| ())
                }
            };
            tokio::select! {_=stop.changed()=>break,result=work=>if let Err(e)=result{tracing::warn!(error=%e,retry,"funding scheduled warm failed")}}
            if retry && current >= final_retry_at {
                retried = true;
                if self
                    .chunks
                    .get(&boundary)
                    .await
                    .is_some_and(|chunk| chunk.is_empty())
                {
                    for cadence in [4, 8] {
                        if self
                            .chunks
                            .get(&(boundary - cadence * H))
                            .await
                            .is_some_and(|c| !c.is_empty())
                            && self
                                .chunks
                                .get(&(boundary - 2 * cadence * H))
                                .await
                                .is_some_and(|c| !c.is_empty())
                        {
                            tracing::warn!(
                                boundary,
                                cadence,
                                "funding settlement snapshot remains unexpectedly empty after publication retries"
                            );
                            break;
                        }
                    }
                }
            }
            warmed = true;
            // Skip missed checkpoints instead of issuing catch-up bursts after a
            // slow upstream call. The next hour always starts a fresh schedule.
            current = now();
        }
    }
}
fn encode(funding: Chunk) -> Result<Bytes> {
    serde_json::to_vec(&serde_json::json!({"ts_ms":now(),"fundingRates":funding}))
        .map(Bytes::from)
        .map_err(ApiError::internal)
}

// Seven background attempts at most, relative to the configured publication
// grace. Keep the existing absolute :05 refresh; attempts beyond it are skipped.
fn next_publication_retry(boundary: i64, grace_ms: u64, after: i64) -> Option<i64> {
    [1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000]
        .into_iter()
        .map(|delay| {
            boundary
                .saturating_add(grace_ms as i64)
                .saturating_add(delay)
        })
        .find(|&due| due > after && due < boundary.saturating_add(300_000))
}

#[cfg(test)]
mod publication_retry_tests {
    use super::*;

    #[test]
    fn publication_retries_are_bounded_and_respect_grace_and_missed_checkpoints() {
        let boundary = 10 * H;
        let mut after = boundary;
        let mut due = Vec::new();
        while let Some(next) = next_publication_retry(boundary, 0, after) {
            due.push(next - boundary);
            after = next;
        }
        assert_eq!(due, [1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000]);
        assert_eq!(
            next_publication_retry(boundary, 500, boundary + 500),
            Some(boundary + 1_500)
        );
        assert_eq!(
            next_publication_retry(boundary, 0, boundary + 65_000),
            Some(boundary + 120_000)
        );
        assert_eq!(next_publication_retry(boundary, 300_000, boundary), None);
        assert_eq!(
            next_publication_retry(boundary, 0, boundary + 300_000),
            None
        );
    }
}
