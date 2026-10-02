//! Pooled public REST transport with per-market pacing and shared Retry-After backoff.
#[cfg(test)]
use crate::pacing::PaceWait;
use crate::pacing::Pacer;
pub use crate::pacing::Priority;
use anyhow::{Context, Result};
use kline_core::{Bar, Interval, Market};
use reqwest::{Client, Url};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

struct Reply {
    bytes: Vec<u8>,
    sent_ms: i64,
    elapsed_ms: u64,
}
pub struct ClockSample {
    pub server_time: i64,
    pub sent_ms: i64,
    pub elapsed_ms: u64,
}
pub struct RestApi {
    number_type: kline_core::NumberType,
    pub cpu: Arc<crate::cpu::CpuPool>,
    client: Client,
    roots: [Url; 2],
    pace: [Arc<Pacer>; 3],
    next_request: AtomicU64,
}
impl RestApi {
    pub fn new(spot: &str, future: &str, weight_per_minute: u32) -> Result<Arc<Self>> {
        Self::new_with_type(
            spot,
            future,
            weight_per_minute,
            kline_core::NumberType::Double,
        )
    }
    pub fn new_with_type(
        spot: &str,
        future: &str,
        weight_per_minute: u32,
        number_type: kline_core::NumberType,
    ) -> Result<Arc<Self>> {
        let roots = [endpoint(future)?, endpoint(spot)?];
        anyhow::ensure!(
            (1..=6000).contains(&weight_per_minute),
            "REST budget must be 1..6000 weight/minute"
        );
        Ok(Arc::new(Self {
            number_type,
            cpu: crate::cpu::CpuPool::new(2),
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .pool_max_idle_per_host(8)
                .pool_idle_timeout(Duration::from_secs(90))
                .tcp_nodelay(true)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            roots,
            pace: std::array::from_fn(|lane| {
                if lane == 2 {
                    Pacer::funding(100)
                } else {
                    Pacer::new(weight_per_minute)
                }
            }),
            next_request: AtomicU64::new(1),
        }))
    }
    #[cfg(test)]
    async fn acquire(&self, lane: usize, weight: u32) -> PaceWait {
        self.pace[lane].acquire(weight, Priority::Foreground).await
    }
    #[tracing::instrument(name = "rest_request", skip_all, fields(
        rest_id = self.next_request.fetch_add(1, Ordering::Relaxed), ?market, ?priority, path = path, weight = weight,
        symbol = query.iter().find(|(key, _)| key == "symbol").map(|(_, value)| value.as_str()).unwrap_or("*")
    ))]
    async fn bytes(
        &self,
        market: Market,
        path: &str,
        query: &[(String, String)],
        weight: u32,
        priority: Priority,
    ) -> Result<Reply> {
        let url = self.roots[market as usize].join(path)?;
        let lane = if path.ends_with("/fundingRate") {
            2
        } else {
            market as usize
        };
        let detailed = path.ends_with("/ticker/price") || path.ends_with("/exchangeInfo");
        for attempt in 0..3 {
            let pacing = self.pace[lane]
                .acquire(if lane == 2 { 1 } else { weight }, priority)
                .await;
            if detailed || pacing.elapsed >= Duration::from_secs(1) {
                tracing::info!(
                    attempt,
                    lane,
                    queue_ms = pacing.elapsed.as_secs_f64() * 1000.,
                    wait_rounds = pacing.rounds,
                    cooldown_rounds = pacing.cooldown_rounds,
                    "REST pacing acquired"
                );
            }
            let sent_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let started = Instant::now();
            let response = self.client.get(url.clone()).query(query).send().await;
            let headers_ms = started.elapsed().as_secs_f64() * 1000.;
            if detailed || headers_ms >= 1000. {
                tracing::info!(
                    attempt,
                    headers_ms,
                    status = response.as_ref().map(|r| r.status().as_u16()).unwrap_or(0),
                    "REST headers received"
                );
            }
            let mut response = match response {
                Ok(r) => r,
                Err(error) if attempt < 2 => {
                    tracing::warn!(
                        ?market,
                        attempt,
                        timeout = error.is_timeout(),
                        "REST transport retry"
                    );
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    continue;
                }
                Err(error) => return Err(error.without_url().into()),
            };
            let status = response.status();
            if status.as_u16() == 429 || status.as_u16() == 418 {
                let seconds = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(if status.as_u16() == 418 { 300 } else { 60 })
                    .clamp(1, 7 * 86400);
                let deadline = Instant::now() + Duration::from_secs(seconds);
                self.pace[lane].cooldown(deadline);
                tracing::warn!(status = status.as_u16(), seconds, "REST shared cooldown");
            }
            if status.is_server_error() && attempt < 2 {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                continue;
            }
            let max = if path.ends_with("/exchangeInfo") {
                32 * 1024 * 1024
            } else if path.contains("/ticker/") {
                8 * 1024 * 1024
            } else {
                2 * 1024 * 1024
            };
            anyhow::ensure!(
                response.content_length().unwrap_or(0) <= max as u64,
                "REST response too large"
            );
            let mut bytes = Vec::with_capacity(
                response.content_length().unwrap_or(4096).min(max as u64) as usize,
            );
            while let Some(chunk) = response.chunk().await.context("read REST body")? {
                anyhow::ensure!(bytes.len() + chunk.len() <= max, "REST response too large");
                bytes.extend_from_slice(&chunk);
            }
            if detailed || started.elapsed() >= Duration::from_secs(1) {
                tracing::info!(
                    attempt,
                    status = status.as_u16(),
                    http_ms = started.elapsed().as_secs_f64() * 1000.,
                    queue_ms = pacing.elapsed.as_secs_f64() * 1000.,
                    bytes = bytes.len(),
                    "REST body received"
                );
            }
            if !status.is_success() {
                return Err(http_error(&response, &bytes));
            }
            if matches!(status.as_u16(), 204 | 205) {
                return Err(crate::error::ApiError::upstream_null().into());
            }
            return Ok(Reply {
                bytes,
                sent_ms,
                elapsed_ms: started.elapsed().as_millis() as u64,
            });
        }
        unreachable!()
    }
    pub async fn json<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        market: Market,
        path: &str,
        query: &[(String, String)],
        weight: u32,
    ) -> Result<T> {
        self.json_with_priority(market, path, query, weight, Priority::Foreground)
            .await
    }
    pub async fn json_with_priority<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        market: Market,
        path: &str,
        query: &[(String, String)],
        weight: u32,
        priority: Priority,
    ) -> Result<T> {
        let bytes = self
            .bytes(market, path, query, weight, priority)
            .await?
            .bytes;
        if bytes.len() > 64 * 1024 {
            self.cpu.run_large(move || decode_json(&bytes)).await?
        } else {
            decode_json(&bytes)
        }
    }
    pub async fn external(
        &self,
        url: &str,
        query: &[(String, String)],
        max: usize,
    ) -> Result<Vec<u8>> {
        let url = endpoint(url)?;
        let mut response = self
            .client
            .get(url)
            .query(query)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        anyhow::ensure!(
            response.content_length().unwrap_or(0) <= max as u64,
            "external response too large"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(
                bytes.len() + chunk.len() <= max,
                "external response too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        if !response.status().is_success() {
            return Err(http_error(&response, &bytes));
        }
        if matches!(response.status().as_u16(), 204 | 205) {
            return Err(crate::error::ApiError::upstream_null().into());
        }
        Ok(bytes)
    }
    pub async fn klines(
        &self,
        market: Market,
        symbol: &str,
        interval: Interval,
        start: Option<i64>,
        end: i64,
        limit: usize,
    ) -> Result<Vec<Bar>> {
        self.klines_priority(
            market,
            symbol,
            interval,
            start,
            end,
            limit,
            Priority::Foreground,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn klines_priority(
        &self,
        market: Market,
        symbol: &str,
        interval: Interval,
        start: Option<i64>,
        end: i64,
        limit: usize,
        priority: Priority,
    ) -> Result<Vec<Bar>> {
        let max = match market {
            Market::Spot => 1000,
            Market::Future => 1500,
        };
        anyhow::ensure!(
            limit > 0 && limit <= max && end >= 0,
            "invalid REST range/limit"
        );
        let mut query = vec![
            ("symbol".into(), symbol.into()),
            ("interval".into(), interval.code().into()),
            ("endTime".into(), end.to_string()),
            ("limit".into(), limit.to_string()),
        ];
        if let Some(start) = start {
            anyhow::ensure!(start >= 0 && start <= end, "invalid REST start time");
            query.push(("startTime".into(), start.to_string()));
        }
        let weight = match market {
            Market::Spot => 2,
            Market::Future => match limit {
                1..100 => 1,
                100..500 => 2,
                500..=1000 => 5,
                _ => 10,
            },
        };
        let path = match market {
            Market::Spot => "/api/v3/klines",
            Market::Future => "/fapi/v1/klines",
        };
        let bytes = self
            .bytes(market, path, &query, weight, priority)
            .await?
            .bytes;
        let mode = self.number_type;
        let bars = if bytes.len() > 64 * 1024 {
            self.cpu
                .run_large(move || {
                    let bytes = nonnull_json(&bytes)?;
                    Ok::<_, anyhow::Error>(binance_wire::parse_rest_bars_with_type(bytes, mode)?)
                })
                .await??
        } else {
            binance_wire::parse_rest_bars_with_type(nonnull_json(&bytes)?, mode)?
        };
        anyhow::ensure!(bars.len() <= limit, "REST page exceeds requested limit");
        for pair in bars.windows(2) {
            anyhow::ensure!(
                pair[0].open_time < pair[1].open_time,
                "REST rows not strictly ordered"
            );
        }
        anyhow::ensure!(
            bars.iter()
                .all(|b| b.open_time <= end && start.is_none_or(|s| b.open_time >= s)),
            "REST page outside requested range"
        );
        Ok(bars)
    }
    pub async fn clock_sample(&self, market: Market) -> Result<ClockSample> {
        #[derive(serde::Deserialize)]
        struct Time {
            #[serde(rename = "serverTime")]
            time: i64,
        }
        let path = if market == Market::Spot {
            "/api/v3/time"
        } else {
            "/fapi/v1/time"
        };
        let reply = self
            .bytes(market, path, &[], 1, Priority::Foreground)
            .await?;
        let response: Time = decode_json(&reply.bytes)?;
        anyhow::ensure!(response.time > 0, "invalid server time");
        Ok(ClockSample {
            server_time: response.time,
            sent_ms: reply.sent_ms,
            elapsed_ms: reply.elapsed_ms,
        })
    }
}

/// Retrofit's Jackson converter reads one JSON value; ClientUtil then requires
/// a nonnull body. Keep this shared with callers using `external` for CMS JSON.
pub fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let bytes = nonnull_json(bytes)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    T::deserialize(&mut decoder).map_err(|error| crate::error::ApiError::upstream_io(error).into())
}
fn nonnull_json(bytes: &[u8]) -> Result<&[u8]> {
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    if bytes
        .iter()
        .find(|b| !matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        == Some(&b'n')
    {
        // Only the null branch needs a preliminary parse; normal market arrays
        // and objects are still decoded once without a JSON Value allocation.
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        let _: Option<serde::de::IgnoredAny> = serde::Deserialize::deserialize(&mut decoder)
            .map_err(crate::error::ApiError::upstream_io)?;
        return Err(crate::error::ApiError::upstream_null().into());
    }
    Ok(bytes)
}

fn http_error(response: &reqwest::Response, bytes: &[u8]) -> anyhow::Error {
    // The current Java ClientUtil's bare ObjectMapper cannot construct its
    // BinanceErrorResponse DTO (no default/annotated creator). Its observed
    // fallback is HTTP status as code and the complete error body as message,
    // including bodies which themselves contain {code,msg} JSON.
    let status = response.status().as_u16();
    let error = crate::error::ApiError {
        negotiation_fallback: None,
        status,
        code: i64::from(status),
        message: String::from_utf8_lossy(bytes).into_owned(),
    };
    match response.error_for_status_ref().err() {
        // Preserve the underlying status for the Vision archive loader's 404
        // handling, while HTTP callers receive the Java-compatible envelope.
        Some(cause) => anyhow::Error::new(cause.without_url()).context(error),
        None => error.into(),
    }
}

pub fn endpoint(value: &str) -> Result<Url> {
    let url = Url::parse(value)?;
    let local = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    anyhow::ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && local),
        "REST requires HTTPS (HTTP loopback is allowed for tests)"
    );
    anyhow::ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "REST root must not contain credentials/query/fragment"
    );
    Ok(url)
}

#[cfg(test)]
mod pacing_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn admission_is_fifo_weighted_and_a_cancelled_waiter_does_not_block_successors() {
        let api = RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        api.acquire(0, 2).await;
        let start = Instant::now();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut jobs = vec![];
        for id in 1..=3 {
            let api = api.clone();
            let tx = tx.clone();
            jobs.push(tokio::spawn(async move {
                api.acquire(0, 2).await;
                tx.send((id, Instant::now())).unwrap();
            }));
            tokio::task::yield_now().await;
        }
        jobs[1].abort();
        let a = rx.recv().await.unwrap();
        let b = rx.recv().await.unwrap();
        assert_eq!((a.0, b.0), (1, 3));
        assert!(a.1.duration_since(start) >= Duration::from_millis(20));
        assert!(b.1.duration_since(a.1) >= Duration::from_millis(20));
    }

    #[tokio::test(start_paused = true)]
    async fn queued_admission_observes_later_cooldown_without_blocking_other_markets() {
        let api = RestApi::new("http://127.0.0.1", "http://127.0.0.1", 6000).unwrap();
        api.acquire(0, 1).await;
        let task = tokio::spawn({
            let api = api.clone();
            async move { api.acquire(0, 1).await }
        });
        tokio::task::yield_now().await;
        api.pace[0].cooldown(Instant::now() + Duration::from_secs(1));
        assert_eq!(api.acquire(1, 1).await.rounds, 0);
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        let waited = task.await.unwrap();
        assert!(waited.elapsed >= Duration::from_secs(1));
        assert!(waited.cooldown_rounds > 0);
    }
}
