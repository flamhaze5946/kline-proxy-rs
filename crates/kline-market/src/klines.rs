//! Cache query semantics preserve Java's inclusive start/end and fixed interval alignment.
use crate::{
    error::{ApiError, Result},
    transport::RestApi,
};
use binance_wire::DisplayBar;
use kline_core::{Bar, Interval, Market};
use kline_service::Engine;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<i32>,
    pub time_zone: Option<String>,
}
pub fn range(
    now: i64,
    interval: Interval,
    start: Option<i64>,
    end: Option<i64>,
    limit: usize,
) -> (i64, i64) {
    let p = interval.millis();
    let span = p.saturating_mul(i64::try_from(limit.saturating_sub(1)).unwrap_or(i64::MAX));
    let floor = |t: i64| t.div_euclid(p).saturating_mul(p);
    let ceil = |t: i64| floor(t).saturating_add(if t.rem_euclid(p) == 0 { 0 } else { p });
    match (start, end) {
        (Some(a), Some(b)) => {
            let a = ceil(a);
            (a, floor(b).min(a.saturating_add(span)))
        }
        (Some(a), None) => {
            let a = ceil(a);
            (a, a.saturating_add(span))
        }
        (None, end) => {
            let b = floor(end.unwrap_or(now));
            (b.saturating_sub(span), b)
        }
    }
}
pub fn cached(
    engine: &Engine,
    market: Market,
    symbol: &str,
    interval: Interval,
    start: Option<i64>,
    end: Option<i64>,
    limit: usize,
) -> Vec<Bar> {
    cached_query(
        engine,
        market,
        symbol,
        interval,
        start,
        end,
        i32::try_from(limit).unwrap_or(i32::MAX),
    )
    .unwrap_or_default()
}
/// The legacy controller passes a signed int limit directly to Java's time
/// window calculation. Reproduce its arithmetic, but only visit stored bars:
/// even MIN_VALUE must never allocate or loop `limit` times.
fn cached_query(
    engine: &Engine,
    market: Market,
    symbol: &str,
    interval: Interval,
    start: Option<i64>,
    end: Option<i64>,
    limit: i32,
) -> Result<Vec<Bar>> {
    let Some(id) = engine.catalog.find(market, interval, symbol) else {
        return Ok(vec![]);
    };
    let slot = engine.catalog.slot(id);
    let now = engine.now_ms();
    let p = interval.millis();
    let duration = p.wrapping_mul(i64::from(limit.wrapping_sub(1)));
    let floor = |t: i64| ((t as f64 / p as f64).floor() as i64).wrapping_mul(p);
    let ceil = |t: i64| ((t as f64 / p as f64).ceil() as i64).wrapping_mul(p);
    let (a, b) = match (start, end) {
        (Some(a), Some(b)) => (ceil(a), floor(b).min(ceil(a).wrapping_add(duration))),
        (Some(a), None) => (ceil(a), ceil(a).wrapping_add(duration)),
        (None, end) => {
            let b = floor(end.unwrap_or(now));
            (b.wrapping_sub(duration), b)
        }
    };
    slot.select_bars(|series| {
        if series.latest_bar().is_none() {
            return Ok(vec![]);
        }
        if a > b {
            return Err(ApiError::binding("inconsistent range"));
        }
        let placeholder = slot
            .is_trading()
            .then(|| series.provisional_bar(interval, now))
            .flatten();
        let select = |first, last| {
            let mut bars = series.range_snapshot(first, last);
            if let Some(bar) = &placeholder
                && (first..=last).contains(&bar.open_time)
            {
                bars.push(bar.clone());
            }
            bars
        };
        if start.is_none()
            && end.is_none()
            && let Some(bar) = &placeholder
        {
            let first = bar.open_time.wrapping_sub(duration);
            if first > bar.open_time {
                return Err(ApiError::binding("inconsistent range"));
            }
            return Ok(select(first, bar.open_time));
        }
        let bars = select(a, b);
        let span = bars
            .first()
            .zip(bars.last())
            .map(|(first, last)| (last.open_time - first.open_time) / interval.millis() + 1)
            .unwrap_or(0);
        if start.is_none() && end.is_none() && span < i64::from(limit) {
            // Java falls back to a time window, not N records: internal holes must stay holes.
            if let Some(last) = placeholder.as_ref().or_else(|| series.latest_bar()) {
                // Java anchors fallback on the actual last open, not an epoch
                // multiple. Weekly/monthly opens need not align to epoch ticks.
                let first = last.open_time.wrapping_sub(duration);
                if first > last.open_time {
                    return Err(ApiError::binding("inconsistent range"));
                }
                return Ok(select(first, last.open_time));
            }
        }
        Ok(bars)
    })
}
/// Compatibility entry point for library callers; HTTP uses encoded bytes directly.
pub async fn query(engine: &Engine, api: &Arc<RestApi>, market: Market, q: Query) -> Result<Value> {
    serde_json::from_slice(&query_bytes(engine, api, market, q).await?).map_err(ApiError::internal)
}
pub async fn query_bytes(
    engine: &Engine,
    api: &Arc<RestApi>,
    market: Market,
    q: Query,
) -> Result<bytes::Bytes> {
    if market == Market::Spot
        && (q
            .time_zone
            .as_ref()
            .is_some_and(|s| !kline_service::http_compat::is_blank(s))
            || q.interval == "1M")
    {
        let mut args = vec![
            ("symbol".into(), q.symbol),
            ("interval".into(), q.interval),
            ("limit".into(), q.limit.unwrap_or(500).to_string()),
        ];
        for (name, t) in [("startTime", q.start_time), ("endTime", q.end_time)] {
            if let Some(t) = t {
                args.push((name.into(), t.to_string()));
            }
        }
        if let Some(tz) = q.time_zone {
            args.push(("timeZone".into(), tz));
        }
        let value: Value = api
            .json(market, "/api/v3/klines", &args, 2)
            .await
            .map_err(ApiError::from)?;
        return serde_json::to_vec(&value)
            .map(Into::into)
            .map_err(ApiError::internal);
    }
    let interval =
        Interval::parse(&q.interval).ok_or_else(|| ApiError::bad(-1120, "Invalid interval."))?;
    let limit = q.limit.unwrap_or(500);
    let bars = cached_query(
        engine,
        market,
        &q.symbol,
        interval,
        q.start_time,
        q.end_time,
        limit,
    )?;
    let offload = bars.len() > 100;
    let encode = move || serde_json::to_vec(&bars.into_iter().map(DisplayBar).collect::<Vec<_>>());
    // Small recent windows stay inline; large histories must not monopolize the
    // socket executor. No dynamic JSON tree or second serialization on this path.
    if offload {
        api.cpu
            .run(encode)
            .await
            .map_err(ApiError::internal)?
            .map(Into::into)
            .map_err(ApiError::internal)
    } else {
        encode().map(Into::into).map_err(ApiError::internal)
    }
}
