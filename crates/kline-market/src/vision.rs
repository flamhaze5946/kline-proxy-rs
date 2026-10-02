//! Optional public archive bootstrap, isolated from request and socket tasks.
use crate::{
    error::{ApiError, Result},
    funding::{Chunk, Funding, H, Rate, now},
};
use chrono::{Datelike, TimeZone, Utc};
use futures_util::{StreamExt, stream};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    sync::Arc,
};
pub async fn warm(funding: Arc<Funding>) -> Result<usize> {
    let symbols = funding
        .metadata
        .symbols(kline_core::Market::Future, true)
        .await?;
    let end = now();
    let start = end - i64::from(funding.config.vision_days) * 24 * H;
    let first = Utc
        .timestamp_millis_opt(start)
        .single()
        .ok_or_else(|| ApiError::internal("vision date"))?;
    let last = Utc
        .timestamp_millis_opt(end)
        .single()
        .ok_or_else(|| ApiError::internal("vision date"))?;
    let mut month = first.year() * 12 + first.month0() as i32;
    let last = last.year() * 12 + last.month0() as i32;
    let mut months = vec![];
    while month <= last {
        months.push(format!(
            "{:04}-{:02}",
            month.div_euclid(12),
            month.rem_euclid(12) + 1
        ));
        month += 1;
    }
    let jobs: Vec<_> = symbols
        .iter()
        .flat_map(|s| months.iter().map(move |m| (s.clone(), m.clone())))
        .collect();
    let workers = funding.config.vision_workers.clamp(1, 32);
    let mut completed=stream::iter(jobs).map(|(symbol,month)|{let funding=funding.clone();async move{
        let url=format!("{}/data/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip",funding.config.vision_url.trim_end_matches('/'));
        let result=async {
            let bytes=match funding.api.external(&url,&[],16*1024*1024).await {
                Ok(bytes) => bytes,
                Err(error) if error.downcast_ref::<reqwest::Error>().and_then(reqwest::Error::status) == Some(reqwest::StatusCode::NOT_FOUND) => return Ok(vec![]),
                Err(error) => return Err(ApiError::from(error)),
            };
            funding.api.cpu.run_large(move||parse_archive(&bytes,&symbol,start,end)).await.map_err(ApiError::internal)?
        }.await;
        (month,result)
    }}).buffer_unordered(workers);
    let mut failed = BTreeSet::new();
    let mut staged: BTreeMap<i64, Chunk> = BTreeMap::new();
    while let Some((month, result)) = completed.next().await {
        match result {
            Ok(rows) => {
                for row in rows {
                    let t = row.funding_time.unwrap();
                    let symbol = row.symbol.clone().unwrap();
                    staged
                        .entry(t.div_euclid(H) * H)
                        .or_default()
                        .entry(symbol)
                        .or_default()
                        .push(row);
                }
            }
            Err(e) => {
                failed.insert(month.clone());
                tracing::debug!(month,error=%e,"Vision month unavailable; REST remains authoritative");
            }
        }
    }
    let mut seeded = 0;
    for (start, mut chunk) in staged {
        let month = Utc
            .timestamp_millis_opt(start)
            .single()
            .unwrap()
            .format("%Y-%m")
            .to_string();
        if failed.contains(&month) {
            continue;
        }
        for bucket in chunk.values_mut() {
            bucket.sort_by_key(|r| r.funding_time);
            bucket.dedup_by_key(|r| r.funding_time);
        }
        funding
            .chunks
            .get_with(start, async { Arc::new(chunk) })
            .await;
        seeded += 1;
    }
    tracing::info!(
        seeded,
        failed_months = failed.len(),
        "Vision funding bootstrap complete"
    );
    Ok(seeded)
}
pub fn parse_archive(bytes: &[u8], symbol: &str, start: i64, end: i64) -> Result<Vec<Rate>> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).map_err(ApiError::internal)?;
    let mut out = vec![];
    let mut total = 0_usize;
    let mut csv_files = 0;
    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(ApiError::internal)?;
        if file.is_dir() || !file.name().ends_with(".csv") {
            continue;
        }
        csv_files += 1;
        let mut csv = String::new();
        file.take(4 * 1024 * 1024 + 1)
            .read_to_string(&mut csv)
            .map_err(ApiError::internal)?;
        total += csv.len();
        if total > 4 * 1024 * 1024 {
            return Err(ApiError::internal("Vision CSV exceeds limit"));
        }
        let mut lines = csv.lines();
        if lines.next().map(str::trim) != Some("calc_time,funding_interval_hours,last_funding_rate")
        {
            return Err(ApiError::internal("unexpected Vision CSV header"));
        }
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let mut fields = line.split(',');
            let Some(time) = fields.next().and_then(|v| v.trim().parse::<i64>().ok()) else {
                continue;
            };
            fields.next();
            let Some(rate) = fields.next().and_then(|v| crate::decimal(v.trim()).ok()) else {
                continue;
            };
            if time < start || time >= end {
                continue;
            }
            out.push(Rate {
                symbol: Some(symbol.into()),
                funding_time: Some(time),
                funding_rate: Some(rate),
                mark_price: None,
            });
        }
    }
    if csv_files == 0 {
        return Err(ApiError::internal("Vision archive contains no CSV"));
    }
    Ok(out)
}
