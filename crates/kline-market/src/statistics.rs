use crate::{
    config::StatisticsConfig,
    cpu::CpuPool,
    error::{ApiError, Result},
    metadata::Metadata,
    transport::RestApi,
};
use bigdecimal::{BigDecimal, RoundingMode};
use chrono::{Local, TimeZone, Timelike, Utc};
use kline_core::{Bar, Interval, Market};
use kline_service::Engine;
use moka::future::Cache;
use num_traits::{ToPrimitive, Zero};
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
mod java_hashmap_order;
pub struct Statistics {
    engine: Arc<Engine>,
    metadata: Arc<Metadata>,
    api: Arc<RestApi>,
    config: StatisticsConfig,
    cache: Cache<&'static str, Arc<Value>>,
    images: Cache<&'static str, bytes::Bytes>,
    cpu: Arc<CpuPool>,
    yama: Cache<(), Arc<YamaValues>>,
}
struct YamaValues {
    a: Arc<Value>,
    b: Arc<Value>,
    merged: Arc<Value>,
}
#[derive(Clone, Copy, Default)]
struct Point {
    change: f32,
    volume: f32,
}
impl Statistics {
    pub fn new(
        engine: Arc<Engine>,
        metadata: Arc<Metadata>,
        api: Arc<RestApi>,
        config: StatisticsConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            metadata,
            api,
            config,
            cache: Cache::builder()
                .max_capacity(4)
                .time_to_live(Duration::from_secs(60))
                .build(),
            images: Cache::builder()
                .max_capacity(3)
                .time_to_live(Duration::from_secs(60))
                .build(),
            cpu: CpuPool::named(1, "statistics-cpu"),
            yama: Cache::builder()
                .max_capacity(1)
                .time_to_live(Duration::from_secs(60))
                .build(),
        })
    }
    pub async fn query(&self, kind: &'static str) -> Result<Arc<Value>> {
        if kind == "alt" {
            return self
                .cache
                .try_get_with(kind, async {
                    Ok::<_, ApiError>(Arc::new(self.alt().await))
                })
                .await
                .map_err(|e| (*e).clone());
        }
        // All three endpoints depend on the same calculation and expiry. Cache
        // one generation so concurrent JSON/image requests only compute it once.
        let values = self
            .yama
            .try_get_with((), async {
                let meta = self.metadata.get(Market::Future).await?;
                let symbols: Vec<String> = meta["symbols"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|s| s["status"] == "TRADING" && s["quoteAsset"] == "USDT")
                    .filter_map(|s| s["symbol"].as_str().map(str::to_owned))
                    .collect();
                let engine = self.engine.clone();
                let config = self.config.clone();
                self.cpu
                    .run(move || {
                        let (start, end, retain) = window(&config);
                        let interval = Interval::parse("1d").unwrap();
                        let rows: Vec<_> = symbols
                            .into_iter()
                            .map(|s| {
                                let bars = crate::klines::cached(
                                    &engine,
                                    Market::Future,
                                    &s,
                                    interval,
                                    Some(start),
                                    Some(end),
                                    usize::MAX,
                                );
                                (s, bars)
                            })
                            .collect();
                        let (a, b) =
                            calculate(&rows, config.days, config.volume_days, config.volume_rank)?;
                        let mut merged = a.clone();
                        for (&t, &v) in &b {
                            *merged.entry(t).or_default() += v;
                        }
                        let encode = |values: BTreeMap<i64, f32>| {
                            float_json(values.into_iter().filter(|(t, _)| *t >= retain).collect())
                                .map(Arc::new)
                        };
                        Ok::<_, ApiError>(Arc::new(YamaValues {
                            a: encode(a)?,
                            b: encode(b)?,
                            merged: encode(merged)?,
                        }))
                    })
                    .await
                    .map_err(ApiError::internal)?
            })
            .await
            .map_err(|e| (*e).clone())?;
        Ok(match kind {
            "yama01" => values.a.clone(),
            "yama02" => values.b.clone(),
            _ => values.merged.clone(),
        })
    }
    async fn alt(&self) -> Value {
        let result = async {
            let raw = self
                .api
                .external(&self.config.altcoin_url, &[], 8 * 1024 * 1024)
                .await?;
            let start = self.config.start_date.clone();
            self.cpu
                .run(move || {
                    let text = std::str::from_utf8(&raw)?;
                    Ok::<_, anyhow::Error>(parse_alt(text, &start)?)
                })
                .await?
        }
        .await;
        result.unwrap_or_else(|e| {
            tracing::warn!(error=%e,"altcoin index fetch failed");
            serde_json::json!({})
        })
    }
    pub async fn image(&self, kind: &'static str) -> Result<bytes::Bytes> {
        self.images
            .try_get_with(kind, async {
                let data = self.query(kind).await?;
                self.cpu
                    .run(move || render(kind, &data))
                    .await
                    .map_err(ApiError::internal)?
            })
            .await
            .map_err(|e| (*e).clone())
    }
    pub async fn atr(&self) -> Result<Option<String>> {
        let rows = crate::klines::cached(
            &self.engine,
            Market::Spot,
            &self.config.atr_symbol,
            Interval::parse("1h").unwrap(),
            None,
            Some(crate::funding::now()),
            self.config.atr_period,
        );
        atr(&rows, self.config.atr_period)
    }
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        loop {
            tokio::select! {_=stop.changed()=>break,result=self.atr()=>match result{Ok(Some(value))=>tracing::info!(atr=%value,"ATR statistic"),Err(e)=>tracing::warn!(error=%e,"ATR statistic failed"),_=>{}}}
            tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(3600))=>{}}
        }
    }
}
// Preserve Java Float JSON precision; to_value(f32) first widens it to f64.
fn float_json<K: serde::Serialize + Ord>(values: BTreeMap<K, f32>) -> Result<Value> {
    let bytes = serde_json::to_vec(&values).map_err(ApiError::internal)?;
    serde_json::from_slice(&bytes).map_err(ApiError::internal)
}
fn window(config: &StatisticsConfig) -> (i64, i64, i64) {
    let end = if let Some(minutes) = config.timezone_offset_minutes {
        let offset = i64::from(minutes) * 60_000;
        let now = Utc::now().timestamp_millis();
        (now + offset).div_euclid(86_400_000) * 86_400_000 - offset
    } else {
        let today = Local::now();
        today
            .with_hour(0)
            .unwrap()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            .timestamp_millis()
    };
    let span = config.days as i64 * 86_400_000;
    (end - 2 * span, end, end - span)
}
fn bd(bar: &Bar, i: usize) -> Result<BigDecimal> {
    match bar.exact.as_deref() {
        Some(kline_core::ExactNumbers::Decimals(a)) => Ok(a[i].clone()),
        Some(kline_core::ExactNumbers::Strings(a)) => {
            BigDecimal::from_str(&a[i]).map_err(ApiError::internal)
        }
        None => BigDecimal::from_str(&bar.values[i].to_string()).map_err(ApiError::internal),
    }
}
fn divide(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    if b.is_zero() {
        return Err(ApiError::internal("division by zero in statistics"));
    }
    Ok((a / b).with_scale_round(8, RoundingMode::Down))
}
pub fn calculate(
    rows: &[(String, Vec<Bar>)],
    days: usize,
    volume_days: usize,
    rank: usize,
) -> Result<(BTreeMap<i64, f32>, BTreeMap<i64, f32>)> {
    let mut dates: BTreeMap<i64, Vec<(String, Point)>> = BTreeMap::new();
    for (symbol, bars) in rows {
        let mut volume = BigDecimal::zero();
        for (i, bar) in bars.iter().enumerate() {
            volume += bd(bar, 5)?;
            if i >= volume_days.max(1) {
                volume -= bd(&bars[i - volume_days.max(1)], 5)?;
            }
            let change = if i + 1 >= days {
                (divide(&bd(bar, 0)?, &bd(&bars[i + 1 - days], 0)?)? - BigDecimal::from(1))
                    .to_f32()
                    .ok_or_else(|| ApiError::internal("statistic overflow"))?
            } else {
                0.
            };
            let volume = volume
                .to_f32()
                .ok_or_else(|| ApiError::internal("volume overflow"))?;
            dates
                .entry(bar.open_time)
                .or_default()
                .push((symbol.clone(), Point { change, volume }));
        }
    }
    let mut yama01 = BTreeMap::new();
    let mut yama02 = BTreeMap::new();
    for (time, mut points) in dates {
        if points.is_empty() {
            continue;
        }
        // Stable sort ties and f32 accumulation depend on the Java map's actual
        // iteration order, including collision resizes and replacement puts.
        let keys: Vec<_> = points.iter().map(|(symbol, _)| symbol.as_str()).collect();
        let order = java_hashmap_order::indices(&keys);
        points = order
            .into_iter()
            .map(|index| std::mem::take(&mut points[index]))
            .collect();
        let mut changes = points.clone();
        changes.sort_by(|a, b| b.1.change.total_cmp(&a.1.change));
        let btc = changes
            .iter()
            .position(|(s, _)| s == "BTCUSDT")
            .map_or(points.len(), |i| i + 1);
        yama01.insert(time, btc as f32 / points.len() as f32);
        points.sort_by(|a, b| b.1.volume.total_cmp(&a.1.volume));
        let count = rank.min(points.len());
        if count == 0 {
            return Err(ApiError::internal("volume rank must be positive"));
        }
        let sum = points
            .iter()
            .take(count)
            .fold(0_f32, |sum, (_, p)| sum + p.change);
        yama02.insert(time, sum / count as f32);
    }
    Ok((yama01, yama02))
}
pub fn atr(bars: &[Bar], period: usize) -> Result<Option<String>> {
    if bars.len() < 2 {
        return Ok(None);
    }
    let bars = &bars[..bars.len() - 1];
    let mut closes: Vec<BigDecimal> = vec![];
    let mut ranges = vec![];
    for (i, bar) in bars.iter().enumerate() {
        let high = bd(bar, 1)?;
        let low = bd(bar, 2)?;
        let close = bd(bar, 3)?;
        let previous = if i == 0 {
            close.clone()
        } else {
            closes[i - 1].clone()
        };
        let range = (&high - &low)
            .max((&high - &previous).abs())
            .max((&low - &previous).abs());
        closes.push(close);
        ranges.push(range);
    }
    let count = period.min(bars.len());
    let sum_close: BigDecimal = closes.iter().rev().take(count).cloned().sum();
    let sum_range: BigDecimal = ranges.iter().rev().take(count).cloned().sum();
    let divisor = BigDecimal::from(count as u64);
    let mean_close = divide(&sum_close, &divisor)?;
    let mean_range = divide(&sum_range, &divisor)?;
    Ok(Some(
        divide(
            &mean_range,
            &(mean_close + BigDecimal::from_str("0.00000001").unwrap()),
        )?
        .to_plain_string(),
    ))
}
pub fn parse_alt(html: &str, start: &str) -> Result<Value> {
    let json = html
        .split_once("chartdata[30] = ")
        .ok_or_else(|| ApiError::internal("altcoin chart not found"))?
        .1;
    let value = serde_json::Deserializer::from_str(json)
        .into_iter::<Value>()
        .next()
        .ok_or_else(|| ApiError::internal("altcoin chart empty"))?
        .map_err(ApiError::internal)?;
    let labels = value["labels"]["all"]
        .as_array()
        .ok_or_else(|| ApiError::internal("altcoin labels missing"))?;
    let values = value["values"]["all"]
        .as_array()
        .ok_or_else(|| ApiError::internal("altcoin values missing"))?;
    let mut result = BTreeMap::new();
    for (label, value) in labels.iter().zip(values) {
        let Some(date) = label.as_str() else { continue };
        if date < start {
            continue;
        }
        let value = match value.as_str() {
            Some(s) => s.parse::<f32>().map_err(ApiError::internal)?,
            None => value
                .as_f64()
                .ok_or_else(|| ApiError::internal("altcoin value"))? as f32,
        };
        result.insert(date, value / 100.);
    }
    float_json(result)
}
pub fn render(kind: &str, data: &Value) -> Result<bytes::Bytes> {
    use plotters::prelude::*;
    static FONT: std::sync::OnceLock<std::result::Result<(), String>> = std::sync::OnceLock::new();
    FONT.get_or_init(|| {
        plotters::style::register_font(
            "sans-serif",
            FontStyle::Normal,
            include_bytes!("../assets/NotoSans.ttf"),
        )
        .map_err(|_| "bundled Noto Sans font is invalid".to_owned())
    })
    .as_ref()
    .map_err(ApiError::internal)?;
    let points: Vec<_> = data
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(t, v)| Some((t.parse::<i64>().ok()?, v.as_f64()?)))
        .collect();
    let width = 1024;
    let height = 768;
    let mut rgb = vec![255_u8; width * height * 3];
    {
        let area =
            BitMapBackend::with_buffer(&mut rgb, (width as u32, height as u32)).into_drawing_area();
        area.fill(&WHITE).map_err(ApiError::internal)?;
        let first = points.iter().map(|p| p.0).min().unwrap_or(0);
        let last = points
            .iter()
            .map(|p| p.0)
            .max()
            .unwrap_or(86_400_000)
            .max(first + 1);
        let low = points.iter().map(|p| p.1).fold(0_f64, f64::min);
        let high = points.iter().map(|p| p.1).fold(1_f64, f64::max);
        let pad = (high - low) * 0.05;
        let mut chart = ChartBuilder::on(&area)
            .caption(
                match kind {
                    "yama01" => "yama 01 index",
                    "yama02" => "yama 02 index",
                    _ => "agg yama index",
                },
                ("sans-serif", 28),
            )
            .margin(24)
            .x_label_area_size(45)
            .y_label_area_size(65)
            .build_cartesian_2d(first..last, (low - pad)..(high + pad))
            .map_err(ApiError::internal)?;
        chart
            .configure_mesh()
            .x_labels(8)
            .x_label_formatter(&|time| {
                Utc.timestamp_millis_opt(*time)
                    .single()
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default()
            })
            .draw()
            .map_err(ApiError::internal)?;
        let mut points = points;
        points.sort_by_key(|p| p.0);
        chart
            .draw_series(LineSeries::new(points, &BLUE))
            .map_err(ApiError::internal)?;
        area.present().map_err(ApiError::internal)?;
    }
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, width as u32, height as u32);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().map_err(ApiError::internal)?;
        writer.write_image_data(&rgb).map_err(ApiError::internal)?;
    }
    Ok(bytes.into())
}
