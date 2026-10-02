//! Versioned, checksummed per-series snapshots. Only final bars are durable.
//! A single background writer owns acknowledgements; commits never perform disk IO.
use anyhow::{Context, Result};
use kline_core::{Bar, NumberType, Source, Update};
use kline_service::{Engine, Slot};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const MAGIC: &[u8; 8] = b"KLINE\0\x01\0";
const MAGIC_EXACT: &[u8; 8] = b"KLINE\0\x02\0";
const MAX_BYTES: usize = 256 * 1024 * 1024;
const ROW_BYTES: usize = 84;
static TEMP_ID: AtomicU64 = AtomicU64::new(0);
pub struct Store {
    root: PathBuf,
    acknowledged: Vec<u64>,
    config: Option<crate::config::PersistenceConfig>,
}
#[derive(Default, Debug)]
pub struct RestoreReport {
    pub series: usize,
    pub bars: usize,
    pub corrupt: usize,
}
#[derive(Default, Debug)]
pub struct DumpReport {
    pub written: usize,
    pub skipped: usize,
    pub failures: usize,
    pub bytes: usize,
}
fn identity(slot: &Slot) -> String {
    format!(
        "{}\0{}\0{}",
        crate::config::market_name(slot.market),
        slot.interval.code(),
        slot.symbol
    )
}
impl Store {
    pub fn new(root: impl AsRef<Path>, series: usize) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_owned(),
            acknowledged: vec![0; series],
            config: None,
        })
    }
    pub fn configure(&mut self, config: crate::config::PersistenceConfig) {
        self.config = Some(config);
    }
    fn enabled(&self, slot: &Slot) -> bool {
        self.config.as_ref().is_none_or(|c| c.includes(slot))
    }
    fn count(&self, slot: &Slot) -> usize {
        self.config
            .as_ref()
            .map(|c| c.count(slot))
            .unwrap_or(slot.capacity())
    }
    pub fn path(&self, slot: &Slot) -> PathBuf {
        self.root.join(format!(
            "{:x}.kpr",
            Sha256::digest(identity(slot).as_bytes())
        ))
    }
    pub fn is_dirty(&self, engine: &Engine) -> bool {
        engine.catalog.slots().iter().enumerate().any(|(id, s)| {
            s.is_tracked()
                && self.enabled(s)
                && s.durable_generation() != self.acknowledged.get(id).copied().unwrap_or(0)
                && !s.is_empty()
        })
    }
    pub fn restore(&mut self, engine: &Engine) -> RestoreReport {
        let mut report = RestoreReport::default();
        let slots = engine.catalog.slots();
        self.acknowledged.resize(slots.len(), 0);
        for (id, slot) in slots.iter().enumerate() {
            let path = self.path(slot);
            if !slot.is_tracked() || !self.enabled(slot) {
                continue;
            }
            let existed = path.exists();
            let loaded = if existed {
                load(&path, slot, engine.now_ms(), engine.number_type())
            } else {
                Ok(vec![])
            };
            let loaded = match loaded {
                Ok(bars) if !bars.is_empty() => Ok(bars),
                original => {
                    if let Some(root) = self
                        .config
                        .as_ref()
                        .and_then(|c| c.legacy_directory.as_ref())
                    {
                        match crate::legacy::load(
                            root,
                            slot,
                            self.count(slot),
                            engine.now_ms(),
                            engine.number_type(),
                        ) {
                            Ok(bars) if !bars.is_empty() => encode(slot, engine.now_ms(), &bars)
                                .and_then(|bytes| atomic_write(&path, &bytes))
                                .map(|_| bars),
                            Ok(_) => original,
                            Err(error) => Err(error),
                        }
                    } else {
                        original
                    }
                }
            };
            match loaded {
                Ok(bars) => {
                    if bars.is_empty() {
                        continue;
                    }
                    slot.reserve_history(bars.len());
                    for bar in bars.iter().rev().take(slot.capacity()).rev() {
                        // Complete files are verified before any value is published.
                        engine
                            .commit(
                                id,
                                Update {
                                    bar: bar.clone(),
                                    closed: true,
                                    source: Source::Restore,
                                    event_time: None,
                                    sequence: 0,
                                },
                            )
                            .expect("validated snapshot");
                    }
                    report.series += 1;
                    report.bars += bars.len().min(slot.capacity());
                    self.acknowledged[id] = slot.durable_generation();
                }
                Err(error) => {
                    report.corrupt += 1;
                    tracing::warn!(symbol=%slot.symbol,interval=slot.interval.code(),%error,"snapshot skipped; REST recovery required");
                }
            }
        }
        report
    }
    /// The guard is checked before each series. Deferred/failed writes stay dirty.
    pub fn dump(&mut self, engine: &Engine, mut permitted: impl FnMut() -> bool) -> DumpReport {
        let mut report = DumpReport::default();
        let slots = engine.catalog.slots();
        self.acknowledged.resize(slots.len(), 0);
        for (id, slot) in slots.iter().enumerate() {
            // Disabled periods keep existing files intact and do not cause dirty retries.
            if !self.enabled(slot) {
                report.skipped += 1;
                continue;
            }
            if !slot.is_tracked() {
                match fs::remove_file(self.path(slot)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        report.failures += 1;
                        tracing::warn!(error=%e,"retired snapshot cleanup failed");
                    }
                }
                self.acknowledged[id] = u64::MAX;
                continue;
            }
            if !permitted() {
                break;
            }
            if slot.durable_generation() == self.acknowledged[id] {
                report.skipped += 1;
                continue;
            }
            let now = engine.now_ms();
            let (generation, mut bars) = slot.durable_snapshot(now);
            // Never replace useful disk history with an empty or entirely forming cache.
            if bars.is_empty() {
                report.skipped += 1;
                continue;
            }
            let count = self.count(slot);
            if count > bars.len() && self.path(slot).exists() {
                match load(&self.path(slot), slot, now, engine.number_type()) {
                    Ok(previous) => {
                        let mut merged: std::collections::BTreeMap<_, _> =
                            previous.into_iter().map(|b| (b.open_time, b)).collect();
                        merged.extend(bars.into_iter().map(|b| (b.open_time, b)));
                        bars = merged.into_values().collect();
                    }
                    Err(e) => {
                        tracing::warn!(error=%e,"replacing corrupt snapshot with validated memory data")
                    }
                }
            }
            if bars.len() > count {
                bars.drain(..bars.len() - count);
            }
            let result = encode(slot, now, &bars).and_then(|bytes| {
                atomic_write(&self.path(slot), &bytes)?;
                Ok(bytes.len())
            });
            match result {
                Ok(length) => {
                    if !slot.has_deferred_final(now) {
                        self.acknowledged[id] = generation;
                    }
                    report.written += 1;
                    report.bytes += length;
                }
                Err(error) => {
                    report.failures += 1;
                    tracing::warn!(symbol=%slot.symbol,%error,"snapshot write failed; remains dirty");
                }
            }
        }
        report
    }
}
fn encode(slot: &Slot, now: i64, bars: &[Bar]) -> Result<Vec<u8>> {
    let key = identity(slot);
    let exact = bars.iter().any(|b| b.number_type != NumberType::Double);
    let mut bytes = Vec::with_capacity(22 + key.len() + ROW_BYTES * bars.len() + 32);
    bytes.extend_from_slice(if exact { MAGIC_EXACT } else { MAGIC });
    bytes.extend_from_slice(&now.to_le_bytes());
    bytes.extend_from_slice(&(key.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&(bars.len() as u32).to_le_bytes());
    bytes.extend_from_slice(key.as_bytes());
    for bar in bars {
        bytes.extend_from_slice(&bar.open_time.to_le_bytes());
        bytes.extend_from_slice(&bar.close_time.to_le_bytes());
        bytes.extend_from_slice(&bar.trades.to_le_bytes());
        if exact {
            bytes.push(bar.number_type as u8);
        }
        if matches!(bar.number_type, NumberType::Double | NumberType::Float) {
            for value in bar.values {
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
        } else {
            for value in binance_wire::stored_numbers(bar) {
                anyhow::ensure!(value.len() <= 4096, "snapshot number too large");
                bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
        }
        anyhow::ensure!(bytes.len() + 32 <= MAX_BYTES, "snapshot exceeds size limit");
    }
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}
fn take<const N: usize>(raw: &mut &[u8]) -> Result<[u8; N]> {
    anyhow::ensure!(raw.len() >= N, "truncated snapshot");
    let (head, tail) = raw.split_at(N);
    *raw = tail;
    Ok(head.try_into().unwrap())
}
fn load(path: &Path, slot: &Slot, now: i64, mode: NumberType) -> Result<Vec<Bar>> {
    anyhow::ensure!(
        fs::metadata(path)?.len() <= MAX_BYTES as u64,
        "snapshot exceeds size limit"
    );
    let bytes = fs::read(path)?;
    anyhow::ensure!(bytes.len() >= 54, "truncated snapshot");
    let (content, checksum) = bytes.split_at(bytes.len() - 32);
    anyhow::ensure!(
        Sha256::digest(content).as_slice() == checksum,
        "snapshot checksum mismatch"
    );
    let mut raw = content;
    let magic = take::<8>(&mut raw)?;
    anyhow::ensure!(
        &magic == MAGIC || &magic == MAGIC_EXACT,
        "unsupported snapshot format"
    );
    let exact = &magic == MAGIC_EXACT;
    let saved = i64::from_le_bytes(take(&mut raw)?);
    let key_len = u16::from_le_bytes(take(&mut raw)?) as usize;
    let count = u32::from_le_bytes(take(&mut raw)?) as usize;
    anyhow::ensure!(
        count <= 100_000
            && key_len <= 1024
            && raw.len() >= key_len
            && (exact || raw.len() == key_len + count * ROW_BYTES),
        "invalid snapshot length"
    );
    anyhow::ensure!(
        &raw[..key_len] == identity(slot).as_bytes(),
        "snapshot identity mismatch"
    );
    raw = &raw[key_len..];
    let mut bars = Vec::with_capacity(count);
    for _ in 0..count {
        let open_time = i64::from_le_bytes(take(&mut raw)?);
        let close_time = i64::from_le_bytes(take(&mut raw)?);
        let trades = u32::from_le_bytes(take(&mut raw)?);
        let stored_mode = if exact {
            match take::<1>(&mut raw)?[0] {
                0 => NumberType::Double,
                1 => NumberType::Float,
                2 => NumberType::String,
                3 => NumberType::BigDecimal,
                _ => anyhow::bail!("invalid snapshot numeric mode"),
            }
        } else {
            NumberType::Double
        };
        let mut bar = Bar {
            open_time,
            close_time,
            trades,
            number_type: stored_mode,
            ..Bar::default()
        };
        if matches!(stored_mode, NumberType::Double | NumberType::Float) {
            for v in &mut bar.values {
                *v = f64::from_bits(u64::from_le_bytes(take(&mut raw)?));
            }
        } else {
            let mut fields = [""; 8];
            for field in &mut fields {
                let len = u16::from_le_bytes(take(&mut raw)?) as usize;
                anyhow::ensure!(
                    len <= 4096 && raw.len() >= len,
                    "invalid snapshot number length"
                );
                let (value, rest) = raw.split_at(len);
                *field = std::str::from_utf8(value)?;
                raw = rest;
            }
            bar.set_numbers(stored_mode, fields)?;
        }
        bar.validate()?;
        anyhow::ensure!(
            close_time <= saved && close_time <= now,
            "snapshot contains future/unclosed bar"
        );
        anyhow::ensure!(
            bars.last().is_none_or(|p: &Bar| p.open_time < open_time),
            "unordered snapshot"
        );
        bars.push(binance_wire::convert_bar(bar, mode)?);
    }
    anyhow::ensure!(raw.is_empty(), "trailing snapshot bytes");
    Ok(bars)
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path).context("publish snapshot")?;
        #[cfg(unix)]
        File::open(path.parent().context("snapshot directory")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
