//! Evidence-only state files and the process-lifetime advisory lock.
use crate::config::Config;
use anyhow::{Context, Result};
use kline_service::{Engine, Instrument, evidence::BreakerState, evidence_bulk::RetainedRecord};
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Default)]
pub struct RetentionRuntime {
    pub records: BTreeMap<String, RetainedRecord>,
    pub previous: Vec<Instrument>,
    pub delivery: BTreeMap<String, i64>,
    pub generation: u64,
    pub acknowledged: Option<u64>,
}
pub struct StateDirectory {
    _lock: File,
    root: PathBuf,
    breaker_ack: Mutex<Option<u64>>,
}
impl StateDirectory {
    pub fn lock(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("evidence.lock"))?;
        file.try_lock()
            .with_context(|| format!("evidence state directory is locked: {}", root.display()))?;
        Ok(Self {
            _lock: file,
            root: root.to_owned(),
            breaker_ack: Mutex::new(None),
        })
    }
    pub fn load(config: &Config, engine: &Engine) -> Result<Arc<Self>> {
        let evidence = config.evidence.as_ref().context("evidence disabled")?;
        let root = &config
            .persistence
            .as_ref()
            .context("evidence persistence missing")?
            .directory;
        let directory = Arc::new(Self::lock(root)?);
        let breaker = match read_breaker(&root.join("breaker.json")) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(%error,"breaker missing/unreadable; revised_total=0, WS final trust disabled");
                BreakerState {
                    last_conflict_ms: engine.now_ms(),
                    revised_total: 0,
                }
            }
        };
        engine.restore_breaker(breaker);
        let records = match fs::read(root.join("retained.json")) {
            Ok(bytes) => serde_json::from_slice::<Vec<RetainedRecord>>(&bytes)
                .context("retained.json unreadable or invalid")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => vec![],
            Err(error) => return Err(error).context("retained.json unreadable; refusing startup"),
        };
        let mut runtime = evidence.runtime.lock();
        runtime.records.clear();
        for record in records {
            crate::config::validate_symbol(&record.symbol)?;
            anyhow::ensure!(
                record.pair == record.symbol
                    && matches!(
                        record.contract_type.as_str(),
                        "PERPETUAL" | "TRADIFI_PERPETUAL"
                    )
                    && record.since >= 0
                    && record.expires_ms >= record.since,
                "invalid retained identity/times"
            );
            anyhow::ensure!(
                runtime
                    .records
                    .insert(record.symbol.clone(), record)
                    .is_none(),
                "duplicate retained catalog key"
            );
        }
        runtime.generation = runtime.generation.wrapping_add(1);
        runtime.acknowledged = None;
        drop(runtime);
        Ok(directory)
    }
    pub fn flush(&self, config: &Config, engine: &Engine) -> Result<()> {
        let mut acknowledged = self.breaker_ack.lock();
        let (generation, state) = engine.breaker_snapshot();
        let breaker_result = if *acknowledged != Some(generation) {
            crate::storage::atomic_write(
                &self.root.join("breaker.json"),
                &serde_json::to_vec(&state)?,
            )
            .map(|()| {
                *acknowledged = Some(generation);
            })
        } else {
            Ok(())
        };
        drop(acknowledged);
        let retained_result = persist_retained(config);
        breaker_result.and(retained_result)
    }
    pub async fn persist(
        self: Arc<Self>,
        config: Arc<Config>,
        engine: Arc<Engine>,
        mut finish: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            // A shutdown requested during an earlier pass still needs its own
            // flush: that earlier snapshot can precede the last WS conflict.
            let final_pass = *finish.borrow();
            let directory = self.clone();
            let config = config.clone();
            let engine = engine.clone();
            let persisted = match tokio::task::spawn_blocking(move || {
                directory.flush(&config, &engine)
            })
            .await
            {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    tracing::warn!(%error,"evidence state write failed; remains dirty");
                    false
                }
                Err(error) => {
                    tracing::error!(%error,"evidence state writer failed");
                    false
                }
            };
            if final_pass && persisted {
                break;
            }
            if final_pass {
                // Keep the lock and dirty state until the final writes succeed.
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                tokio::select! {_=finish.changed()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            }
        }
    }
}
fn read_breaker(path: &Path) -> Result<BreakerState> {
    let state: BreakerState = serde_json::from_slice(&fs::read(path)?)?;
    anyhow::ensure!(state.last_conflict_ms >= 0, "invalid breaker timestamp");
    Ok(state)
}
pub fn reset_breaker(config: &Config) -> Result<BreakerState> {
    anyhow::ensure!(
        config.evidence.is_some(),
        "--reset-breaker requires evidence mode"
    );
    let root = &config
        .persistence
        .as_ref()
        .context("persistence missing")?
        .directory;
    let _directory = StateDirectory::lock(root)?;
    let mut state = match read_breaker(&root.join("breaker.json")) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("old revised_total unreadable; using 0: {error}");
            BreakerState::default()
        }
    };
    state.last_conflict_ms = 0;
    crate::storage::atomic_write(&root.join("breaker.json"), &serde_json::to_vec(&state)?)?;
    Ok(state)
}
pub fn retained_records(config: &Config) -> Vec<RetainedRecord> {
    config.evidence.as_ref().map_or_else(Vec::new, |e| {
        e.runtime.lock().records.values().cloned().collect()
    })
}
pub fn persist_retained(config: &Config) -> Result<()> {
    let Some(evidence) = &config.evidence else {
        return Ok(());
    };
    // Discovery and the periodic writer share this lock through publication and
    // acknowledgement: an older rename can never replace an acknowledged file.
    let mut runtime = evidence.runtime.lock();
    if runtime.acknowledged == Some(runtime.generation) {
        return Ok(());
    }
    let records = runtime.records.values().cloned().collect::<Vec<_>>();
    let root = &config
        .persistence
        .as_ref()
        .context("retained persistence missing")?
        .directory;
    crate::storage::atomic_write(&root.join("retained.json"), &serde_json::to_vec(&records)?)?;
    runtime.acknowledged = Some(runtime.generation);
    Ok(())
}
