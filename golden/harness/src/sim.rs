//! Scenario context. Boots the 0.4.24 runtime components through their public lib API in the
//! order `kline-proxy serve` uses (discover -> refresh_catalog -> restore -> reconcile/persist,
//! real router), but drives WebSocket input through `Engine::ingest` and the connection state
//! through `StreamStatus`, and the clock through the controller.
use crate::{
    control::{Control, VirtualClock},
    mock::{Mock, bar_fields},
};
use anyhow::{Context, Result};
use http_body_util::BodyExt;
use kline_core::Market;
use kline_market::metadata::Metadata;
use kline_runtime::{
    config::Config,
    directory,
    lifecycle::{self, Feed, Health, SyncedClock},
    rest::RestApi,
    storage::Store,
};
use kline_service::{Engine, http, upstream::StreamStatus};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tower::ServiceExt;

pub const MINUTE: i64 = 60_000;
pub const HOUR: i64 = 3_600_000;
/// 2026-10-26T00:00:00Z
pub const DAY0: i64 = 1_792_972_800_000;

struct FeedState {
    ids: Vec<usize>,
    topics: Vec<String>,
    status: Arc<StreamStatus>,
    mapped: bool,
}
pub struct Instance {
    pub engine: Arc<Engine>,
    config: Arc<Config>,
    metadata: Arc<Metadata>,
    health: Arc<Health>,
    router: axum::Router,
    stop: watch::Sender<bool>,
    persist_stop: watch::Sender<bool>,
    reconcile: JoinHandle<()>,
    persist: Option<JoinHandle<()>>,
    feeds: BTreeMap<String, FeedState>,
    market: Market,
    period: i64,
}
pub struct Sim {
    pub control: Arc<Control>,
    pub clock: Arc<VirtualClock>,
    hclock: Arc<SyncedClock>,
    pub mock: Arc<Mock>,
    url: String,
    start: tokio::time::Instant,
    work: PathBuf,
    pub inst: Option<Instance>,
    /// Symbols whose WebSocket frames are being delivered.
    pub streaming: BTreeSet<String>,
    frame_cursor: i64,
    http_lines: Vec<String>,
    event_lines: Vec<String>,
    snap_index: Vec<String>,
    snap_files: Vec<(String, Vec<u8>)>,
}
impl Sim {
    pub async fn new(
        control: Arc<Control>,
        clock: Arc<VirtualClock>,
        plan: crate::mock::Plan,
        work: PathBuf,
    ) -> Self {
        let (mock, url) = Mock::start(control.clone(), clock.clone(), plan).await;
        Self {
            control,
            hclock: Arc::new(SyncedClock::new(0)),
            mock,
            url,
            start: tokio::time::Instant::now(),
            work,
            inst: None,
            streaming: BTreeSet::new(),
            frame_cursor: 0,
            clock,
            http_lines: vec![],
            event_lines: vec![],
            snap_index: vec![],
            snap_files: vec![],
        }
    }
    pub fn now(&self) -> i64 {
        self.clock.read()
    }
    fn rel(&self) -> i64 {
        self.now() - self.clock.base_ms()
    }
    pub fn event(&mut self, value: Value) {
        let mut value = value;
        value["t"] = self.rel().into();
        self.event_lines.push(value.to_string());
    }
    /// Sleep until an absolute virtual time (ms since the Unix epoch).
    pub async fn at(&self, absolute: i64) {
        let target = self.start + Duration::from_millis((absolute - self.clock.base_ms()) as u64);
        tokio::time::sleep_until(target).await;
    }
    pub fn persist_dir(&self) -> PathBuf {
        self.work.join("persist")
    }
    /// Boot one process life. `config` is a 0.4.24 configuration without REST URLs/directory.
    pub async fn boot(&mut self, mut config: Value) -> Result<()> {
        config["rest"]["future_url"] = self.url.clone().into();
        config["rest"]["spot_url"] = self.url.clone().into();
        if config.get("persistence").is_some() {
            config["persistence"]["directory"] = self.persist_dir().display().to_string().into();
        }
        let config: Arc<Config> = Arc::new(serde_json::from_value(config).context("config")?);
        let engine = Engine::new(
            config.catalog()?,
            self.clock.clone(),
            config.engine_settings()?,
        );
        let rest = config.rest.clone().context("rest config")?;
        let api = RestApi::new_with_type(
            &rest.spot_url,
            &rest.future_url,
            rest.weight_per_minute,
            engine.number_type(),
        )?;
        let metadata = Metadata::new(api.clone(), config.market_api.metadata_refresh_seconds);
        if !config.subscriptions.is_empty() {
            let _hold = self.control.hold();
            let definitions = directory::discover(&config, &metadata).await?;
            let change = engine
                .refresh_catalog(definitions)
                .map_err(anyhow::Error::msg)?;
            self.event(json!({"event":"discover","changed":change.changed,"added":change.added,"removed":change.removed}));
        }
        let store = if let Some(persistence) = config.persistence.clone() {
            let e = engine.clone();
            let (store, report) = tokio::task::spawn_blocking(move || -> Result<_> {
                let mut store = Store::new(&persistence.directory, e.catalog.slots().len())?;
                store.configure(persistence.clone());
                let report = persistence.load_on_startup.then(|| store.restore(&e));
                Ok((store, report))
            })
            .await??;
            if let Some(report) = report {
                self.event(json!({"event":"restore","series":report.series,"bars":report.bars,"corrupt":report.corrupt}));
            }
            Some(store)
        } else {
            None
        };
        let health = Health::new(engine.clone(), vec![], self.hclock.clone(), rest.sync_clock);
        let (stop, stop_rx) = watch::channel(false);
        let (persist_stop, persist_rx) = watch::channel(false);
        let reconcile = tokio::spawn(lifecycle::reconcile(
            engine.clone(),
            api,
            rest,
            health.clone(),
            stop_rx,
        ));
        // The snapshot writer starts 125 ms after boot, so its dumps (whole seconds after that)
        // never share a virtual millisecond with REST commits or frames, which all fall on the
        // 50 ms grid of reconcile ticks, pacing and scripted delays. A dump reads the series on a
        // blocking thread; sharing an instant with a writer would make its contents racy.
        let persist = store.zip(config.persistence.clone()).map(|(store, c)| {
            let engine = engine.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(125)).await;
                lifecycle::persist(store, engine, c, persist_rx).await
            })
        });
        let router = http::router_with_policy(
            engine.clone(),
            Some(health.clone() as Arc<dyn http::Readiness>),
            config.strict_readiness,
        );
        let market = engine
            .catalog
            .slots()
            .iter()
            .next()
            .map_or(Market::Future, |s| s.market);
        let period = engine
            .catalog
            .slots()
            .iter()
            .next()
            .map_or(MINUTE, |s| s.interval.millis());
        self.inst = Some(Instance {
            engine,
            config,
            metadata,
            health,
            router,
            stop,
            persist_stop,
            reconcile,
            persist,
            feeds: BTreeMap::new(),
            market,
            period,
        });
        self.sync_feeds(true);
        self.frame_cursor = self.next_frame_after(self.now() - 1);
        self.event(json!({"event":"boot","series":self.inst().engine.catalog.instruments().len()}));
        Ok(())
    }
    pub fn inst(&self) -> &Instance {
        self.inst.as_ref().expect("booted")
    }
    /// The subscription grouping of `directory::supervise` (batch of 200 stable numeric IDs).
    fn desired(&self) -> BTreeMap<String, (Vec<String>, Vec<usize>)> {
        let inst = self.inst();
        let batch = inst.config.websocket.batch_size;
        let mut groups: BTreeMap<String, (Vec<String>, Vec<usize>)> = BTreeMap::new();
        for i in inst.engine.catalog.instruments() {
            if !i.trading {
                continue;
            }
            let id = inst
                .engine
                .catalog
                .find(i.market, i.interval, &i.symbol)
                .unwrap();
            let topic = match &i.continuous {
                Some((pair, contract)) => format!(
                    "{}_{}@continuousKline_{}",
                    pair.to_lowercase(),
                    contract.to_lowercase(),
                    i.interval.code()
                ),
                None => format!("{}@kline_{}", i.symbol.to_lowercase(), i.interval.code()),
            };
            let group = groups
                .entry(format!("{:?}-{}", i.market, id / batch))
                .or_default();
            group.0.push(topic);
            group.1.push(id);
        }
        groups
    }
    /// Reconnect groups whose topic set changed, as `directory::supervise` does.
    pub fn sync_feeds(&mut self, connect: bool) {
        let desired = self.desired();
        let now = self.hclock_now();
        let inst = self.inst.as_mut().unwrap();
        let mut changed = false;
        inst.feeds.retain(|key, feed| {
            let keep = desired
                .get(key)
                .is_some_and(|(topics, ids)| *topics == feed.topics && *ids == feed.ids);
            if !keep {
                feed.status.connected.store(false, Ordering::Release);
                feed.status.epoch.fetch_add(1, Ordering::Release);
                changed = true;
            }
            keep
        });
        for (key, (topics, ids)) in desired {
            if inst.feeds.contains_key(&key) {
                continue;
            }
            let status = Arc::new(StreamStatus::default());
            if connect {
                connect_status(&status, now);
            }
            inst.feeds.insert(
                key,
                FeedState {
                    ids,
                    topics,
                    status,
                    mapped: false,
                },
            );
            changed = true;
        }
        if changed {
            inst.health.replace_feeds(
                inst.feeds
                    .values()
                    .map(|f| Feed {
                        market: inst.market,
                        status: f.status.clone(),
                        ids: Some(Arc::from(f.ids.clone())),
                    })
                    .collect(),
            );
        }
    }
    fn hclock_now(&self) -> i64 {
        use kline_service::Clock;
        self.hclock.now_ms()
    }
    /// Drop (`false`) or re-establish (`true`) every upstream connection: a new epoch.
    pub fn set_connected(&mut self, connected: bool) {
        let now = self.hclock_now();
        for feed in self.inst.as_mut().unwrap().feeds.values_mut() {
            if connected {
                connect_status(&feed.status, now);
                feed.mapped = false;
            } else {
                feed.status.connected.store(false, Ordering::Release);
                feed.status.epoch.fetch_add(1, Ordering::Release);
            }
        }
        self.event(json!({"event": if connected {"ws_connect"} else {"ws_disconnect"}}));
    }
    fn next_frame_after(&self, after: i64) -> i64 {
        let period = self.inst.as_ref().map_or(MINUTE, |i| i.period);
        let boundary = after.div_euclid(period) * period;
        [
            boundary + 120,
            boundary + period / 2,
            boundary + period + 120,
        ]
        .into_iter()
        .find(|&t| t > after)
        .unwrap()
    }
    /// Advance to `absolute`, delivering the WebSocket frames due meanwhile.
    pub async fn until(&mut self, absolute: i64) {
        while self.frame_cursor <= absolute {
            let at = self.frame_cursor;
            self.at(at).await;
            self.frames(at);
            self.frame_cursor = self.next_frame_after(at);
        }
        self.at(absolute).await;
    }
    fn frames(&mut self, at: i64) {
        let Some(inst) = self.inst.as_ref() else {
            return;
        };
        let period = inst.period;
        let boundary = at.div_euclid(period) * period;
        let opens: Vec<(i64, bool)> = if at == boundary + 120 {
            vec![(boundary - period, true), (boundary, false)]
        } else {
            vec![(boundary, false)]
        };
        for symbol in self.streaming.clone() {
            for &(open, _) in &opens {
                self.ws(&symbol, open, at);
            }
        }
    }
    /// Inject one upstream frame for `symbol`'s bar at `open`, as observed at `now`.
    pub fn ws(&mut self, symbol: &str, open: i64, now: i64) {
        let hnow = self.hclock_now();
        let inst = self.inst.as_mut().unwrap();
        let plan = self.mock.plan.lock().unwrap();
        let Some(spec) = plan.symbols.get(symbol).cloned() else {
            return;
        };
        drop(plan);
        if open < spec.listed_from || spec.holes.contains(&open) {
            return;
        }
        let Some(id) = inst
            .engine
            .catalog
            .slots()
            .iter()
            .position(|s| s.symbol.as_ref() == symbol && s.is_tracked())
        else {
            return;
        };
        let Some(feed) = inst.feeds.values_mut().find(|f| f.ids.contains(&id)) else {
            return;
        };
        if !feed.status.connected.load(Ordering::Acquire) {
            return;
        }
        let slot = inst.engine.catalog.slot(id);
        let period = slot.interval.millis();
        let (f, trades, closed) = bar_fields(spec.seed, open, period, now);
        let continuous = inst
            .engine
            .catalog
            .instruments()
            .into_iter()
            .find(|i| i.symbol == symbol)
            .and_then(|i| i.continuous);
        let head = match &continuous {
            Some((pair, contract)) => {
                format!(r#""e":"continuous_kline","E":{now},"ps":"{pair}","ct":"{contract}""#)
            }
            None => format!(r#""e":"kline","E":{now},"s":"{symbol}""#),
        };
        let frame = format!(
            r#"{{{head},"k":{{"t":{open},"T":{},"i":"{}","f":1,"L":2,"o":"{}","c":"{}","h":"{}","l":"{}","v":"{}","n":{trades},"x":{closed},"q":"{}","V":"{}","Q":"{}","B":"0"}}}}"#,
            open + period - 1,
            slot.interval.code(),
            f[0],
            f[3],
            f[1],
            f[2],
            f[4],
            f[5],
            f[6],
            f[7]
        );
        feed.status.last_io_ms.store(hnow, Ordering::Relaxed);
        let mapped = inst
            .engine
            .ingest(inst.market, frame.as_bytes())
            .expect("valid frame");
        if mapped.is_some() && !feed.mapped {
            feed.status
                .mapped_epoch
                .store(feed.status.epoch.load(Ordering::Acquire), Ordering::Release);
            feed.mapped = true;
        }
    }
    /// One request through the real router while virtual time is held.
    pub async fn http(&mut self, label: &str, method: &str, uri: &str, body: Option<&str>) {
        self.control.quiesce().await;
        let hold = self.control.freeze();
        let mut request = axum::http::Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let request = request
            .body(axum::body::Body::from(body.unwrap_or("").to_owned()))
            .unwrap();
        let response = self
            .inst()
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("router");
        let status = response.status().as_u16();
        let headers: BTreeMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        drop(hold);
        let body_text = match std::str::from_utf8(&bytes) {
            Ok(text) => Value::from(text),
            Err(_) => json!({"hex": bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()}),
        };
        let line = json!({"t":self.rel(),"label":label,"method":method,"uri":uri,"request_body":body,
            "status":status,"headers":headers,"body":body_text});
        self.http_lines.push(line.to_string());
    }
    pub async fn get(&mut self, label: &str, uri: &str) {
        self.http(label, "GET", uri, None).await
    }
    /// Directory refresh as `directory::refresh` performs it (metadata refresh, then discover).
    pub async fn refresh_directory(&mut self) -> Result<()> {
        self.control.quiesce().await;
        let hold = self.control.hold();
        let inst = self.inst();
        let result = async {
            inst.metadata.refresh(inst.market).await?;
            let instruments = directory::discover(&inst.config, &inst.metadata).await?;
            inst.engine
                .refresh_catalog(instruments)
                .map_err(anyhow::Error::msg)
        }
        .await;
        drop(hold);
        match result {
            Ok(change) => self.event(json!({"event":"directory_refresh","changed":change.changed,"added":change.added,"removed":change.removed})),
            Err(error) => self.event(json!({"event":"directory_refresh_failed","error":error.to_string()})),
        }
        self.sync_feeds(true);
        Ok(())
    }
    /// Capture the snapshot directory, header timestamp and checksum normalized.
    pub async fn snap(&mut self, label: &str) {
        self.control.quiesce().await;
        let _hold = self.control.freeze();
        self.snap_now(label);
    }
    fn snap_now(&mut self, label: &str) {
        let index = self.snap_index.len();
        let dir = self.persist_dir();
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        let mut files = vec![];
        for name in names {
            assert!(
                !name.contains(".tmp-"),
                "snapshot captured while a dump was writing {name}"
            );
            if !name.ends_with(".kpr") {
                files.push(json!({"name":name,"ignored":true}));
                continue;
            }
            let mut bytes = std::fs::read(dir.join(&name)).unwrap();
            let ok = bytes.len() >= 54 && {
                let (content, checksum) = bytes.split_at(bytes.len() - 32);
                Sha256::digest(content).as_slice() == checksum
            };
            if bytes.len() >= 54 {
                bytes[8..16].fill(0);
                let n = bytes.len();
                bytes[n - 32..].fill(0);
            }
            files.push(json!({"name":name,"len":bytes.len(),"checksum_ok":ok}));
            self.snap_files
                .push((format!("{index:03}-{label}/{name}"), bytes));
        }
        self.snap_index
            .push(json!({"t":self.rel(),"label":label,"files":files}).to_string());
    }
    /// Stop the life: reconcile stops, then the final snapshot pass runs.
    pub async fn shutdown(&mut self) {
        let inst = self.inst.take().unwrap();
        let _ = inst.stop.send(true);
        let _ = inst.reconcile.await;
        for feed in inst.feeds.values() {
            feed.status.connected.store(false, Ordering::Release);
        }
        let _ = inst.persist_stop.send(true);
        if let Some(persist) = inst.persist {
            let _ = persist.await;
        }
        self.event(json!({"event":"shutdown"}));
        self.snap_now("shutdown");
        self.streaming.clear();
    }
    pub fn write(&self, out: &std::path::Path) -> Result<()> {
        let _ = std::fs::remove_dir_all(out);
        std::fs::create_dir_all(out.join("snapshots"))?;
        let lines = |v: &[String]| v.iter().map(|l| format!("{l}\n")).collect::<String>();
        std::fs::write(out.join("http.jsonl"), lines(&self.http_lines))?;
        std::fs::write(out.join("rest.jsonl"), lines(&self.mock.trace_lines()))?;
        std::fs::write(out.join("events.jsonl"), lines(&self.event_lines))?;
        std::fs::write(out.join("snapshots/index.jsonl"), lines(&self.snap_index))?;
        for (path, bytes) in &self.snap_files {
            let path = out.join("snapshots").join(path);
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, bytes)?;
        }
        Ok(())
    }
}
fn connect_status(status: &StreamStatus, now: i64) {
    status.last_io_ms.store(now, Ordering::Relaxed);
    status.connected_since_ms.store(now, Ordering::Relaxed);
    status.epoch.fetch_add(1, Ordering::Release);
    status.connected.store(true, Ordering::Release);
}

/// Write a 0.4.24-format (`KLINE\0\x01\0`, double) snapshot exactly as `storage.rs` encodes it.
pub fn write_v1_snapshot(
    dir: &std::path::Path,
    market: &str,
    interval: &str,
    symbol: &str,
    saved: i64,
    bars: &[(i64, i64, u32, [f64; 8])],
) {
    let key = format!("{market}\0{interval}\0{symbol}");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"KLINE\0\x01\0");
    bytes.extend_from_slice(&saved.to_le_bytes());
    bytes.extend_from_slice(&(key.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&(bars.len() as u32).to_le_bytes());
    bytes.extend_from_slice(key.as_bytes());
    for (open, close, trades, values) in bars {
        bytes.extend_from_slice(&open.to_le_bytes());
        bytes.extend_from_slice(&close.to_le_bytes());
        bytes.extend_from_slice(&trades.to_le_bytes());
        for v in values {
            bytes.extend_from_slice(&v.to_bits().to_le_bytes());
        }
    }
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    std::fs::create_dir_all(dir).unwrap();
    let name = format!("{:x}.kpr", Sha256::digest(key.as_bytes()));
    std::fs::write(dir.join(name), bytes).unwrap();
}
