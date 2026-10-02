use anyhow::{Context, Result, bail};
use kline_core::Market;
use kline_market::http::App;
use kline_runtime::{
    config::{Config, market},
    directory,
    lifecycle::{self, Health, SyncedClock},
    listener,
    rest::RestApi,
    storage::Store,
};
use kline_service::{Engine, http, upstream};
use serde::Deserialize;
use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .context("usage: kline-proxy [--check-config] <config.json>")?;
    if path == "--version" {
        println!("kline-proxy {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let check = path == "--check-config";
    let path = if check {
        args.next().context("missing configuration file")?
    } else {
        path
    };
    let config = Arc::new(Config::load(Path::new(&path))?);
    if check {
        config.catalog()?;
        println!(
            "configuration valid: {} static instruments, {} subscriptions",
            config.instruments.len(),
            config.subscriptions.len()
        );
        return Ok(());
    }
    let clock = Arc::new(SyncedClock::new(config.clock_offset_ms));
    let engine = Engine::new(config.catalog()?, clock.clone(), config.engine_settings()?);
    let workers = std::env::var("KLINE_IO_WORKERS")
        .ok()
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(2)
        .clamp(1, 64);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(4)
        .enable_all()
        .build()?
        .block_on(serve(config, engine, clock))
}

async fn serve(config: Arc<Config>, engine: Arc<Engine>, clock: Arc<SyncedClock>) -> Result<()> {
    let rest = config.rest.clone().unwrap_or_default();
    let api = RestApi::new_with_type(
        &rest.spot_url,
        &rest.future_url,
        rest.weight_per_minute,
        engine.number_type(),
    )?;
    let app = App::new(engine.clone(), api.clone(), config.market_api.clone());
    app.tickers
        .set_price_intervals(config.ticker_price_intervals()?);
    if !config.subscriptions.is_empty() {
        let definitions = directory::discover(&config, &app.metadata).await?;
        engine
            .refresh_catalog(definitions)
            .map_err(anyhow::Error::msg)?;
    }
    // Discovery precedes restore so newly discovered symbols can import Java snapshots too.
    let store = if let Some(persistence) = config.persistence.clone() {
        let e = engine.clone();
        Some(
            tokio::task::spawn_blocking(move || -> Result<Store> {
                let mut store = Store::new(&persistence.directory, e.catalog.slots().len())?;
                store.configure(persistence.clone());
                if persistence.load_on_startup {
                    let report = store.restore(&e);
                    tracing::info!(
                        series = report.series,
                        bars = report.bars,
                        corrupt = report.corrupt,
                        "snapshot restore complete"
                    );
                }
                Ok(store)
            })
            .await??,
        )
    } else {
        None
    };
    if let Some(seed) = config.seed.clone() {
        let e = engine.clone();
        tokio::task::spawn_blocking(move || load_seed(&e, &seed)).await??;
    }
    let (stop_tx, stop_rx) = watch::channel(false);
    let (persist_tx, persist_rx) = watch::channel(false);
    let mut tasks = Vec::new();
    let listener = listener::bind(&config.listen, config.listen_backlog).await?;
    let readiness = if config.rest.is_some() {
        let health = Health::new(engine.clone(), vec![], clock.clone(), rest.sync_clock);
        tasks.push(tokio::spawn(directory::supervise(
            engine.clone(),
            config.clone(),
            health.clone(),
            stop_rx.clone(),
        )));
        if !config.subscriptions.is_empty() {
            tasks.push(tokio::spawn(directory::refresh(
                engine.clone(),
                config.clone(),
                app.metadata.clone(),
                stop_rx.clone(),
            )));
        }
        if rest.sync_clock {
            let clock_market = if engine
                .catalog
                .slots()
                .iter()
                .any(|s| s.market == Market::Future)
            {
                Market::Future
            } else {
                Market::Spot
            };
            tasks.push(tokio::spawn(lifecycle::sync_clock(
                api.clone(),
                clock,
                clock_market,
                stop_rx.clone(),
            )));
        }
        tasks.push(tokio::spawn(lifecycle::reconcile(
            engine.clone(),
            api,
            rest,
            health.clone(),
            stop_rx.clone(),
        )));
        Some(health as Arc<dyn http::Readiness>)
    } else {
        for stream in config.resolved_streams()? {
            tasks.push(tokio::spawn(upstream::run(
                engine.clone(),
                market(&stream.market)?,
                stream.url,
                stop_rx.clone(),
            )));
        }
        None
    };
    if config.market_api.enabled {
        tasks.push(tokio::spawn(app.metadata.clone().run(stop_rx.clone())));
        for market in [Market::Future, Market::Spot] {
            let active = config.rest.is_some()
                && (engine.catalog.slots().iter().any(|s| s.market == market)
                    || config
                        .subscriptions
                        .iter()
                        .any(|s| crate::market(&s.market).ok() == Some(market)));
            tasks.push(tokio::spawn(app.tickers.clone().run(
                market,
                active,
                stop_rx.clone(),
            )));
            if !active {
                continue;
            }
            let root = if market == Market::Future {
                &config.websocket.future_url
            } else {
                &config.websocket.spot_url
            };
            tasks.push(tokio::spawn(kline_runtime::tickers::supervise(
                app.tickers.clone(),
                app.metadata.clone(),
                root.clone(),
                market,
                stop_rx.clone(),
            )));
        }
        if config.rest.is_some() && config.market_api.funding.enabled {
            tasks.push(tokio::spawn(app.funding.clone().run(stop_rx.clone())));
            if config.market_api.funding.vision_enabled {
                let funding = app.funding.clone();
                let mut stop = stop_rx.clone();
                tasks.push(tokio::spawn(async move {
                    tokio::select! {_=stop.changed()=>{},result=kline_market::vision::warm(funding)=>match result{
                        Ok(hours)=>tracing::info!(hours,"funding archive bootstrap complete"),
                        Err(e)=>tracing::warn!(error=%e,"funding archive bootstrap incomplete; REST remains available"),
                    }}
                }));
            }
        }
        if config.rest.is_some() && config.market_api.statistics.enabled {
            tasks.push(tokio::spawn(app.statistics.clone().run(stop_rx.clone())));
        }
    }
    let e = engine.clone();
    let mut diagnostic_stop = stop_rx.clone();
    tasks.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = diagnostic_stop.changed() => break,
                _ = e.diagnostics.changed() => {},
                _ = e.windows_changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
            let engine = e.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || {
                engine.diagnostics.maintain(&engine);
                engine.prepare_closed_windows();
            })
            .await
            {
                tracing::warn!(%error, "diagnostic worker failed");
            }
        }
    }));
    let persister = store
        .zip(config.persistence.clone())
        .map(|(store, config)| {
            tokio::spawn(lifecycle::persist(
                store,
                engine.clone(),
                config,
                persist_rx,
            ))
        });
    let mut router = http::router_with_policy(engine.clone(), readiness, config.strict_readiness);
    if config.market_api.enabled {
        router = router.merge(kline_market::http::router(app));
    }
    tracing::info!(version=env!("CARGO_PKG_VERSION"),address=%listener.local_addr()?,listen_backlog=config.listen_backlog,series=engine.catalog.instruments().len(),"kline proxy listening");
    let mut server_stop = stop_rx;
    let server = axum::serve(
        http::low_latency_listener(listener),
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !*server_stop.borrow() {
            if server_stop.changed().await.is_err() {
                break;
            }
        }
    })
    .into_future();
    tokio::pin!(server);
    let result = tokio::select! {
        result=&mut server=>result,
        _=shutdown_signal()=>{
            let _=stop_tx.send(true);
            match tokio::time::timeout(Duration::from_secs(35),&mut server).await {
                Ok(result)=>result,Err(_)=>{tracing::warn!("HTTP drain timed out");Ok(())}
            }
        }
    };
    let _ = stop_tx.send(true);
    // Join every writer even if one task panics; always attempt the final durable snapshot.
    let mut task_error = None;
    for task in tasks {
        if let Err(error) = task.await {
            tracing::error!(%error,"background task failed");
            task_error = Some(error);
        }
    }
    let _ = persist_tx.send(true);
    if let Some(task) = persister {
        task.await?;
    }
    result?;
    if let Some(error) = task_error {
        return Err(error.into());
    }
    Ok(())
}
fn load_seed(engine: &Engine, path: &Path) -> Result<()> {
    #[derive(Deserialize)]
    struct SeedRow {
        market: String,
        frame: Box<serde_json::value::RawValue>,
    }
    for (index, line) in BufReader::new(std::fs::File::open(path)?)
        .lines()
        .enumerate()
    {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        anyhow::ensure!(
            line.len() <= 64 * 1024,
            "seed line {} exceeds 64KiB",
            index + 1
        );
        let row: SeedRow =
            serde_json::from_str(&line).with_context(|| format!("seed line {}", index + 1))?;
        if engine
            .ingest(market(&row.market)?, row.frame.get().as_bytes())?
            .is_none()
        {
            bail!("unmapped seed at line {}", index + 1);
        }
    }
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
