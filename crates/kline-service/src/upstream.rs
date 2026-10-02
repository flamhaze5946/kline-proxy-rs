//! Socket adapter. Each frame is parsed/committed once; no unbounded work queue.
//! A read task applies backpressure to its socket and cooperatively yields in bursts.
use crate::Engine;
use futures_util::{SinkExt, StreamExt};
use kline_core::Market;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

static STREAM_ID: AtomicU64 = AtomicU64::new(1);
pub struct StreamStatus {
    pub id: u64,
    pub connected: AtomicBool,
    pub epoch: AtomicU64,
    pub last_io_ms: AtomicI64,
    pub mapped_epoch: AtomicU64,
    pub connected_since_ms: AtomicI64,
}
impl Default for StreamStatus {
    fn default() -> Self {
        Self {
            id: STREAM_ID.fetch_add(1, Ordering::Relaxed),
            connected: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            last_io_ms: AtomicI64::new(0),
            mapped_epoch: AtomicU64::new(0),
            connected_since_ms: AtomicI64::new(0),
        }
    }
}
impl StreamStatus {
    fn transition(&self, connected: bool) {
        if connected {
            self.epoch.fetch_add(1, Ordering::Release);
            self.connected.store(true, Ordering::Release);
        } else {
            self.connected.store(false, Ordering::Release);
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }
}
pub async fn run(
    engine: Arc<Engine>,
    market: Market,
    url: String,
    shutdown: watch::Receiver<bool>,
) {
    run_monitored(
        engine,
        market,
        url,
        shutdown,
        Arc::new(StreamStatus::default()),
    )
    .await;
}
pub async fn run_monitored(
    engine: Arc<Engine>,
    market: Market,
    url: String,
    shutdown: watch::Receiver<bool>,
    status: Arc<StreamStatus>,
) {
    run_controlled(engine, market, url, shutdown, status, None).await;
}
/// Least time between two resubscriptions (two control messages each) on one connection.
pub const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(1);
pub async fn run_controlled(
    engine: Arc<Engine>,
    market: Market,
    url: String,
    mut shutdown: watch::Receiver<bool>,
    status: Arc<StreamStatus>,
    mut commands: Option<tokio::sync::mpsc::Receiver<Vec<String>>>,
) {
    let mut failures: u32 = 0;
    let url_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        url.hash(&mut hasher);
        hasher.finish()
    };
    while !*shutdown.borrow() {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = engine.connect_pacer().turn() => {}
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(64 * 1024))
            .max_frame_size(Some(64 * 1024));
        let result = tokio::select! {
            _ = shutdown.changed() => break,
            result = tokio::time::timeout(Duration::from_secs(15), connect_async_with_config(&url, Some(config), true)) => result,
        };
        if let Ok(Ok((mut socket, _))) = result {
            status.last_io_ms.store(engine.now_ms(), Ordering::Relaxed);
            status
                .connected_since_ms
                .store(engine.now_ms(), Ordering::Relaxed);
            status.transition(true);
            tracing::info!(?market, "upstream connected");
            let connected = tokio::time::Instant::now();
            let epoch = status.epoch.load(Ordering::Acquire);
            let mut mapped = false;
            let mut count = 0_u32;
            // Topics waiting to be resubscribed, merged from every command received meanwhile.
            // One UNSUBSCRIBE/SUBSCRIBE pair goes out per RESUBSCRIBE_INTERVAL at most, keeping
            // this connection's control messages (PONGs included) inside Binance's per-second
            // limit (5 for spot, 10 for futures). Reads go on while a resubscription waits out the
            // interval; only the two sends themselves (at most 2 s) hold the loop.
            let mut pending: Vec<String> = Vec::new();
            let mut next_resubscribe = tokio::time::Instant::now();
            loop {
                let next = tokio::select! {
                    _ = shutdown.changed() => { let _ = tokio::time::timeout(Duration::from_secs(1),socket.close(None)).await; break; },
                    _ = tokio::time::sleep_until(connected + Duration::from_secs(23*3600)) => break,
                    Some(topics)=async {match commands.as_mut(){Some(rx)=>rx.recv().await,None=>std::future::pending().await}}=>{
                        for topic in topics {
                            if !pending.contains(&topic) {
                                pending.push(topic);
                            }
                        }
                        continue;
                    },
                    _ = tokio::time::sleep_until(next_resubscribe), if !pending.is_empty() => {
                        let topics = std::mem::take(&mut pending);
                        let resubscribe=async {
                            for method in ["UNSUBSCRIBE","SUBSCRIBE"] {
                                let text=serde_json::json!({"method":method,"params":topics,"id":count as u64+1}).to_string();
                                socket.send(Message::Text(text.into())).await?;
                                count=count.wrapping_add(1);
                            }
                            Ok::<_,tokio_tungstenite::tungstenite::Error>(())
                        };
                        if !matches!(tokio::time::timeout(Duration::from_secs(2),resubscribe).await,Ok(Ok(()))){break}
                        next_resubscribe = tokio::time::Instant::now() + RESUBSCRIBE_INTERVAL;
                        continue;
                    },
                    next = tokio::time::timeout(Duration::from_secs(180), socket.next()) => next,
                };
                status.last_io_ms.store(engine.now_ms(), Ordering::Relaxed);
                match next {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        match engine.ingest(market, text.as_bytes()) {
                            Ok(Some(_)) if !mapped => {
                                status.mapped_epoch.store(epoch, Ordering::Release);
                                mapped = true;
                            }
                            Err(error) => tracing::warn!(%error,"invalid upstream frame"),
                            _ => {}
                        }
                    }
                    Ok(Some(Ok(Message::Ping(_)))) => {
                        if socket.flush().await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => {}
                    _ => break,
                }
                count += 1;
                if count.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            if connected.elapsed() > Duration::from_secs(60) {
                failures = 0;
            }
            status.transition(false);
        }
        if *shutdown.borrow() {
            break;
        }
        failures = failures.saturating_add(1);
        let delay_ms = (250_u64 << failures.min(7)).min(30_000);
        // Per-connection jitter keeps connections that dropped together from retrying together;
        // the connect pacer then spaces the attempts that still coincide.
        let jitter =
            (engine.now_ms().unsigned_abs() ^ u64::from(std::process::id()) ^ url_hash) % 251;
        tracing::warn!(
            ?market,
            retry_ms = delay_ms + jitter,
            "upstream disconnected; connection epoch requires recovery"
        );
        tokio::select! { _ = shutdown.changed() => break, _ = tokio::time::sleep(Duration::from_millis(delay_ms+jitter)) => {} }
    }
    if status.connected.load(Ordering::Acquire) {
        status.transition(false);
    }
}
