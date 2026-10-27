//! Virtual-time controller.
//!
//! The runtime is a current-thread Tokio runtime with a paused clock. Tokio advances a paused
//! clock whenever the scheduler parks with no ready task, even if a loopback socket exchange is
//! still in flight in the kernel. That would make REST completion times depend on wall-clock
//! scheduling. This controller keeps an auto-advance inhibitor (a parked `spawn_blocking` task,
//! which Tokio counts as "do not auto-advance") and drops it only at a quiescent point:
//!
//! * no client TCP connect announced by hyper-util's `connecting to` event is still unaccepted;
//! * no mock-server connection is reading a request, writing a response or waiting for the
//!   client to close after the response;
//! * no harness operation (directory refresh, HTTP checkpoint) holds the clock.
//!
//! Mock responses are released one at a time at quiescent points, in a canonical order, so the
//! order in which the client tasks observe responses never depends on kernel delivery order.
//! The clock is re-inhibited from `on_thread_unpark` as soon as virtual time has moved, before any
//! task woken by that advance runs.
use kline_service::Clock;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

/// Engine clock driven by the paused Tokio clock. Threads without a runtime context (bulk build
/// workers) read the value cached at the last runtime-thread observation; no virtual time passes
/// while such work runs, because the controller holds the clock for every harness HTTP request.
pub struct VirtualClock {
    base_ms: i64,
    start: OnceLock<tokio::time::Instant>,
    cache: AtomicI64,
}
impl VirtualClock {
    pub fn new(base_ms: i64) -> Self {
        Self {
            base_ms,
            start: OnceLock::new(),
            cache: AtomicI64::new(base_ms),
        }
    }
    /// Must be called on the runtime thread before the clock is used.
    pub fn start(&self) {
        self.start.get_or_init(tokio::time::Instant::now);
        self.read();
    }
    pub fn read(&self) -> i64 {
        match (tokio::runtime::Handle::try_current(), self.start.get()) {
            (Ok(_), Some(start)) => {
                let value = self.base_ms + start.elapsed().as_millis() as i64;
                self.cache.store(value, Ordering::Release);
                value
            }
            _ => self.cache.load(Ordering::Acquire),
        }
    }
    pub fn base_ms(&self) -> i64 {
        self.base_ms
    }
}
impl Clock for VirtualClock {
    fn now_ms(&self) -> i64 {
        self.read()
    }
    fn host_ms(&self) -> i64 {
        self.read()
    }
}

#[derive(Default)]
struct Inner {
    inhibitor: Option<mpsc::Sender<()>>,
    holds: usize,
    frozen: usize,
    hold_since: Option<std::time::Instant>,
    waiters: std::collections::VecDeque<tokio::sync::oneshot::Sender<()>>,
    pending_connects: usize,
    server_io: usize,
    ready: BTreeMap<(i64, String, u64), tokio::sync::oneshot::Sender<()>>,
    last_virtual: Option<tokio::time::Instant>,
    violations: Vec<String>,
    stuck_since: Option<std::time::Instant>,
}

#[derive(Default)]
pub struct Control {
    inner: Mutex<Inner>,
    finished: AtomicBool,
    clock: OnceLock<Arc<VirtualClock>>,
    pub advances: AtomicU64,
    pub connects: AtomicU64,
    pub accepts: AtomicU64,
    ready_seq: AtomicU64,
}
pub struct Hold(Arc<Control>, bool);
impl Drop for Hold {
    fn drop(&mut self) {
        let mut g = self.0.inner.lock().unwrap();
        g.holds -= 1;
        g.frozen -= usize::from(self.1);
        if g.holds == 0 {
            g.hold_since = None;
        }
    }
}
impl Control {
    pub fn set_clock(&self, clock: Arc<VirtualClock>) {
        let _ = self.clock.set(clock);
    }
    fn ensure_held(&self, g: &mut Inner) {
        if g.inhibitor.is_none() && !self.finished.load(Ordering::Acquire) {
            let (tx, rx) = mpsc::channel::<()>();
            // A running blocking task inhibits Tokio's auto-advance until it returns.
            drop(tokio::task::spawn_blocking(move || {
                let _ = rx.recv();
            }));
            g.inhibitor = Some(tx);
        }
    }
    pub fn hold_now(&self) {
        let mut g = self.inner.lock().unwrap();
        self.ensure_held(&mut g);
    }
    /// Stop virtual time until the guard is dropped; mock responses are still released
    /// (directory refresh needs its exchangeInfo reply).
    pub fn hold(self: &Arc<Self>) -> Hold {
        self.guard(false)
    }
    /// Stop virtual time and mock-response release (HTTP checkpoints, snapshot capture).
    pub fn freeze(self: &Arc<Self>) -> Hold {
        self.guard(true)
    }
    fn guard(self: &Arc<Self>, frozen: bool) -> Hold {
        let mut g = self.inner.lock().unwrap();
        g.holds += 1;
        g.frozen += usize::from(frozen);
        g.hold_since.get_or_insert_with(std::time::Instant::now);
        self.ensure_held(&mut g);
        Hold(self.clone(), frozen)
    }
    /// Resolve at the next point of this virtual instant where every socket exchange has
    /// finished and only timers remain, instead of letting time advance there.
    pub async fn quiesce(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut g = self.inner.lock().unwrap();
            g.waiters.push_back(tx);
            self.ensure_held(&mut g);
        }
        let _ = rx.await;
    }
    pub fn before_park(&self) {
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        if g.pending_connects > 0 || g.server_io > 0 {
            self.ensure_held(&mut g);
            return;
        }
        if g.frozen == 0
            && let Some((_, release)) = g.ready.pop_first()
        {
            g.server_io += 1;
            self.ensure_held(&mut g);
            let _ = release.send(());
            return;
        }
        if g.holds > 0 {
            self.ensure_held(&mut g);
            return;
        }
        if let Some(waiter) = g.waiters.pop_front() {
            self.ensure_held(&mut g);
            let _ = waiter.send(());
            return;
        }
        // Quiescent: only virtual timers can make progress. Allow one auto-advance.
        g.inhibitor = None;
    }
    pub fn after_unpark(&self) {
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        let now = tokio::time::Instant::now();
        let mut g = self.inner.lock().unwrap();
        if g.last_virtual != Some(now) {
            let advanced = g.last_virtual.is_some();
            g.last_virtual = Some(now);
            if let Some(clock) = self.clock.get() {
                clock.read();
            }
            if advanced {
                self.advances.fetch_add(1, Ordering::Relaxed);
                if g.pending_connects > 0
                    || g.server_io > 0
                    || (g.frozen == 0 && !g.ready.is_empty())
                {
                    let detail = format!(
                        "virtual time advanced while socket I/O was outstanding (connects={} io={} ready={} at {})",
                        g.pending_connects,
                        g.server_io,
                        g.ready.len(),
                        self.clock.get().map(|c| c.read()).unwrap_or_default()
                    );
                    g.violations.push(detail);
                }
            }
            self.ensure_held(&mut g);
        }
    }
    pub fn connect_started(&self) {
        self.connects.fetch_add(1, Ordering::Relaxed);
        let mut g = self.inner.lock().unwrap();
        g.pending_connects += 1;
        self.ensure_held(&mut g);
    }
    pub fn connect_failed(&self) {
        let mut g = self.inner.lock().unwrap();
        g.pending_connects = g.pending_connects.saturating_sub(1);
    }
    pub fn accepted(&self) {
        self.accepts.fetch_add(1, Ordering::Relaxed);
        let mut g = self.inner.lock().unwrap();
        if g.pending_connects == 0 {
            g.violations
                .push("mock server accepted a connection no hyper-util connect announced".into());
        }
        g.pending_connects = g.pending_connects.saturating_sub(1);
        g.server_io += 1;
        self.ensure_held(&mut g);
    }
    pub fn io_done(&self) {
        let mut g = self.inner.lock().unwrap();
        g.server_io -= 1;
    }
    /// Queue a finished mock response; it is written when the controller releases it.
    pub fn ready(&self, at_ms: i64, signature: String) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let seq = self.ready_seq.fetch_add(1, Ordering::Relaxed);
        self.inner
            .lock()
            .unwrap()
            .ready
            .insert((at_ms, signature, seq), tx);
        rx
    }
    pub fn violations(&self) -> Vec<String> {
        self.inner.lock().unwrap().violations.clone()
    }
    /// Stop inhibiting so the runtime can shut down its blocking pool.
    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.inner.lock().unwrap().inhibitor = None;
    }
    fn status(&self) -> String {
        let g = self.inner.lock().unwrap();
        format!(
            "holds={} pending_connects={} server_io={} ready={} advances={} virtual_ms={}",
            g.holds,
            g.pending_connects,
            g.server_io,
            g.ready.len(),
            self.advances.load(Ordering::Relaxed),
            self.clock.get().map(|c| c.read()).unwrap_or_default()
        )
    }
}

/// Fail loudly (exit 3) instead of hanging when a hold waits on virtual time or I/O never ends.
pub fn watchdog(control: Arc<Control>, limit: Duration) {
    std::thread::spawn(move || {
        let started = std::time::Instant::now();
        loop {
            std::thread::sleep(Duration::from_millis(200));
            if control.finished.load(Ordering::Acquire) {
                return;
            }
            let reason = {
                let mut g = control.inner.lock().unwrap();
                let stuck = g.pending_connects > 0 || g.server_io > 0;
                if stuck {
                    g.stuck_since.get_or_insert_with(std::time::Instant::now);
                } else {
                    g.stuck_since = None;
                }
                if g.hold_since
                    .is_some_and(|t| t.elapsed() > Duration::from_secs(20))
                {
                    Some(
                        "a harness hold lasted >20 s real time (checkpoint waiting on virtual time?)",
                    )
                } else if g
                    .stuck_since
                    .is_some_and(|t| t.elapsed() > Duration::from_secs(20))
                {
                    Some("socket I/O outstanding for >20 s real time")
                } else if started.elapsed() > limit {
                    Some("scenario exceeded its real-time limit")
                } else {
                    None
                }
            };
            if let Some(reason) = reason {
                eprintln!("kp-golden watchdog: {reason}; {}", control.status());
                std::process::exit(3);
            }
        }
    });
}

/// Announce hyper-util connect attempts to the controller (the only client sockets in a run).
struct ConnectLayer(Arc<Control>);
struct Message(String);
impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ConnectLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if !event
            .metadata()
            .target()
            .starts_with("hyper_util::client::legacy::connect::http")
        {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        if message.0.starts_with("connecting to ") {
            self.0.connect_started();
        } else if message.0.starts_with("connect error for ") {
            self.0.connect_failed();
        }
    }
}
pub fn install_tracing(control: Arc<Control>) {
    use tracing_subscriber::{Layer, layer::SubscriberExt};
    let connect =
        ConnectLayer(control).with_filter(tracing_subscriber::filter::Targets::new().with_target(
            "hyper_util::client::legacy::connect::http",
            tracing::Level::TRACE,
        ));
    // Optional diagnostics on stderr only (never part of the recorded artifacts).
    let log = std::env::var("KP_GOLDEN_LOG").ok().map(|filter| {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(tracing_subscriber::EnvFilter::new(filter))
    });
    let subscriber = tracing_subscriber::registry().with(connect).with(log);
    tracing::subscriber::set_global_default(subscriber).expect("install tracing");
}
