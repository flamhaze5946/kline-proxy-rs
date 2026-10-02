//! Paces outgoing WebSocket connection attempts of one process.
//!
//! Binance allows 300 connection attempts per IP every five minutes (stated for spot streams and
//! assumed here for futures); a connection that exceeds a limit is disconnected, and an IP that
//! keeps exceeding them may be banned. A restart, the 24-hour rotation or a network fault would
//! otherwise reconnect every stream at once. Every attempt (first connect, reconnect or rotation;
//! klines and tickers alike) first takes its turn here: attempts start at least [`SPACING`]
//! apart, and at most [`CAP`] start within any [`WINDOW`].
//!
//! Only admitted attempts count, from when they go ahead: a waiter cancelled before its turn
//! leaves nothing behind, while an admitted attempt counts even if no connection follows. Waiters
//! queue in arrival order on a fair lock and the first one rechecks the clock after every sleep,
//! so waiters that oversleep (a stalled runtime, a suspended host) still start one [`SPACING`]
//! apart.
//!
//! The engine owns one pacer per process. With a journal file (the runtime keeps one in its
//! persistence directory) the start times are also written there and read back by the next
//! process, so a crash loop shares one allowance instead of getting a fresh one per restart.
//! Once admitted, an attempt is recorded and the journal written before the next waiter goes,
//! even if the caller is cancelled meanwhile; its attempt then still counts. The journal assumes
//! one writer (one service instance) and a wall clock that does not step forward between
//! recording and reloading; a recorded time can precede the actual attempt by the length of one
//! journal write. It covers process crashes and restarts; when the journal cannot be read or
//! written the count falls back to this process (with a warning). Other programs on the same IP
//! are not counted.
use std::sync::Arc;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex, time::Instant};

/// At most ten attempts per second.
pub const SPACING: Duration = Duration::from_millis(100);
pub const WINDOW: Duration = Duration::from_secs(300);
/// Half of Binance's per-IP allowance for [`WINDOW`], leaving room for restarts and anything
/// else on the host.
pub const CAP: usize = 150;

pub struct ConnectPacer {
    spacing: Duration,
    window: Duration,
    cap: usize,
    /// When admitted attempts started, oldest first; held by the waiter whose turn is next, and
    /// by an admitted attempt until its journal write is done.
    started: Arc<Mutex<VecDeque<Instant>>>,
    /// Where start times are kept across restarts, if anywhere.
    journal: Option<PathBuf>,
    /// Writes the journal (replaced in tests to control when writes finish).
    write: fn(&Path, &[u64]),
}
impl Default for ConnectPacer {
    fn default() -> Self {
        Self::new(SPACING, WINDOW, CAP)
    }
}
impl ConnectPacer {
    pub fn new(spacing: Duration, window: Duration, cap: usize) -> Self {
        Self {
            spacing,
            window,
            cap: cap.max(1),
            started: Arc::default(),
            journal: None,
            write: save,
        }
    }
    /// Continues from the attempts that earlier processes recorded in `journal`, and records
    /// this process's attempts there. An unreadable journal starts empty, with a warning.
    pub fn with_journal(self, journal: PathBuf) -> Self {
        let started = load(&journal, self.window, self.cap);
        Self {
            started: Arc::new(Mutex::new(started)),
            journal: Some(journal),
            ..self
        }
    }
    /// Waits until an attempt may start, then counts it as started.
    pub async fn turn(&self) {
        let mut started = self.started.clone().lock_owned().await;
        let mut warned = false;
        loop {
            let now = Instant::now();
            while started.front().is_some_and(|t| *t + self.window <= now) {
                started.pop_front();
            }
            let spaced = started.back().map_or(now, |last| *last + self.spacing);
            let capped = (started.len() >= self.cap)
                .then(|| started[started.len() - self.cap] + self.window);
            let at = capped.map_or(spaced, |free| free.max(spaced));
            if at <= now {
                started.push_back(now);
                let Some(journal) = self.journal.clone() else {
                    return;
                };
                // Finish on a task of its own, holding the queue, so that a caller cancelled
                // here cannot let the next attempt (and its write) overtake this write. The
                // write itself runs on a blocking thread: a slow disk must not stall a worker.
                // The task hands the queue back, so the next attempt waits until this caller
                // has actually resumed.
                let write = self.write;
                let finish = tokio::spawn(async move {
                    let times = wall_clock(&started);
                    let _ = tokio::task::spawn_blocking(move || write(&journal, &times)).await;
                    // Stamped here for a caller cancelled meanwhile (its queue is released when
                    // this task ends); a live caller stamps again when it resumes.
                    if let Some(last) = started.back_mut() {
                        *last = Instant::now();
                    }
                    started
                });
                if let Ok(mut started) = finish.await {
                    // Space the next attempt from when this one can actually start.
                    if let Some(last) = started.back_mut() {
                        *last = Instant::now();
                    }
                }
                return;
            }
            if capped.is_some_and(|free| free > spaced) && !warned {
                warned = true;
                tracing::warn!(
                    wait_ms = (at - now).as_millis() as u64,
                    "websocket connection attempts held back to stay within the upstream limit"
                );
            }
            tokio::time::sleep_until(at).await;
        }
    }
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
/// Start times within the last `window` from a journal of Unix milliseconds. A start time ahead of
/// the wall clock (which may have stepped back) counts as just now.
fn load(path: &Path, window: Duration, cap: usize) -> VecDeque<Instant> {
    let times = match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice::<Vec<u64>>(&bytes).map_err(|e| e.to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
        Err(e) => Err(e.to_string()),
    };
    let times = times.unwrap_or_else(|error| {
        tracing::warn!(%error, path = %path.display(), "ignoring unreadable websocket connection journal");
        vec![]
    });
    let (wall, now) = (unix_ms(SystemTime::now()), Instant::now());
    let mut started: Vec<Instant> = times
        .into_iter()
        .map(|t| Duration::from_millis(wall.saturating_sub(t)))
        .filter(|age| *age < window)
        .filter_map(|age| now.checked_sub(age))
        .collect();
    started.sort();
    let skip = started.len().saturating_sub(cap);
    started.into_iter().skip(skip).collect()
}
/// Unix milliseconds for each start time.
fn wall_clock(started: &VecDeque<Instant>) -> Vec<u64> {
    let (wall, now) = (unix_ms(SystemTime::now()), Instant::now());
    started
        .iter()
        .map(|t| wall.saturating_sub(now.saturating_duration_since(*t).as_millis() as u64))
        .collect()
}
/// Replaces the journal durably: a per-process temporary file, synced, renamed over the journal,
/// then the directory synced (as the snapshot store does).
fn save(path: &Path, times: &[u64]) {
    use std::io::Write;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let written = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(times).map_err(std::io::Error::other)?)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        if let Some(directory) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::File::open(directory)?.sync_all()?;
        }
        Ok(())
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&temporary);
        tracing::warn!(%error, path = %path.display(), "could not record websocket connection attempts; a restart will not count them");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Starts `n` waiters at once; each reports when its attempt was admitted (ms after start).
    fn waiters(pacer: &Arc<ConnectPacer>, n: usize) -> Vec<tokio::task::JoinHandle<u64>> {
        let start = Instant::now();
        (0..n)
            .map(|_| {
                let pacer = pacer.clone();
                tokio::spawn(async move {
                    pacer.turn().await;
                    (Instant::now() - start).as_millis() as u64
                })
            })
            .collect()
    }
    async fn admitted(waiters: Vec<tokio::task::JoinHandle<u64>>) -> Vec<u64> {
        let mut times = vec![];
        for waiter in waiters {
            times.push(waiter.await.unwrap());
        }
        times
    }
    fn pacer(spacing_ms: u64, window_ms: u64, cap: usize) -> Arc<ConnectPacer> {
        Arc::new(ConnectPacer::new(
            Duration::from_millis(spacing_ms),
            Duration::from_millis(window_ms),
            cap,
        ))
    }
    async fn settle() {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn simultaneous_attempts_start_spaced_apart_in_arrival_order() {
        let pacer = pacer(100, 300_000, 150);
        assert_eq!(
            admitted(waiters(&pacer, 5)).await,
            vec![0, 100, 200, 300, 400]
        );
        // A later attempt that finds the spacing already passed goes at once.
        tokio::time::advance(Duration::from_secs(2)).await;
        let started = Instant::now();
        pacer.turn().await;
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn no_window_holds_more_attempts_than_the_cap() {
        let pacer = pacer(10, 1_000, 3);
        assert_eq!(
            admitted(waiters(&pacer, 7)).await,
            vec![0, 10, 20, 1000, 1010, 1020, 2000]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiters_leave_no_debt() {
        let pacer = pacer(10, 1_000, 2);
        let origin = Instant::now();
        assert_eq!(admitted(waiters(&pacer, 2)).await, vec![0, 10]);
        // Many waiters queue behind the full window and are cancelled.
        let cancelled = waiters(&pacer, 200);
        settle().await;
        for waiter in &cancelled {
            waiter.abort();
        }
        for waiter in cancelled {
            assert!(waiter.await.unwrap_err().is_cancelled());
        }
        // The next attempt waits only until the first one leaves the window, as if they had
        // never queued.
        pacer.turn().await;
        assert_eq!(origin.elapsed(), Duration::from_millis(1_000));
    }

    #[tokio::test(start_paused = true)]
    async fn waiters_that_oversleep_still_start_spaced_apart() {
        let pacer = pacer(100, 1_000, 3);
        let queued = waiters(&pacer, 6);
        settle().await;
        // The runtime stalls for five seconds while five waiters are due within 400 ms.
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            admitted(queued).await,
            vec![0, 5000, 5100, 5200, 6000, 6100],
            "counted when admitted, not when first scheduled"
        );
    }

    fn journal(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "kline-connect-journal-{name}-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn a_restarted_process_continues_the_window_from_the_journal() {
        let path = journal("restart");
        let first = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(2), 2)
            .with_journal(path.clone());
        let started = std::time::Instant::now();
        first.turn().await;
        first.turn().await;
        drop(first);
        // A new process with the same journal finds the window full until the first attempt
        // leaves it, two seconds after it started.
        let second = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(2), 2)
            .with_journal(path.clone());
        second.turn().await;
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(1_950) && waited < Duration::from_secs(3),
            "{waited:?}"
        );
        let recorded: Vec<u64> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(recorded.len(), 2, "the first attempt left the window");
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn an_unreadable_or_stale_journal_starts_empty() {
        let path = journal("stale");
        std::fs::write(&path, b"not json").unwrap();
        let pacer = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(300), 1)
            .with_journal(path.clone());
        assert!(pacer.started.lock().await.is_empty());
        let old = unix_ms(SystemTime::now()) - 301_000;
        std::fs::write(&path, serde_json::to_vec(&[old]).unwrap()).unwrap();
        let pacer = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(300), 1)
            .with_journal(path.clone());
        assert!(pacer.started.lock().await.is_empty(), "outside the window");
        // A start time ahead of the wall clock counts as just now.
        let ahead = unix_ms(SystemTime::now()) + 60_000;
        std::fs::write(&path, serde_json::to_vec(&[ahead]).unwrap()).unwrap();
        let pacer = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(300), 1)
            .with_journal(path.clone());
        assert_eq!(pacer.started.lock().await.len(), 1);
        std::fs::remove_file(path).unwrap();
    }

    mod gated {
        //! A journal writer that waits until the test opens the gate, and counts overlapping
        //! writes.
        use super::super::save;
        use std::{
            path::Path,
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
            time::Duration,
        };
        pub static OPEN: AtomicBool = AtomicBool::new(false);
        pub static ENTERED: AtomicUsize = AtomicUsize::new(0);
        static ACTIVE: AtomicUsize = AtomicUsize::new(0);
        pub static MOST_ACTIVE: AtomicUsize = AtomicUsize::new(0);
        /// Opens the gate when dropped, also when the test fails.
        pub struct OpenOnDrop;
        impl Drop for OpenOnDrop {
            fn drop(&mut self) {
                OPEN.store(true, Ordering::SeqCst);
            }
        }
        pub fn write(path: &Path, times: &[u64]) {
            ENTERED.fetch_add(1, Ordering::SeqCst);
            let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            MOST_ACTIVE.fetch_max(active, Ordering::SeqCst);
            // Bounded, so that a failing test cannot leave this thread (and the runtime) hanging.
            for _ in 0..10_000 {
                if OPEN.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            save(path, times);
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn a_cancelled_admission_finishes_its_journal_write_before_the_next_attempt() {
        use std::sync::atomic::Ordering::SeqCst;
        let path = journal("cancel");
        let mut pacer = ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(300), 150)
            .with_journal(path.clone());
        pacer.write = gated::write;
        let pacer = Arc::new(pacer);
        let _open_on_exit = gated::OpenOnDrop;
        let turn = || {
            let pacer = pacer.clone();
            tokio::spawn(async move { pacer.turn().await })
        };
        let a = turn();
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while gated::ENTERED.load(SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < until,
                "A never started its write"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // A is cancelled while its write waits; B must not start (or write) before A's write ends.
        a.abort();
        assert!(a.await.unwrap_err().is_cancelled());
        let b = turn();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!b.is_finished());
        assert_eq!(
            gated::ENTERED.load(SeqCst),
            1,
            "B wrote while A's write was pending"
        );
        gated::OPEN.store(true, SeqCst);
        b.await.unwrap();
        assert_eq!(
            gated::MOST_ACTIVE.load(SeqCst),
            1,
            "journal writes overlapped"
        );
        let recorded: Vec<u64> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(recorded.len(), 2, "A's admitted attempt still counts");
        assert!(recorded[0] <= recorded[1]);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn the_next_attempt_waits_until_an_admitted_caller_resumes() {
        let path = journal("resume");
        let pacer = Arc::new(
            ConnectPacer::new(Duration::from_millis(10), Duration::from_secs(300), 150)
                .with_journal(path.clone()),
        );
        // A is admitted and its journal write completes, but A is not polled again for a while.
        let a = pacer.turn();
        tokio::pin!(a);
        assert!(futures_util::poll!(&mut a).is_pending());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(path.exists(), "A's journal write finished");
        let b = tokio::spawn({
            let pacer = pacer.clone();
            async move {
                pacer.turn().await;
                std::time::Instant::now()
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!b.is_finished(), "B went ahead before A resumed");
        a.await;
        let resumed = std::time::Instant::now();
        let b_started = b.await.unwrap();
        assert!(b_started >= resumed + Duration::from_millis(9));
        std::fs::remove_file(path).unwrap();
    }
}
