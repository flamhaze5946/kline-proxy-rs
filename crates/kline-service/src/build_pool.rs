//! Bounded response workers. One large response at a time leaves a worker for
//! small recent-window queries; an eight-job turn prevents large-job starvation.
use parking_lot::{Condvar, Mutex};
use std::{collections::VecDeque, sync::Arc};
type Job = Box<dyn FnOnce() + Send + 'static>;
#[derive(Default)]
struct Queue {
    small: VecDeque<Job>,
    large: VecDeque<Job>,
    large_running: usize,
    small_turn: usize,
    closed: bool,
}
struct Shared {
    queue: Mutex<Queue>,
    changed: Condvar,
}
pub(crate) struct BuildPool {
    shared: Arc<Shared>,
    slots: Arc<tokio::sync::Semaphore>,
    large_slots: Arc<tokio::sync::Semaphore>,
}
impl BuildPool {
    pub fn new(workers: usize) -> Self {
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue::default()),
            changed: Condvar::new(),
        });
        for id in 0..workers.clamp(1, 64) {
            let s = shared.clone();
            std::thread::Builder::new()
                .name(format!("kline-bulk-{id}"))
                .spawn(move || {
                    loop {
                        let (job, large) = {
                            let mut q = s.queue.lock();
                            loop {
                                if q.closed {
                                    return;
                                }
                                let can_large = q.large_running == 0 && !q.large.is_empty();
                                if can_large && (q.small.is_empty() || q.small_turn >= 8) {
                                    q.large_running += 1;
                                    q.small_turn = 0;
                                    break (q.large.pop_front().unwrap(), true);
                                }
                                if let Some(job) = q.small.pop_front() {
                                    q.small_turn = q.small_turn.saturating_add(1);
                                    break (job, false);
                                }
                                s.changed.wait(&mut q);
                            }
                        };
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                            tracing::error!("bulk response worker recovered from a job panic");
                        }
                        if large {
                            s.queue.lock().large_running -= 1;
                        }
                        s.changed.notify_all();
                    }
                })
                .expect("start bulk response worker");
        }
        Self {
            shared,
            slots: Arc::new(tokio::sync::Semaphore::new(64)),
            large_slots: Arc::new(tokio::sync::Semaphore::new(8)),
        }
    }
    pub async fn run<T: Send + 'static>(
        &self,
        large: bool,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> anyhow::Result<T> {
        // Reserve admission as well as execution capacity for small requests.
        // Otherwise a large-query flood fills all 64 slots before priority helps.
        let large_permit = if large {
            Some(self.large_slots.clone().acquire_owned().await?)
        } else {
            None
        };
        let permit = self.slots.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move || {
            let _permit = permit;
            let _large_permit = large_permit;
            if !tx.is_closed() {
                let _ = tx.send(work());
            }
        });
        {
            let mut q = self.shared.queue.lock();
            anyhow::ensure!(!q.closed, "response workers stopped");
            if large {
                q.large.push_back(job);
            } else {
                q.small.push_back(job);
            }
        }
        self.shared.changed.notify_all();
        Ok(rx.await?)
    }
}
impl Drop for BuildPool {
    fn drop(&mut self) {
        self.shared.queue.lock().closed = true;
        self.shared.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn large_query_flood_cannot_fill_small_query_admission() {
        let pool = Arc::new(BuildPool::new(2));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.run(true, move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
            }
        });
        entered_rx.await.unwrap();
        let mut queued = Vec::new();
        for _ in 0..128 {
            let pool = pool.clone();
            queued.push(tokio::spawn(async move { pool.run(true, || ()).await }));
        }
        tokio::task::yield_now().await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pool.run(false, || 42))
                .await
                .unwrap()
                .unwrap(),
            42
        );
        assert!(queued.iter().all(|job| !job.is_finished()));
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        for job in queued {
            job.await.unwrap().unwrap();
        }
    }
    #[tokio::test]
    async fn large_work_leaves_room_for_small_queries_and_panics_do_not_poison_pool() {
        let pool = Arc::new(BuildPool::new(2));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let large = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.run(true, move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    1
                })
                .await
            }
        });
        entered_rx.await.unwrap();
        let queued_large = tokio::spawn({
            let pool = pool.clone();
            async move { pool.run(true, || 2).await }
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pool.run(false, || 3))
                .await
                .unwrap()
                .unwrap(),
            3
        );
        assert!(!queued_large.is_finished());
        queued_large.abort();
        let _ = queued_large.await;
        release_tx.send(()).unwrap();
        assert_eq!(large.await.unwrap().unwrap(), 1);
        assert!(pool.run(false, || panic!("test job")).await.is_err());
        assert_eq!(pool.run(true, || 4).await.unwrap(), 4);
    }
}
