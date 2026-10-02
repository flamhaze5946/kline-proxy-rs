//! Bounded auxiliary CPU workers, separate from Tokio's bulk encoding/disk pool.
use std::sync::{
    Arc, Mutex,
    mpsc::{self, SyncSender},
};
type Job = Box<dyn FnOnce() + Send + 'static>;
pub struct CpuPool {
    sender: SyncSender<Job>,
    capacity: Arc<tokio::sync::Semaphore>,
    large_capacity: Arc<tokio::sync::Semaphore>,
}
impl CpuPool {
    pub fn new(workers: usize) -> Arc<Self> {
        Self::named(workers, "market-cpu")
    }
    pub fn named(workers: usize, name: &'static str) -> Arc<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Job>(32);
        let receiver = Arc::new(Mutex::new(receiver));
        for id in 0..workers.clamp(1, 4) {
            let receiver = receiver.clone();
            std::thread::Builder::new()
                .name(format!("{name}-{id}"))
                .spawn(move || {
                    loop {
                        let job = receiver.lock().unwrap().recv();
                        let Ok(job) = job else { break };
                        // A failed calculation must not remove a worker from the pool.
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                            tracing::error!("auxiliary CPU job panicked");
                        }
                    }
                })
                .expect("start auxiliary CPU worker");
        }
        Arc::new(Self {
            sender,
            capacity: Arc::new(tokio::sync::Semaphore::new(32)),
            large_capacity: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }
    /// Large decodes must leave a worker and queue capacity for price publication.
    pub async fn run_large<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> anyhow::Result<T> {
        let permit = self.large_capacity.clone().acquire_owned().await?;
        self.run(move || {
            let _permit = permit;
            work()
        })
        .await
    }
    pub async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> anyhow::Result<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let permit = self.capacity.clone().acquire_owned().await?;
        self.sender
            .try_send(Box::new(move || {
                let _permit = permit;
                if !sender.is_closed() {
                    let result = work();
                    let _ = sender.send(result);
                }
            }))
            .map_err(|_| anyhow::anyhow!("auxiliary CPU capacity exceeded"))?;
        Ok(receiver.await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn large_decode_flood_leaves_price_worker_and_queue_capacity() {
        let pool = CpuPool::new(2);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.run_large(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
            }
        });
        entered_rx.await.unwrap();
        let mut queued = Vec::new();
        for _ in 0..64 {
            let pool = pool.clone();
            queued.push(tokio::spawn(async move { pool.run_large(|| ()).await }));
        }
        tokio::task::yield_now().await;
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), pool.run(|| 42))
                .await
                .unwrap()
                .unwrap(),
            42
        );
        assert!(queued.iter().all(|job| !job.is_finished()));
        // Cancelling the waiter must not release the running job's reservation.
        first.abort();
        let _ = first.await;
        assert_eq!(pool.large_capacity.available_permits(), 0);
        release_tx.send(()).unwrap();
        for job in queued {
            job.await.unwrap().unwrap();
        }
    }
}
