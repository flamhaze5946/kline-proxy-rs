use std::time::{SystemTime, UNIX_EPOCH};
pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> i64;
}
pub struct SystemClock {
    pub offset_ms: i64,
}
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            + self.offset_ms
    }
}
