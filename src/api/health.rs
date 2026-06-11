use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks the health status of various subsystems.
#[derive(Clone)]
pub struct HealthStatus {
    inner: Arc<HealthStatusInner>,
}

struct HealthStatusInner {
    raft_ready: AtomicBool,
    storage_ready: AtomicBool,
}

impl HealthStatus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HealthStatusInner {
                raft_ready: AtomicBool::new(false),
                storage_ready: AtomicBool::new(false),
            }),
        }
    }

    pub fn set_raft_ready(&self, ready: bool) {
        self.inner.raft_ready.store(ready, Ordering::Release);
    }

    pub fn set_storage_ready(&self, ready: bool) {
        self.inner.storage_ready.store(ready, Ordering::Release);
    }

    pub fn is_raft_ready(&self) -> bool {
        self.inner.raft_ready.load(Ordering::Acquire)
    }

    pub fn is_storage_ready(&self) -> bool {
        self.inner.storage_ready.load(Ordering::Acquire)
    }

    /// Returns true if the node is ready to serve traffic.
    pub fn is_ready(&self) -> bool {
        self.is_raft_ready() && self.is_storage_ready()
    }
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::new()
    }
}
