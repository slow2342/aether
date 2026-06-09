use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio::sync::{Mutex, broadcast, mpsc};

use crate::raft::{WatchEvent, WatchEventType};

/// A filtered watch event subscription for a single watcher.
struct Watcher {
    /// Key prefix or exact key to match.
    key: Vec<u8>,
    /// Range end for prefix matching. Empty = single key.
    range_end: Vec<u8>,
    /// Event type filters. Empty = all types.
    filters: Vec<WatchEventType>,
    /// Whether to include prev_kv in events.
    prev_kv: bool,
    /// Channel to send matched events to the watcher's gRPC handler.
    tx: mpsc::Sender<WatchResponse>,
}

/// Response sent to a watcher's gRPC handler.
#[derive(Debug, Clone)]
pub struct WatchResponse {
    pub watch_id: i64,
    pub created: bool,
    pub events: Vec<WatchEvent>,
    pub canceled: bool,
    pub cancel_reason: String,
}

/// Manages all active watchers and dispatches events from the state machine.
pub struct WatchManager {
    next_id: AtomicI64,
    watchers: Mutex<HashMap<i64, Watcher>>,
    watch_tx: broadcast::Sender<WatchEvent>,
}

impl WatchManager {
    pub fn new(watch_tx: broadcast::Sender<WatchEvent>) -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicI64::new(1),
            watchers: Mutex::new(HashMap::new()),
            watch_tx,
        })
    }

    /// Create a new watch and return (watch_id, receiver).
    pub async fn create(
        self: &Arc<Self>,
        key: Vec<u8>,
        range_end: Vec<u8>,
        filters: Vec<WatchEventType>,
        prev_kv: bool,
    ) -> (i64, mpsc::Receiver<WatchResponse>) {
        let watch_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(256);

        let watcher = Watcher {
            key,
            range_end,
            filters,
            prev_kv,
            tx,
        };

        self.watchers.lock().await.insert(watch_id, watcher);

        // Spawn a task that subscribes to the broadcast channel and forwards
        // matching events to this watcher.
        let manager = Arc::clone(self);
        let mut broadcast_rx = self.watch_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(event) => {
                        let should_send = {
                            let watchers = manager.watchers.lock().await;
                            if let Some(watcher) = watchers.get(&watch_id) {
                                matches_event(watcher, &event)
                            } else {
                                // Watch was canceled.
                                return;
                            }
                        };

                        if should_send {
                            let watchers = manager.watchers.lock().await;
                            if let Some(watcher) = watchers.get(&watch_id) {
                                let kv = if watcher.prev_kv {
                                    event.kv.clone()
                                } else {
                                    crate::raft::KeyValue {
                                        key: event.kv.key.clone(),
                                        value: event.kv.value.clone(),
                                        create_revision: 0,
                                        mod_revision: 0,
                                        version: 0,
                                        lease: 0,
                                    }
                                };
                                let prev_kv = if watcher.prev_kv {
                                    event.prev_kv.clone()
                                } else {
                                    None
                                };
                                let resp = WatchResponse {
                                    watch_id,
                                    created: false,
                                    events: vec![WatchEvent {
                                        event_type: event.event_type,
                                        kv,
                                        prev_kv,
                                    }],
                                    canceled: false,
                                    cancel_reason: String::new(),
                                };
                                if watcher.tx.send(resp).await.is_err() {
                                    // Receiver dropped, clean up.
                                    drop(watchers);
                                    manager.watchers.lock().await.remove(&watch_id);
                                    return;
                                }
                            } else {
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(watch_id, skipped = n, "watch lagged, events were dropped");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return;
                    }
                }
            }
        });

        (watch_id, rx)
    }

    /// Cancel a watch by ID.
    pub async fn cancel(&self, watch_id: i64, reason: String) {
        if let Some(watcher) = self.watchers.lock().await.remove(&watch_id) {
            let _ = watcher
                .tx
                .send(WatchResponse {
                    watch_id,
                    created: false,
                    events: vec![],
                    canceled: true,
                    cancel_reason: reason,
                })
                .await;
        }
    }

    /// Get the number of active watchers.
    pub async fn active_count(&self) -> usize {
        self.watchers.lock().await.len()
    }
}

/// Check if an event matches a watcher's filter criteria.
fn matches_event(watcher: &Watcher, event: &WatchEvent) -> bool {
    // Check event type filter.
    if !watcher.filters.is_empty() && !watcher.filters.contains(&event.event_type) {
        return false;
    }

    // Check key/range match.
    let event_key = &event.kv.key;
    if watcher.range_end.is_empty() {
        // Single key match.
        event_key == &watcher.key
    } else if watcher.range_end == b"\0" {
        // All keys from watcher.key onwards.
        event_key.as_slice() >= watcher.key.as_slice()
    } else {
        // Range match: [key, range_end).
        event_key.as_slice() >= watcher.key.as_slice()
            && event_key.as_slice() < watcher.range_end.as_slice()
    }
}
