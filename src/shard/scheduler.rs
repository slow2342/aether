use std::sync::{Arc, Mutex};

use crate::raft::{RaftHandle, RaftRequest};
use crate::shard::manager::ShardManager;

/// Signals the scheduler that a region may need splitting.
pub struct SplitSignal {
    pub region_id: u64,
    pub approximate_bytes: u64,
}

/// Background task that monitors region sizes and triggers splits.
pub struct SplitScheduler {
    max_region_size_bytes: u64,
    shard_manager: Arc<Mutex<ShardManager>>,
    raft_handle: Arc<dyn RaftHandle>,
    signal_rx: tokio::sync::mpsc::Receiver<SplitSignal>,
}

impl SplitScheduler {
    pub fn new(
        max_region_size_bytes: u64,
        shard_manager: Arc<Mutex<ShardManager>>,
        raft_handle: Arc<dyn RaftHandle>,
        signal_rx: tokio::sync::mpsc::Receiver<SplitSignal>,
    ) -> Self {
        Self {
            max_region_size_bytes,
            shard_manager,
            raft_handle,
            signal_rx,
        }
    }

    /// Run the scheduler loop. Spawn as a tokio task.
    pub async fn run(mut self) {
        while let Some(signal) = self.signal_rx.recv().await {
            if signal.approximate_bytes < self.max_region_size_bytes {
                continue;
            }

            let split_key = {
                let mgr = match self.shard_manager.lock() {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, "shard manager mutex poisoned");
                        continue;
                    }
                };
                match mgr.get_region(signal.region_id) {
                    Some(region) => {
                        // Pick the midpoint of the key range as split key.
                        // For the MVP, use a simple midpoint of start/end keys.
                        midpoint_key(&region.start_key, &region.end_key)
                    }
                    None => continue,
                }
            };

            let Some(split_key) = split_key else {
                continue;
            };

            tracing::info!(
                region_id = signal.region_id,
                bytes = signal.approximate_bytes,
                split_key = %String::from_utf8_lossy(&split_key),
                "region exceeds size threshold, proposing split"
            );

            if let Err(e) = self
                .raft_handle
                .propose(RaftRequest::RegionSplit {
                    region_id: signal.region_id,
                    split_key,
                })
                .await
            {
                tracing::error!(
                    region_id = signal.region_id,
                    error = %e,
                    "failed to propose region split"
                );
            }
        }
    }
}

/// Compute a midpoint key between `start` and `end` (big-endian byte arrays).
///
/// Returns None if no meaningful midpoint exists (both empty, or equal).
/// Empty `end` is treated as positive infinity — appends a zero byte to `start`.
fn midpoint_key(start: &[u8], end: &[u8]) -> Option<Vec<u8>> {
    if start.is_empty() && end.is_empty() {
        return None;
    }

    // Empty end_key = positive infinity. Split by extending start.
    if end.is_empty() {
        let mut key = start.to_vec();
        key.push(0x00);
        return Some(key);
    }

    // Empty start_key = negative infinity. Midpoint is end / 2.
    if start.is_empty() {
        let result = halve_big_endian(end);
        return if result.is_empty() || result == end {
            let mut key = end.to_vec();
            key.push(0x80);
            Some(key)
        } else {
            Some(result)
        };
    }

    // Both non-empty: two-pass (start + end) / 2.
    let max_len = start.len().max(end.len());
    let start_padded = pad_key(start, max_len);
    let end_padded = pad_key(end, max_len);

    // Pass 1: compute sum = start + end (right to left, carry 0 or 1).
    let mut sum = Vec::with_capacity(max_len + 1);
    let mut carry: u16 = 0;
    for i in (0..max_len).rev() {
        let s = start_padded[i] as u16 + end_padded[i] as u16 + carry;
        sum.push((s & 0xFF) as u8);
        carry = s >> 8;
    }
    if carry > 0 {
        sum.push(carry as u8);
    }
    sum.reverse(); // big-endian

    // Pass 2: divide sum by 2 (left to right).
    let result = halve_big_endian(&sum);

    if result.is_empty() || result == start {
        let mut key = start.to_vec();
        key.push(0x80);
        Some(key)
    } else {
        Some(result)
    }
}

/// Divide a big-endian byte array by 2.
fn halve_big_endian(bytes: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(bytes.len());
    let mut remainder: u16 = 0;
    for &byte in bytes {
        let val = remainder * 256 + byte as u16;
        result.push((val / 2) as u8);
        remainder = val % 2;
    }
    // Trim leading zeros but keep at least one byte.
    while result.len() > 1 && result[0] == 0 {
        result.remove(0);
    }
    result
}

fn pad_key(key: &[u8], len: usize) -> Vec<u8> {
    let mut padded = vec![0u8; len];
    padded[len - key.len()..].copy_from_slice(key);
    padded
}
