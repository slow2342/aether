use std::collections::HashMap;

use crate::storage::{RocksStorage, StorageEngine};

/// Key prefix for queue item keys in storage.
/// Items are stored as `_aether_queue/<name>/<seq: u64 BE>`.
pub const QUEUE_KEY_PREFIX: &[u8] = b"_aether_queue/";

/// Key prefix for queue metadata (next sequence number).
pub const QUEUE_META_PREFIX: &[u8] = b"_aether_queue_meta/";

/// Reserved prefix for system use.
pub const RESERVED_PREFIX: &[u8] = b"_aether_";

/// Maximum allowed queue name length (1 KB).
pub const MAX_QUEUE_NAME_LEN: usize = 1024;

/// In-memory queue manager. Owned by the state machine.
///
/// Tracks queue metadata (next sequence number per queue).
/// Actual queue items are stored in the KV store with sequential keys.
pub struct QueueManager {
    /// Next sequence number per queue name.
    next_seq: HashMap<Box<[u8]>, u64>,
}

impl Default for QueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            next_seq: HashMap::new(),
        }
    }

    /// Restore queue state from persistent storage.
    /// Scans the queue meta prefix to rebuild sequence counters.
    pub fn restore(&mut self, storage: &RocksStorage) -> Result<(), crate::error::StorageError> {
        let entries = storage.scan(QUEUE_META_PREFIX, usize::MAX)?;

        self.next_seq.clear();

        for entry in entries {
            let meta_key = &entry.key;
            let value = entry.value;

            // Extract queue name from meta key
            let name = match queue_meta_name(meta_key) {
                Some(n) => n,
                None => {
                    tracing::warn!(
                        key = %String::from_utf8_lossy(meta_key),
                        "invalid queue meta key, skipping"
                    );
                    continue;
                }
            };

            if value.len() < 8 {
                tracing::warn!(
                    key = %String::from_utf8_lossy(meta_key),
                    "invalid queue meta value, skipping"
                );
                continue;
            }

            let seq_bytes: [u8; 8] = match value[..8].try_into() {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let seq = u64::from_be_bytes(seq_bytes);

            self.next_seq.insert(name.into(), seq);
        }

        Ok(())
    }

    /// Get and increment the next sequence number for a queue.
    /// Returns the current sequence number.
    pub fn next_seq(&mut self, name: &[u8]) -> u64 {
        let seq = self.next_seq.get(name).copied().unwrap_or(1);
        self.next_seq.insert(name.into(), seq + 1);
        seq
    }

    /// Get the current sequence number without incrementing.
    pub fn peek_seq(&self, name: &[u8]) -> u64 {
        self.next_seq.get(name).copied().unwrap_or(1)
    }

    /// Get the number of tracked queues.
    pub fn queue_count(&self) -> usize {
        self.next_seq.len()
    }
}

/// Generate a queue item key: `_aether_queue/<name>/<seq: u64 BE>`.
pub fn queue_item_key(name: &[u8], seq: u64) -> Vec<u8> {
    let seq_bytes = seq.to_be_bytes();
    let mut key = Vec::with_capacity(QUEUE_KEY_PREFIX.len() + name.len() + 1 + 8);
    key.extend_from_slice(QUEUE_KEY_PREFIX);
    key.extend_from_slice(name);
    key.push(b'/');
    key.extend_from_slice(&seq_bytes);
    key
}

/// Generate the prefix for scanning all items in a queue: `_aether_queue/<name>/`.
pub fn queue_scan_prefix(name: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(QUEUE_KEY_PREFIX.len() + name.len() + 1);
    prefix.extend_from_slice(QUEUE_KEY_PREFIX);
    prefix.extend_from_slice(name);
    prefix.push(b'/');
    prefix
}

/// Generate the meta key for a queue: `_aether_queue_meta/<name>`.
pub fn queue_meta_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(QUEUE_META_PREFIX.len() + name.len());
    key.extend_from_slice(QUEUE_META_PREFIX);
    key.extend_from_slice(name);
    key
}

/// Extract the queue name from a meta key.
fn queue_meta_name(key: &[u8]) -> Option<&[u8]> {
    if key.starts_with(QUEUE_META_PREFIX) {
        Some(&key[QUEUE_META_PREFIX.len()..])
    } else {
        None
    }
}

/// Validate queue name.
pub fn validate_queue_name(name: &[u8]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("queue name must not be empty");
    }
    if name.len() > MAX_QUEUE_NAME_LEN {
        return Err("queue name too long");
    }
    if name.contains(&0) {
        return Err("queue name must not contain null bytes");
    }
    if name.contains(&b'/') {
        return Err("queue name must not contain '/'");
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err("queue name must not start with reserved prefix '_aether_'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_item_key_generation() {
        let name = b"my-queue";
        let key = queue_item_key(name, 1);
        let expected_seq = 1u64.to_be_bytes();
        assert!(key.starts_with(b"_aether_queue/my-queue/"));
        assert_eq!(&key[key.len() - 8..], expected_seq);
    }

    #[test]
    fn test_queue_scan_prefix() {
        let prefix = queue_scan_prefix(b"my-queue");
        assert_eq!(prefix, b"_aether_queue/my-queue/");
    }

    #[test]
    fn test_queue_meta_key() {
        let key = queue_meta_key(b"my-queue");
        assert_eq!(key, b"_aether_queue_meta/my-queue");
    }

    #[test]
    fn test_queue_meta_name_extraction() {
        let key = b"_aether_queue_meta/my-queue";
        let name = queue_meta_name(key);
        assert_eq!(name, Some(b"my-queue".as_slice()));
    }

    #[test]
    fn test_queue_manager_next_seq() {
        let mut mgr = QueueManager::new();
        let name = b"test-queue";

        assert_eq!(mgr.next_seq(name), 1);
        assert_eq!(mgr.next_seq(name), 2);
        assert_eq!(mgr.next_seq(name), 3);
    }

    #[test]
    fn test_queue_manager_peek_seq() {
        let mut mgr = QueueManager::new();
        let name = b"test-queue";

        assert_eq!(mgr.peek_seq(name), 1);
        mgr.next_seq(name);
        assert_eq!(mgr.peek_seq(name), 2);
    }

    #[test]
    fn test_queue_manager_multiple_queues() {
        let mut mgr = QueueManager::new();

        assert_eq!(mgr.next_seq(b"queue-a"), 1);
        assert_eq!(mgr.next_seq(b"queue-b"), 1);
        assert_eq!(mgr.next_seq(b"queue-a"), 2);
        assert_eq!(mgr.next_seq(b"queue-b"), 2);
    }

    #[test]
    fn test_validate_queue_name() {
        assert!(validate_queue_name(b"valid-name").is_ok());
        assert!(validate_queue_name(b"").is_err());
        assert!(validate_queue_name(b"null\x00byte").is_err());
        assert!(validate_queue_name(b"_aether_reserved").is_err());
        assert!(validate_queue_name(b"has/slash").is_err());
    }

    #[test]
    fn test_queue_item_key_ordering() {
        let name = b"test";
        let key1 = queue_item_key(name, 1);
        let key2 = queue_item_key(name, 2);
        let key10 = queue_item_key(name, 10);

        // Big-endian encoding ensures lexicographic order matches logical order
        assert!(key1 < key2);
        assert!(key2 < key10);
    }
}
