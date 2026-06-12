use std::collections::{HashMap, HashSet};

use crate::storage::{RocksStorage, StorageEngine};

/// Key prefix for lock keys in storage.
/// Uses `_aether_` prefix to avoid collision with user keys.
pub const LOCK_KEY_PREFIX: &[u8] = b"_aether_lock/";

/// Reserved prefix for system use.
pub const RESERVED_PREFIX: &[u8] = b"_aether_";

/// Maximum allowed lock name length (1 KB).
pub const MAX_LOCK_NAME_LEN: usize = 1024;

/// Lease ID size in bytes (i64 = 8 bytes).
const LEASE_ID_SIZE: usize = 8;

/// In-memory lock manager. Owned by the state machine.
///
/// Tracks which locks are currently held. The actual lock data is stored
/// in the KV store with the `_aether_lock/` prefix.
///
/// Value format in KV store: `[lease_id: i64 BE][name_bytes]`
pub struct LockManager {
    /// Active locks: lock_name -> lock_key
    locks: HashMap<Box<[u8]>, Box<[u8]>>,
    /// Reverse index: lock_key -> lock_name (for O(1) release by key)
    key_to_name: HashMap<Box<[u8]>, Box<[u8]>>,
    /// Lease association: lock_key -> lease_id
    key_lease: HashMap<Box<[u8]>, i64>,
    /// Reverse index: lease_id -> set of lock_keys (for O(1) release by lease)
    lease_keys: HashMap<i64, HashSet<Box<[u8]>>>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            key_to_name: HashMap::new(),
            key_lease: HashMap::new(),
            lease_keys: HashMap::new(),
        }
    }

    /// Restore lock state from persistent storage.
    /// Scans the KV store for keys with the lock prefix and rebuilds in-memory state.
    pub fn restore(&mut self, storage: &RocksStorage) -> Result<(), crate::error::StorageError> {
        let entries = storage.scan(LOCK_KEY_PREFIX, usize::MAX)?;

        self.locks.clear();
        self.key_to_name.clear();
        self.key_lease.clear();
        self.lease_keys.clear();

        for entry in entries {
            let key: Box<[u8]> = entry.key.into();
            let value = entry.value;

            // Decode value: [lease_id: i64 BE][name_bytes]
            if value.len() < LEASE_ID_SIZE {
                tracing::warn!(
                    key = %String::from_utf8_lossy(&key),
                    "invalid lock value, skipping"
                );
                continue;
            }

            // Safe conversion: we already checked the length
            let lease_id_bytes: [u8; 8] = match value[..LEASE_ID_SIZE].try_into() {
                Ok(bytes) => bytes,
                Err(_) => {
                    tracing::warn!(
                        key = %String::from_utf8_lossy(&key),
                        "failed to convert lease_id bytes, skipping"
                    );
                    continue;
                }
            };
            let lease_id = i64::from_be_bytes(lease_id_bytes);
            let name: Box<[u8]> = value[LEASE_ID_SIZE..].into();

            self.locks.insert(name.clone(), key.clone());
            self.key_to_name.insert(key.clone(), name);
            if lease_id > 0 {
                self.key_lease.insert(key.clone(), lease_id);
                self.lease_keys.entry(lease_id).or_default().insert(key);
            }
        }

        Ok(())
    }

    /// Encode lock value with lease_id.
    pub fn encode_value(name: &[u8], lease_id: i64) -> Vec<u8> {
        let mut value = Vec::with_capacity(LEASE_ID_SIZE + name.len());
        value.extend_from_slice(&lease_id.to_be_bytes());
        value.extend_from_slice(name);
        value
    }

    /// Acquire a lock. Returns the lock key.
    /// Always updates all mappings to ensure consistency.
    pub fn acquire(&mut self, name: Vec<u8>, key: Vec<u8>, lease_id: i64) -> Vec<u8> {
        let name_box: Box<[u8]> = name.into();
        let key_box: Box<[u8]> = key.into();

        // Update lease mapping
        if lease_id > 0 {
            self.key_lease.insert(key_box.clone(), lease_id);
            self.lease_keys
                .entry(lease_id)
                .or_default()
                .insert(key_box.clone());
        } else {
            if let Some(old_lease_id) = self.key_lease.remove(&key_box)
                && let Some(keys) = self.lease_keys.get_mut(&old_lease_id)
            {
                keys.remove(&key_box);
                if keys.is_empty() {
                    self.lease_keys.remove(&old_lease_id);
                }
            }
        }
        // Insert into reverse index
        self.key_to_name.insert(key_box.clone(), name_box.clone());
        // Insert into forward index
        self.locks.insert(name_box, key_box.clone());

        key_box.into_vec()
    }

    /// Release a lock by key. Returns true if the lock was found and released.
    pub fn release(&mut self, key: &[u8]) -> bool {
        // Use reverse index for O(1) lookup
        if let Some(name) = self.key_to_name.remove(key) {
            self.locks.remove(&name);
            if let Some(lease_id) = self.key_lease.remove(key)
                && let Some(keys) = self.lease_keys.get_mut(&lease_id)
            {
                keys.remove(key);
                if keys.is_empty() {
                    self.lease_keys.remove(&lease_id);
                }
            }
            true
        } else {
            false
        }
    }

    /// Release all locks associated with a lease.
    /// Returns the list of lock keys that were released.
    pub fn release_by_lease(&mut self, lease_id: i64) -> Vec<Vec<u8>> {
        // Use reverse index for O(1) lookup
        let keys: Vec<Box<[u8]>> = match self.lease_keys.remove(&lease_id) {
            Some(keys) => keys.into_iter().collect(),
            None => return Vec::new(),
        };

        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(name) = self.key_to_name.remove(&key) {
                self.locks.remove(&name);
            }
            self.key_lease.remove(&key);
            result.push(key.into_vec());
        }

        result
    }

    /// Check if a lock is held.
    pub fn is_locked(&self, name: &[u8]) -> bool {
        self.locks.contains_key(name)
    }

    /// Get the lock key for a given lock name.
    pub fn get_key(&self, name: &[u8]) -> Option<&[u8]> {
        self.locks.get(name).map(|k| k.as_ref())
    }

    /// Get the lease_id for a given lock key.
    pub fn get_lease_id(&self, key: &[u8]) -> Option<i64> {
        self.key_lease.get(key).copied()
    }

    /// Get the number of active locks.
    pub fn lock_count(&self) -> usize {
        self.locks.len()
    }

    /// Get all lock keys associated with a lease (without removing them).
    pub fn get_keys_by_lease(&self, lease_id: i64) -> Vec<Vec<u8>> {
        // Use reverse index for O(1) lookup
        match self.lease_keys.get(&lease_id) {
            Some(keys) => keys.iter().map(|k| k.to_vec()).collect(),
            None => Vec::new(),
        }
    }
}

/// Generate a lock key for a given lock name.
pub fn lock_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(LOCK_KEY_PREFIX.len() + name.len());
    key.extend_from_slice(LOCK_KEY_PREFIX);
    key.extend_from_slice(name);
    key
}

/// Extract the lock name from a lock key.
pub fn lock_name(key: &[u8]) -> Option<&[u8]> {
    if key.starts_with(LOCK_KEY_PREFIX) {
        Some(&key[LOCK_KEY_PREFIX.len()..])
    } else {
        None
    }
}

/// Validate lock name - must not be empty, must not contain null bytes,
/// and must not start with the reserved prefix `_aether_`.
pub fn validate_lock_name(name: &[u8]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("lock name must not be empty");
    }
    if name.len() > MAX_LOCK_NAME_LEN {
        return Err("lock name too long");
    }
    if name.contains(&0) {
        return Err("lock name must not contain null bytes");
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err("lock name must not start with reserved prefix '_aether_'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_key_generation() {
        let name = b"my-lock";
        let key = lock_key(name);
        assert_eq!(key, b"_aether_lock/my-lock");
    }

    #[test]
    fn test_lock_name_extraction() {
        let key = b"_aether_lock/my-lock";
        let name = lock_name(key);
        assert_eq!(name, Some(b"my-lock".as_slice()));
    }

    #[test]
    fn test_lock_name_extraction_invalid() {
        let key = b"not-a-lock";
        let name = lock_name(key);
        assert_eq!(name, None);
    }

    #[test]
    fn test_encode_decode_value() {
        let name = b"test-lock";
        let lease_id = 42i64;
        let encoded = LockManager::encode_value(name, lease_id);

        assert_eq!(encoded.len(), LEASE_ID_SIZE + name.len());
        assert_eq!(&encoded[..LEASE_ID_SIZE], &lease_id.to_be_bytes());
        assert_eq!(&encoded[LEASE_ID_SIZE..], name);
    }

    #[test]
    fn test_lock_manager_acquire_release() {
        let mut mgr = LockManager::new();
        let name = b"test-lock".to_vec();
        let key = lock_key(&name);

        assert!(!mgr.is_locked(&name));
        assert_eq!(mgr.lock_count(), 0);

        mgr.acquire(name.clone(), key.clone(), 0);
        assert!(mgr.is_locked(&name));
        assert_eq!(mgr.get_key(&name), Some(key.as_slice()));
        assert_eq!(mgr.lock_count(), 1);

        assert!(mgr.release(&key));
        assert!(!mgr.is_locked(&name));
        assert_eq!(mgr.lock_count(), 0);
    }

    #[test]
    fn test_lock_manager_release_nonexistent() {
        let mut mgr = LockManager::new();
        assert!(!mgr.release(b"nonexistent"));
    }

    #[test]
    fn test_lock_manager_overwrite() {
        let mut mgr = LockManager::new();
        let name = b"test-lock".to_vec();
        let key1 = lock_key(&name);
        let key2 = b"_aether_lock/test-lock-v2".to_vec();

        mgr.acquire(name.clone(), key1.clone(), 0);
        assert_eq!(mgr.get_key(&name), Some(key1.as_slice()));

        // Overwrite with new key
        mgr.acquire(name.clone(), key2.clone(), 0);
        assert_eq!(mgr.get_key(&name), Some(key2.as_slice()));
        assert_eq!(mgr.lock_count(), 1);
    }

    #[test]
    fn test_lock_manager_lease_association() {
        let mut mgr = LockManager::new();
        let name = b"test-lock".to_vec();
        let key = lock_key(&name);

        mgr.acquire(name.clone(), key.clone(), 42);
        assert_eq!(mgr.get_lease_id(&key), Some(42));
    }

    #[test]
    fn test_lock_manager_lease_clear_on_zero() {
        let mut mgr = LockManager::new();
        let name = b"test-lock".to_vec();
        let key = lock_key(&name);

        // First acquire with lease_id=42
        mgr.acquire(name.clone(), key.clone(), 42);
        assert_eq!(mgr.get_lease_id(&key), Some(42));

        // Second acquire with lease_id=0 should clear lease
        mgr.acquire(name.clone(), key.clone(), 0);
        assert_eq!(mgr.get_lease_id(&key), None);
    }

    #[test]
    fn test_lock_manager_release_by_lease() {
        let mut mgr = LockManager::new();
        let name1 = b"lock-1".to_vec();
        let name2 = b"lock-2".to_vec();
        let key1 = lock_key(&name1);
        let key2 = lock_key(&name2);

        mgr.acquire(name1.clone(), key1.clone(), 42);
        mgr.acquire(name2.clone(), key2.clone(), 42);

        assert_eq!(mgr.lock_count(), 2);

        let released = mgr.release_by_lease(42);
        assert_eq!(released.len(), 2);
        assert_eq!(mgr.lock_count(), 0);
        assert!(!mgr.is_locked(&name1));
        assert!(!mgr.is_locked(&name2));
    }

    #[test]
    fn test_lock_manager_release_by_lease_partial() {
        let mut mgr = LockManager::new();
        let name1 = b"lock-1".to_vec();
        let name2 = b"lock-2".to_vec();
        let key1 = lock_key(&name1);
        let key2 = lock_key(&name2);

        mgr.acquire(name1.clone(), key1.clone(), 42);
        mgr.acquire(name2.clone(), key2.clone(), 43);

        let released = mgr.release_by_lease(42);
        assert_eq!(released.len(), 1);
        assert_eq!(mgr.lock_count(), 1);
        assert!(!mgr.is_locked(&name1));
        assert!(mgr.is_locked(&name2));
    }

    #[test]
    fn test_validate_lock_name() {
        assert!(validate_lock_name(b"valid-name").is_ok());
        assert!(validate_lock_name(b"").is_err());
        assert!(validate_lock_name(b"null\x00byte").is_err());
        assert!(validate_lock_name(b"_aether_reserved").is_err());
    }

    #[test]
    fn test_lock_manager_get_keys_by_lease() {
        let mut mgr = LockManager::new();
        let name1 = b"lock-1".to_vec();
        let name2 = b"lock-2".to_vec();
        let key1 = lock_key(&name1);
        let key2 = lock_key(&name2);

        mgr.acquire(name1.clone(), key1.clone(), 42);
        mgr.acquire(name2.clone(), key2.clone(), 42);

        let keys = mgr.get_keys_by_lease(42);
        assert_eq!(keys.len(), 2);
    }
}
