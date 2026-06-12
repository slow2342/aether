use std::collections::{HashMap, HashSet};

use crate::storage::{RocksStorage, StorageEngine};

/// Key prefix for barrier keys in storage.
pub const BARRIER_KEY_PREFIX: &[u8] = b"_aether_barrier/";

/// Reserved prefix for system use.
pub const RESERVED_PREFIX: &[u8] = b"_aether_";

/// Maximum allowed barrier name length (1 KB).
pub const MAX_BARRIER_NAME_LEN: usize = 1024;

/// Lease ID size in bytes (i64 = 8 bytes).
const LEASE_ID_SIZE: usize = 8;

/// In-memory barrier manager. Owned by the state machine.
///
/// Tracks which barriers are currently held. The actual barrier data is stored
/// in the KV store with the `_aether_barrier/` prefix.
///
/// Value format in KV store: `[lease_id: i64 BE][name_bytes]`
pub struct BarrierManager {
    /// Active barriers: barrier_name -> barrier_key
    barriers: HashMap<Box<[u8]>, Box<[u8]>>,
    /// Reverse index: barrier_key -> barrier_name
    key_to_name: HashMap<Box<[u8]>, Box<[u8]>>,
    /// Lease association: barrier_key -> lease_id
    key_lease: HashMap<Box<[u8]>, i64>,
    /// Reverse index: lease_id -> set of barrier_keys
    lease_keys: HashMap<i64, HashSet<Box<[u8]>>>,
}

impl Default for BarrierManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BarrierManager {
    pub fn new() -> Self {
        Self {
            barriers: HashMap::new(),
            key_to_name: HashMap::new(),
            key_lease: HashMap::new(),
            lease_keys: HashMap::new(),
        }
    }

    /// Restore barrier state from persistent storage.
    pub fn restore(&mut self, storage: &RocksStorage) -> Result<(), crate::error::StorageError> {
        let entries = storage.scan(BARRIER_KEY_PREFIX, usize::MAX)?;

        self.barriers.clear();
        self.key_to_name.clear();
        self.key_lease.clear();
        self.lease_keys.clear();

        for entry in entries {
            let key: Box<[u8]> = entry.key.into();
            let value = entry.value;

            if value.len() < LEASE_ID_SIZE {
                tracing::warn!(
                    key = %String::from_utf8_lossy(&key),
                    "invalid barrier value, skipping"
                );
                continue;
            }

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

            self.barriers.insert(name.clone(), key.clone());
            self.key_to_name.insert(key.clone(), name);
            if lease_id > 0 {
                self.key_lease.insert(key.clone(), lease_id);
                self.lease_keys.entry(lease_id).or_default().insert(key);
            }
        }

        Ok(())
    }

    /// Encode barrier value with lease_id.
    pub fn encode_value(name: &[u8], lease_id: i64) -> Vec<u8> {
        let mut value = Vec::with_capacity(LEASE_ID_SIZE + name.len());
        value.extend_from_slice(&lease_id.to_be_bytes());
        value.extend_from_slice(name);
        value
    }

    /// Create a barrier. Returns the barrier key.
    pub fn create(&mut self, name: Vec<u8>, key: Vec<u8>, lease_id: i64) -> Vec<u8> {
        let name_box: Box<[u8]> = name.into();
        let key_box: Box<[u8]> = key.into();

        if lease_id > 0 {
            self.key_lease.insert(key_box.clone(), lease_id);
            self.lease_keys
                .entry(lease_id)
                .or_default()
                .insert(key_box.clone());
        } else if let Some(old_lease_id) = self.key_lease.remove(&key_box)
            && let Some(keys) = self.lease_keys.get_mut(&old_lease_id)
        {
            keys.remove(&key_box);
            if keys.is_empty() {
                self.lease_keys.remove(&old_lease_id);
            }
        }

        self.key_to_name.insert(key_box.clone(), name_box.clone());
        self.barriers.insert(name_box, key_box.clone());

        key_box.into_vec()
    }

    /// Release a barrier by key. Returns true if found and released.
    pub fn release(&mut self, key: &[u8]) -> bool {
        if let Some(name) = self.key_to_name.remove(key) {
            self.barriers.remove(&name);
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

    /// Release all barriers associated with a lease.
    pub fn release_by_lease(&mut self, lease_id: i64) -> Vec<Vec<u8>> {
        let keys: Vec<Box<[u8]>> = match self.lease_keys.remove(&lease_id) {
            Some(keys) => keys.into_iter().collect(),
            None => return Vec::new(),
        };

        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(name) = self.key_to_name.remove(&key) {
                self.barriers.remove(&name);
            }
            self.key_lease.remove(&key);
            result.push(key.into_vec());
        }

        result
    }

    /// Check if a barrier is held.
    pub fn is_held(&self, name: &[u8]) -> bool {
        self.barriers.contains_key(name)
    }

    /// Get the barrier key for a given name.
    pub fn get_key(&self, name: &[u8]) -> Option<&[u8]> {
        self.barriers.get(name).map(|k| k.as_ref())
    }

    /// Get the lease_id for a given barrier key.
    pub fn get_lease_id(&self, key: &[u8]) -> Option<i64> {
        self.key_lease.get(key).copied()
    }

    /// Get the number of active barriers.
    pub fn barrier_count(&self) -> usize {
        self.barriers.len()
    }

    /// Get all barrier keys associated with a lease.
    pub fn get_keys_by_lease(&self, lease_id: i64) -> Vec<Vec<u8>> {
        match self.lease_keys.get(&lease_id) {
            Some(keys) => keys.iter().map(|k| k.to_vec()).collect(),
            None => Vec::new(),
        }
    }
}

/// Generate a barrier key for a given name.
pub fn barrier_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(BARRIER_KEY_PREFIX.len() + name.len());
    key.extend_from_slice(BARRIER_KEY_PREFIX);
    key.extend_from_slice(name);
    key
}

/// Extract the barrier name from a barrier key.
pub fn barrier_name(key: &[u8]) -> Option<&[u8]> {
    if key.starts_with(BARRIER_KEY_PREFIX) {
        Some(&key[BARRIER_KEY_PREFIX.len()..])
    } else {
        None
    }
}

/// Validate barrier name.
pub fn validate_barrier_name(name: &[u8]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("barrier name must not be empty");
    }
    if name.len() > MAX_BARRIER_NAME_LEN {
        return Err("barrier name too long");
    }
    if name.contains(&0) {
        return Err("barrier name must not contain null bytes");
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err("barrier name must not start with reserved prefix '_aether_'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_key_generation() {
        let name = b"my-barrier";
        let key = barrier_key(name);
        assert_eq!(key, b"_aether_barrier/my-barrier");
    }

    #[test]
    fn test_barrier_name_extraction() {
        let key = b"_aether_barrier/my-barrier";
        let name = barrier_name(key);
        assert_eq!(name, Some(b"my-barrier".as_slice()));
    }

    #[test]
    fn test_barrier_name_extraction_invalid() {
        let key = b"not-a-barrier";
        let name = barrier_name(key);
        assert_eq!(name, None);
    }

    #[test]
    fn test_encode_decode_value() {
        let name = b"test-barrier";
        let lease_id = 42i64;
        let encoded = BarrierManager::encode_value(name, lease_id);

        assert_eq!(encoded.len(), LEASE_ID_SIZE + name.len());
        assert_eq!(&encoded[..LEASE_ID_SIZE], &lease_id.to_be_bytes());
        assert_eq!(&encoded[LEASE_ID_SIZE..], name);
    }

    #[test]
    fn test_barrier_manager_create_release() {
        let mut mgr = BarrierManager::new();
        let name = b"test-barrier".to_vec();
        let key = barrier_key(&name);

        assert!(!mgr.is_held(&name));
        assert_eq!(mgr.barrier_count(), 0);

        mgr.create(name.clone(), key.clone(), 0);
        assert!(mgr.is_held(&name));
        assert_eq!(mgr.get_key(&name), Some(key.as_slice()));
        assert_eq!(mgr.barrier_count(), 1);

        assert!(mgr.release(&key));
        assert!(!mgr.is_held(&name));
        assert_eq!(mgr.barrier_count(), 0);
    }

    #[test]
    fn test_barrier_manager_release_nonexistent() {
        let mut mgr = BarrierManager::new();
        assert!(!mgr.release(b"nonexistent"));
    }

    #[test]
    fn test_barrier_manager_lease_association() {
        let mut mgr = BarrierManager::new();
        let name = b"test-barrier".to_vec();
        let key = barrier_key(&name);

        mgr.create(name.clone(), key.clone(), 42);
        assert_eq!(mgr.get_lease_id(&key), Some(42));
    }

    #[test]
    fn test_barrier_manager_release_by_lease() {
        let mut mgr = BarrierManager::new();
        let name1 = b"barrier-1".to_vec();
        let name2 = b"barrier-2".to_vec();
        let key1 = barrier_key(&name1);
        let key2 = barrier_key(&name2);

        mgr.create(name1.clone(), key1.clone(), 42);
        mgr.create(name2.clone(), key2.clone(), 42);

        assert_eq!(mgr.barrier_count(), 2);

        let released = mgr.release_by_lease(42);
        assert_eq!(released.len(), 2);
        assert_eq!(mgr.barrier_count(), 0);
    }

    #[test]
    fn test_validate_barrier_name() {
        assert!(validate_barrier_name(b"valid-name").is_ok());
        assert!(validate_barrier_name(b"").is_err());
        assert!(validate_barrier_name(b"null\x00byte").is_err());
        assert!(validate_barrier_name(b"_aether_reserved").is_err());
    }
}
