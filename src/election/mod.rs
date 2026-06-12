use crate::storage::{RocksStorage, StorageEngine};

/// Key prefix for election keys in storage.
/// Uses `_aether_` prefix to avoid collision with user keys.
pub const ELECTION_KEY_PREFIX: &[u8] = b"_aether_election/";

/// Reserved prefix for system use.
const RESERVED_PREFIX: &[u8] = b"_aether_";

/// Maximum allowed election name length (1 KB).
const MAX_ELECTION_NAME_LEN: usize = 1024;

/// Lease ID size in bytes (i64 = 8 bytes).
const LEASE_ID_SIZE: usize = 8;

/// In-memory election manager. Owned by the API layer.
///
/// Tracks which elections have active leaders. The actual election data is
/// stored in the KV store with the `_aether_election/` prefix.
///
/// Value format in KV store: `[lease_id: i64 BE][value_bytes]`
pub struct ElectionManager {
    /// Active leaders: election_name -> leader_key
    leaders: std::collections::HashMap<Box<[u8]>, Box<[u8]>>,
    /// Reverse index: leader_key -> election_name (for O(1) lookup by key)
    key_to_name: std::collections::HashMap<Box<[u8]>, Box<[u8]>>,
    /// Lease association: leader_key -> lease_id
    key_lease: std::collections::HashMap<Box<[u8]>, i64>,
    /// Reverse index: lease_id -> set of leader_keys (for O(1) resign by lease)
    lease_keys: std::collections::HashMap<i64, std::collections::HashSet<Box<[u8]>>>,
}

impl Default for ElectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ElectionManager {
    pub fn new() -> Self {
        Self {
            leaders: std::collections::HashMap::new(),
            key_to_name: std::collections::HashMap::new(),
            key_lease: std::collections::HashMap::new(),
            lease_keys: std::collections::HashMap::new(),
        }
    }

    /// Restore election state from persistent storage.
    /// Scans the KV store for keys with the election prefix and rebuilds
    /// in-memory state.
    pub fn restore(&mut self, storage: &RocksStorage) -> Result<(), crate::error::StorageError> {
        let entries = storage.scan(ELECTION_KEY_PREFIX, usize::MAX)?;

        self.leaders.clear();
        self.key_to_name.clear();
        self.key_lease.clear();
        self.lease_keys.clear();

        for entry in entries {
            let key: Box<[u8]> = entry.key.into();
            let value = entry.value;

            // Decode value: [lease_id: i64 BE][value_bytes]
            if value.len() < LEASE_ID_SIZE {
                tracing::warn!(
                    key = %String::from_utf8_lossy(&key),
                    "invalid election value, skipping"
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

            // Extract election name from key
            if let Some(name) = election_name(&key) {
                let name_box: Box<[u8]> = name.into();
                self.key_to_name.insert(key.clone(), name_box.clone());
                self.leaders.insert(name_box, key.clone());
                if lease_id > 0 {
                    self.key_lease.insert(key.clone(), lease_id);
                    self.lease_keys.entry(lease_id).or_default().insert(key);
                }
            }
        }

        Ok(())
    }

    /// Encode election value with lease_id.
    pub fn encode_value(value: &[u8], lease_id: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(LEASE_ID_SIZE + value.len());
        encoded.extend_from_slice(&lease_id.to_be_bytes());
        encoded.extend_from_slice(value);
        encoded
    }

    /// Set the leader for an election. Returns the leader key.
    pub fn set_leader(&mut self, name: Vec<u8>, key: Vec<u8>, lease_id: i64) -> Vec<u8> {
        let name_box: Box<[u8]> = name.into();
        let key_box: Box<[u8]> = key.into();

        // Update reverse index
        self.key_to_name.insert(key_box.clone(), name_box.clone());

        // Update lease mapping
        if lease_id > 0 {
            self.key_lease.insert(key_box.clone(), lease_id);
            self.lease_keys
                .entry(lease_id)
                .or_default()
                .insert(key_box.clone());
        }

        self.leaders.insert(name_box, key_box.clone());
        key_box.into_vec()
    }

    /// Remove a leader by key. Returns true if found and removed.
    pub fn remove_leader(&mut self, key: &[u8]) -> bool {
        // Use reverse index for O(1) lookup
        if let Some(name) = self.key_to_name.remove(key) {
            self.leaders.remove(&name);
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

    /// Remove all leaders associated with a lease.
    /// Returns the list of leader keys that were removed.
    pub fn remove_by_lease(&mut self, lease_id: i64) -> Vec<Vec<u8>> {
        let keys: Vec<Box<[u8]>> = match self.lease_keys.remove(&lease_id) {
            Some(keys) => keys.into_iter().collect(),
            None => return Vec::new(),
        };

        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            // Use reverse index for O(1) lookup
            if let Some(name) = self.key_to_name.remove(&key) {
                self.leaders.remove(&name);
            }
            self.key_lease.remove(&key);
            result.push(key.into_vec());
        }

        result
    }

    /// Get the leader key for a given election name.
    pub fn get_leader_key(&self, name: &[u8]) -> Option<&[u8]> {
        self.leaders.get(name).map(|k| k.as_ref())
    }

    /// Check if a leader key exists. Returns the election name if found.
    pub fn get_election_name_by_key(&self, key: &[u8]) -> Option<&[u8]> {
        self.key_to_name.get(key).map(|n| n.as_ref())
    }

    /// Check if an election has a leader.
    pub fn has_leader(&self, name: &[u8]) -> bool {
        self.leaders.contains_key(name)
    }

    /// Get the lease_id for a given leader key.
    pub fn get_lease_id(&self, key: &[u8]) -> Option<i64> {
        self.key_lease.get(key).copied()
    }

    /// Get the number of active elections with leaders.
    pub fn election_count(&self) -> usize {
        self.leaders.len()
    }

    /// Get all leader keys associated with a lease (without removing them).
    pub fn get_keys_by_lease(&self, lease_id: i64) -> Vec<Vec<u8>> {
        match self.lease_keys.get(&lease_id) {
            Some(keys) => keys.iter().map(|k| k.to_vec()).collect(),
            None => Vec::new(),
        }
    }
}

/// Generate an election key for a given election name.
pub fn election_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(ELECTION_KEY_PREFIX.len() + name.len());
    key.extend_from_slice(ELECTION_KEY_PREFIX);
    key.extend_from_slice(name);
    key
}

/// Extract the election name from an election key.
pub fn election_name(key: &[u8]) -> Option<&[u8]> {
    if key.starts_with(ELECTION_KEY_PREFIX) {
        Some(&key[ELECTION_KEY_PREFIX.len()..])
    } else {
        None
    }
}

/// Validate election name - must not be empty, must not contain null bytes,
/// and must not start with the reserved prefix `_aether_`.
pub fn validate_election_name(name: &[u8]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("election name must not be empty");
    }
    if name.len() > MAX_ELECTION_NAME_LEN {
        return Err("election name too long");
    }
    if name.contains(&0) {
        return Err("election name must not contain null bytes");
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err("election name must not start with reserved prefix '_aether_'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_key_generation() {
        let name = b"my-election";
        let key = election_key(name);
        assert_eq!(key, b"_aether_election/my-election");
    }

    #[test]
    fn test_election_name_extraction() {
        let key = b"_aether_election/my-election";
        let name = election_name(key);
        assert_eq!(name, Some(b"my-election".as_slice()));
    }

    #[test]
    fn test_election_name_extraction_invalid() {
        let key = b"not-an-election";
        let name = election_name(key);
        assert_eq!(name, None);
    }

    #[test]
    fn test_encode_decode_value() {
        let value = b"candidate-1";
        let lease_id = 42i64;
        let encoded = ElectionManager::encode_value(value, lease_id);

        assert_eq!(encoded.len(), LEASE_ID_SIZE + value.len());
        assert_eq!(&encoded[..LEASE_ID_SIZE], &lease_id.to_be_bytes());
        assert_eq!(&encoded[LEASE_ID_SIZE..], value);
    }

    #[test]
    fn test_election_manager_set_remove_leader() {
        let mut mgr = ElectionManager::new();
        let name = b"test-election".to_vec();
        let key = election_key(&name);

        assert!(!mgr.has_leader(&name));
        assert_eq!(mgr.election_count(), 0);

        mgr.set_leader(name.clone(), key.clone(), 0);
        assert!(mgr.has_leader(&name));
        assert_eq!(mgr.get_leader_key(&name), Some(key.as_slice()));
        assert_eq!(mgr.election_count(), 1);

        // Test reverse index
        assert_eq!(mgr.get_election_name_by_key(&key), Some(name.as_slice()));

        assert!(mgr.remove_leader(&key));
        assert!(!mgr.has_leader(&name));
        assert_eq!(mgr.election_count(), 0);
        assert!(mgr.get_election_name_by_key(&key).is_none());
    }

    #[test]
    fn test_election_manager_remove_nonexistent() {
        let mut mgr = ElectionManager::new();
        assert!(!mgr.remove_leader(b"nonexistent"));
    }

    #[test]
    fn test_election_manager_lease_association() {
        let mut mgr = ElectionManager::new();
        let name = b"test-election".to_vec();
        let key = election_key(&name);

        mgr.set_leader(name.clone(), key.clone(), 42);
        assert_eq!(mgr.get_lease_id(&key), Some(42));
    }

    #[test]
    fn test_election_manager_remove_by_lease() {
        let mut mgr = ElectionManager::new();
        let name1 = b"election-1".to_vec();
        let name2 = b"election-2".to_vec();
        let key1 = election_key(&name1);
        let key2 = election_key(&name2);

        mgr.set_leader(name1.clone(), key1.clone(), 42);
        mgr.set_leader(name2.clone(), key2.clone(), 42);

        assert_eq!(mgr.election_count(), 2);

        let removed = mgr.remove_by_lease(42);
        assert_eq!(removed.len(), 2);
        assert_eq!(mgr.election_count(), 0);
        assert!(!mgr.has_leader(&name1));
        assert!(!mgr.has_leader(&name2));
    }

    #[test]
    fn test_election_manager_remove_by_lease_partial() {
        let mut mgr = ElectionManager::new();
        let name1 = b"election-1".to_vec();
        let name2 = b"election-2".to_vec();
        let key1 = election_key(&name1);
        let key2 = election_key(&name2);

        mgr.set_leader(name1.clone(), key1.clone(), 42);
        mgr.set_leader(name2.clone(), key2.clone(), 43);

        let removed = mgr.remove_by_lease(42);
        assert_eq!(removed.len(), 1);
        assert_eq!(mgr.election_count(), 1);
        assert!(!mgr.has_leader(&name1));
        assert!(mgr.has_leader(&name2));
    }

    #[test]
    fn test_validate_election_name() {
        assert!(validate_election_name(b"valid-name").is_ok());
        assert!(validate_election_name(b"").is_err());
        assert!(validate_election_name(b"null\x00byte").is_err());
        assert!(validate_election_name(b"_aether_reserved").is_err());
    }
}
