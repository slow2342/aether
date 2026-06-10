use std::sync::Arc;

use rocksdb::WriteBatch;

use crate::error::StorageError;
use crate::storage::RocksStorage;

use super::LeaseInfo;

/// Persistence layer for lease data in RocksDB.
#[derive(Clone)]
pub struct LeaseStore {
    storage: Arc<RocksStorage>,
}

impl LeaseStore {
    pub fn new(storage: Arc<RocksStorage>) -> Self {
        Self { storage }
    }

    /// Save a lease to the lease CF.
    pub fn save_lease(&self, info: &LeaseInfo) -> Result<(), StorageError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(info)
            .map_err(|e| StorageError::Codec(e.to_string()))?;
        self.storage
            .db()
            .put_cf(
                self.storage.lease_cf(),
                info.id.to_be_bytes(),
                bytes.as_ref(),
            )
            .map_err(StorageError::RocksDb)
    }

    /// Load all leases from the lease CF.
    pub fn load_all_leases(&self) -> Result<Vec<LeaseInfo>, StorageError> {
        use rocksdb::IteratorMode;
        let mut leases = Vec::new();
        let iter = self
            .storage
            .db()
            .iterator_cf(self.storage.lease_cf(), IteratorMode::Start);
        for item in iter {
            let (_, value) = item.map_err(StorageError::RocksDb)?;
            let info: LeaseInfo = rkyv::from_bytes::<LeaseInfo, rkyv::rancor::BoxedError>(&value)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            leases.push(info);
        }
        Ok(leases)
    }

    /// Get the lease_id for a key from key_lease CF.
    pub fn get_key_lease_id(&self, key: &[u8]) -> Result<Option<i64>, StorageError> {
        match self
            .storage
            .db()
            .get_cf(self.storage.key_lease_cf(), key)
            .map_err(StorageError::RocksDb)?
        {
            Some(bytes) if bytes.len() == 8 => {
                let id = i64::from_be_bytes(bytes.try_into().unwrap());
                Ok(Some(id))
            }
            _ => Ok(None),
        }
    }

    /// Batch cleanup of lease associations for multiple keys.
    /// Adds key_lease and lease_keys deletes to `data_batch`.
    /// Returns (lease_id, key) pairs for in-memory detach.
    pub fn batch_lease_cleanup(
        &self,
        keys: &[Vec<u8>],
        data_batch: &mut WriteBatch,
    ) -> Result<Vec<(i64, Vec<u8>)>, StorageError> {
        // Single batch read instead of N individual point reads.
        let results = self.storage.db().multi_get_cf(
            keys.iter()
                .map(|k| (self.storage.key_lease_cf(), k.as_slice())),
        );
        let mut detach_ops = Vec::new();
        for (key, result) in keys.iter().zip(results) {
            let bytes = match result {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        key = ?key,
                        error = %e,
                        "multi_get failed for key_lease, skipping cleanup for this key"
                    );
                    continue;
                }
            };
            if let Some(bytes) = bytes
                && bytes.len() == 8
            {
                let lease_id = i64::from_be_bytes(bytes.try_into().unwrap());
                if lease_id > 0 {
                    detach_ops.push((lease_id, key.clone()));
                    data_batch.delete_cf(self.storage.key_lease_cf(), key);
                    let mut lk_key = Vec::with_capacity(8 + key.len());
                    lk_key.extend_from_slice(&lease_id.to_be_bytes());
                    lk_key.extend_from_slice(key);
                    data_batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
                }
            }
        }
        Ok(detach_ops)
    }

    /// Batch write for Put with lease_id: saves key_lease and key_lease_id mappings.
    /// Removes old association if different from new lease_id.
    pub fn batch_put_lease_association(
        &self,
        key: &[u8],
        lease_id: i64,
        old_lease_id: Option<i64>,
        batch: &mut WriteBatch,
    ) {
        if let Some(old_id) = old_lease_id
            && old_id != lease_id
            && old_id > 0
        {
            batch.delete_cf(self.storage.key_lease_cf(), key);
            let mut lk_key = Vec::with_capacity(8 + key.len());
            lk_key.extend_from_slice(&old_id.to_be_bytes());
            lk_key.extend_from_slice(key);
            batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
        }
        batch.put_cf(self.storage.key_lease_cf(), key, lease_id.to_be_bytes());
        let mut lk_key = Vec::with_capacity(8 + key.len());
        lk_key.extend_from_slice(&lease_id.to_be_bytes());
        lk_key.extend_from_slice(key);
        batch.put_cf(self.storage.lease_keys_cf(), lk_key, []);
    }

    /// Load all lease-to-key mappings in a single scan. Returns (lease_id, key) pairs.
    pub fn load_all_lease_key_pairs(&self) -> Result<Vec<(i64, Vec<u8>)>, StorageError> {
        use rocksdb::IteratorMode;
        let mut pairs = Vec::new();
        let iter = self
            .storage
            .db()
            .iterator_cf(self.storage.lease_keys_cf(), IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(StorageError::RocksDb)?;
            if key.len() >= 8 {
                let lease_id = i64::from_be_bytes(key[..8].try_into().unwrap());
                pairs.push((lease_id, key[8..].to_vec()));
            }
        }
        Ok(pairs)
    }

    /// Load the lease ID counter from meta CF.
    pub fn load_lease_counter(&self) -> Result<i64, StorageError> {
        let meta_cf = self
            .storage
            .db()
            .cf_handle("meta")
            .expect("meta CF not found");
        match self
            .storage
            .db()
            .get_cf(meta_cf, b"lease_counter")
            .map_err(StorageError::RocksDb)?
        {
            Some(bytes) if bytes.len() == 8 => Ok(i64::from_be_bytes(bytes.try_into().unwrap())),
            _ => Ok(0),
        }
    }
}
