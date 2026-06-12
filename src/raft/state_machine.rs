use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;

use super::{KeyValue, RaftRequest, RaftResponse, WatchEvent};
use crate::auth::AuthCache;
use crate::lease::{LeaseManager, LeaseStore};
use crate::shard::manager::ShardManager;
use crate::storage::mvcc::{
    KeyIndex, MvccValue, encode_mvcc_key, load_key_indexes, save_global_revision,
};
use crate::storage::{RocksStorage, StorageEngine};

/// Snapshot data containing all user-data column families.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotCfData {
    default_entries: Vec<(Vec<u8>, Vec<u8>)>,
    mvcc_entries: Vec<(Vec<u8>, Vec<u8>)>,
    meta_entries: Vec<(Vec<u8>, Vec<u8>)>,
    lease_entries: Vec<(Vec<u8>, Vec<u8>)>,
    lease_keys_entries: Vec<(Vec<u8>, Vec<u8>)>,
    key_lease_entries: Vec<(Vec<u8>, Vec<u8>)>,
    region_entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Per-key metadata tracked in memory and persisted to the meta CF.
#[derive(Debug, Clone)]
pub struct KeyMeta {
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

impl KeyMeta {
    fn to_kv(&self, key: Vec<u8>, value: Vec<u8>) -> KeyValue {
        KeyValue {
            key,
            value,
            create_revision: self.create_revision,
            mod_revision: self.mod_revision,
            version: self.version,
            lease: self.lease,
        }
    }
}

/// Aether state machine implementation.
/// All user data is stored in RocksDB.
pub struct AetherStateMachine {
    /// Applied log index
    pub last_applied: u64,
    /// RocksDB storage for persistent user data
    pub storage: Arc<RocksStorage>,
    /// Watch event notifier
    pub watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
    /// In-memory lease manager (shared with API layer and expiry task).
    /// Uses std::sync::Mutex (not tokio::sync::Mutex) because lock hold times
    /// are microseconds and the state machine thread is a dedicated std::thread,
    /// not a tokio worker. The API layer holds the lock only briefly to read
    /// granted_ttl or list leases, so blocking a tokio worker is acceptable.
    pub lease_manager: Arc<Mutex<LeaseManager>>,
    /// Persistent lease storage
    pub lease_store: LeaseStore,
    /// In-memory auth cache (shared with interceptor)
    pub auth_cache: Arc<AuthCache>,
    /// Shared auth enabled flag (updated on AuthEnable/AuthDisable)
    pub auth_enabled: Arc<AtomicBool>,
    /// MVCC: per-key version history index (rebuilt from mvcc CF on startup)
    pub key_indexes: HashMap<Vec<u8>, KeyIndex>,
    /// MVCC: per-key current metadata (create_revision, mod_revision, version, lease)
    pub key_metas: HashMap<Vec<u8>, KeyMeta>,
    /// In-memory key-to-lease mapping (mirrors key_lease CF).
    /// Tracks the lease_id for each key so that Txn sub-operations can look up
    /// the current lease association without reading uncommitted batch writes.
    pub key_leases: HashMap<Vec<u8>, i64>,
    /// Shard manager: in-memory index of all regions (shared with API layer).
    pub shard_manager: Arc<Mutex<ShardManager>>,
}

/// Captures pre-mutation in-memory state for keys touched during an apply.
///
/// Before `batch_put`/`batch_delete` modify `key_indexes`, `key_metas`, or
/// `key_leases`, they call [`TxnSnapshot::record_key`] to save the original
/// values.  After `db.write(batch)` the caller either:
/// - calls [`commit`](TxnSnapshot::commit) on success (drops the saved state), or
/// - calls [`rollback`](TxnSnapshot::rollback) on failure (restores every saved
///   key to its pre-mutation value and reverses lease attach/detach operations).
struct TxnSnapshot {
    ki: HashMap<Vec<u8>, Option<KeyIndex>>,
    meta: HashMap<Vec<u8>, Option<KeyMeta>>,
    /// Per-key initial lease state (before any mutation).
    /// `Some(lease_id)` = key was attached to that lease; `None` = no lease.
    lease: HashMap<Vec<u8>, Option<i64>>,
}

impl TxnSnapshot {
    fn new() -> Self {
        Self {
            ki: HashMap::new(),
            meta: HashMap::new(),
            lease: HashMap::new(),
        }
    }

    /// Save the pre-mutation state of `key` (only once per key).
    fn record_key(
        &mut self,
        key: &[u8],
        key_indexes: &HashMap<Vec<u8>, KeyIndex>,
        key_metas: &HashMap<Vec<u8>, KeyMeta>,
        key_leases: &HashMap<Vec<u8>, i64>,
    ) {
        if !self.ki.contains_key(key) {
            self.ki.insert(key.to_vec(), key_indexes.get(key).cloned());
            self.meta.insert(key.to_vec(), key_metas.get(key).cloned());
            self.lease
                .insert(key.to_vec(), key_leases.get(key).copied());
        }
    }

    /// Discard saved state — batch write succeeded.
    fn commit(self) {
        // Drop self — nothing to restore.
    }

    /// Restore every saved key to its pre-mutation state and reconcile
    /// lease_manager to match the saved per-key lease state.
    /// Called when `db.write(batch)` fails.
    fn rollback(self, sm: &mut AetherStateMachine) {
        // Reconcile lease_manager FIRST, before restoring key_leases.
        // At this point, key_leases still reflects the failed mutation,
        // so we can detect what changed and reverse it.
        let mut mgr = sm.lease_manager.lock().unwrap();
        for (key, initial_lease) in &self.lease {
            let mutated_lease = sm.key_leases.get(key).copied();
            match (*initial_lease, mutated_lease) {
                (Some(init_id), Some(mut_id)) if init_id != mut_id => {
                    mgr.detach_key(mut_id, key);
                    mgr.attach_key(init_id, key.clone());
                }
                (None, Some(mut_id)) => {
                    mgr.detach_key(mut_id, key);
                }
                (Some(init_id), None) => {
                    mgr.attach_key(init_id, key.clone());
                }
                _ => {}
            }
        }
        drop(mgr);

        // Now restore key_metas, key_indexes, key_leases.
        for (key, old) in &self.meta {
            match old {
                Some(m) => {
                    sm.key_metas.insert(key.clone(), m.clone());
                }
                None => {
                    sm.key_metas.remove(key);
                }
            }
        }
        for (key, old) in &self.ki {
            match old {
                Some(ki) => {
                    sm.key_indexes.insert(key.clone(), ki.clone());
                }
                None => {
                    sm.key_indexes.remove(key);
                }
            }
        }
        for (key, old) in &self.lease {
            match old {
                Some(id) => {
                    sm.key_leases.insert(key.clone(), *id);
                }
                None => {
                    sm.key_leases.remove(key);
                }
            }
        }
    }
}

impl AetherStateMachine {
    pub fn new(
        watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
        storage: Arc<RocksStorage>,
        lease_manager: Arc<Mutex<LeaseManager>>,
        lease_store: LeaseStore,
        auth_cache: Arc<AuthCache>,
        auth_enabled: Arc<AtomicBool>,
        shard_manager: Arc<Mutex<ShardManager>>,
    ) -> Self {
        let key_indexes = load_key_indexes(storage.db(), storage.mvcc_cf())
            .expect("failed to load key indexes from mvcc CF");
        let key_metas = Self::load_key_metas_from_indexes(&storage, &key_indexes);
        let key_leases = Self::load_key_leases_from_meta(&key_metas);
        Self {
            last_applied: 0,
            storage,
            watch_tx,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
            key_indexes,
            key_metas,
            key_leases,
            shard_manager,
        }
    }

    /// Load per-key metadata from pre-loaded key indexes.
    fn load_key_metas_from_indexes(
        storage: &RocksStorage,
        key_indexes: &HashMap<Vec<u8>, KeyIndex>,
    ) -> HashMap<Vec<u8>, KeyMeta> {
        let mut metas = HashMap::new();
        for (user_key, ki) in key_indexes {
            if let Some(rev) = ki.get(0) {
                let mvcc_key = encode_mvcc_key(user_key, rev);
                if let Ok(Some(bytes)) = storage.db().get_cf(storage.mvcc_cf(), &mvcc_key)
                    && !bytes.is_empty()
                    && let Ok(mv) = rkyv::from_bytes::<MvccValue, rkyv::rancor::BoxedError>(&bytes)
                {
                    metas.insert(
                        user_key.clone(),
                        KeyMeta {
                            create_revision: mv.create_revision,
                            mod_revision: mv.mod_revision,
                            version: mv.version,
                            lease: mv.lease,
                        },
                    );
                }
            }
        }
        metas
    }

    /// Build key_leases map from key_metas (lease field in metadata).
    fn load_key_leases_from_meta(key_metas: &HashMap<Vec<u8>, KeyMeta>) -> HashMap<Vec<u8>, i64> {
        key_metas
            .iter()
            .filter(|(_, m)| m.lease > 0)
            .map(|(k, m)| (k.clone(), m.lease))
            .collect()
    }

    fn storage_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.storage.get(key) {
            Ok(val) => val,
            Err(e) => {
                tracing::error!(error = %e, key = ?key, "storage get failed");
                None
            }
        }
    }

    /// Append a Put operation to `batch` and update in-memory state.
    /// Does NOT call `db.write()` — the caller is responsible for committing
    /// (and calling `snapshot.commit()`) or rolling back (`snapshot.rollback()`).
    ///
    /// `snapshot` records the pre-mutation state of each touched key so that
    /// `rollback()` can restore the exact prior state if the batch write fails.
    #[allow(clippy::too_many_arguments)]
    fn batch_put(
        &mut self,
        batch: &mut WriteBatch,
        snapshot: &mut TxnSnapshot,
        value_cache: &mut HashMap<Vec<u8>, Vec<u8>>,
        key: Vec<u8>,
        value: &[u8],
        lease_id: i64,
        revision: u64,
    ) -> Result<Option<KeyValue>, String> {
        // Read existing metadata for prev_kv response.
        // Use value_cache for the value if available (same-batch prior write),
        // otherwise fall back to RocksDB.
        let cached_value = value_cache.get(&key).cloned();
        let prev_value = cached_value.or_else(|| self.storage_get(&key));
        let prev_kv = self
            .key_metas
            .get(&key)
            .map(|m| m.to_kv(key.clone(), prev_value.unwrap_or_default()));

        // Snapshot the key before any mutation.
        snapshot.record_key(&key, &self.key_indexes, &self.key_metas, &self.key_leases);

        // Compute new MVCC metadata.
        let rev = revision as i64;
        let (create_rev, ver) = match self.key_metas.get(&key) {
            Some(old) => (old.create_revision, old.version + 1),
            None => (rev, 1),
        };
        let new_meta = KeyMeta {
            create_revision: create_rev,
            mod_revision: rev,
            version: ver,
            lease: lease_id,
        };

        // Serialize MVCC value.
        let mvcc_val = MvccValue {
            create_revision: new_meta.create_revision,
            mod_revision: new_meta.mod_revision,
            version: new_meta.version,
            lease: new_meta.lease,
            value: value.to_vec(),
        };
        let mvcc_val_bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&mvcc_val)
            .map_err(|e| format!("serialize failed: {e}"))?;
        let mvcc_key = encode_mvcc_key(&key, revision);

        // Prepare key_index mutation on a clone.
        let mut new_ki = self.key_indexes.get(&key).cloned().unwrap_or_default();
        new_ki.put(revision);

        // Look up old lease from in-memory state (accurate within a multi-op Txn).
        let old_lease_id = self.key_leases.get(&key).copied();

        // Add writes to the shared batch.
        batch.put_cf(self.storage.default_cf(), &key, value);
        batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, mvcc_val_bytes.as_ref());

        // Handle lease association changes.
        if lease_id > 0 {
            self.lease_store
                .batch_put_lease_association(&key, lease_id, old_lease_id, batch);
        } else if let Some(old_id) = old_lease_id
            && old_id > 0
        {
            // Clear old lease association when new lease_id is 0.
            batch.delete_cf(self.storage.key_lease_cf(), &key);
            let mut lk_key = Vec::with_capacity(8 + key.len());
            lk_key.extend_from_slice(&old_id.to_be_bytes());
            lk_key.extend_from_slice(&key);
            batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
        }

        // Update in-memory state immediately so subsequent ops see the change.
        // These changes will be rolled back by snapshot.rollback() if the batch
        // write fails.
        self.key_indexes.insert(key.clone(), new_ki);
        self.key_metas.insert(key.clone(), new_meta);
        value_cache.insert(key.clone(), value.to_vec());
        if lease_id > 0 {
            self.key_leases.insert(key.clone(), lease_id);
        } else {
            self.key_leases.remove(&key);
        }

        if lease_id > 0 {
            let mut mgr = self.lease_manager.lock().unwrap();
            if let Some(old_id) = old_lease_id
                && old_id != lease_id
                && old_id > 0
            {
                mgr.detach_key(old_id, &key);
            }
            mgr.attach_key(lease_id, key.clone());
        } else if let Some(old_id) = old_lease_id
            && old_id > 0
        {
            self.lease_manager.lock().unwrap().detach_key(old_id, &key);
        }

        Ok(prev_kv)
    }

    /// Append a Delete operation to `batch` and update in-memory state.
    /// Returns (deleted_count, prev_kvs, watch_events).
    fn batch_delete(
        &mut self,
        batch: &mut WriteBatch,
        snapshot: &mut TxnSnapshot,
        value_cache: &mut HashMap<Vec<u8>, Vec<u8>>,
        key: Vec<u8>,
        range_end: Vec<u8>,
        revision: u64,
    ) -> Result<(i64, Vec<KeyValue>, Vec<WatchEvent>), String> {
        let mut deleted = 0i64;
        let mut prev_kvs = Vec::new();
        let mut watch_events = Vec::new();

        if range_end.is_empty() {
            // Single key delete.
            if self.key_metas.contains_key(&key) {
                snapshot.record_key(&key, &self.key_indexes, &self.key_metas, &self.key_leases);

                let meta = self.key_metas.get(&key).unwrap().clone();
                let value = value_cache
                    .remove(&key)
                    .or_else(|| self.storage_get(&key))
                    .unwrap_or_default();
                let prev_kv = meta.to_kv(key.clone(), value);

                let mut new_ki = self.key_indexes.get(&key).cloned().unwrap_or_default();
                new_ki.tombstone(revision);

                // Clean up lease associations.
                let detach_ops: Vec<(i64, Vec<u8>)> = match self.key_leases.get(&key) {
                    Some(&lid) if lid > 0 => {
                        batch.delete_cf(self.storage.key_lease_cf(), &key);
                        let mut lk_key = Vec::with_capacity(8 + key.len());
                        lk_key.extend_from_slice(&lid.to_be_bytes());
                        lk_key.extend_from_slice(&key);
                        batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
                        vec![(lid, key.clone())]
                    }
                    _ => Vec::new(),
                };

                batch.delete_cf(self.storage.default_cf(), &key);

                // Write MVCC tombstone.
                let mvcc_key = encode_mvcc_key(&key, revision);
                batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, b"");

                // Update in-memory state.
                self.key_indexes.insert(key.clone(), new_ki);
                self.key_metas.remove(&key);
                self.key_leases.remove(&key);
                {
                    let mut mgr = self.lease_manager.lock().unwrap();
                    for (lease_id, k) in &detach_ops {
                        mgr.detach_key(*lease_id, k);
                    }
                }

                deleted = 1;
                watch_events.push(WatchEvent {
                    event_type: super::WatchEventType::Delete,
                    kv: prev_kv.clone(),
                    prev_kv: None,
                });
                prev_kvs.push(prev_kv);
            }
        } else {
            // Range delete.
            let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
            let mut prevs: Vec<KeyValue> = Vec::new();

            for (k, meta) in &self.key_metas {
                if k.as_slice() >= key.as_slice()
                    && (range_end == b"\0" || k.as_slice() < range_end.as_slice())
                {
                    let value = value_cache
                        .remove(k)
                        .or_else(|| self.storage_get(k))
                        .unwrap_or_default();
                    prevs.push(meta.to_kv(k.clone(), value));
                    keys_to_delete.push(k.clone());
                }
            }
            keys_to_delete.sort();

            if !keys_to_delete.is_empty() {
                for k in &keys_to_delete {
                    snapshot.record_key(k, &self.key_indexes, &self.key_metas, &self.key_leases);
                }

                // Prepare key_index mutations on clones.
                let mut ki_mutations: Vec<(Vec<u8>, KeyIndex)> = Vec::new();
                for k in &keys_to_delete {
                    let mut new_ki = self.key_indexes.get(k).cloned().unwrap_or_default();
                    new_ki.tombstone(revision);
                    ki_mutations.push((k.clone(), new_ki));
                }

                // Clean up lease associations.
                let mut detach_ops: Vec<(i64, Vec<u8>)> = Vec::new();
                for k in &keys_to_delete {
                    if let Some(&lid) = self.key_leases.get(k)
                        && lid > 0
                    {
                        batch.delete_cf(self.storage.key_lease_cf(), k);
                        let mut lk_key = Vec::with_capacity(8 + k.len());
                        lk_key.extend_from_slice(&lid.to_be_bytes());
                        lk_key.extend_from_slice(k);
                        batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
                        detach_ops.push((lid, k.clone()));
                    }
                }

                for k in &keys_to_delete {
                    batch.delete_cf(self.storage.default_cf(), k);
                    let mvcc_key = encode_mvcc_key(k, revision);
                    batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, b"");
                }

                // Update in-memory state.
                for (k, ki) in ki_mutations {
                    self.key_indexes.insert(k, ki);
                }
                for k in &keys_to_delete {
                    self.key_metas.remove(k);
                    self.key_leases.remove(k);
                }
                {
                    let mut mgr = self.lease_manager.lock().unwrap();
                    for (lease_id, k) in &detach_ops {
                        mgr.detach_key(*lease_id, k);
                    }
                }

                deleted = keys_to_delete.len() as i64;
                for kv in &prevs {
                    watch_events.push(WatchEvent {
                        event_type: super::WatchEventType::Delete,
                        kv: kv.clone(),
                        prev_kv: None,
                    });
                }
                prev_kvs = prevs;
            }
        }

        Ok((deleted, prev_kvs, watch_events))
    }

    /// Apply a request to the state machine.
    /// `revision` is the Raft log entry index used as the global MVCC revision.
    fn apply_request(&mut self, request: RaftRequest, revision: u64) -> RaftResponse {
        match request {
            RaftRequest::Put {
                key,
                value,
                lease_id,
            } => {
                let mut batch = WriteBatch::default();
                let mut snapshot = TxnSnapshot::new();
                let mut value_cache = HashMap::new();
                let prev_kv = match self.batch_put(
                    &mut batch,
                    &mut snapshot,
                    &mut value_cache,
                    key.clone(),
                    &value,
                    lease_id,
                    revision,
                ) {
                    Ok(p) => p,
                    Err(message) => {
                        snapshot.rollback(self);
                        return RaftResponse::Error { message };
                    }
                };
                save_global_revision(&mut batch, self.storage.meta_cf(), revision);
                if let Err(e) = self.storage.db().write(batch) {
                    tracing::error!(error = %e, "failed to put to storage");
                    snapshot.rollback(self);
                    return RaftResponse::Error {
                        message: format!("put failed: {e}"),
                    };
                }
                snapshot.commit();
                let kv = self
                    .key_metas
                    .get(&key)
                    .unwrap()
                    .to_kv(key.clone(), self.storage_get(&key).unwrap_or_default());
                let _ = self.watch_tx.send(WatchEvent {
                    event_type: super::WatchEventType::Put,
                    kv,
                    prev_kv: prev_kv.clone(),
                });
                RaftResponse::Put { prev_kv }
            }
            RaftRequest::Delete { key, range_end } => {
                let mut batch = WriteBatch::default();
                let mut snapshot = TxnSnapshot::new();
                let mut value_cache = HashMap::new();
                let (deleted, prev_kvs, watch_events) = match self.batch_delete(
                    &mut batch,
                    &mut snapshot,
                    &mut value_cache,
                    key,
                    range_end,
                    revision,
                ) {
                    Ok(r) => r,
                    Err(message) => {
                        snapshot.rollback(self);
                        return RaftResponse::Error { message };
                    }
                };
                save_global_revision(&mut batch, self.storage.meta_cf(), revision);
                if let Err(e) = self.storage.db().write(batch) {
                    tracing::error!(error = %e, "failed to delete from storage");
                    snapshot.rollback(self);
                    return RaftResponse::Error {
                        message: format!("delete failed: {e}"),
                    };
                }
                snapshot.commit();
                for event in watch_events {
                    let _ = self.watch_tx.send(event);
                }
                RaftResponse::Delete { deleted, prev_kvs }
            }
            RaftRequest::Txn {
                compare,
                success,
                failure,
            } => {
                let succeeded = compare.iter().all(|cmp| {
                    let meta = self.key_metas.get(&cmp.key);
                    let current_value = self.storage_get(&cmp.key);
                    Self::evaluate_compare(cmp, meta, current_value.as_deref())
                });

                // All sub-operations share a single WriteBatch for atomicity.
                let mut batch = WriteBatch::default();
                let mut snapshot = TxnSnapshot::new();
                let mut value_cache = HashMap::new();
                let mut watch_events = Vec::new();

                let ops = if succeeded { &success } else { &failure };
                let mut responses = Vec::new();

                for op in ops {
                    if let Some(request) = &op.request {
                        let response = match request {
                            super::Request::Put(put) => {
                                let prev_kv = match self.batch_put(
                                    &mut batch,
                                    &mut snapshot,
                                    &mut value_cache,
                                    put.key.clone(),
                                    &put.value,
                                    put.lease,
                                    revision,
                                ) {
                                    Ok(p) => p,
                                    Err(message) => {
                                        snapshot.rollback(self);
                                        return RaftResponse::Error {
                                            message: format!("txn put failed: {message}"),
                                        };
                                    }
                                };
                                let kv = self
                                    .key_metas
                                    .get(&put.key)
                                    .unwrap()
                                    .to_kv(put.key.clone(), put.value.clone());
                                watch_events.push(WatchEvent {
                                    event_type: super::WatchEventType::Put,
                                    kv,
                                    prev_kv: prev_kv.clone(),
                                });
                                super::Response::Put(super::PutResponse { prev_kv })
                            }
                            super::Request::Delete(del) => {
                                let (deleted, prev_kvs, events) = match self.batch_delete(
                                    &mut batch,
                                    &mut snapshot,
                                    &mut value_cache,
                                    del.key.clone(),
                                    del.range_end.clone(),
                                    revision,
                                ) {
                                    Ok(r) => r,
                                    Err(message) => {
                                        snapshot.rollback(self);
                                        return RaftResponse::Error {
                                            message: format!("txn delete failed: {message}"),
                                        };
                                    }
                                };
                                watch_events.extend(events);
                                super::Response::Delete(super::DeleteResponse { deleted, prev_kvs })
                            }
                            super::Request::Get(get) => {
                                let kvs = if get.range_end.is_empty() {
                                    match self.key_metas.get(&get.key) {
                                        Some(meta) => {
                                            let value = value_cache
                                                .get(&get.key)
                                                .cloned()
                                                .or_else(|| self.storage_get(&get.key))
                                                .unwrap_or_default();
                                            vec![meta.to_kv(get.key.clone(), value)]
                                        }
                                        None => vec![],
                                    }
                                } else {
                                    self.key_metas
                                        .iter()
                                        .filter(|(k, _)| {
                                            k.as_slice() >= get.key.as_slice()
                                                && (get.range_end == b"\0"
                                                    || k.as_slice() < get.range_end.as_slice())
                                        })
                                        .map(|(k, meta)| {
                                            let value = value_cache
                                                .get(k)
                                                .cloned()
                                                .or_else(|| self.storage_get(k))
                                                .unwrap_or_default();
                                            meta.to_kv(k.clone(), value)
                                        })
                                        .collect()
                                };
                                let count = kvs.len() as i64;
                                super::Response::Get(super::RangeResponse { kvs, count })
                            }
                            super::Request::Range(range) => {
                                let kvs: Vec<KeyValue> = if range.range_end.is_empty() {
                                    match self.key_metas.get(&range.key) {
                                        Some(meta) => {
                                            let value = value_cache
                                                .get(&range.key)
                                                .cloned()
                                                .or_else(|| self.storage_get(&range.key))
                                                .unwrap_or_default();
                                            vec![meta.to_kv(range.key.clone(), value)]
                                        }
                                        None => vec![],
                                    }
                                } else {
                                    self.key_metas
                                        .iter()
                                        .filter(|(k, _)| {
                                            k.as_slice() >= range.key.as_slice()
                                                && (range.range_end == b"\0"
                                                    || k.as_slice() < range.range_end.as_slice())
                                        })
                                        .map(|(k, meta)| {
                                            let value = value_cache
                                                .get(k)
                                                .cloned()
                                                .or_else(|| self.storage_get(k))
                                                .unwrap_or_default();
                                            meta.to_kv(k.clone(), value)
                                        })
                                        .collect()
                                };
                                let count = kvs.len() as i64;
                                super::Response::Range(super::RangeResponse { kvs, count })
                            }
                            super::Request::Txn(inner) => {
                                // Nested Txn: evaluate compares and execute sub-ops inline.
                                let inner_succeeded = inner.compare.iter().all(|cmp| {
                                    let meta = self.key_metas.get(&cmp.key);
                                    let current_value = value_cache
                                        .get(&cmp.key)
                                        .cloned()
                                        .or_else(|| self.storage_get(&cmp.key));
                                    Self::evaluate_compare(cmp, meta, current_value.as_deref())
                                });
                                let inner_ops = if inner_succeeded {
                                    &inner.success
                                } else {
                                    &inner.failure
                                };
                                let mut inner_responses = Vec::new();
                                for inner_op in inner_ops {
                                    if let Some(inner_req) = &inner_op.request {
                                        let inner_resp = match inner_req {
                                            super::Request::Put(p) => {
                                                let prev_kv = match self.batch_put(
                                                    &mut batch,
                                                    &mut snapshot,
                                                    &mut value_cache,
                                                    p.key.clone(),
                                                    &p.value,
                                                    p.lease,
                                                    revision,
                                                ) {
                                                    Ok(pk) => pk,
                                                    Err(msg) => {
                                                        snapshot.rollback(self);
                                                        return RaftResponse::Error {
                                                            message: format!(
                                                                "nested txn put failed: {msg}"
                                                            ),
                                                        };
                                                    }
                                                };
                                                let kv = self
                                                    .key_metas
                                                    .get(&p.key)
                                                    .unwrap()
                                                    .to_kv(p.key.clone(), p.value.clone());
                                                watch_events.push(WatchEvent {
                                                    event_type: super::WatchEventType::Put,
                                                    kv,
                                                    prev_kv: prev_kv.clone(),
                                                });
                                                super::Response::Put(super::PutResponse { prev_kv })
                                            }
                                            super::Request::Delete(d) => {
                                                let (deleted, prev_kvs, evts) = match self
                                                    .batch_delete(
                                                        &mut batch,
                                                        &mut snapshot,
                                                        &mut value_cache,
                                                        d.key.clone(),
                                                        d.range_end.clone(),
                                                        revision,
                                                    ) {
                                                    Ok(r) => r,
                                                    Err(msg) => {
                                                        snapshot.rollback(self);
                                                        return RaftResponse::Error {
                                                            message: format!(
                                                                "nested txn delete failed: {msg}"
                                                            ),
                                                        };
                                                    }
                                                };
                                                watch_events.extend(evts);
                                                super::Response::Delete(super::DeleteResponse {
                                                    deleted,
                                                    prev_kvs,
                                                })
                                            }
                                            super::Request::Get(g) => {
                                                let kvs = if g.range_end.is_empty() {
                                                    match self.key_metas.get(&g.key) {
                                                        Some(m) => {
                                                            let v = value_cache
                                                                .get(&g.key)
                                                                .cloned()
                                                                .or_else(|| {
                                                                    self.storage_get(&g.key)
                                                                })
                                                                .unwrap_or_default();
                                                            vec![m.to_kv(g.key.clone(), v)]
                                                        }
                                                        None => vec![],
                                                    }
                                                } else {
                                                    self.key_metas
                                                        .iter()
                                                        .filter(|(k, _)| {
                                                            k.as_slice() >= g.key.as_slice()
                                                                && (g.range_end == b"\0"
                                                                    || k.as_slice()
                                                                        < g.range_end.as_slice())
                                                        })
                                                        .map(|(k, m)| {
                                                            let v = value_cache
                                                                .get(k)
                                                                .cloned()
                                                                .or_else(|| self.storage_get(k))
                                                                .unwrap_or_default();
                                                            m.to_kv(k.clone(), v)
                                                        })
                                                        .collect()
                                                };
                                                let count = kvs.len() as i64;
                                                super::Response::Get(super::RangeResponse {
                                                    kvs,
                                                    count,
                                                })
                                            }
                                            super::Request::Range(r) => {
                                                let kvs: Vec<KeyValue> = if r.range_end.is_empty() {
                                                    match self.key_metas.get(&r.key) {
                                                        Some(m) => {
                                                            let v = value_cache
                                                                .get(&r.key)
                                                                .cloned()
                                                                .or_else(|| {
                                                                    self.storage_get(&r.key)
                                                                })
                                                                .unwrap_or_default();
                                                            vec![m.to_kv(r.key.clone(), v)]
                                                        }
                                                        None => vec![],
                                                    }
                                                } else {
                                                    self.key_metas
                                                        .iter()
                                                        .filter(|(k, _)| {
                                                            k.as_slice() >= r.key.as_slice()
                                                                && (r.range_end == b"\0"
                                                                    || k.as_slice()
                                                                        < r.range_end.as_slice())
                                                        })
                                                        .map(|(k, m)| {
                                                            let v = value_cache
                                                                .get(k)
                                                                .cloned()
                                                                .or_else(|| self.storage_get(k))
                                                                .unwrap_or_default();
                                                            m.to_kv(k.clone(), v)
                                                        })
                                                        .collect()
                                                };
                                                let count = kvs.len() as i64;
                                                super::Response::Range(super::RangeResponse {
                                                    kvs,
                                                    count,
                                                })
                                            }
                                            super::Request::Txn(_) => {
                                                snapshot.rollback(self);
                                                return RaftResponse::Error {
                                                    message: "nested txn depth > 1 not supported"
                                                        .into(),
                                                };
                                            }
                                        };
                                        inner_responses.push(super::ResponseOp {
                                            response: Some(inner_resp),
                                        });
                                    }
                                }
                                super::Response::Txn(Box::new(super::TxnResponse {
                                    succeeded: inner_succeeded,
                                    responses: inner_responses,
                                }))
                            }
                        };
                        responses.push(super::ResponseOp {
                            response: Some(response),
                        });
                    }
                }

                // Write global revision once for the entire Txn.
                save_global_revision(&mut batch, self.storage.meta_cf(), revision);

                // Atomically commit all sub-operations in a single write.
                if let Err(e) = self.storage.db().write(batch) {
                    tracing::error!(error = %e, "txn: failed to commit batch write");
                    snapshot.rollback(self);
                    return RaftResponse::Error {
                        message: format!("txn commit failed: {e}"),
                    };
                }
                snapshot.commit();

                // Emit watch events after successful commit.
                for event in watch_events {
                    let _ = self.watch_tx.send(event);
                }

                RaftResponse::Txn {
                    succeeded,
                    responses,
                }
            }
            RaftRequest::MemberAdd { member } => RaftResponse::MemberAdd { member },
            RaftRequest::MemberRemove { node_id: _ } => RaftResponse::MemberRemove {},
            RaftRequest::LeaseGrant { ttl, expiry_time } => {
                self.apply_lease_grant(ttl, expiry_time)
            }
            RaftRequest::LeaseRevoke { id } => self.apply_lease_revoke(id, revision),
            RaftRequest::LeaseKeepAlive { id, expiry_time } => {
                self.apply_lease_keep_alive(id, expiry_time)
            }
            RaftRequest::AuthUserAdd {
                name,
                password_hash,
            } => self.apply_auth_user_add(name, password_hash),
            RaftRequest::AuthUserDelete { name } => self.apply_auth_user_delete(name),
            RaftRequest::AuthUserChangePassword {
                name,
                password_hash,
            } => self.apply_auth_user_change_password(name, password_hash),
            RaftRequest::AuthUserGrantRole { user, role } => {
                self.apply_auth_user_grant_role(user, role)
            }
            RaftRequest::AuthUserRevokeRole { user, role } => {
                self.apply_auth_user_revoke_role(user, role)
            }
            RaftRequest::AuthRoleAdd { name } => self.apply_auth_role_add(name),
            RaftRequest::AuthRoleDelete { name } => self.apply_auth_role_delete(name),
            RaftRequest::AuthRoleGrantPermission { role, permission } => {
                self.apply_auth_role_grant_permission(role, permission)
            }
            RaftRequest::AuthRoleRevokePermission { role, permission } => {
                self.apply_auth_role_revoke_permission(role, permission)
            }
            RaftRequest::AuthEnable { root_password_hash } => {
                self.apply_auth_enable(root_password_hash)
            }
            RaftRequest::AuthDisable {} => self.apply_auth_disable(),
            RaftRequest::RegionSplit {
                region_id,
                split_key,
            } => self.apply_region_split(region_id, split_key),
            RaftRequest::RegionUpdate { region } => self.apply_region_update(region),
        }
    }

    fn compare_i64(result: super::CompareResult, current: i64, expected: i64) -> bool {
        match result {
            super::CompareResult::Equal => current == expected,
            super::CompareResult::NotEqual => current != expected,
            super::CompareResult::Greater => current > expected,
            super::CompareResult::Less => current < expected,
        }
    }

    fn evaluate_compare(
        cmp: &super::Compare,
        meta: Option<&KeyMeta>,
        current_value: Option<&[u8]>,
    ) -> bool {
        match (&cmp.target, &cmp.target_union) {
            (super::CompareTarget::Value, super::TargetUnion::Value(expected)) => {
                match cmp.result {
                    super::CompareResult::Equal => current_value == Some(expected.as_slice()),
                    super::CompareResult::NotEqual => current_value != Some(expected.as_slice()),
                    super::CompareResult::Greater => {
                        current_value.is_some_and(|v| v > expected.as_slice())
                    }
                    super::CompareResult::Less => {
                        current_value.is_some_and(|v| v < expected.as_slice())
                    }
                }
            }
            (super::CompareTarget::Version, super::TargetUnion::Version(expected)) => {
                let current = meta.map_or(0i64, |m| m.version);
                Self::compare_i64(cmp.result, current, *expected)
            }
            (super::CompareTarget::Create, super::TargetUnion::CreateRevision(expected)) => {
                let current = meta.map_or(0i64, |m| m.create_revision);
                Self::compare_i64(cmp.result, current, *expected)
            }
            (super::CompareTarget::Mod, super::TargetUnion::ModRevision(expected)) => {
                let current = meta.map_or(0i64, |m| m.mod_revision);
                Self::compare_i64(cmp.result, current, *expected)
            }
            (super::CompareTarget::Lease, super::TargetUnion::Lease(expected)) => {
                let current = meta.map_or(0i64, |m| m.lease);
                Self::compare_i64(cmp.result, current, *expected)
            }
            _ => false,
        }
    }

    fn apply_lease_grant(&mut self, ttl: i64, expiry_time: i64) -> RaftResponse {
        let mut mgr = self.lease_manager.lock().unwrap();
        if mgr.lease_count() >= mgr.max_leases() {
            return RaftResponse::Error {
                message: "max leases exceeded".to_string(),
            };
        }

        let id = mgr.next_lease_id();
        let lease = crate::lease::Lease::new(id, ttl, expiry_time);

        // Atomic WriteBatch: persist lease + counter in a single write.
        // Persist FIRST — if this fails, in-memory state is untouched.
        let lease_bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&lease.to_info());
        let lease_bytes = match lease_bytes {
            Ok(b) => b,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("failed to serialize lease: {e}"),
                };
            }
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.storage.lease_cf(),
            id.to_be_bytes(),
            lease_bytes.as_ref(),
        );
        batch.put_cf(
            self.storage
                .db()
                .cf_handle("meta")
                .expect("meta CF not found"),
            b"lease_counter",
            mgr.next_id().to_be_bytes(),
        );
        if let Err(e) = self.storage.db().write(batch) {
            return RaftResponse::Error {
                message: format!("failed to persist lease: {e}"),
            };
        }

        // Update in-memory state after successful persistence.
        mgr.grant(id, ttl, expiry_time);
        RaftResponse::LeaseGrant { id, ttl }
    }

    fn apply_lease_revoke(&mut self, id: i64, revision: u64) -> RaftResponse {
        // Read keys and values under lock, then drop before I/O.
        let key_values: Vec<(Vec<u8>, Vec<u8>, Option<KeyMeta>)> = {
            let mgr = self.lease_manager.lock().unwrap();
            let keys = match mgr.get_keys(id) {
                Some(s) => s.iter().cloned().collect::<Vec<_>>(),
                None => return RaftResponse::LeaseRevoke {}, // idempotent
            };
            keys.into_iter()
                .map(|k| {
                    let v = self.storage_get(&k).unwrap_or_default();
                    let meta = self.key_metas.get(&k).cloned();
                    (k, v, meta)
                })
                .collect()
        };

        // Prepare key_index mutations on clones.
        let mut ki_mutations: Vec<(Vec<u8>, KeyIndex)> = Vec::new();
        for (key, _, _) in &key_values {
            let mut new_ki = self.key_indexes.get(key).cloned().unwrap_or_default();
            new_ki.tombstone(revision);
            ki_mutations.push((key.clone(), new_ki));
        }

        let mut batch = rocksdb::WriteBatch::default();
        let default_cf = self.storage.default_cf();

        for (key, _, _) in &key_values {
            batch.delete_cf(default_cf, key);
            batch.delete_cf(self.storage.key_lease_cf(), key);
            let mut lk_key = Vec::with_capacity(8 + key.len());
            lk_key.extend_from_slice(&id.to_be_bytes());
            lk_key.extend_from_slice(key);
            batch.delete_cf(self.storage.lease_keys_cf(), lk_key);

            // Write MVCC tombstone.
            let mvcc_key = encode_mvcc_key(key, revision);
            batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, b"");
        }
        batch.delete_cf(self.storage.lease_cf(), id.to_be_bytes());
        save_global_revision(&mut batch, self.storage.meta_cf(), revision);

        if let Err(e) = self.storage.db().write(batch) {
            tracing::error!(error = %e, lease_id = id, "failed to batch delete on revoke");
            return RaftResponse::Error {
                message: format!("revoke batch write failed: {e}"),
            };
        }

        // Update in-memory state after successful persistence.
        for (key, ki) in ki_mutations {
            self.key_indexes.insert(key, ki);
        }
        for (key, _, _) in &key_values {
            self.key_metas.remove(key);
        }
        self.lease_manager.lock().unwrap().revoke(id);

        for (key, value, meta) in key_values {
            let kv = match meta {
                Some(m) => m.to_kv(key, value),
                None => KeyValue {
                    key,
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: id,
                },
            };
            let _ = self.watch_tx.send(WatchEvent {
                event_type: super::WatchEventType::Delete,
                kv,
                prev_kv: None,
            });
        }

        RaftResponse::LeaseRevoke {}
    }

    fn apply_lease_keep_alive(&mut self, id: i64, expiry_time: i64) -> RaftResponse {
        let mut mgr = self.lease_manager.lock().unwrap();

        // Check lease exists and read granted_ttl before modifying anything.
        let granted_ttl = match mgr.get(id) {
            Some(l) => l.granted_ttl,
            None => {
                return RaftResponse::Error {
                    message: format!("lease not found: {id}"),
                };
            }
        };

        // Persist FIRST — if this fails, in-memory state is untouched.
        let info = crate::lease::LeaseInfo {
            id,
            ttl: granted_ttl,
            granted_ttl,
            expiry_time,
        };
        if let Err(e) = self.lease_store.save_lease(&info) {
            return RaftResponse::Error {
                message: format!("failed to persist lease keep_alive: {e}"),
            };
        }

        // Update in-memory state after successful persistence.
        mgr.keep_alive(id, expiry_time);
        RaftResponse::LeaseKeepAlive { ttl: granted_ttl }
    }

    // --- Auth apply methods ---

    fn apply_auth_user_add(&mut self, name: Vec<u8>, password_hash: Vec<u8>) -> RaftResponse {
        let name = match String::from_utf8(name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid user name: {e}"),
                };
            }
        };
        let password_hash = match String::from_utf8(password_hash) {
            Ok(h) => h,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid password hash: {e}"),
                };
            }
        };
        if self.auth_cache.get_user(&name).is_some() {
            return RaftResponse::Error {
                message: format!("user already exists: {name}"),
            };
        }
        let user = crate::auth::User::new(name.clone(), password_hash);
        let key = crate::auth::user_key(&name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&user);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_user(user);
                RaftResponse::AuthUserAdd {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_user_delete(&mut self, name: Vec<u8>) -> RaftResponse {
        let name = match String::from_utf8(name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid user name: {e}"),
                };
            }
        };
        if self.auth_cache.get_user(&name).is_none() {
            return RaftResponse::Error {
                message: format!("user not found: {name}"),
            };
        }
        let key = crate::auth::user_key(&name);
        if let Err(e) = self.storage.delete(&key) {
            return RaftResponse::Error {
                message: format!("storage delete failed: {e}"),
            };
        }
        self.auth_cache.remove_user(&name);
        RaftResponse::AuthUserDelete {}
    }

    fn apply_auth_user_change_password(
        &mut self,
        name: Vec<u8>,
        password_hash: Vec<u8>,
    ) -> RaftResponse {
        let name = match String::from_utf8(name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid user name: {e}"),
                };
            }
        };
        let password_hash = match String::from_utf8(password_hash) {
            Ok(h) => h,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid password hash: {e}"),
                };
            }
        };
        let mut user = match self.auth_cache.get_user(&name) {
            Some(u) => u,
            None => {
                return RaftResponse::Error {
                    message: format!("user not found: {name}"),
                };
            }
        };
        user.password_hash = password_hash;
        let key = crate::auth::user_key(&name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&user);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_user(user);
                RaftResponse::AuthUserChangePassword {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_user_grant_role(&mut self, user: Vec<u8>, role: Vec<u8>) -> RaftResponse {
        let user_name = match String::from_utf8(user) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid user name: {e}"),
                };
            }
        };
        let role_name = match String::from_utf8(role) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        let mut u = match self.auth_cache.get_user(&user_name) {
            Some(u) => u,
            None => {
                return RaftResponse::Error {
                    message: format!("user not found: {user_name}"),
                };
            }
        };
        if u.roles.contains(&role_name) {
            return RaftResponse::Error {
                message: format!("user already has role: {role_name}"),
            };
        }
        if u.roles.len() >= 10 {
            return RaftResponse::Error {
                message: "max roles per user exceeded (10)".to_string(),
            };
        }
        u.roles.push(role_name);
        let key = crate::auth::user_key(&user_name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&u);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_user(u);
                RaftResponse::AuthUserGrantRole {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_user_revoke_role(&mut self, user: Vec<u8>, role: Vec<u8>) -> RaftResponse {
        let user_name = match String::from_utf8(user) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid user name: {e}"),
                };
            }
        };
        let role_name = match String::from_utf8(role) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        let mut u = match self.auth_cache.get_user(&user_name) {
            Some(u) => u,
            None => {
                return RaftResponse::Error {
                    message: format!("user not found: {user_name}"),
                };
            }
        };
        u.roles.retain(|r| r != &role_name);
        let key = crate::auth::user_key(&user_name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&u);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_user(u);
                RaftResponse::AuthUserRevokeRole {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_role_add(&mut self, name: Vec<u8>) -> RaftResponse {
        let name = match String::from_utf8(name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        if self.auth_cache.get_role(&name).is_some() {
            return RaftResponse::Error {
                message: format!("role already exists: {name}"),
            };
        }
        let role = crate::auth::Role::new(name.clone());
        let key = crate::auth::role_key(&name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&role);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_role(role);
                RaftResponse::AuthRoleAdd {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_role_delete(&mut self, name: Vec<u8>) -> RaftResponse {
        let name = match String::from_utf8(name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        if self.auth_cache.get_role(&name).is_none() {
            return RaftResponse::Error {
                message: format!("role not found: {name}"),
            };
        }
        if self.auth_cache.is_role_in_use(&name) {
            return RaftResponse::Error {
                message: format!("role is in use by a user: {name}"),
            };
        }
        let key = crate::auth::role_key(&name);
        if let Err(e) = self.storage.delete(&key) {
            return RaftResponse::Error {
                message: format!("storage delete failed: {e}"),
            };
        }
        self.auth_cache.remove_role(&name);
        RaftResponse::AuthRoleDelete {}
    }

    fn apply_auth_role_grant_permission(
        &mut self,
        role_name: Vec<u8>,
        permission: crate::auth::Permission,
    ) -> RaftResponse {
        let role_name = match String::from_utf8(role_name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        let mut role = match self.auth_cache.get_role(&role_name) {
            Some(r) => r,
            None => {
                return RaftResponse::Error {
                    message: format!("role not found: {role_name}"),
                };
            }
        };
        if role.permissions.len() >= 100 {
            return RaftResponse::Error {
                message: "max permissions per role exceeded (100)".to_string(),
            };
        }
        role.permissions.push(permission);
        let key = crate::auth::role_key(&role_name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&role);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_role(role);
                RaftResponse::AuthRoleGrantPermission {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_role_revoke_permission(
        &mut self,
        role_name: Vec<u8>,
        permission: crate::auth::Permission,
    ) -> RaftResponse {
        let role_name = match String::from_utf8(role_name) {
            Ok(n) => n,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid role name: {e}"),
                };
            }
        };
        let mut role = match self.auth_cache.get_role(&role_name) {
            Some(r) => r,
            None => {
                return RaftResponse::Error {
                    message: format!("role not found: {role_name}"),
                };
            }
        };
        role.permissions.retain(|p| {
            p.perm_type != permission.perm_type
                || p.key != permission.key
                || p.range_end != permission.range_end
        });
        let key = crate::auth::role_key(&role_name);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&role);
        match bytes {
            Ok(b) => {
                if let Err(e) = self.storage.put(&key, b.as_ref()) {
                    return RaftResponse::Error {
                        message: format!("storage write failed: {e}"),
                    };
                }
                self.auth_cache.insert_role(role);
                RaftResponse::AuthRoleRevokePermission {}
            }
            Err(e) => RaftResponse::Error {
                message: format!("serialize failed: {e}"),
            },
        }
    }

    fn apply_auth_enable(&mut self, root_password_hash: Vec<u8>) -> RaftResponse {
        // AuthEnable is only allowed when auth is currently disabled.
        // To change root password when auth is already on, use UserChangePassword.
        if self.auth_enabled.load(Ordering::Acquire) {
            return RaftResponse::Error {
                message: "auth is already enabled".to_string(),
            };
        }

        let root_name = "root".to_string();
        let password_hash = match String::from_utf8(root_password_hash) {
            Ok(h) => h,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("invalid password hash: {e}"),
                };
            }
        };

        // Create or update root user, then enable auth — all in one atomic batch.
        let root_user = if let Some(mut root_user) = self.auth_cache.get_user(&root_name) {
            root_user.password_hash = password_hash;
            root_user
        } else {
            let mut root_user = crate::auth::User::new(root_name.clone(), password_hash);
            root_user.enabled = true;
            root_user
        };

        let user_key = crate::auth::user_key(&root_name);
        let user_bytes = match rkyv::to_bytes::<rkyv::rancor::BoxedError>(&root_user) {
            Ok(b) => b,
            Err(e) => {
                return RaftResponse::Error {
                    message: format!("serialize failed: {e}"),
                };
            }
        };

        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(self.storage.default_cf(), &user_key, user_bytes.as_ref());
        batch.put_cf(
            self.storage.default_cf(),
            crate::auth::AUTH_ENABLED_KEY,
            b"true",
        );
        batch.put_cf(
            self.storage.default_cf(),
            crate::auth::AUTH_BOOTSTRAPPED_KEY,
            b"true",
        );
        if let Err(e) = self.storage.db().write(batch) {
            return RaftResponse::Error {
                message: format!("storage write failed: {e}"),
            };
        }
        // Update in-memory state after successful atomic write.
        self.auth_cache.insert_user(root_user);
        self.auth_enabled.store(true, Ordering::Release);
        RaftResponse::AuthEnable {}
    }

    fn apply_auth_disable(&mut self) -> RaftResponse {
        // Idempotent: if already disabled, return success without side effects
        if !self.auth_enabled.load(Ordering::Acquire) {
            return RaftResponse::AuthDisable {};
        }
        if let Err(e) = self.storage.delete(crate::auth::AUTH_ENABLED_KEY) {
            return RaftResponse::Error {
                message: format!("storage write failed: {e}"),
            };
        }
        // Update the shared auth_enabled flag so the interceptor takes effect immediately
        self.auth_enabled.store(false, Ordering::Release);
        RaftResponse::AuthDisable {}
    }

    fn apply_region_split(&mut self, region_id: u64, split_key: Vec<u8>) -> RaftResponse {
        let mut mgr = self.shard_manager.lock().unwrap();
        let max_regions = mgr.max_regions();

        let (parent, child) = match mgr.apply_split(region_id, split_key) {
            Ok(result) => result,
            Err(message) => return RaftResponse::Error { message },
        };

        let mut batch = WriteBatch::default();
        if let Err(message) = mgr.save_region_to_batch(&mut batch, &self.storage, &parent) {
            drop(mgr);
            let mut mgr = self.shard_manager.lock().unwrap();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
            return RaftResponse::Error { message };
        }
        if let Err(message) = mgr.save_region_to_batch(&mut batch, &self.storage, &child) {
            drop(mgr);
            let mut mgr = self.shard_manager.lock().unwrap();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
            return RaftResponse::Error { message };
        }

        if let Err(e) = self.storage.db().write(batch) {
            tracing::error!(error = %e, "failed to persist region split");
            drop(mgr);
            let mut mgr = self.shard_manager.lock().unwrap();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
            return RaftResponse::Error {
                message: format!("region split write failed: {e}"),
            };
        }

        tracing::info!(
            parent_id = parent.id,
            child_id = child.id,
            parent_end = %String::from_utf8_lossy(&parent.end_key),
            child_start = %String::from_utf8_lossy(&child.start_key),
            "region split applied"
        );

        RaftResponse::RegionSplit { parent, child }
    }

    fn apply_region_update(&mut self, region: crate::shard::Region) -> RaftResponse {
        let mut mgr = self.shard_manager.lock().unwrap();
        let max_regions = mgr.max_regions();

        if let Err(message) = mgr.apply_update(region.clone()) {
            return RaftResponse::Error { message };
        }

        let mut batch = WriteBatch::default();
        if let Err(message) = mgr.save_region_to_batch(&mut batch, &self.storage, &region) {
            drop(mgr);
            let mut mgr = self.shard_manager.lock().unwrap();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
            return RaftResponse::Error { message };
        }

        if let Err(e) = self.storage.db().write(batch) {
            tracing::error!(error = %e, "failed to persist region update");
            drop(mgr);
            let mut mgr = self.shard_manager.lock().unwrap();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
            return RaftResponse::Error {
                message: format!("region update write failed: {e}"),
            };
        }

        RaftResponse::RegionUpdate {}
    }

    /// Maximum allowed snapshot size (512 MiB).  Prevents OOM when the database
    /// is larger than available memory.  The serialized snapshot is typically
    /// 1-2x the raw CF data size, so this limits total memory to ~1.5 GiB.
    const MAX_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;

    /// Serialize the current state machine state into snapshot bytes.
    ///
    /// Captures all user-data column families (default, mvcc, meta, lease,
    /// lease_keys, key_lease) so a follower can restore from this blob alone.
    ///
    /// This is a free function (not `&self`) because it only reads from
    /// RocksDB, which is thread-safe.  Callers do **not** need to hold the
    /// state machine lock, avoiding event-loop stalls during large scans.
    ///
    /// Returns an error if the cumulative CF data exceeds [`MAX_SNAPSHOT_BYTES`].
    pub fn create_snapshot(storage: &RocksStorage) -> Result<Vec<u8>, String> {
        let db = storage.db();
        let mut total_bytes: usize = 0;

        let default_entries = Self::cf_to_vec_checked(db, storage.default_cf(), &mut total_bytes)
            .map_err(|e| format!("snapshot default CF: {e}"))?;
        let mvcc_entries = Self::cf_to_vec_checked(db, storage.mvcc_cf(), &mut total_bytes)
            .map_err(|e| format!("snapshot mvcc CF: {e}"))?;
        let meta_entries = Self::cf_to_vec_checked(db, storage.meta_cf(), &mut total_bytes)
            .map_err(|e| format!("snapshot meta CF: {e}"))?;
        let lease_entries = Self::cf_to_vec_checked(db, storage.lease_cf(), &mut total_bytes)
            .map_err(|e| format!("snapshot lease CF: {e}"))?;
        let lease_keys_entries =
            Self::cf_to_vec_checked(db, storage.lease_keys_cf(), &mut total_bytes)
                .map_err(|e| format!("snapshot lease_keys CF: {e}"))?;
        let key_lease_entries =
            Self::cf_to_vec_checked(db, storage.key_lease_cf(), &mut total_bytes)
                .map_err(|e| format!("snapshot key_lease CF: {e}"))?;
        let region_entries = Self::cf_to_vec_checked(db, storage.region_cf(), &mut total_bytes)
            .map_err(|e| format!("snapshot region CF: {e}"))?;

        let snapshot = SnapshotCfData {
            default_entries,
            mvcc_entries,
            meta_entries,
            lease_entries,
            lease_keys_entries,
            key_lease_entries,
            region_entries,
        };

        bincode::serialize(&snapshot).map_err(|e| format!("serialize snapshot: {e}"))
    }

    /// Restore state machine from snapshot bytes, replacing all existing data.
    ///
    /// Clears the user-data column families, replays the snapshot entries, and
    /// rebuilds the in-memory indexes (`key_indexes`, `key_metas`, `key_leases`)
    /// plus the `LeaseManager`.
    ///
    /// The entire operation (clear + write) is performed in a single RocksDB
    /// WriteBatch for atomicity — a crash at any point leaves the database in
    /// either the old state or the new state, never a partial mix.
    pub fn restore_snapshot(&mut self, data: &[u8], applied_index: u64) -> Result<(), String> {
        // Reject oversized snapshots before attempting deserialization to prevent
        // unbounded memory allocation from a corrupt/malicious payload.
        if data.len() > Self::MAX_SNAPSHOT_BYTES {
            return Err(format!(
                "snapshot payload ({} bytes) exceeds {} MiB limit",
                data.len(),
                Self::MAX_SNAPSHOT_BYTES / (1024 * 1024),
            ));
        }
        let snapshot: SnapshotCfData =
            bincode::deserialize(data).map_err(|e| format!("deserialize snapshot: {e}"))?;

        let db = self.storage.db();

        // Build a single atomic batch: delete all existing keys from user CFs,
        // then write all snapshot entries.  If the process crashes before the
        // batch is committed, the old data is untouched.
        let mut batch = WriteBatch::default();

        // Delete existing keys from all user-data column families.
        Self::delete_cf_keys(&mut batch, db, self.storage.default_cf())
            .map_err(|e| format!("collect default CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.mvcc_cf())
            .map_err(|e| format!("collect mvcc CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.meta_cf())
            .map_err(|e| format!("collect meta CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.lease_cf())
            .map_err(|e| format!("collect lease CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.lease_keys_cf())
            .map_err(|e| format!("collect lease_keys CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.key_lease_cf())
            .map_err(|e| format!("collect key_lease CF keys: {e}"))?;
        Self::delete_cf_keys(&mut batch, db, self.storage.region_cf())
            .map_err(|e| format!("collect region CF keys: {e}"))?;

        // Write all snapshot entries.
        Self::load_batch(
            &mut batch,
            self.storage.default_cf(),
            &snapshot.default_entries,
        );
        Self::load_batch(&mut batch, self.storage.mvcc_cf(), &snapshot.mvcc_entries);
        Self::load_batch(&mut batch, self.storage.meta_cf(), &snapshot.meta_entries);
        Self::load_batch(&mut batch, self.storage.lease_cf(), &snapshot.lease_entries);
        Self::load_batch(
            &mut batch,
            self.storage.lease_keys_cf(),
            &snapshot.lease_keys_entries,
        );
        Self::load_batch(
            &mut batch,
            self.storage.key_lease_cf(),
            &snapshot.key_lease_entries,
        );
        Self::load_batch(
            &mut batch,
            self.storage.region_cf(),
            &snapshot.region_entries,
        );

        // Commit atomically.
        db.write(batch)
            .map_err(|e| format!("write snapshot batch: {e}"))?;

        // Rebuild in-memory state from persisted data.
        self.key_indexes = load_key_indexes(db, self.storage.mvcc_cf())
            .map_err(|e| format!("reload key indexes: {e}"))?;
        self.key_metas = Self::load_key_metas_from_indexes(&self.storage, &self.key_indexes);
        self.key_leases = Self::load_key_leases_from_meta(&self.key_metas);
        {
            let mut mgr = self.shard_manager.lock().unwrap();
            let max_regions = mgr.max_regions();
            *mgr = ShardManager::load_from_storage_with_limit(&self.storage, max_regions);
        }

        // Rebuild lease manager from persisted lease data.
        {
            let mut mgr = self.lease_manager.lock().unwrap();
            if let Err(e) = mgr.restore(&self.lease_store) {
                tracing::warn!(error = %e, "failed to restore lease manager from snapshot");
            }
        }

        self.last_applied = applied_index;
        tracing::info!(
            applied_index,
            keys = self.key_metas.len(),
            "restored from snapshot"
        );
        Ok(())
    }

    /// Read all key-value pairs from a column family, accumulating total byte
    /// count.  Returns an error if the running total exceeds
    /// [`MAX_SNAPSHOT_BYTES`].
    #[allow(clippy::type_complexity)]
    fn cf_to_vec_checked(
        db: &rocksdb::DB,
        cf: &rocksdb::ColumnFamily,
        total_bytes: &mut usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut entries = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(|e| format!("iterator error: {e}"))?;
            let entry_size = k.len() + v.len();
            // Check BEFORE allocating to avoid exceeding the limit in memory.
            if *total_bytes + entry_size > Self::MAX_SNAPSHOT_BYTES {
                return Err(format!(
                    "snapshot data exceeds {} MiB limit (at least {} bytes so far)",
                    Self::MAX_SNAPSHOT_BYTES / (1024 * 1024),
                    *total_bytes + entry_size,
                ));
            }
            *total_bytes += entry_size;
            entries.push((k.to_vec(), v.to_vec()));
        }
        Ok(entries)
    }

    /// Enqueue delete operations for all keys in a column family into `batch`.
    fn delete_cf_keys(
        batch: &mut WriteBatch,
        db: &rocksdb::DB,
        cf: &rocksdb::ColumnFamily,
    ) -> Result<(), rocksdb::Error> {
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, _) = item?;
            batch.delete_cf(cf, &k);
        }
        Ok(())
    }

    /// Load snapshot entries into a WriteBatch.
    fn load_batch(
        batch: &mut WriteBatch,
        cf: &rocksdb::ColumnFamily,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) {
        for (k, v) in entries {
            batch.put_cf(cf, k, v);
        }
    }

    /// Apply a normal entry's data (after request_id prefix) for raft-rs.
    /// `revision` is the Raft log entry index, used as the MVCC global revision.
    /// Returns serialized RaftResponse, or an error string on deserialization failure.
    pub fn apply_normal_entry(&mut self, data: &[u8], revision: u64) -> Result<Vec<u8>, String> {
        let request: RaftRequest = bincode::deserialize(data).map_err(|e| {
            tracing::error!(error = %e, "failed to deserialize RaftRequest");
            format!("deserialize failed: {e}")
        })?;
        let response = self.apply_request(request, revision);
        bincode::serialize(&response).map_err(|e| format!("serialize failed: {e}"))
    }

    /// Apply a configuration change for raft-rs.
    pub fn apply_conf_change(&mut self, cc: &raft::eraftpb::ConfChange, index: u64) {
        self.last_applied = index;
        tracing::info!(
            change_type = ?cc.change_type,
            node_id = cc.node_id,
            index = index,
            "applied conf change"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::now_millis;
    use crate::raft::{self, WatchEventType};
    use tempfile::tempdir;

    fn setup() -> (tempfile::TempDir, Arc<RocksStorage>, AetherStateMachine) {
        let dir = tempdir().unwrap();
        let storage = Arc::new(RocksStorage::open(dir.path()).unwrap());
        let lease_store = LeaseStore::new(storage.clone());
        let (lease_manager, _expiry_rx) = LeaseManager::new(10000, 1);
        let lease_manager = Arc::new(Mutex::new(lease_manager));
        let (tx, _rx) = tokio::sync::broadcast::channel(64);
        let auth_cache = Arc::new(AuthCache::new());
        let auth_enabled = Arc::new(AtomicBool::new(false));
        let shard_manager = Arc::new(Mutex::new(ShardManager::new()));
        let sm = AetherStateMachine::new(
            tx.clone(),
            storage.clone(),
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
            shard_manager,
        );
        (dir, storage, sm)
    }

    #[test]
    fn test_put_and_get() {
        let (_dir, storage, mut sm) = setup();

        let req = RaftRequest::Put {
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            lease_id: 0,
        };
        let resp = sm.apply_request(req, 1);
        match resp {
            RaftResponse::Put { prev_kv } => assert!(prev_kv.is_none()),
            other => panic!("expected Put response, got: {other:?}"),
        }

        let stored = storage.get(b"key1").unwrap();
        assert_eq!(stored, Some(b"val1".to_vec()));
    }

    #[test]
    fn test_put_returns_prev_kv() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
                lease_id: 0,
            },
            2,
        );
        match resp {
            RaftResponse::Put { prev_kv } => {
                let kv = prev_kv.expect("should have prev_kv");
                assert_eq!(kv.key, b"k");
                assert_eq!(kv.value, b"v1");
            }
            other => panic!("expected Put response, got: {other:?}"),
        }
    }

    #[test]
    fn test_delete_single_key() {
        let (_dir, storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Delete {
                key: b"k".to_vec(),
                range_end: vec![],
            },
            2,
        );
        match resp {
            RaftResponse::Delete { deleted, prev_kvs } => {
                assert_eq!(deleted, 1);
                assert_eq!(prev_kvs.len(), 1);
                assert_eq!(prev_kvs[0].value, b"v");
            }
            other => panic!("expected Delete response, got: {other:?}"),
        }

        assert_eq!(storage.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_delete_missing_key() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(
            RaftRequest::Delete {
                key: b"missing".to_vec(),
                range_end: vec![],
            },
            1,
        );
        match resp {
            RaftResponse::Delete { deleted, prev_kvs } => {
                assert_eq!(deleted, 0);
                assert!(prev_kvs.is_empty());
            }
            other => panic!("expected Delete response, got: {other:?}"),
        }
    }

    #[test]
    fn test_member_add_passthrough() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(
            RaftRequest::MemberAdd {
                member: raft::RaftNode {
                    addr: "127.0.0.1:2380".to_string(),
                    data: String::new(),
                },
            },
            1,
        );
        match resp {
            RaftResponse::MemberAdd { member } => {
                assert_eq!(member.addr, "127.0.0.1:2380");
            }
            other => panic!("expected MemberAdd response, got: {other:?}"),
        }
    }

    #[test]
    fn test_member_remove_passthrough() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(RaftRequest::MemberRemove { node_id: 2 }, 1);
        match resp {
            RaftResponse::MemberRemove {} => {}
            other => panic!("expected MemberRemove response, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_normal_entry_roundtrip() {
        let (_dir, _storage, mut sm) = setup();

        let req = RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
        };
        let data = bincode::serialize(&req).unwrap();

        let resp_bytes = sm.apply_normal_entry(&data, 1).unwrap();
        let resp: RaftResponse = bincode::deserialize(&resp_bytes).unwrap();
        match resp {
            RaftResponse::Put { prev_kv } => assert!(prev_kv.is_none()),
            other => panic!("expected Put, got: {other:?}"),
        }
    }

    #[test]
    fn test_watch_event_on_put() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(RocksStorage::open(dir.path()).unwrap());
        let lease_store = LeaseStore::new(storage.clone());
        let (lease_manager, _expiry_rx) = LeaseManager::new(10000, 1);
        let lease_manager = Arc::new(Mutex::new(lease_manager));
        let (tx, mut rx) = tokio::sync::broadcast::channel(64);
        let auth_cache = Arc::new(AuthCache::new());
        let auth_enabled = Arc::new(AtomicBool::new(false));
        let shard_manager = Arc::new(Mutex::new(ShardManager::new()));
        let mut sm = AetherStateMachine::new(
            tx,
            storage,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
            shard_manager,
        );

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 42,
            },
            1,
        );

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Put);
        assert_eq!(event.kv.key, b"k");
        assert_eq!(event.kv.value, b"v");
        assert_eq!(event.kv.lease, 42);
    }

    #[test]
    fn test_watch_event_on_delete() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(RocksStorage::open(dir.path()).unwrap());
        let lease_store = LeaseStore::new(storage.clone());
        let (lease_manager, _expiry_rx) = LeaseManager::new(10000, 1);
        let lease_manager = Arc::new(Mutex::new(lease_manager));
        let (tx, mut rx) = tokio::sync::broadcast::channel(64);
        let auth_cache = Arc::new(AuthCache::new());
        let auth_enabled = Arc::new(AtomicBool::new(false));
        let shard_manager = Arc::new(Mutex::new(ShardManager::new()));
        let mut sm = AetherStateMachine::new(
            tx,
            storage,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
            shard_manager,
        );

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );
        let _ = rx.try_recv(); // consume put event

        sm.apply_request(
            RaftRequest::Delete {
                key: b"k".to_vec(),
                range_end: vec![],
            },
            2,
        );

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Delete);
        assert_eq!(event.kv.key, b"k");
    }

    #[test]
    fn test_lease_grant_and_revoke() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 10,
                expiry_time: now_millis() + 10_000,
            },
            1,
        );
        match resp {
            RaftResponse::LeaseGrant { id, ttl } => {
                assert_eq!(id, 1);
                assert_eq!(ttl, 10);
            }
            other => panic!("expected LeaseGrant, got: {other:?}"),
        }

        let resp = sm.apply_request(RaftRequest::LeaseRevoke { id: 1 }, 2);
        match resp {
            RaftResponse::LeaseRevoke {} => {}
            other => panic!("expected LeaseRevoke, got: {other:?}"),
        }
    }

    #[test]
    fn test_lease_keep_alive() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 10,
                expiry_time: now_millis() + 10_000,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::LeaseKeepAlive {
                id: 1,
                expiry_time: now_millis() + 20_000,
            },
            2,
        );
        match resp {
            RaftResponse::LeaseKeepAlive { ttl } => assert_eq!(ttl, 10),
            other => panic!("expected LeaseKeepAlive, got: {other:?}"),
        }
    }

    #[test]
    fn test_put_with_lease_attach() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 10,
                expiry_time: now_millis() + 10_000,
            },
            1,
        );

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 1,
            },
            2,
        );

        assert!(
            sm.lease_manager
                .lock()
                .unwrap()
                .get_keys(1)
                .unwrap()
                .contains(&b"k"[..])
        );

        let lease_id = sm.lease_store.get_key_lease_id(b"k").unwrap();
        assert_eq!(lease_id, Some(1));
    }

    #[test]
    fn test_revoke_deletes_attached_keys() {
        let (_dir, storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 10,
                expiry_time: now_millis() + 10_000,
            },
            1,
        );
        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 1,
            },
            2,
        );

        sm.apply_request(RaftRequest::LeaseRevoke { id: 1 }, 3);

        assert_eq!(storage.get(b"k").unwrap(), None);
        assert!(sm.lease_manager.lock().unwrap().get(1).is_none());
    }

    #[test]
    fn test_txn_multi_put_atomic() {
        let (_dir, storage, mut sm) = setup();

        // Txn that puts two keys — both should appear atomically.
        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![],
                success: vec![
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k1".to_vec(),
                            value: b"v1".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k2".to_vec(),
                            value: b"v2".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                ],
                failure: vec![],
            },
            1,
        );
        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 2);
            }
            other => panic!("expected Txn, got: {other:?}"),
        }

        // Both keys should be visible at the same revision.
        assert_eq!(storage.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(storage.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(sm.key_metas.get(b"k1".as_slice()).unwrap().mod_revision, 1);
        assert_eq!(sm.key_metas.get(b"k2".as_slice()).unwrap().mod_revision, 1);
    }

    #[test]
    fn test_txn_put_delete_atomic() {
        let (_dir, storage, mut sm) = setup();

        // Pre-populate a key.
        sm.apply_request(
            RaftRequest::Put {
                key: b"k1".to_vec(),
                value: b"old".to_vec(),
                lease_id: 0,
            },
            1,
        );

        // Txn: overwrite k1 and delete k1 — both at the same revision.
        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![],
                success: vec![
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k1".to_vec(),
                            value: b"new".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                    raft::RequestOp {
                        request: Some(raft::Request::Delete(raft::DeleteRequest {
                            key: b"k1".to_vec(),
                            range_end: vec![],
                            prev_kv: false,
                        })),
                    },
                ],
                failure: vec![],
            },
            2,
        );
        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 2);
            }
            other => panic!("expected Txn, got: {other:?}"),
        }

        // k1 should be deleted (the delete came after the put in the same batch).
        assert_eq!(storage.get(b"k1").unwrap(), None);
        assert!(!sm.key_metas.contains_key(b"k1".as_slice()));
    }

    #[test]
    fn test_txn_watch_events_emitted() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(RocksStorage::open(dir.path()).unwrap());
        let lease_store = LeaseStore::new(storage.clone());
        let (lease_manager, _expiry_rx) = LeaseManager::new(10000, 1);
        let lease_manager = Arc::new(Mutex::new(lease_manager));
        let (tx, mut rx) = tokio::sync::broadcast::channel(64);
        let auth_cache = Arc::new(AuthCache::new());
        let auth_enabled = Arc::new(AtomicBool::new(false));
        let shard_manager = Arc::new(Mutex::new(ShardManager::new()));
        let mut sm = AetherStateMachine::new(
            tx,
            storage,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
            shard_manager,
        );

        sm.apply_request(
            RaftRequest::Txn {
                compare: vec![],
                success: vec![
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k1".to_vec(),
                            value: b"v1".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k2".to_vec(),
                            value: b"v2".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                ],
                failure: vec![],
            },
            1,
        );

        // Should receive two watch events.
        let ev1 = rx.try_recv().unwrap();
        assert_eq!(ev1.event_type, WatchEventType::Put);
        let ev2 = rx.try_recv().unwrap();
        assert_eq!(ev2.event_type, WatchEventType::Put);
        assert!(rx.try_recv().is_err());
    }

    // --- CAS (Compare-And-Swap) tests ---

    #[test]
    fn test_cas_version_equal_success() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 1);
            }
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert_eq!(sm.key_metas.get(b"k".as_slice()).unwrap().version, 2);
    }

    #[test]
    fn test_cas_version_equal_failure() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(999),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"should-not-happen".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"fallback".to_vec(),
                        value: b"executed".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
            },
            2,
        );

        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(!succeeded);
                assert_eq!(responses.len(), 1);
            }
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert_eq!(sm.key_metas.get(b"k".as_slice()).unwrap().version, 1);
        assert!(sm.key_metas.contains_key(b"fallback".as_slice()));
    }

    #[test]
    fn test_cas_value_equal_success() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"expected".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Value,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Value(b"expected".to_vec()),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"updated".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_value_not_equal() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::NotEqual,
                    target: raft::CompareTarget::Value,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Value(b"other".to_vec()),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_create_revision() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Create,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::CreateRevision(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_mod_revision() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Mod,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::ModRevision(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_lease_equal() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 10,
                expiry_time: now_millis() + 10_000,
            },
            1,
        );

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 1,
            },
            2,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Lease,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Lease(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            3,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_greater_than() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );
        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
                lease_id: 0,
            },
            2,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Greater,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v3".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            3,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_less_than() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Less,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(5),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_multiple_compares() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );
        sm.apply_request(
            RaftRequest::Put {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                lease_id: 0,
            },
            2,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![
                    raft::Compare {
                        result: raft::CompareResult::Equal,
                        target: raft::CompareTarget::Version,
                        key: b"k1".to_vec(),
                        target_union: raft::TargetUnion::Version(1),
                    },
                    raft::Compare {
                        result: raft::CompareResult::Equal,
                        target: raft::CompareTarget::Value,
                        key: b"k2".to_vec(),
                        target_union: raft::TargetUnion::Value(b"v2".to_vec()),
                    },
                ],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"result".to_vec(),
                        value: b"ok".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            3,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(sm.key_metas.contains_key(b"result".as_slice()));
    }

    #[test]
    fn test_cas_multiple_compares_one_fails() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
            },
            1,
        );
        sm.apply_request(
            RaftRequest::Put {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                lease_id: 0,
            },
            2,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![
                    raft::Compare {
                        result: raft::CompareResult::Equal,
                        target: raft::CompareTarget::Version,
                        key: b"k1".to_vec(),
                        target_union: raft::TargetUnion::Version(1),
                    },
                    raft::Compare {
                        result: raft::CompareResult::Equal,
                        target: raft::CompareTarget::Value,
                        key: b"k2".to_vec(),
                        target_union: raft::TargetUnion::Value(b"wrong".to_vec()),
                    },
                ],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"should-not-exist".to_vec(),
                        value: b"nope".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"fallback".to_vec(),
                        value: b"executed".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
            },
            3,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(!succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(!sm.key_metas.contains_key(b"should-not-exist".as_slice()));
        assert!(sm.key_metas.contains_key(b"fallback".as_slice()));
    }

    #[test]
    fn test_cas_nonexistent_key_version_zero() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"no-such-key".to_vec(),
                    target_union: raft::TargetUnion::Version(0),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Put(raft::PutRequest {
                        key: b"no-such-key".to_vec(),
                        value: b"created".to_vec(),
                        lease: 0,
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            1,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(sm.key_metas.contains_key(b"no-such-key".as_slice()));
    }

    #[test]
    fn test_cas_get_in_success_branch() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Get(raft::RangeRequest {
                        key: b"k".to_vec(),
                        range_end: vec![],
                        limit: 0,
                        revision: 0,
                        sort_order: raft::SortOrder::None,
                        sort_target: raft::SortTarget::Key,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 1);
                match &responses[0].response {
                    Some(raft::Response::Get(r)) => {
                        assert_eq!(r.kvs.len(), 1);
                        assert_eq!(r.kvs[0].key, b"k");
                    }
                    other => panic!("expected Get response, got: {other:?}"),
                }
            }
            other => panic!("expected Txn, got: {other:?}"),
        }
    }

    #[test]
    fn test_cas_delete_in_success_branch() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Delete(raft::DeleteRequest {
                        key: b"k".to_vec(),
                        range_end: vec![],
                        prev_kv: false,
                    })),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn { succeeded, .. } => assert!(succeeded),
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(!sm.key_metas.contains_key(b"k".as_slice()));
    }

    #[test]
    fn test_nested_txn() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(
            RaftRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
            },
            1,
        );

        // Outer Txn: if version == 1, execute inner Txn that puts two keys.
        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![raft::Compare {
                    result: raft::CompareResult::Equal,
                    target: raft::CompareTarget::Version,
                    key: b"k".to_vec(),
                    target_union: raft::TargetUnion::Version(1),
                }],
                success: vec![raft::RequestOp {
                    request: Some(raft::Request::Txn(Box::new(raft::TxnRequest {
                        compare: vec![],
                        success: vec![
                            raft::RequestOp {
                                request: Some(raft::Request::Put(raft::PutRequest {
                                    key: b"nk1".to_vec(),
                                    value: b"nv1".to_vec(),
                                    lease: 0,
                                    prev_kv: false,
                                })),
                            },
                            raft::RequestOp {
                                request: Some(raft::Request::Put(raft::PutRequest {
                                    key: b"nk2".to_vec(),
                                    value: b"nv2".to_vec(),
                                    lease: 0,
                                    prev_kv: false,
                                })),
                            },
                        ],
                        failure: vec![],
                    }))),
                }],
                failure: vec![],
            },
            2,
        );

        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 1);
                // The inner response should be a Txn response.
                match &responses[0].response {
                    Some(raft::Response::Txn(inner)) => {
                        assert!(inner.succeeded);
                        assert_eq!(inner.responses.len(), 2);
                    }
                    other => panic!("expected nested Txn response, got: {other:?}"),
                }
            }
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(sm.key_metas.contains_key(b"nk1".as_slice()));
        assert!(sm.key_metas.contains_key(b"nk2".as_slice()));
    }

    #[test]
    fn test_nested_txn_compare_reads_uncommitted_value() {
        let (_dir, _storage, mut sm) = setup();

        // Outer Txn: Put "k" = "new", then nested Txn checks value == "new".
        let resp = sm.apply_request(
            RaftRequest::Txn {
                compare: vec![],
                success: vec![
                    raft::RequestOp {
                        request: Some(raft::Request::Put(raft::PutRequest {
                            key: b"k".to_vec(),
                            value: b"new".to_vec(),
                            lease: 0,
                            prev_kv: false,
                        })),
                    },
                    raft::RequestOp {
                        request: Some(raft::Request::Txn(Box::new(raft::TxnRequest {
                            compare: vec![raft::Compare {
                                result: raft::CompareResult::Equal,
                                target: raft::CompareTarget::Value,
                                key: b"k".to_vec(),
                                target_union: raft::TargetUnion::Value(b"new".to_vec()),
                            }],
                            success: vec![raft::RequestOp {
                                request: Some(raft::Request::Put(raft::PutRequest {
                                    key: b"result".to_vec(),
                                    value: b"ok".to_vec(),
                                    lease: 0,
                                    prev_kv: false,
                                })),
                            }],
                            failure: vec![],
                        }))),
                    },
                ],
                failure: vec![],
            },
            1,
        );

        match resp {
            RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(responses.len(), 2);
                // Nested Txn should have succeeded (compare read uncommitted value).
                match &responses[1].response {
                    Some(raft::Response::Txn(inner)) => {
                        assert!(inner.succeeded);
                    }
                    other => panic!("expected nested Txn response, got: {other:?}"),
                }
            }
            other => panic!("expected Txn, got: {other:?}"),
        }
        assert!(sm.key_metas.contains_key(b"result".as_slice()));
    }

    #[test]
    fn test_create_and_restore_snapshot_roundtrip() {
        let (_dir, storage, mut sm) = setup();

        // Write some data.
        sm.apply_request(
            RaftRequest::Put {
                key: b"key1".to_vec(),
                value: b"val1".to_vec(),
                lease_id: 0,
            },
            1,
        );
        sm.apply_request(
            RaftRequest::Put {
                key: b"key2".to_vec(),
                value: b"val2".to_vec(),
                lease_id: 0,
            },
            2,
        );

        // Create snapshot (uses storage directly, no lock needed).
        let snapshot_data = AetherStateMachine::create_snapshot(&storage).unwrap();
        assert!(!snapshot_data.is_empty());

        // Create a fresh state machine from the same storage to verify restore.
        let lease_store2 = LeaseStore::new(storage.clone());
        let (lease_manager2, _rx2) = LeaseManager::new(10000, 1);
        let lease_manager2 = Arc::new(Mutex::new(lease_manager2));
        let (tx2, _rx2) = tokio::sync::broadcast::channel(64);
        let auth_cache2 = Arc::new(AuthCache::new());
        let auth_enabled2 = Arc::new(AtomicBool::new(false));
        let shard_manager2 = Arc::new(Mutex::new(ShardManager::new()));
        let mut sm2 = AetherStateMachine::new(
            tx2,
            storage.clone(),
            lease_manager2,
            lease_store2,
            auth_cache2,
            auth_enabled2,
            shard_manager2,
        );

        // Write different data to sm2 to verify it gets overwritten.
        sm2.apply_request(
            RaftRequest::Put {
                key: b"other".to_vec(),
                value: b"data".to_vec(),
                lease_id: 0,
            },
            1,
        );

        // Restore from snapshot.
        sm2.restore_snapshot(&snapshot_data, 2).unwrap();

        // Verify data matches original.
        assert_eq!(storage.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(storage.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        // The "other" key should be gone (cleared by restore).
        assert_eq!(storage.get(b"other").unwrap(), None);
        assert_eq!(sm2.last_applied, 2);
        assert!(sm2.key_metas.contains_key(b"key1".as_slice()));
        assert!(sm2.key_metas.contains_key(b"key2".as_slice()));
    }

    #[test]
    fn test_snapshot_preserves_lease_associations() {
        let (_dir, storage, mut sm) = setup();

        // Grant a lease.
        let resp = sm.apply_request(
            RaftRequest::LeaseGrant {
                ttl: 60,
                expiry_time: now_millis() + 60_000,
            },
            1,
        );
        let lease_id = match resp {
            RaftResponse::LeaseGrant { id, .. } => id,
            other => panic!("expected LeaseGrant, got: {other:?}"),
        };

        // Attach a key to the lease.
        sm.apply_request(
            RaftRequest::Put {
                key: b"leased_key".to_vec(),
                value: b"val".to_vec(),
                lease_id,
            },
            2,
        );

        // Create snapshot.
        let snapshot_data = AetherStateMachine::create_snapshot(&storage).unwrap();

        // Restore to a fresh state machine.
        let lease_store2 = LeaseStore::new(storage.clone());
        let (lease_manager2, _rx2) = LeaseManager::new(10000, 1);
        let lease_manager2 = Arc::new(Mutex::new(lease_manager2));
        let (tx2, _rx2) = tokio::sync::broadcast::channel(64);
        let auth_cache2 = Arc::new(AuthCache::new());
        let auth_enabled2 = Arc::new(AtomicBool::new(false));
        let shard_manager2 = Arc::new(Mutex::new(ShardManager::new()));
        let mut sm2 = AetherStateMachine::new(
            tx2,
            storage.clone(),
            lease_manager2,
            lease_store2,
            auth_cache2,
            auth_enabled2,
            shard_manager2,
        );

        sm2.restore_snapshot(&snapshot_data, 2).unwrap();

        // Verify lease association survived the roundtrip.
        assert_eq!(
            sm2.key_leases.get(b"leased_key".as_slice()),
            Some(&lease_id)
        );
        let mgr = sm2.lease_manager.lock().unwrap();
        assert!(mgr.get(lease_id).is_some());
        assert!(
            mgr.get_keys(lease_id)
                .unwrap()
                .contains(b"leased_key".as_slice())
        );
    }
}
