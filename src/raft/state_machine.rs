use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;

use super::{KeyValue, RaftRequest, RaftResponse, WatchEvent};
use crate::auth::AuthCache;
use crate::lease::{LeaseManager, LeaseStore};
use crate::storage::mvcc::{
    KeyIndex, MvccValue, encode_mvcc_key, load_key_indexes, save_global_revision,
};
use crate::storage::{RocksStorage, StorageEngine};

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
}

impl AetherStateMachine {
    pub fn new(
        watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
        storage: Arc<RocksStorage>,
        lease_manager: Arc<Mutex<LeaseManager>>,
        lease_store: LeaseStore,
        auth_cache: Arc<AuthCache>,
        auth_enabled: Arc<AtomicBool>,
    ) -> Self {
        let key_indexes = load_key_indexes(storage.db(), storage.mvcc_cf())
            .expect("failed to load key indexes from mvcc CF");
        let key_metas = Self::load_key_metas_from_indexes(&storage, &key_indexes);
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

    fn storage_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.storage.get(key) {
            Ok(val) => val,
            Err(e) => {
                tracing::error!(error = %e, key = ?key, "storage get failed");
                None
            }
        }
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
                // Read existing metadata for prev_kv response.
                let prev_kv = self
                    .key_metas
                    .get(&key)
                    .map(|m| m.to_kv(key.clone(), self.storage_get(&key).unwrap_or_default()));

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

                // Write MVCC versioned entry + default CF atomically.
                let mvcc_val = MvccValue {
                    create_revision: new_meta.create_revision,
                    mod_revision: new_meta.mod_revision,
                    version: new_meta.version,
                    lease: new_meta.lease,
                    value: value.clone(),
                };
                let mvcc_val_bytes = match rkyv::to_bytes::<rkyv::rancor::BoxedError>(&mvcc_val) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to serialize mvcc value");
                        return RaftResponse::Error {
                            message: format!("serialize failed: {e}"),
                        };
                    }
                };
                let mvcc_key = encode_mvcc_key(&key, revision);

                // Prepare key_index mutation on a clone to avoid corrupting
                // in-memory state if the WriteBatch fails.
                let mut new_ki = self.key_indexes.get(&key).cloned().unwrap_or_default();
                new_ki.put(revision);

                let mut batch = WriteBatch::default();
                batch.put_cf(self.storage.default_cf(), &key, &value);
                batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, mvcc_val_bytes.as_ref());
                save_global_revision(&mut batch, self.storage.meta_cf(), revision);

                let old_lease_id = if lease_id > 0 {
                    let old = self.lease_store.get_key_lease_id(&key).ok().flatten();
                    self.lease_store
                        .batch_put_lease_association(&key, lease_id, old, &mut batch);
                    old
                } else {
                    None
                };

                if let Err(e) = self.storage.db().write(batch) {
                    tracing::error!(error = %e, "failed to put to storage");
                    return RaftResponse::Error {
                        message: format!("put failed: {e}"),
                    };
                }

                // Update in-memory state after successful persistence.
                self.key_indexes.insert(key.clone(), new_ki);
                self.key_metas.insert(key.clone(), new_meta);

                if lease_id > 0 {
                    let mut mgr = self.lease_manager.lock().unwrap();
                    if let Some(old_id) = old_lease_id
                        && old_id != lease_id
                        && old_id > 0
                    {
                        mgr.detach_key(old_id, &key);
                    }
                    mgr.attach_key(lease_id, key.clone());
                }

                let kv = self.key_metas.get(&key).unwrap().to_kv(key.clone(), value);
                let _ = self.watch_tx.send(WatchEvent {
                    event_type: super::WatchEventType::Put,
                    kv: kv.clone(),
                    prev_kv: prev_kv.clone(),
                });

                RaftResponse::Put { prev_kv }
            }
            RaftRequest::Delete { key, range_end } => {
                let deleted;
                let prev_kvs;

                if range_end.is_empty() {
                    // Single key delete.
                    if self.key_metas.contains_key(&key) {
                        let meta = self.key_metas.get(&key).unwrap().clone();
                        let value = self.storage_get(&key).unwrap_or_default();
                        let prev_kv = meta.to_kv(key.clone(), value);

                        // Prepare key_index mutation on a clone.
                        let mut new_ki = self.key_indexes.get(&key).cloned().unwrap_or_default();
                        new_ki.tombstone(revision);

                        let mut batch = WriteBatch::default();
                        let detach_ops = self
                            .lease_store
                            .batch_lease_cleanup(std::slice::from_ref(&key), &mut batch)
                            .unwrap_or_default();
                        batch.delete_cf(self.storage.default_cf(), &key);

                        // Write MVCC tombstone.
                        let mvcc_key = encode_mvcc_key(&key, revision);
                        batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, b"");
                        save_global_revision(&mut batch, self.storage.meta_cf(), revision);

                        if let Err(e) = self.storage.db().write(batch) {
                            tracing::error!(error = %e, "failed to delete from storage");
                            return RaftResponse::Error {
                                message: format!("delete failed: {e}"),
                            };
                        }

                        // Update in-memory state after successful persistence.
                        self.key_indexes.insert(key.clone(), new_ki);
                        self.key_metas.remove(&key);
                        {
                            let mut mgr = self.lease_manager.lock().unwrap();
                            for (lease_id, k) in &detach_ops {
                                mgr.detach_key(*lease_id, k);
                            }
                        }
                        deleted = 1;
                        prev_kvs = vec![prev_kv];
                    } else {
                        deleted = 0;
                        prev_kvs = vec![];
                    }
                } else {
                    // Range delete.
                    let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
                    let mut prevs: Vec<KeyValue> = Vec::new();

                    // Collect keys that exist in key_metas within the range.
                    for (k, meta) in &self.key_metas {
                        if k.as_slice() >= key.as_slice()
                            && (range_end == b"\0" || k.as_slice() < range_end.as_slice())
                        {
                            let value = self.storage_get(k).unwrap_or_default();
                            prevs.push(meta.to_kv(k.clone(), value));
                            keys_to_delete.push(k.clone());
                        }
                    }
                    keys_to_delete.sort();

                    if keys_to_delete.is_empty() {
                        deleted = 0;
                        prev_kvs = vec![];
                    } else {
                        // Prepare key_index mutations on clones.
                        let mut ki_mutations: Vec<(Vec<u8>, KeyIndex)> = Vec::new();
                        for k in &keys_to_delete {
                            let mut new_ki = self.key_indexes.get(k).cloned().unwrap_or_default();
                            new_ki.tombstone(revision);
                            ki_mutations.push((k.clone(), new_ki));
                        }

                        let mut batch = WriteBatch::default();
                        let detach_ops = self
                            .lease_store
                            .batch_lease_cleanup(&keys_to_delete, &mut batch)
                            .unwrap_or_default();

                        for k in &keys_to_delete {
                            batch.delete_cf(self.storage.default_cf(), k);

                            // Write MVCC tombstone.
                            let mvcc_key = encode_mvcc_key(k, revision);
                            batch.put_cf(self.storage.mvcc_cf(), &mvcc_key, b"");
                        }
                        save_global_revision(&mut batch, self.storage.meta_cf(), revision);

                        if let Err(e) = self.storage.db().write(batch) {
                            tracing::error!(error = %e, "failed to batch delete from storage");
                            return RaftResponse::Error {
                                message: format!("batch delete failed: {e}"),
                            };
                        }

                        // Update in-memory state after successful persistence.
                        for (k, ki) in ki_mutations {
                            self.key_indexes.insert(k, ki);
                        }
                        for k in &keys_to_delete {
                            self.key_metas.remove(k);
                        }
                        {
                            let mut mgr = self.lease_manager.lock().unwrap();
                            for (lease_id, k) in &detach_ops {
                                mgr.detach_key(*lease_id, k);
                            }
                        }
                        deleted = keys_to_delete.len() as i64;
                        prev_kvs = prevs;
                    }
                }

                for kv in &prev_kvs {
                    let _ = self.watch_tx.send(WatchEvent {
                        event_type: super::WatchEventType::Delete,
                        kv: kv.clone(),
                        prev_kv: None,
                    });
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
                    match (&cmp.target, &cmp.target_union) {
                        (super::CompareTarget::Value, super::TargetUnion::Value(expected)) => {
                            match cmp.result {
                                super::CompareResult::Equal => {
                                    current_value.as_deref() == Some(expected.as_slice())
                                }
                                super::CompareResult::NotEqual => {
                                    current_value.as_deref() != Some(expected.as_slice())
                                }
                                super::CompareResult::Greater => current_value
                                    .as_deref()
                                    .is_some_and(|v| v > expected.as_slice()),
                                super::CompareResult::Less => current_value
                                    .as_deref()
                                    .is_some_and(|v| v < expected.as_slice()),
                            }
                        }
                        (super::CompareTarget::Version, super::TargetUnion::Version(expected)) => {
                            let current = meta.map_or(0i64, |m| m.version);
                            Self::compare_i64(cmp.result, current, *expected)
                        }
                        (
                            super::CompareTarget::Create,
                            super::TargetUnion::CreateRevision(expected),
                        ) => {
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
                });

                let ops = if succeeded { &success } else { &failure };
                let mut responses = Vec::new();

                for op in ops {
                    if let Some(request) = &op.request {
                        let response = match request {
                            super::Request::Put(put) => {
                                let raft_req = RaftRequest::Put {
                                    key: put.key.clone(),
                                    value: put.value.clone(),
                                    lease_id: put.lease,
                                };
                                match self.apply_request(raft_req, revision) {
                                    RaftResponse::Put { prev_kv } => {
                                        super::Response::Put(super::PutResponse { prev_kv })
                                    }
                                    RaftResponse::Error { message } => {
                                        return RaftResponse::Error {
                                            message: format!("txn put failed: {message}"),
                                        };
                                    }
                                    _ => continue,
                                }
                            }
                            super::Request::Delete(del) => {
                                let raft_req = RaftRequest::Delete {
                                    key: del.key.clone(),
                                    range_end: del.range_end.clone(),
                                };
                                match self.apply_request(raft_req, revision) {
                                    RaftResponse::Delete { deleted, prev_kvs } => {
                                        super::Response::Delete(super::DeleteResponse {
                                            deleted,
                                            prev_kvs,
                                        })
                                    }
                                    RaftResponse::Error { message } => {
                                        return RaftResponse::Error {
                                            message: format!("txn delete failed: {message}"),
                                        };
                                    }
                                    _ => continue,
                                }
                            }
                            super::Request::Get(get) => {
                                let kvs = if get.range_end.is_empty() {
                                    match self.key_metas.get(&get.key) {
                                        Some(meta) => {
                                            let value =
                                                self.storage_get(&get.key).unwrap_or_default();
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
                                            let value = self.storage_get(k).unwrap_or_default();
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
                                            let value =
                                                self.storage_get(&range.key).unwrap_or_default();
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
                                            let value = self.storage_get(k).unwrap_or_default();
                                            meta.to_kv(k.clone(), value)
                                        })
                                        .collect()
                                };
                                let count = kvs.len() as i64;
                                super::Response::Range(super::RangeResponse { kvs, count })
                            }
                        };
                        responses.push(super::ResponseOp {
                            response: Some(response),
                        });
                    }
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

    /// Apply a normal entry's data (after request_id prefix) for raft-rs.
    /// `revision` is the Raft log entry index, used as the MVCC global revision.
    /// Returns serialized RaftResponse, or an error string on deserialization failure.
    pub fn apply_normal_entry(&mut self, data: &[u8], revision: u64) -> Result<Vec<u8>, String> {
        let request: RaftRequest = rkyv::from_bytes::<RaftRequest, rkyv::rancor::BoxedError>(data)
            .map_err(|e| {
                tracing::error!(error = %e, "failed to deserialize RaftRequest");
                format!("deserialize failed: {e}")
            })?;
        let response = self.apply_request(request, revision);
        rkyv::to_bytes::<rkyv::rancor::BoxedError>(&response)
            .map(|b| b.into_vec())
            .map_err(|e| format!("serialize failed: {e}"))
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
        let sm = AetherStateMachine::new(
            tx.clone(),
            storage.clone(),
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
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
        let data = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&req)
            .unwrap()
            .into_vec();

        let resp_bytes = sm.apply_normal_entry(&data, 1).unwrap();
        let resp: RaftResponse =
            rkyv::from_bytes::<RaftResponse, rkyv::rancor::BoxedError>(&resp_bytes).unwrap();
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
        let mut sm = AetherStateMachine::new(
            tx,
            storage,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
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
        let mut sm = AetherStateMachine::new(
            tx,
            storage,
            lease_manager,
            lease_store,
            auth_cache,
            auth_enabled,
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
}
