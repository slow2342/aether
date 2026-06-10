use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;

use super::{RaftRequest, RaftResponse, WatchEvent};
use crate::lease::{LeaseManager, LeaseStore};
use crate::storage::{RocksStorage, StorageEngine};

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
}

impl AetherStateMachine {
    pub fn new(
        watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
        storage: Arc<RocksStorage>,
        lease_manager: Arc<Mutex<LeaseManager>>,
        lease_store: LeaseStore,
    ) -> Self {
        Self {
            last_applied: 0,
            storage,
            watch_tx,
            lease_manager,
            lease_store,
        }
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

    fn storage_range(&self, key: &[u8], range_end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let end = if range_end == b"\0" { &[] } else { range_end };
        match self.storage.range_scan(key, end, usize::MAX) {
            Ok(kvs) => kvs.into_iter().map(|kv| (kv.key, kv.value)).collect(),
            Err(e) => {
                tracing::error!(error = %e, key = ?key, "storage range_scan failed");
                vec![]
            }
        }
    }

    /// Apply a request to the state machine
    fn apply_request(&mut self, request: RaftRequest) -> RaftResponse {
        match request {
            RaftRequest::Put {
                key,
                value,
                lease_id,
            } => {
                let prev_kv = self.storage_get(&key).map(|v| super::KeyValue {
                    key: key.clone(),
                    value: v,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                });

                // Combine data write and lease association into a single WriteBatch
                // for atomicity — both succeed or both fail.
                let mut batch = WriteBatch::default();
                batch.put_cf(self.storage.default_cf(), &key, &value);

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

                let _ = self.watch_tx.send(WatchEvent {
                    event_type: super::WatchEventType::Put,
                    kv: super::KeyValue {
                        key,
                        value,
                        create_revision: 0,
                        mod_revision: 0,
                        version: 0,
                        lease: lease_id,
                    },
                    prev_kv: prev_kv.clone(),
                });

                RaftResponse::Put { prev_kv }
            }
            RaftRequest::Delete { key, range_end } => {
                let deleted;
                let prev_kvs;

                if range_end.is_empty() {
                    match self.storage_get(&key) {
                        Some(value) => {
                            let mut batch = WriteBatch::default();
                            let detach_ops = self
                                .lease_store
                                .batch_lease_cleanup(std::slice::from_ref(&key), &mut batch)
                                .unwrap_or_default();
                            batch.delete_cf(self.storage.default_cf(), &key);
                            if let Err(e) = self.storage.db().write(batch) {
                                tracing::error!(error = %e, "failed to delete from storage");
                                return RaftResponse::Error {
                                    message: format!("delete failed: {e}"),
                                };
                            }
                            {
                                let mut mgr = self.lease_manager.lock().unwrap();
                                for (lease_id, k) in &detach_ops {
                                    mgr.detach_key(*lease_id, k);
                                }
                            }
                            deleted = 1;
                            prev_kvs = vec![super::KeyValue {
                                key: key.clone(),
                                value,
                                create_revision: 0,
                                mod_revision: 0,
                                version: 0,
                                lease: 0,
                            }];
                        }
                        None => {
                            deleted = 0;
                            prev_kvs = vec![];
                        }
                    }
                } else {
                    let pairs = self.storage_range(&key, &range_end);
                    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
                    let mut batch = WriteBatch::default();
                    let detach_ops = self
                        .lease_store
                        .batch_lease_cleanup(&keys, &mut batch)
                        .unwrap_or_default();
                    for k in &keys {
                        batch.delete_cf(self.storage.default_cf(), k);
                    }
                    if let Err(e) = self.storage.db().write(batch) {
                        tracing::error!(error = %e, "failed to batch delete from storage");
                        return RaftResponse::Error {
                            message: format!("batch delete failed: {e}"),
                        };
                    }
                    {
                        let mut mgr = self.lease_manager.lock().unwrap();
                        for (lease_id, k) in &detach_ops {
                            mgr.detach_key(*lease_id, k);
                        }
                    }
                    deleted = pairs.len() as i64;
                    prev_kvs = pairs
                        .into_iter()
                        .map(|(k, v)| super::KeyValue {
                            key: k,
                            value: v,
                            create_revision: 0,
                            mod_revision: 0,
                            version: 0,
                            lease: 0,
                        })
                        .collect();
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
                                match self.apply_request(raft_req) {
                                    RaftResponse::Put { prev_kv } => {
                                        super::Response::Put(super::PutResponse { prev_kv })
                                    }
                                    _ => continue,
                                }
                            }
                            super::Request::Delete(del) => {
                                let raft_req = RaftRequest::Delete {
                                    key: del.key.clone(),
                                    range_end: del.range_end.clone(),
                                };
                                match self.apply_request(raft_req) {
                                    RaftResponse::Delete { deleted, prev_kvs } => {
                                        super::Response::Delete(super::DeleteResponse {
                                            deleted,
                                            prev_kvs,
                                        })
                                    }
                                    _ => continue,
                                }
                            }
                            super::Request::Get(get) => {
                                let kvs = if get.range_end.is_empty() {
                                    match self.storage_get(&get.key) {
                                        Some(value) => vec![super::KeyValue {
                                            key: get.key.clone(),
                                            value,
                                            create_revision: 0,
                                            mod_revision: 0,
                                            version: 0,
                                            lease: 0,
                                        }],
                                        None => vec![],
                                    }
                                } else {
                                    let pairs = self.storage_range(&get.key, &get.range_end);
                                    pairs
                                        .into_iter()
                                        .map(|(k, v)| super::KeyValue {
                                            key: k,
                                            value: v,
                                            create_revision: 0,
                                            mod_revision: 0,
                                            version: 0,
                                            lease: 0,
                                        })
                                        .collect()
                                };
                                let count = kvs.len() as i64;
                                super::Response::Get(super::RangeResponse { kvs, count })
                            }
                            super::Request::Range(range) => {
                                let pairs = if range.range_end.is_empty() {
                                    match self.storage_get(&range.key) {
                                        Some(value) => vec![(range.key.clone(), value)],
                                        None => vec![],
                                    }
                                } else {
                                    self.storage_range(&range.key, &range.range_end)
                                };
                                let kvs: Vec<_> = pairs
                                    .into_iter()
                                    .map(|(k, v)| super::KeyValue {
                                        key: k,
                                        value: v,
                                        create_revision: 0,
                                        mod_revision: 0,
                                        version: 0,
                                        lease: 0,
                                    })
                                    .collect();
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
            RaftRequest::LeaseRevoke { id } => self.apply_lease_revoke(id),
            RaftRequest::LeaseKeepAlive { id, expiry_time } => {
                self.apply_lease_keep_alive(id, expiry_time)
            }
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

    fn apply_lease_revoke(&mut self, id: i64) -> RaftResponse {
        // Read keys and values under lock, then drop before I/O.
        let key_values: Vec<(Vec<u8>, Vec<u8>)> = {
            let mgr = self.lease_manager.lock().unwrap();
            let keys = match mgr.get_keys(id) {
                Some(s) => s.iter().cloned().collect::<Vec<_>>(),
                None => return RaftResponse::LeaseRevoke {}, // idempotent
            };
            keys.into_iter()
                .map(|k| {
                    let v = self.storage_get(&k).unwrap_or_default();
                    (k, v)
                })
                .collect()
        };

        let mut batch = rocksdb::WriteBatch::default();
        let default_cf = self.storage.default_cf();

        for (key, _) in &key_values {
            batch.delete_cf(default_cf, key);
            batch.delete_cf(self.storage.key_lease_cf(), key);
            let mut lk_key = Vec::with_capacity(8 + key.len());
            lk_key.extend_from_slice(&id.to_be_bytes());
            lk_key.extend_from_slice(key);
            batch.delete_cf(self.storage.lease_keys_cf(), lk_key);
        }
        batch.delete_cf(self.storage.lease_cf(), id.to_be_bytes());

        if let Err(e) = self.storage.db().write(batch) {
            tracing::error!(error = %e, lease_id = id, "failed to batch delete on revoke");
            return RaftResponse::Error {
                message: format!("revoke batch write failed: {e}"),
            };
        }

        self.lease_manager.lock().unwrap().revoke(id);

        for (key, value) in key_values {
            let _ = self.watch_tx.send(WatchEvent {
                event_type: super::WatchEventType::Delete,
                kv: super::KeyValue {
                    key,
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: id,
                },
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

    /// Apply a normal entry's data (after request_id prefix) for raft-rs.
    /// Returns serialized RaftResponse, or an error string on deserialization failure.
    pub fn apply_normal_entry(&mut self, data: &[u8]) -> Result<Vec<u8>, String> {
        let request: RaftRequest = rkyv::from_bytes::<RaftRequest, rkyv::rancor::BoxedError>(data)
            .map_err(|e| {
                tracing::error!(error = %e, "failed to deserialize RaftRequest");
                format!("deserialize failed: {e}")
            })?;
        let response = self.apply_request(request);
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
        let sm = AetherStateMachine::new(tx.clone(), storage.clone(), lease_manager, lease_store);
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
        let resp = sm.apply_request(req);
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

        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            lease_id: 0,
        });

        let resp = sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
            lease_id: 0,
        });
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

        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
        });

        let resp = sm.apply_request(RaftRequest::Delete {
            key: b"k".to_vec(),
            range_end: vec![],
        });
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

        let resp = sm.apply_request(RaftRequest::Delete {
            key: b"missing".to_vec(),
            range_end: vec![],
        });
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

        let resp = sm.apply_request(RaftRequest::MemberAdd {
            member: raft::RaftNode {
                addr: "127.0.0.1:2380".to_string(),
                data: String::new(),
            },
        });
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

        let resp = sm.apply_request(RaftRequest::MemberRemove { node_id: 2 });
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

        let resp_bytes = sm.apply_normal_entry(&data).unwrap();
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
        let mut sm = AetherStateMachine::new(tx, storage, lease_manager, lease_store);

        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 42,
        });

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
        let mut sm = AetherStateMachine::new(tx, storage, lease_manager, lease_store);

        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
        });
        let _ = rx.try_recv(); // consume put event

        sm.apply_request(RaftRequest::Delete {
            key: b"k".to_vec(),
            range_end: vec![],
        });

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Delete);
        assert_eq!(event.kv.key, b"k");
    }

    #[test]
    fn test_lease_grant_and_revoke() {
        let (_dir, _storage, mut sm) = setup();

        let resp = sm.apply_request(RaftRequest::LeaseGrant {
            ttl: 10,
            expiry_time: now_millis() + 10_000,
        });
        match resp {
            RaftResponse::LeaseGrant { id, ttl } => {
                assert_eq!(id, 1);
                assert_eq!(ttl, 10);
            }
            other => panic!("expected LeaseGrant, got: {other:?}"),
        }

        let resp = sm.apply_request(RaftRequest::LeaseRevoke { id: 1 });
        match resp {
            RaftResponse::LeaseRevoke {} => {}
            other => panic!("expected LeaseRevoke, got: {other:?}"),
        }
    }

    #[test]
    fn test_lease_keep_alive() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(RaftRequest::LeaseGrant {
            ttl: 10,
            expiry_time: now_millis() + 10_000,
        });

        let resp = sm.apply_request(RaftRequest::LeaseKeepAlive {
            id: 1,
            expiry_time: now_millis() + 20_000,
        });
        match resp {
            RaftResponse::LeaseKeepAlive { ttl } => assert_eq!(ttl, 10),
            other => panic!("expected LeaseKeepAlive, got: {other:?}"),
        }
    }

    #[test]
    fn test_put_with_lease_attach() {
        let (_dir, _storage, mut sm) = setup();

        sm.apply_request(RaftRequest::LeaseGrant {
            ttl: 10,
            expiry_time: now_millis() + 10_000,
        });

        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 1,
        });

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

        sm.apply_request(RaftRequest::LeaseGrant {
            ttl: 10,
            expiry_time: now_millis() + 10_000,
        });
        sm.apply_request(RaftRequest::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 1,
        });

        sm.apply_request(RaftRequest::LeaseRevoke { id: 1 });

        assert_eq!(storage.get(b"k").unwrap(), None);
        assert!(sm.lease_manager.lock().unwrap().get(1).is_none());
    }
}
