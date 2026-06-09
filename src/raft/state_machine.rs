use std::io::Cursor;

use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};

use std::sync::Arc;

use super::{RaftNode, RaftRequest, RaftResponse, TypeConfig, WatchEvent};
use crate::storage::{RocksStorage, StorageEngine};

/// Snapshot data persisted on disk
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, RaftNode>,
    pub data: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Aether state machine implementation.
/// All user data is stored in RocksDB. The BTreeMap was removed in favor of
/// a single source of truth in the storage engine.
pub struct AetherStateMachine {
    /// Applied log index
    pub last_applied: Option<LogId<u64>>,
    /// Last membership config
    pub last_membership: StoredMembership<u64, RaftNode>,
    /// RocksDB storage for persistent user data
    pub storage: Arc<RocksStorage>,
    /// Watch event notifier
    pub watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
    /// Current snapshot data
    pub current_snapshot: Option<Vec<u8>>,
}

impl AetherStateMachine {
    /// Create a new state machine
    pub fn new(
        watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
        storage: Arc<RocksStorage>,
    ) -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            storage,
            watch_tx,
            current_snapshot: None,
        }
    }

    /// Read a key from storage, returning None on error.
    fn storage_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.storage.get(key).ok().flatten()
    }

    /// Read a range from storage, returning empty vec on error.
    fn storage_range(&self, key: &[u8], range_end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let end = if range_end == b"\0" { &[] } else { range_end };
        self.storage
            .range_scan(key, end, usize::MAX)
            .map(|kvs| kvs.into_iter().map(|kv| (kv.key, kv.value)).collect())
            .unwrap_or_default()
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
                if let Err(e) = self.storage.put(&key, &value) {
                    tracing::error!(error = %e, "failed to put to storage");
                    // Don't send watch event on storage failure.
                    return RaftResponse::Put { prev_kv: None };
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
                            if let Err(e) = self.storage.delete(&key) {
                                tracing::error!(error = %e, "failed to delete from storage");
                                return RaftResponse::Delete {
                                    deleted: 0,
                                    prev_kvs: vec![],
                                };
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
                    let ops: Vec<_> = pairs
                        .iter()
                        .map(|(k, _)| crate::storage::WriteOp::Delete { key: k.clone() })
                        .collect();
                    if let Err(e) = self.storage.batch_write(ops) {
                        tracing::error!(error = %e, "failed to batch delete from storage");
                        return RaftResponse::Delete {
                            deleted: 0,
                            prev_kvs: vec![],
                        };
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
                        prev_kv: Some(kv.clone()),
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
                        // Version/Create/Mod/Lease comparisons require MVCC metadata,
                        // which is not yet supported. Fail safe.
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
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for AetherStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        // Read all user data from RocksDB for the snapshot.
        let kvs = self.storage.range_scan(&[], &[], usize::MAX).map_err(|e| {
            tracing::error!(error = %e, "failed to read storage for snapshot");
            StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                ),
            }
        })?;
        let data: Vec<(Vec<u8>, Vec<u8>)> = kvs.into_iter().map(|kv| (kv.key, kv.value)).collect();

        let snapshot_data = SnapshotData {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            data,
        };

        let data = serde_json::to_vec(&snapshot_data).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&e),
            ),
        })?;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", self.last_applied.map_or(0, |id| id.index)),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for AetherStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, RaftNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            let resp = match entry.payload {
                EntryPayload::Blank => RaftResponse::Put { prev_kv: None },
                EntryPayload::Normal(req) => self.apply_request(req),
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    RaftResponse::Put { prev_kv: None }
                }
            };

            responses.push(resp);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AetherStateMachine {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            storage: self.storage.clone(),
            watch_tx: self.watch_tx.clone(),
            current_snapshot: None,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let snapshot_data: SnapshotData =
            serde_json::from_slice(snapshot.get_ref()).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                ),
            })?;

        self.last_applied = snapshot_data.last_applied;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(snapshot.into_inner());

        // Clear existing user data in RocksDB before writing snapshot.
        // This removes stale keys that are not in the snapshot.
        self.storage.clear_default_cf().map_err(|e| {
            tracing::error!(error = %e, "failed to clear storage before snapshot restore");
            StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            }
        })?;

        // Write snapshot data to RocksDB.
        let ops: Vec<_> = snapshot_data
            .data
            .into_iter()
            .map(|(k, v)| crate::storage::WriteOp::Put { key: k, value: v })
            .collect();
        self.storage.batch_write(ops).map_err(|e| {
            tracing::error!(error = %e, "failed to sync snapshot to storage");
            StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            }
        })?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        match &self.current_snapshot {
            Some(data) => {
                let meta = SnapshotMeta {
                    last_log_id: self.last_applied,
                    last_membership: self.last_membership.clone(),
                    snapshot_id: format!("snapshot-{}", self.last_applied.map_or(0, |id| id.index)),
                };
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data.clone())),
                }))
            }
            None => Ok(None),
        }
    }
}
