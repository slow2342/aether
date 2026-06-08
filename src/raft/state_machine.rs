use std::collections::BTreeMap;
use std::io::Cursor;

use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};

use super::{RaftNode, RaftRequest, RaftResponse, TypeConfig, WatchEvent};

/// Snapshot data persisted on disk
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, RaftNode>,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// Aether state machine implementation
pub struct AetherStateMachine {
    /// Applied log index
    pub last_applied: Option<LogId<u64>>,
    /// Last membership config
    pub last_membership: StoredMembership<u64, RaftNode>,
    /// Storage engine for user data (in-memory for now, will be replaced with RocksDB)
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Watch event notifier
    pub watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
    /// Current snapshot data
    pub current_snapshot: Option<Vec<u8>>,
}

impl AetherStateMachine {
    /// Create a new state machine
    pub fn new(watch_tx: tokio::sync::broadcast::Sender<WatchEvent>) -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            data: BTreeMap::new(),
            watch_tx,
            current_snapshot: None,
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
                let prev_kv = self.data.get(&key).map(|v| super::KeyValue {
                    key: key.clone(),
                    value: v.clone(),
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                });
                self.data.insert(key.clone(), value.clone());

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
                    if let Some(value) = self.data.remove(&key) {
                        deleted = 1;
                        prev_kvs = vec![super::KeyValue {
                            key: key.clone(),
                            value,
                            create_revision: 0,
                            mod_revision: 0,
                            version: 0,
                            lease: 0,
                        }];
                    } else {
                        deleted = 0;
                        prev_kvs = vec![];
                    }
                } else {
                    let keys_to_delete: Vec<Vec<u8>> = if range_end == b"\0" {
                        self.data.range(key.clone()..)
                    } else {
                        self.data.range(key.clone()..range_end)
                    }
                    .map(|(k, _)| k.clone())
                    .collect();

                    deleted = keys_to_delete.len() as i64;
                    prev_kvs = keys_to_delete
                        .iter()
                        .filter_map(|k| {
                            self.data.remove(k).map(|v| super::KeyValue {
                                key: k.clone(),
                                value: v,
                                create_revision: 0,
                                mod_revision: 0,
                                version: 0,
                                lease: 0,
                            })
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
                    let current_value = self.data.get(&cmp.key);
                    match (&cmp.target, &cmp.target_union) {
                        (super::CompareTarget::Value, super::TargetUnion::Value(expected)) => {
                            match cmp.result {
                                super::CompareResult::Equal => current_value == Some(expected),
                                super::CompareResult::NotEqual => current_value != Some(expected),
                                super::CompareResult::Greater => {
                                    current_value.is_some_and(|v| v > expected)
                                }
                                super::CompareResult::Less => {
                                    current_value.is_some_and(|v| v < expected)
                                }
                            }
                        }
                        // Version/Create/Mod/Lease comparisons require MVCC metadata,
                        // which the in-memory store does not yet support. Fail safe.
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
                                    // Single key
                                    self.data
                                        .get(&get.key)
                                        .map(|v| {
                                            vec![super::KeyValue {
                                                key: get.key.clone(),
                                                value: v.clone(),
                                                create_revision: 0,
                                                mod_revision: 0,
                                                version: 0,
                                                lease: 0,
                                            }]
                                        })
                                        .unwrap_or_default()
                                } else if get.range_end == b"\0" {
                                    // All keys from key to end of keyspace
                                    self.data
                                        .range(get.key.clone()..)
                                        .map(|(k, v)| super::KeyValue {
                                            key: k.clone(),
                                            value: v.clone(),
                                            create_revision: 0,
                                            mod_revision: 0,
                                            version: 0,
                                            lease: 0,
                                        })
                                        .collect()
                                } else {
                                    self.data
                                        .range(get.key.clone()..get.range_end.clone())
                                        .map(|(k, v)| super::KeyValue {
                                            key: k.clone(),
                                            value: v.clone(),
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
                                let kvs = if range.range_end == b"\0" {
                                    // All keys from key to end of keyspace
                                    self.data.range(range.key.clone()..)
                                } else {
                                    self.data.range(range.key.clone()..range.range_end.clone())
                                }
                                .map(|(k, v)| super::KeyValue {
                                    key: k.clone(),
                                    value: v.clone(),
                                    create_revision: 0,
                                    mod_revision: 0,
                                    version: 0,
                                    lease: 0,
                                })
                                .collect::<Vec<_>>();
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
        let snapshot_data = SnapshotData {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            data: self.data.clone(),
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
            data: self.data.clone(),
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
        self.data = snapshot_data.data;
        self.current_snapshot = Some(snapshot.into_inner());

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
