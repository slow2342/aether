use std::sync::Arc;

use openraft::Raft;
use tonic::{Request, Response, Status};

use crate::proto::aether_kv_server::AetherKv;
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, PutRequest, PutResponse,
    RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use crate::raft::{self, TypeConfig};
use crate::storage::StorageEngine;

pub struct KvService<S: StorageEngine> {
    storage: Arc<S>,
    raft: Arc<Raft<TypeConfig>>,
    node_id: u64,
}

impl<S: StorageEngine> KvService<S> {
    pub fn new(storage: Arc<S>, raft: Arc<Raft<TypeConfig>>, node_id: u64) -> Self {
        Self {
            storage,
            raft,
            node_id,
        }
    }

    fn header(&self) -> ResponseHeader {
        // TODO: populate from Raft state (cluster_id, member_id, revision, raft_term)
        ResponseHeader {
            cluster_id: 0,
            member_id: 0,
            revision: 0,
            raft_term: 0,
        }
    }

    /// Returns `Ok(())` if this node is the leader, `Err` with leader info otherwise.
    fn require_leader(&self) -> Result<(), Status> {
        let rx = self.raft.metrics();
        let metrics = rx.borrow();

        match metrics.current_leader {
            Some(id) if id == self.node_id => Ok(()),
            Some(_) => {
                let leader_addr = metrics.current_leader.and_then(|id| {
                    metrics
                        .membership_config
                        .membership()
                        .get_node(&id)
                        .map(|n| n.addr.clone())
                });
                drop(metrics);

                let mut status = Status::unavailable("not leader");
                if let Some(addr) = leader_addr {
                    let mut metadata = tonic::metadata::MetadataMap::new();
                    metadata.insert(
                        "x-aether-leader",
                        addr.parse()
                            .map_err(|_| Status::internal("invalid leader addr"))?,
                    );
                    status = Status::with_metadata(status.code(), status.message(), metadata);
                }
                Err(status)
            }
            None => Err(Status::unavailable("no leader elected")),
        }
    }

    /// Propose a write through Raft and return the response.
    async fn propose(&self, request: raft::RaftRequest) -> Result<raft::RaftResponse, Status> {
        self.require_leader()?;
        self.raft
            .client_write(request)
            .await
            .map(|resp| resp.data)
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))
    }
}

#[tonic::async_trait]
impl<S: StorageEngine> AetherKv for KvService<S> {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let raft_req = raft::RaftRequest::Put {
            key: req.key,
            value: req.value,
            lease_id: req.lease,
        };

        let resp = self.propose(raft_req).await?;

        let prev_kv = match resp {
            raft::RaftResponse::Put { prev_kv } if req.prev_kv => prev_kv.map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                create_revision: kv.create_revision,
                mod_revision: kv.mod_revision,
                version: kv.version,
                lease: kv.lease,
            }),
            _ => None,
        };

        Ok(Response::new(PutResponse {
            header: Some(self.header()),
            prev_kv,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let kvs = if req.range_end.is_empty() {
            // Single key lookup
            match self
                .storage
                .get(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?
            {
                Some(value) => vec![KeyValue {
                    key: req.key,
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                }],
                None => vec![],
            }
        } else {
            // Range scan
            let limit = if req.limit > 0 {
                req.limit as usize
            } else {
                usize::MAX
            };
            let end: &[u8] = if req.range_end == b"\0" {
                &[]
            } else {
                &req.range_end
            };
            self.storage
                .range_scan(&req.key, end, limit)
                .map_err(|e| Status::internal(e.to_string()))?
                .into_iter()
                .map(|kv| KeyValue {
                    key: kv.key,
                    value: kv.value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                })
                .collect()
        };

        let count = kvs.len() as i64;
        Ok(Response::new(GetResponse {
            header: Some(self.header()),
            kvs,
            count,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let raft_req = raft::RaftRequest::Delete {
            key: req.key,
            range_end: req.range_end,
        };

        let resp = self.propose(raft_req).await?;

        let (deleted, prev_kvs) = match resp {
            raft::RaftResponse::Delete { deleted, prev_kvs } => {
                let kvs = if req.prev_kv {
                    prev_kvs
                        .into_iter()
                        .map(|kv| KeyValue {
                            key: kv.key,
                            value: kv.value,
                            create_revision: kv.create_revision,
                            mod_revision: kv.mod_revision,
                            version: kv.version,
                            lease: kv.lease,
                        })
                        .collect()
                } else {
                    vec![]
                };
                (deleted, kvs)
            }
            _ => (0, vec![]),
        };

        Ok(Response::new(DeleteResponse {
            header: Some(self.header()),
            deleted,
            prev_kvs,
        }))
    }

    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };

        let end = if req.range_end.is_empty() {
            // Single key
            let kvs = match self
                .storage
                .get(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?
            {
                Some(value) => vec![KeyValue {
                    key: req.key,
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                }],
                None => vec![],
            };
            return Ok(Response::new(RangeResponse {
                header: Some(self.header()),
                count: kvs.len() as i64,
                kvs,
            }));
        } else if req.range_end == b"\0" {
            &[] as &[u8]
        } else {
            &req.range_end
        };

        let kvs = self
            .storage
            .range_scan(&req.key, end, limit)
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                create_revision: 0,
                mod_revision: 0,
                version: 0,
                lease: 0,
            })
            .collect::<Vec<_>>();

        let count = kvs.len() as i64;
        Ok(Response::new(RangeResponse {
            header: Some(self.header()),
            kvs,
            count,
        }))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let req = request.into_inner();

        let compare: Vec<raft::Compare> = req
            .compare
            .into_iter()
            .map(|c| raft::Compare {
                result: match c.result() {
                    crate::proto::CompareResult::Equal => raft::CompareResult::Equal,
                    crate::proto::CompareResult::Greater => raft::CompareResult::Greater,
                    crate::proto::CompareResult::Less => raft::CompareResult::Less,
                    crate::proto::CompareResult::NotEqual => raft::CompareResult::NotEqual,
                },
                target: match c.target() {
                    crate::proto::CompareTarget::Version => raft::CompareTarget::Version,
                    crate::proto::CompareTarget::Create => raft::CompareTarget::Create,
                    crate::proto::CompareTarget::Mod => raft::CompareTarget::Mod,
                    crate::proto::CompareTarget::Value => raft::CompareTarget::Value,
                    crate::proto::CompareTarget::Lease => raft::CompareTarget::Lease,
                },
                key: c.key,
                target_union: match c.target_union {
                    Some(crate::proto::compare::TargetUnion::Version(v)) => {
                        raft::TargetUnion::Version(v)
                    }
                    Some(crate::proto::compare::TargetUnion::CreateRevision(v)) => {
                        raft::TargetUnion::CreateRevision(v)
                    }
                    Some(crate::proto::compare::TargetUnion::ModRevision(v)) => {
                        raft::TargetUnion::ModRevision(v)
                    }
                    Some(crate::proto::compare::TargetUnion::Value(v)) => {
                        raft::TargetUnion::Value(v)
                    }
                    Some(crate::proto::compare::TargetUnion::Lease(v)) => {
                        raft::TargetUnion::Lease(v)
                    }
                    None => raft::TargetUnion::Version(0),
                },
            })
            .collect();

        let success = convert_request_ops(req.success);
        let failure = convert_request_ops(req.failure);

        let raft_req = raft::RaftRequest::Txn {
            compare,
            success,
            failure,
        };

        let resp = self.propose(raft_req).await?;

        let (succeeded, responses) = match resp {
            raft::RaftResponse::Txn {
                succeeded,
                responses,
            } => {
                let proto_responses = responses
                    .into_iter()
                    .map(|r| {
                        let response = r.response.map(|resp| match resp {
                            raft::Response::Put(p) => crate::proto::response_op::Response::Put(
                                crate::proto::PutResponse {
                                    header: None,
                                    prev_kv: p.prev_kv.map(|kv| KeyValue {
                                        key: kv.key,
                                        value: kv.value,
                                        create_revision: kv.create_revision,
                                        mod_revision: kv.mod_revision,
                                        version: kv.version,
                                        lease: kv.lease,
                                    }),
                                },
                            ),
                            raft::Response::Get(g) => crate::proto::response_op::Response::Get(
                                crate::proto::GetResponse {
                                    header: None,
                                    kvs: g
                                        .kvs
                                        .into_iter()
                                        .map(|kv| KeyValue {
                                            key: kv.key,
                                            value: kv.value,
                                            create_revision: kv.create_revision,
                                            mod_revision: kv.mod_revision,
                                            version: kv.version,
                                            lease: kv.lease,
                                        })
                                        .collect(),
                                    count: g.count,
                                },
                            ),
                            raft::Response::Delete(d) => {
                                crate::proto::response_op::Response::Delete(
                                    crate::proto::DeleteResponse {
                                        header: None,
                                        deleted: d.deleted,
                                        prev_kvs: d
                                            .prev_kvs
                                            .into_iter()
                                            .map(|kv| KeyValue {
                                                key: kv.key,
                                                value: kv.value,
                                                create_revision: kv.create_revision,
                                                mod_revision: kv.mod_revision,
                                                version: kv.version,
                                                lease: kv.lease,
                                            })
                                            .collect(),
                                    },
                                )
                            }
                            raft::Response::Range(r) => crate::proto::response_op::Response::Range(
                                crate::proto::RangeResponse {
                                    header: None,
                                    kvs: r
                                        .kvs
                                        .into_iter()
                                        .map(|kv| KeyValue {
                                            key: kv.key,
                                            value: kv.value,
                                            create_revision: kv.create_revision,
                                            mod_revision: kv.mod_revision,
                                            version: kv.version,
                                            lease: kv.lease,
                                        })
                                        .collect(),
                                    count: r.count,
                                },
                            ),
                        });
                        crate::proto::ResponseOp { response }
                    })
                    .collect();
                (succeeded, proto_responses)
            }
            _ => (false, vec![]),
        };

        Ok(Response::new(TxnResponse {
            header: Some(self.header()),
            succeeded,
            responses,
        }))
    }
}

fn convert_request_ops(ops: Vec<crate::proto::RequestOp>) -> Vec<raft::RequestOp> {
    ops.into_iter()
        .map(|op| {
            let request = op.request.map(|r| match r {
                crate::proto::request_op::Request::Put(p) => raft::Request::Put(raft::PutRequest {
                    key: p.key,
                    value: p.value,
                    lease: p.lease,
                    prev_kv: p.prev_kv,
                }),
                crate::proto::request_op::Request::Get(g) => {
                    raft::Request::Get(raft::RangeRequest {
                        key: g.key,
                        range_end: g.range_end,
                        limit: g.limit,
                        revision: g.revision,
                        sort_order: raft::SortOrder::None,
                        sort_target: raft::SortTarget::Key,
                    })
                }
                crate::proto::request_op::Request::Delete(d) => {
                    raft::Request::Delete(raft::DeleteRequest {
                        key: d.key,
                        range_end: d.range_end,
                        prev_kv: d.prev_kv,
                    })
                }
                crate::proto::request_op::Request::Range(r) => {
                    raft::Request::Range(raft::RangeRequest {
                        key: r.key,
                        range_end: r.range_end,
                        limit: r.limit,
                        revision: r.revision,
                        sort_order: raft::SortOrder::None,
                        sort_target: raft::SortTarget::Key,
                    })
                }
            });
            raft::RequestOp { request }
        })
        .collect()
}
