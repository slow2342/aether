use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::auth::{AuthInterceptor, PermissionType};
use crate::proto::aether_kv_server::AetherKv;
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, PutRequest, PutResponse,
    RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use crate::raft::{self, RaftHandle, ensure_linearizable, require_leader};
use crate::storage::StorageEngine;

pub struct KvService<S: StorageEngine> {
    storage: Arc<S>,
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    auth_enabled: Arc<AtomicBool>,
    auth_interceptor: Arc<AuthInterceptor>,
}

impl<S: StorageEngine> KvService<S> {
    pub fn new(
        storage: Arc<S>,
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        auth_enabled: Arc<AtomicBool>,
        auth_interceptor: Arc<AuthInterceptor>,
    ) -> Self {
        Self {
            storage,
            raft,
            node_id,
            auth_enabled,
            auth_interceptor,
        }
    }

    fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: 0,
            member_id: self.node_id,
            revision: 0,
            raft_term: 0,
        }
    }

    /// Get the authenticated username from request extensions.
    fn get_username(req: &Request<impl std::fmt::Debug>) -> Option<String> {
        req.extensions().get::<String>().cloned()
    }

    /// Check permission if auth is enabled.
    fn check_perm(
        &self,
        req: &Request<impl std::fmt::Debug>,
        key: &[u8],
        required: PermissionType,
    ) -> Result<(), Status> {
        if !self.auth_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let username =
            Self::get_username(req).ok_or_else(|| Status::unauthenticated("no user in context"))?;
        self.auth_interceptor
            .check_permission(&username, key, required)
    }

    /// Check range permission if auth is enabled.
    fn check_range_perm(
        &self,
        req: &Request<impl std::fmt::Debug>,
        key: &[u8],
        range_end: &[u8],
        required: PermissionType,
    ) -> Result<(), Status> {
        if !self.auth_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let username =
            Self::get_username(req).ok_or_else(|| Status::unauthenticated("no user in context"))?;
        self.auth_interceptor
            .check_range_permission(&username, key, range_end, required)
    }

    /// Check if a key is a reserved internal key (starts with `_aether_`).
    fn is_reserved_key(key: &[u8]) -> bool {
        key.starts_with(b"_aether_")
    }

    /// Recursively check permissions for all operations in a Txn (including nested).
    fn check_txn_ops_permissions(
        auth: &AuthInterceptor,
        username: &str,
        compare: &[crate::proto::Compare],
        success: &[crate::proto::RequestOp],
        failure: &[crate::proto::RequestOp],
    ) -> Result<(), Status> {
        for cmp in compare {
            auth.check_permission(username, &cmp.key, PermissionType::Read)?;
        }
        for op in success.iter().chain(failure.iter()) {
            if let Some(r) = &op.request {
                match r {
                    crate::proto::request_op::Request::Put(p) => {
                        if Self::is_reserved_key(&p.key) {
                            return Err(Status::permission_denied(
                                "keys starting with _aether_ are reserved",
                            ));
                        }
                        auth.check_permission(username, &p.key, PermissionType::Write)?;
                    }
                    crate::proto::request_op::Request::Delete(d) => {
                        if Self::is_reserved_key(&d.key) {
                            return Err(Status::permission_denied(
                                "keys starting with _aether_ are reserved",
                            ));
                        }
                        if !d.range_end.is_empty() {
                            auth.check_range_permission(
                                username,
                                &d.key,
                                &d.range_end,
                                PermissionType::Write,
                            )?;
                        } else {
                            auth.check_permission(username, &d.key, PermissionType::Write)?;
                        }
                    }
                    crate::proto::request_op::Request::Get(g) => {
                        auth.check_permission(username, &g.key, PermissionType::Read)?;
                    }
                    crate::proto::request_op::Request::Range(r) => {
                        auth.check_range_permission(
                            username,
                            &r.key,
                            &r.range_end,
                            PermissionType::Read,
                        )?;
                    }
                    crate::proto::request_op::Request::Txn(t) => {
                        Self::check_txn_ops_permissions(
                            auth, username, &t.compare, &t.success, &t.failure,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Propose a write through Raft and return the response.
    async fn propose(&self, request: raft::RaftRequest) -> Result<raft::RaftResponse, Status> {
        require_leader(self.raft.as_ref(), self.node_id)?;
        self.raft
            .propose(request)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))
    }

    /// Perform a linearizable read: confirm leadership, then wait until the
    /// state machine has applied up to the commit index. This guarantees the
    /// read reflects all previously committed entries.
    async fn linearizable_read(&self) -> Result<(), Status> {
        let commit_idx = ensure_linearizable(self.raft.as_ref(), self.node_id)?;
        // Wait for state machine to catch up (poll with timeout)
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if self.raft.applied_index() >= commit_idx {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "timed out waiting for state machine to apply",
                ));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }
    }
}

#[tonic::async_trait]
impl<S: StorageEngine> AetherKv for KvService<S> {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        self.check_perm(&request, &request.get_ref().key, PermissionType::Write)?;
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if Self::is_reserved_key(&req.key) {
            return Err(Status::permission_denied(
                "keys starting with _aether_ are reserved",
            ));
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
        // Check range permission when range_end is non-empty
        if !request.get_ref().range_end.is_empty() {
            self.check_range_perm(
                &request,
                &request.get_ref().key,
                &request.get_ref().range_end,
                PermissionType::Read,
            )?;
        } else {
            self.check_perm(&request, &request.get_ref().key, PermissionType::Read)?;
        }
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if !req.serializable {
            self.linearizable_read().await?;
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
        // Check range permission when range_end is non-empty
        if !request.get_ref().range_end.is_empty() {
            self.check_range_perm(
                &request,
                &request.get_ref().key,
                &request.get_ref().range_end,
                PermissionType::Write,
            )?;
        } else {
            self.check_perm(&request, &request.get_ref().key, PermissionType::Write)?;
        }
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if Self::is_reserved_key(&req.key) {
            return Err(Status::permission_denied(
                "keys starting with _aether_ are reserved",
            ));
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
        if self.auth_enabled.load(Ordering::Acquire) {
            let username = Self::get_username(&request)
                .ok_or_else(|| Status::unauthenticated("no user in context"))?;
            let inner = request.get_ref();
            self.auth_interceptor.check_range_permission(
                &username,
                &inner.key,
                &inner.range_end,
                PermissionType::Read,
            )?;
        }
        self.linearizable_read().await?;
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
        let txn_username = if self.auth_enabled.load(Ordering::Acquire) {
            Some(
                Self::get_username(&request)
                    .ok_or_else(|| Status::unauthenticated("no user in context"))?,
            )
        } else {
            None
        };
        let req = request.into_inner();

        if let Some(ref username) = txn_username {
            // Check permissions per operation type
            // Compare keys need Read permission
            for cmp in &req.compare {
                self.auth_interceptor
                    .check_permission(username, &cmp.key, PermissionType::Read)?;
            }
            // Check reserved key protection for write operations
            for op in req.success.iter().chain(req.failure.iter()) {
                if let Some(r) = &op.request {
                    match r {
                        crate::proto::request_op::Request::Put(p) => {
                            if Self::is_reserved_key(&p.key) {
                                return Err(Status::permission_denied(
                                    "keys starting with _aether_ are reserved",
                                ));
                            }
                            self.auth_interceptor.check_permission(
                                username,
                                &p.key,
                                PermissionType::Write,
                            )?;
                        }
                        crate::proto::request_op::Request::Delete(d) => {
                            if Self::is_reserved_key(&d.key) {
                                return Err(Status::permission_denied(
                                    "keys starting with _aether_ are reserved",
                                ));
                            }
                            if !d.range_end.is_empty() {
                                self.auth_interceptor.check_range_permission(
                                    username,
                                    &d.key,
                                    &d.range_end,
                                    PermissionType::Write,
                                )?;
                            } else {
                                self.auth_interceptor.check_permission(
                                    username,
                                    &d.key,
                                    PermissionType::Write,
                                )?;
                            }
                        }
                        crate::proto::request_op::Request::Get(g) => {
                            self.auth_interceptor.check_permission(
                                username,
                                &g.key,
                                PermissionType::Read,
                            )?;
                        }
                        crate::proto::request_op::Request::Range(r) => {
                            self.auth_interceptor.check_range_permission(
                                username,
                                &r.key,
                                &r.range_end,
                                PermissionType::Read,
                            )?;
                        }
                        crate::proto::request_op::Request::Txn(t) => {
                            Self::check_txn_ops_permissions(
                                &self.auth_interceptor,
                                username,
                                &t.compare,
                                &t.success,
                                &t.failure,
                            )?;
                        }
                    }
                }
            }
        }

        let compare = convert_compares(req.compare);

        let success = convert_request_ops(req.success)?;
        let failure = convert_request_ops(req.failure)?;

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
                            raft::Response::Txn(t) => {
                                let inner_responses = convert_response_ops(t.responses);
                                crate::proto::response_op::Response::Txn(
                                    crate::proto::TxnResponse {
                                        header: None,
                                        succeeded: t.succeeded,
                                        responses: inner_responses,
                                    },
                                )
                            }
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

/// Maximum nesting depth for transactions (1 = one level of nested Txn).
const MAX_TXN_DEPTH: u32 = 1;

fn convert_request_ops(ops: Vec<crate::proto::RequestOp>) -> Result<Vec<raft::RequestOp>, Status> {
    convert_request_ops_inner(ops, 0)
}

fn convert_request_ops_inner(
    ops: Vec<crate::proto::RequestOp>,
    depth: u32,
) -> Result<Vec<raft::RequestOp>, Status> {
    ops.into_iter()
        .map(|op| {
            let request = op.request.map(|r| match r {
                crate::proto::request_op::Request::Put(p) => {
                    Ok(raft::Request::Put(raft::PutRequest {
                        key: p.key,
                        value: p.value,
                        lease: p.lease,
                        prev_kv: p.prev_kv,
                    }))
                }
                crate::proto::request_op::Request::Get(g) => {
                    Ok(raft::Request::Get(raft::RangeRequest {
                        key: g.key,
                        range_end: g.range_end,
                        limit: g.limit,
                        revision: g.revision,
                        sort_order: raft::SortOrder::None,
                        sort_target: raft::SortTarget::Key,
                    }))
                }
                crate::proto::request_op::Request::Delete(d) => {
                    Ok(raft::Request::Delete(raft::DeleteRequest {
                        key: d.key,
                        range_end: d.range_end,
                        prev_kv: d.prev_kv,
                    }))
                }
                crate::proto::request_op::Request::Range(r) => {
                    Ok(raft::Request::Range(raft::RangeRequest {
                        key: r.key,
                        range_end: r.range_end,
                        limit: r.limit,
                        revision: r.revision,
                        sort_order: raft::SortOrder::None,
                        sort_target: raft::SortTarget::Key,
                    }))
                }
                crate::proto::request_op::Request::Txn(t) => {
                    if depth >= MAX_TXN_DEPTH {
                        return Err(Status::invalid_argument(
                            "transaction nesting depth exceeded",
                        ));
                    }
                    let compare = convert_compares(t.compare);
                    let success = convert_request_ops_inner(t.success, depth + 1)?;
                    let failure = convert_request_ops_inner(t.failure, depth + 1)?;
                    Ok(raft::Request::Txn(Box::new(raft::TxnRequest {
                        compare,
                        success,
                        failure,
                    })))
                }
            });
            let request = match request {
                Some(Ok(r)) => Some(r),
                Some(Err(e)) => return Err(e),
                None => None,
            };
            Ok(raft::RequestOp { request })
        })
        .collect()
}

fn convert_compares(compares: Vec<crate::proto::Compare>) -> Vec<raft::Compare> {
    compares
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
                Some(crate::proto::compare::TargetUnion::Value(v)) => raft::TargetUnion::Value(v),
                Some(crate::proto::compare::TargetUnion::Lease(v)) => raft::TargetUnion::Lease(v),
                None => raft::TargetUnion::Version(0),
            },
        })
        .collect()
}

fn convert_kv(kv: raft::KeyValue) -> KeyValue {
    KeyValue {
        key: kv.key,
        value: kv.value,
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease: kv.lease,
    }
}

fn convert_response_ops(responses: Vec<raft::ResponseOp>) -> Vec<crate::proto::ResponseOp> {
    responses
        .into_iter()
        .map(|r| {
            let response = r.response.map(|resp| match resp {
                raft::Response::Put(p) => {
                    crate::proto::response_op::Response::Put(crate::proto::PutResponse {
                        header: None,
                        prev_kv: p.prev_kv.map(convert_kv),
                    })
                }
                raft::Response::Get(g) => {
                    crate::proto::response_op::Response::Get(crate::proto::GetResponse {
                        header: None,
                        kvs: g.kvs.into_iter().map(convert_kv).collect(),
                        count: g.count,
                    })
                }
                raft::Response::Delete(d) => {
                    crate::proto::response_op::Response::Delete(crate::proto::DeleteResponse {
                        header: None,
                        deleted: d.deleted,
                        prev_kvs: d.prev_kvs.into_iter().map(convert_kv).collect(),
                    })
                }
                raft::Response::Range(r) => {
                    crate::proto::response_op::Response::Range(crate::proto::RangeResponse {
                        header: None,
                        kvs: r.kvs.into_iter().map(convert_kv).collect(),
                        count: r.count,
                    })
                }
                raft::Response::Txn(t) => {
                    crate::proto::response_op::Response::Txn(crate::proto::TxnResponse {
                        header: None,
                        succeeded: t.succeeded,
                        responses: convert_response_ops(t.responses),
                    })
                }
            });
            crate::proto::ResponseOp { response }
        })
        .collect()
}
