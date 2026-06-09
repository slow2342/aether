use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto::aether_kv_server::AetherKv;
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, PutRequest, PutResponse,
    RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use crate::storage::StorageEngine;

pub struct KvService<S: StorageEngine> {
    storage: Arc<S>,
}

impl<S: StorageEngine> KvService<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
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
}

#[tonic::async_trait]
impl<S: StorageEngine> AetherKv for KvService<S> {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let prev_kv = if req.prev_kv {
            self.storage
                .get(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?
                .map(|value| KeyValue {
                    key: req.key.clone(),
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                })
        } else {
            None
        };

        self.storage
            .put(&req.key, &req.value)
            .map_err(|e| Status::internal(e.to_string()))?;

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

        if req.range_end.is_empty() {
            // Single key delete
            let existing = self
                .storage
                .get(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?;
            let existed = existing.is_some();
            let prev_kv = if req.prev_kv {
                existing.map(|value| KeyValue {
                    key: req.key.clone(),
                    value,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 0,
                    lease: 0,
                })
            } else {
                None
            };

            self.storage
                .delete(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?;

            let deleted = if existed { 1 } else { 0 };
            let prev_kvs = prev_kv.into_iter().collect();

            Ok(Response::new(DeleteResponse {
                header: Some(self.header()),
                deleted,
                prev_kvs,
            }))
        } else {
            // Range delete
            let end: &[u8] = if req.range_end == b"\0" {
                &[]
            } else {
                &req.range_end
            };
            let kvs = self
                .storage
                .range_scan(&req.key, end, usize::MAX)
                .map_err(|e| Status::internal(e.to_string()))?;

            let deleted = kvs.len() as i64;
            let prev_kvs: Vec<KeyValue> = if req.prev_kv {
                kvs.iter()
                    .map(|kv| KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                        create_revision: 0,
                        mod_revision: 0,
                        version: 0,
                        lease: 0,
                    })
                    .collect()
            } else {
                vec![]
            };

            let ops: Vec<_> = kvs
                .into_iter()
                .map(|kv| crate::storage::WriteOp::Delete { key: kv.key })
                .collect();
            self.storage
                .batch_write(ops)
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(Response::new(DeleteResponse {
                header: Some(self.header()),
                deleted,
                prev_kvs,
            }))
        }
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

    async fn txn(&self, _request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        Err(Status::unimplemented("txn is not yet implemented"))
    }
}
