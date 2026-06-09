pub mod log_store;
pub mod network;
pub mod rpc;
pub mod snapshot;
pub mod state_machine;

use std::io::Cursor;

use serde::{Deserialize, Serialize};

/// Node identifier type
pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = RaftNode,
        SnapshotData = Cursor<Vec<u8>>
);

/// Node information for cluster membership
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftNode {
    pub addr: String,
    pub data: String,
}

/// Raft request types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRequest {
    /// Put a key-value pair
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
    },
    /// Delete key(s)
    Delete { key: Vec<u8>, range_end: Vec<u8> },
    /// Transaction
    Txn {
        compare: Vec<Compare>,
        success: Vec<RequestOp>,
        failure: Vec<RequestOp>,
    },
    /// Add a member to the cluster
    MemberAdd { member: RaftNode },
    /// Remove a member from the cluster
    MemberRemove { node_id: NodeId },
}

/// Raft response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    Put {
        prev_kv: Option<KeyValue>,
    },
    Delete {
        deleted: i64,
        prev_kvs: Vec<KeyValue>,
    },
    Txn {
        succeeded: bool,
        responses: Vec<ResponseOp>,
    },
    MemberAdd {
        member: RaftNode,
    },
    MemberRemove {},
}

/// Key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

/// Compare operation for transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Compare {
    pub result: CompareResult,
    pub target: CompareTarget,
    pub key: Vec<u8>,
    pub target_union: TargetUnion,
}

/// Compare result
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareResult {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// Compare target
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareTarget {
    Version,
    Create,
    Mod,
    Value,
    Lease,
}

/// Target union for compare operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TargetUnion {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

/// Request operation for transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestOp {
    pub request: Option<Request>,
}

/// Request types for transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Put(PutRequest),
    Get(RangeRequest),
    Delete(DeleteRequest),
    Range(RangeRequest),
}

/// Put request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease: i64,
    pub prev_kv: bool,
}

/// Range request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub limit: i64,
    pub revision: i64,
    pub sort_order: SortOrder,
    pub sort_target: SortTarget,
}

/// Delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub prev_kv: bool,
}

/// Sort order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    None,
    Ascend,
    Descend,
}

/// Sort target
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortTarget {
    Key,
    Version,
    Create,
    Mod,
    Value,
}

/// Response operation for transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseOp {
    pub response: Option<Response>,
}

/// Response types for transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Put(PutResponse),
    Get(RangeResponse),
    Delete(DeleteResponse),
    Range(RangeResponse),
}

/// Put response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResponse {
    pub prev_kv: Option<KeyValue>,
}

/// Range response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeResponse {
    pub kvs: Vec<KeyValue>,
    pub count: i64,
}

/// Delete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: i64,
    pub prev_kvs: Vec<KeyValue>,
}

/// Watch event for state machine notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub kv: KeyValue,
    pub prev_kv: Option<KeyValue>,
}

/// Watch event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEventType {
    Put,
    Delete,
}

/// Lease information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub keys: Vec<Vec<u8>>,
}
