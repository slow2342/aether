pub mod handle;
pub mod network;
pub mod node;
pub mod raftrs_handle;
pub mod raftrs_store;
pub mod rpc;
pub mod state_machine;

pub use self::handle::{MemberInfo, RaftError, RaftHandle, ensure_linearizable, require_leader};

/// Node identifier type
pub type NodeId = u64;

/// Node information for cluster membership
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RaftNode {
    pub addr: String,
    pub data: String,
}

/// Raft request types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// Grant a new lease (ID auto-assigned by state machine)
    LeaseGrant { ttl: i64, expiry_time: i64 },
    /// Revoke a lease and delete all attached keys
    LeaseRevoke { id: i64 },
    /// Keep-alive a lease (reset expiry)
    LeaseKeepAlive { id: i64, expiry_time: i64 },
    /// Add a user (password already hashed)
    AuthUserAdd {
        name: Vec<u8>,
        password_hash: Vec<u8>,
    },
    /// Delete a user
    AuthUserDelete { name: Vec<u8> },
    /// Change user password (password already hashed)
    AuthUserChangePassword {
        name: Vec<u8>,
        password_hash: Vec<u8>,
    },
    /// Grant a role to a user
    AuthUserGrantRole { user: Vec<u8>, role: Vec<u8> },
    /// Revoke a role from a user
    AuthUserRevokeRole { user: Vec<u8>, role: Vec<u8> },
    /// Add a role
    AuthRoleAdd { name: Vec<u8> },
    /// Delete a role
    AuthRoleDelete { name: Vec<u8> },
    /// Grant permission to a role
    AuthRoleGrantPermission {
        role: Vec<u8>,
        permission: crate::auth::Permission,
    },
    /// Revoke permission from a role
    AuthRoleRevokePermission {
        role: Vec<u8>,
        permission: crate::auth::Permission,
    },
    /// Enable auth (root_password_hash already hashed)
    AuthEnable { root_password_hash: Vec<u8> },
    /// Disable auth
    AuthDisable {},
    /// Split a region at the given split key
    RegionSplit { region_id: u64, split_key: Vec<u8> },
    /// Update region metadata (leader change, replica change)
    RegionUpdate { region: crate::shard::Region },
    /// Acquire a distributed lock
    LockAcquire { name: Vec<u8>, lease_id: i64 },
    /// Release a distributed lock
    LockRelease { key: Vec<u8> },
    /// Campaign for leadership in an election
    ElectionCampaign {
        name: Vec<u8>,
        lease_id: i64,
        value: Vec<u8>,
    },
    /// Resign from leadership in an election
    ElectionResign { leader_key: Vec<u8> },
    /// Create and hold a barrier
    BarrierCreate { name: Vec<u8>, lease_id: i64 },
    /// Release a barrier
    BarrierRelease { name: Vec<u8> },
    /// Enqueue an item to a named queue
    QueueEnqueue { name: Vec<u8>, value: Vec<u8> },
    /// Dequeue the front item from a named queue
    QueueDequeue { name: Vec<u8> },
}

/// Raft response types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    LeaseGrant {
        id: i64,
        ttl: i64,
    },
    LeaseRevoke {},
    LeaseKeepAlive {
        ttl: i64,
    },
    AuthUserAdd {},
    AuthUserDelete {},
    AuthUserChangePassword {},
    AuthUserGrantRole {},
    AuthUserRevokeRole {},
    AuthRoleAdd {},
    AuthRoleDelete {},
    AuthRoleGrantPermission {},
    AuthRoleRevokePermission {},
    AuthEnable {},
    AuthDisable {},
    RegionSplit {
        parent: crate::shard::Region,
        child: crate::shard::Region,
    },
    RegionUpdate {},
    LockAcquire {
        key: Vec<u8>,
    },
    LockRelease {},
    ElectionCampaign {
        leader_key: Vec<u8>,
    },
    /// Election already has a leader, returns the current leader key
    ElectionAlreadyHasLeader {
        current_leader_key: Vec<u8>,
    },
    ElectionResign {},
    /// Election resign requested but leader key not found
    ElectionResignNotFound {},
    BarrierCreate {
        key: Vec<u8>,
    },
    /// Barrier already held by another caller
    BarrierAlreadyHeld {
        current_key: Vec<u8>,
    },
    BarrierRelease {},
    QueueEnqueue {
        key: Vec<u8>,
    },
    QueueDequeue {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Queue is empty
    QueueDequeueEmpty {},
    /// Storage error during apply
    Error {
        message: String,
    },
}

/// Key-value pair
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

/// Compare operation for transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Compare {
    pub result: CompareResult,
    pub target: CompareTarget,
    pub key: Vec<u8>,
    pub target_union: TargetUnion,
}

/// Compare result
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompareResult {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// Compare target
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompareTarget {
    Version,
    Create,
    Mod,
    Value,
    Lease,
}

/// Target union for compare operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TargetUnion {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

/// Request operation for transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RequestOp {
    pub request: Option<Request>,
}

/// Request types for transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Request {
    Put(PutRequest),
    Get(RangeRequest),
    Delete(DeleteRequest),
    Range(RangeRequest),
    Txn(Box<TxnRequest>),
}

/// Transaction request for nested transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TxnRequest {
    pub compare: Vec<Compare>,
    pub success: Vec<RequestOp>,
    pub failure: Vec<RequestOp>,
}

/// Put request
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PutRequest {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease: i64,
    pub prev_kv: bool,
}

/// Range request
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub limit: i64,
    pub revision: i64,
    pub sort_order: SortOrder,
    pub sort_target: SortTarget,
}

/// Delete request
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeleteRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub prev_kv: bool,
}

/// Sort order
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SortOrder {
    None,
    Ascend,
    Descend,
}

/// Sort target
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SortTarget {
    Key,
    Version,
    Create,
    Mod,
    Value,
}

/// Response operation for transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResponseOp {
    pub response: Option<Response>,
}

/// Response types for transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Response {
    Put(PutResponse),
    Get(RangeResponse),
    Delete(DeleteResponse),
    Range(RangeResponse),
    Txn(Box<TxnResponse>),
}

/// Transaction response for nested transactions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TxnResponse {
    pub succeeded: bool,
    pub responses: Vec<ResponseOp>,
}

/// Put response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PutResponse {
    pub prev_kv: Option<KeyValue>,
}

/// Range response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeResponse {
    pub kvs: Vec<KeyValue>,
    pub count: i64,
}

/// Delete response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeleteResponse {
    pub deleted: i64,
    pub prev_kvs: Vec<KeyValue>,
}

/// Watch event for state machine notifications
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub kv: KeyValue,
    pub prev_kv: Option<KeyValue>,
}

/// Watch event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WatchEventType {
    Put,
    Delete,
}
