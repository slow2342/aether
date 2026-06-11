use thiserror::Error;

#[derive(Debug, Error)]
pub enum AetherError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not leader, leader address: {leader:?}")]
    NotLeader { leader: Option<String> },

    #[error("raft error: {0}")]
    Raft(String),

    #[error("auth error: {0}")]
    Auth(String),

    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("write batch error: {0}")]
    WriteBatch(String),

    #[error("revision not found: {revision}")]
    RevisionNotFound { revision: u64 },

    #[error("revision compacted: requested {revision}, compacted to {compacted}")]
    RevisionCompacted { revision: u64, compacted: u64 },
}
