use thiserror::Error;

#[derive(Debug, Error)]
pub enum AetherError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("write batch error: {0}")]
    WriteBatch(String),
}
