pub mod rocksdb;

pub use self::rocksdb::RocksStorage;

use crate::error::StorageError;

/// Key-value pair
#[derive(Debug, Clone)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Write operation for batch writes
#[derive(Debug, Clone)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Storage engine trait
pub trait StorageEngine: Send + Sync + 'static {
    /// Get a value by key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Put a key-value pair
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// Delete a key
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

    /// Scan keys with prefix, up to limit
    fn scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<KvPair>, StorageError>;

    /// Atomic batch write
    fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), StorageError>;
}
