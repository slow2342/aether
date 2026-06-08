use std::path::Path;

use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

use super::{KvPair, StorageEngine, WriteOp};
use crate::error::StorageError;

/// RocksDB storage engine implementation
pub struct RocksStorage {
    db: DB,
}

impl RocksStorage {
    /// Open a new RocksDB storage engine
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open(&opts, path).map_err(StorageError::RocksDb)?;

        Ok(Self { db })
    }

    /// Open with custom options
    pub fn open_with_options(path: &Path, opts: Options) -> Result<Self, StorageError> {
        let db = DB::open(&opts, path).map_err(StorageError::RocksDb)?;
        Ok(Self { db })
    }
}

impl StorageEngine for RocksStorage {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.db.get(key).map_err(StorageError::RocksDb)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.put(key, value).map_err(StorageError::RocksDb)
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.db.delete(key).map_err(StorageError::RocksDb)
    }

    fn scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<KvPair>, StorageError> {
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix, Direction::Forward));

        for item in iter {
            let (key, value) = item.map_err(StorageError::RocksDb)?;
            if !key.starts_with(prefix) || results.len() >= limit {
                break;
            }
            results.push(KvPair {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }

        Ok(results)
    }

    fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();

        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.put(key, value),
                WriteOp::Delete { key } => batch.delete(key),
            }
        }

        self.db.write(batch).map_err(StorageError::RocksDb)
    }
}
