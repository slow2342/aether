use std::path::Path;
use std::sync::Arc;

use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

use super::{KvPair, StorageEngine, WriteOp};
use crate::error::StorageError;

/// Column families used by the storage engine
const COLUMN_FAMILIES: &[&str] = &[
    "default",
    "raft_log",
    "raft_state",
    "meta",
    "lease",
    "lease_keys",
    "key_lease",
    "mvcc",
    "region",
];

/// RocksDB storage engine implementation
pub struct RocksStorage {
    db: Arc<DB>,
}

impl RocksStorage {
    /// Open a new RocksDB storage engine with all required column families
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open_cf(&opts, path, COLUMN_FAMILIES).map_err(StorageError::RocksDb)?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Open with custom options
    pub fn open_with_options(path: &Path, mut opts: Options) -> Result<Self, StorageError> {
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, path, COLUMN_FAMILIES).map_err(StorageError::RocksDb)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Get a reference to the underlying RocksDB instance
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn lease_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("lease").expect("lease CF not found")
    }

    pub fn lease_keys_cf(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("lease_keys")
            .expect("lease_keys CF not found")
    }

    pub fn key_lease_cf(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("key_lease")
            .expect("key_lease CF not found")
    }

    pub fn default_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("default").expect("default CF not found")
    }

    pub fn mvcc_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("mvcc").expect("mvcc CF not found")
    }

    pub fn meta_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("meta").expect("meta CF not found")
    }

    pub fn region_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("region").expect("region CF not found")
    }

    /// Clear all user data from the default and mvcc column families.
    /// Used during snapshot restore to remove stale keys not in the snapshot.
    pub fn clear_default_cf(&self) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(self.default_cf(), IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(StorageError::RocksDb)?;
            batch.delete_cf(self.default_cf(), &key);
        }
        let iter = self.db.iterator_cf(self.mvcc_cf(), IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(StorageError::RocksDb)?;
            batch.delete_cf(self.mvcc_cf(), &key);
        }
        self.db.write(batch).map_err(StorageError::RocksDb)?;
        Ok(())
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

    fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, StorageError> {
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(start, Direction::Forward));

        for item in iter {
            let (key, value) = item.map_err(StorageError::RocksDb)?;
            if (!end.is_empty() && key.as_ref() >= end) || results.len() >= limit {
                break;
            }
            results.push(KvPair {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }

        Ok(results)
    }
}
