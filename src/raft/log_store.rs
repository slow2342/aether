use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, OptionalSend, RaftLogReader, StorageError, Vote};
use rocksdb::{ColumnFamily, DB, WriteBatch, WriteOptions};

use super::TypeConfig;

/// Raft log store backed by RocksDB
#[derive(Clone)]
pub struct AetherLogStore {
    db: Arc<DB>,
}

impl AetherLogStore {
    /// Create a new log store.
    ///
    /// The DB must already have `raft_log` and `raft_state` column families.
    pub fn new(db: Arc<DB>) -> Result<Self, std::io::Error> {
        for cf in &["raft_log", "raft_state"] {
            db.cf_handle(cf).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("{cf} column family not found"),
                )
            })?;
        }
        Ok(Self { db })
    }

    /// Get the column family handle for raft logs.
    ///
    /// SAFETY: `new()` validates that this CF exists.
    fn log_cf(&self) -> &ColumnFamily {
        // SAFETY: validated in new()
        self.db
            .cf_handle("raft_log")
            .expect("raft_log CF validated in new()")
    }

    /// Get the column family handle for raft state.
    ///
    /// SAFETY: `new()` validates that this CF exists.
    fn state_cf(&self) -> &ColumnFamily {
        // SAFETY: validated in new()
        self.db
            .cf_handle("raft_state")
            .expect("raft_state CF validated in new()")
    }

    /// Create a key for a log entry (big-endian for lexicographic ordering).
    ///
    /// Uses index-only encoding (8 bytes) instead of the `[term][index]` (16 bytes)
    /// format from storage-conventions.md. This is intentional: Raft log indices are
    /// unique within a log (truncated entries are removed before re-use), and openraft's
    /// `truncate`/`purge` APIs only provide `LogId`, not the full entry with term.
    fn log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

impl RaftLogReader<TypeConfig> for AetherLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&x) => x,
            std::ops::Bound::Excluded(&x) => x + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&x) => x + 1,
            std::ops::Bound::Excluded(&x) => x,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(
            self.log_cf(),
            rocksdb::IteratorMode::From(&Self::log_key(start), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                ),
            })?;

            let index =
                u64::from_be_bytes(key.as_ref().try_into().map_err(|_| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Logs,
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::new(&std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid log key length",
                        )),
                    ),
                })?);

            if index >= end {
                break;
            }

            let entry: openraft::Entry<TypeConfig> =
                serde_json::from_slice(&value).map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Logs,
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::new(&e),
                    ),
                })?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for AetherLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_log_id = {
            let iter = self
                .db
                .iterator_cf(self.log_cf(), rocksdb::IteratorMode::End);
            match iter.into_iter().next() {
                Some(Ok((_, value))) => {
                    let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&value)
                        .map_err(|e| StorageError::IO {
                            source: openraft::StorageIOError::new(
                                openraft::ErrorSubject::Logs,
                                openraft::ErrorVerb::Read,
                                openraft::AnyError::new(&e),
                            ),
                        })?;
                    Some(entry.log_id)
                }
                _ => None,
            }
        };

        let last_purged_log_id = {
            let purge_key = b"last_purged";
            match self
                .db
                .get_cf(self.state_cf(), purge_key)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Logs,
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::new(&e),
                    ),
                })? {
                Some(data) => {
                    let id: LogId<u64> =
                        serde_json::from_slice(&data).map_err(|e| StorageError::IO {
                            source: openraft::StorageIOError::new(
                                openraft::ErrorSubject::Logs,
                                openraft::ErrorVerb::Read,
                                openraft::AnyError::new(&e),
                            ),
                        })?;
                    Some(id)
                }
                None => None,
            }
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let vote_key = b"vote";
        let data = serde_json::to_vec(vote).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Vote,
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&e),
            ),
        })?;

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db
            .put_cf_opt(self.state_cf(), vote_key, data, &opts)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Vote,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let vote_key = b"vote";
        match self
            .db
            .get_cf(self.state_cf(), vote_key)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Vote,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                ),
            })? {
            Some(data) => {
                let vote: Vote<u64> =
                    serde_json::from_slice(&data).map_err(|e| StorageError::IO {
                        source: openraft::StorageIOError::new(
                            openraft::ErrorSubject::Vote,
                            openraft::ErrorVerb::Read,
                            openraft::AnyError::new(&e),
                        ),
                    })?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = WriteBatch::default();

        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;
            batch.put_cf(self.log_cf(), key, value);
        }

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start_key = Self::log_key(log_id.index);

        let mut batch = WriteBatch::default();

        let iter = self.db.iterator_cf(
            self.log_cf(),
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;
            batch.delete_cf(self.log_cf(), key);
        }

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let end_key = Self::log_key(log_id.index + 1);

        let mut batch = WriteBatch::default();

        let iter = self
            .db
            .iterator_cf(self.log_cf(), rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;

            if key.as_ref() >= end_key.as_ref() {
                break;
            }

            batch.delete_cf(self.log_cf(), key);
        }

        // Persist last purged log id
        let purge_key = b"last_purged";
        let purge_data = serde_json::to_vec(&log_id).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&e),
            ),
        })?;
        batch.put_cf(self.state_cf(), purge_key, purge_data);

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            })?;

        Ok(())
    }
}
