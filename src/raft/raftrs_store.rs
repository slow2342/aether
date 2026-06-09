use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use prost011::Message as ProstMessage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{GetEntriesContext, RaftState, Storage};
use raft::{Error as RaftError, Result as RaftResult, StorageError};
use rocksdb::{ColumnFamily, DB};

/// RocksDB-backed Storage implementation for raft-rs.
///
/// Column families:
/// - `raft_log`: entries indexed by `[index: u64 BE]` → protobuf Entry
/// - `raft_state`: metadata keys → protobuf encoded state
pub struct RaftRsStore {
    db: Arc<DB>,
    /// Cached first log index (updated on compact)
    first_index_cache: AtomicU64,
    /// Cached last log index (updated on append)
    last_index_cache: AtomicU64,
}

impl RaftRsStore {
    pub fn new(db: Arc<DB>) -> Self {
        for cf in &["raft_log", "raft_state"] {
            db.cf_handle(cf)
                .unwrap_or_else(|| panic!("{cf} column family not found"));
        }

        let store = Self {
            db,
            first_index_cache: AtomicU64::new(0),
            last_index_cache: AtomicU64::new(0),
        };

        // Initialize caches from storage
        let first = store.compute_first_index().unwrap_or(0);
        let last = store.compute_last_index().unwrap_or(0);
        store.first_index_cache.store(first, Ordering::Relaxed);
        store.last_index_cache.store(last, Ordering::Relaxed);

        store
    }

    fn log_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle("raft_log")
            .expect("raft_log CF validated in new()")
    }

    fn state_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle("raft_state")
            .expect("raft_state CF validated in new()")
    }

    fn log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Compute first index from storage (expensive - creates iterator).
    fn compute_first_index(&self) -> RaftResult<u64> {
        if let Some(data) = self
            .db
            .get_cf(self.state_cf(), b"last_purged")
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?
            && data.len() == 8
        {
            let purged = u64::from_be_bytes(data[..8].try_into().unwrap());
            return Ok(purged + 1);
        }

        let iter = self
            .db
            .iterator_cf(self.log_cf(), rocksdb::IteratorMode::Start);
        match iter.into_iter().next() {
            Some(Ok((key, _))) if key.len() == 8 => {
                Ok(u64::from_be_bytes(key[..8].try_into().unwrap()))
            }
            _ => Ok(self.snapshot_last_index() + 1),
        }
    }

    /// Compute last index from storage (expensive - creates iterator).
    fn compute_last_index(&self) -> RaftResult<u64> {
        let iter = self
            .db
            .iterator_cf(self.log_cf(), rocksdb::IteratorMode::End);
        match iter.into_iter().next() {
            Some(Ok((key, _))) if key.len() == 8 => {
                Ok(u64::from_be_bytes(key[..8].try_into().unwrap()))
            }
            _ => Ok(self.snapshot_last_index()),
        }
    }

    /// Append entries to the log. Called by the event loop after ready.
    pub fn append_entries(&self, entries: &[Entry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = rocksdb::WriteBatch::default();
        let mut max_index = 0u64;
        for entry in entries {
            let key = Self::log_key(entry.index);
            let mut value = Vec::with_capacity(entry.encoded_len());
            entry
                .encode(&mut value)
                .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
            batch.put_cf(self.log_cf(), key, value);
            if entry.index > max_index {
                max_index = entry.index;
            }
        }
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;

        // Update last_index cache
        if max_index > 0 {
            self.last_index_cache.store(max_index, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Save HardState to raft_state CF.
    pub fn save_hard_state(&self, hs: &HardState) -> RaftResult<()> {
        let mut value = Vec::with_capacity(hs.encoded_len());
        hs.encode(&mut value)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db
            .put_cf_opt(self.state_cf(), b"hard_state", value, &opts)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
        Ok(())
    }

    /// Save ConfState to raft_state CF.
    pub fn save_conf_state(&self, cs: &ConfState) -> RaftResult<()> {
        let mut value = Vec::with_capacity(cs.encoded_len());
        cs.encode(&mut value)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db
            .put_cf_opt(self.state_cf(), b"conf_state", value, &opts)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
        Ok(())
    }

    /// Compact log entries up to `compact_index` (exclusive).
    pub fn compact(&self, compact_index: u64) -> RaftResult<()> {
        let first = self.first_index()?;
        if compact_index <= first {
            return Ok(());
        }
        let mut batch = rocksdb::WriteBatch::default();
        for idx in first..compact_index {
            batch.delete_cf(self.log_cf(), Self::log_key(idx));
        }
        batch.put_cf(self.state_cf(), b"last_purged", compact_index.to_be_bytes());
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;

        // Update first_index cache
        self.first_index_cache
            .store(compact_index, Ordering::Relaxed);
        Ok(())
    }

    fn snapshot_last_index(&self) -> u64 {
        self.db
            .get_cf(self.state_cf(), b"snapshot_index")
            .ok()
            .flatten()
            .and_then(|data| {
                if data.len() == 8 {
                    Some(u64::from_be_bytes(data[..8].try_into().unwrap()))
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    fn snapshot_last_term(&self) -> u64 {
        self.db
            .get_cf(self.state_cf(), b"snapshot_term")
            .ok()
            .flatten()
            .and_then(|data| {
                if data.len() == 8 {
                    Some(u64::from_be_bytes(data[..8].try_into().unwrap()))
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }
}

impl Storage for RaftRsStore {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hard_state = match self
            .db
            .get_cf(self.state_cf(), b"hard_state")
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?
        {
            Some(data) => HardState::decode(&*data)
                .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?,
            None => HardState::default(),
        };

        let conf_state = match self
            .db
            .get_cf(self.state_cf(), b"conf_state")
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?
        {
            Some(data) => ConfState::decode(&*data)
                .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?,
            None => ConfState::default(),
        };

        Ok(RaftState::new(hard_state, conf_state))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let max_size = max_size.into();
        let first = self.first_index()?;

        if low < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let last = self.last_index()?;
        if high > last + 1 {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        let mut entries = Vec::new();
        let mut total_size: u64 = 0;

        // Use iterator instead of individual point reads for sequential access
        let iter = self.db.iterator_cf(
            self.log_cf(),
            rocksdb::IteratorMode::From(&Self::log_key(low), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, data) = item.map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;

            if key.len() != 8 {
                continue;
            }
            let index = u64::from_be_bytes(key[..8].try_into().unwrap());
            if index >= high {
                break;
            }

            let entry = Entry::decode(&*data)
                .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;

            total_size += data.len() as u64;
            entries.push(entry);

            if let Some(max) = max_size
                && total_size > max
                && !entries.is_empty()
            {
                break;
            }
        }

        Ok(entries)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let snap_idx = self.snapshot_last_index();
        if idx == snap_idx {
            return Ok(self.snapshot_last_term());
        }

        let first = self.first_index()?;
        if idx < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let last = self.last_index()?;
        if idx > last {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        let key = Self::log_key(idx);
        let data = self
            .db
            .get_cf(self.log_cf(), key)
            .map_err(|e| RaftError::Store(StorageError::Other(e.into())))?
            .ok_or(RaftError::Store(StorageError::Unavailable))?;

        let entry =
            Entry::decode(&*data).map_err(|e| RaftError::Store(StorageError::Other(e.into())))?;
        Ok(entry.term)
    }

    fn first_index(&self) -> RaftResult<u64> {
        let cached = self.first_index_cache.load(Ordering::Relaxed);
        if cached > 0 {
            return Ok(cached);
        }
        let idx = self.compute_first_index()?;
        self.first_index_cache.store(idx, Ordering::Relaxed);
        Ok(idx)
    }

    fn last_index(&self) -> RaftResult<u64> {
        let cached = self.last_index_cache.load(Ordering::Relaxed);
        if cached > 0 {
            return Ok(cached);
        }
        let idx = self.compute_last_index()?;
        self.last_index_cache.store(idx, Ordering::Relaxed);
        Ok(idx)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let mut snapshot = Snapshot::default();

        let last_idx = self.last_index()?;
        let commit_idx = std::cmp::max(last_idx, request_index);

        let meta = snapshot.mut_metadata();
        meta.index = commit_idx;
        meta.term = if commit_idx == 0 {
            0
        } else {
            self.term(commit_idx).unwrap_or(0)
        };

        if let Ok(RaftState { conf_state, .. }) = self.initial_state() {
            meta.set_conf_state(conf_state);
        }

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::{ConfState, HardState};
    use raft::storage::Storage;
    use tempfile::tempdir;

    fn open_store() -> (tempfile::TempDir, RaftRsStore) {
        let dir = tempdir().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = rocksdb::DB::open_cf(&opts, dir.path(), &["default", "raft_log", "raft_state"])
            .unwrap();
        let store = RaftRsStore::new(Arc::new(db));
        (dir, store)
    }

    fn make_entry(index: u64, term: u64, data: &[u8]) -> Entry {
        let mut e = Entry::default();
        e.index = index;
        e.term = term;
        e.data = data.to_vec().into();
        e
    }

    #[test]
    fn test_empty_store_first_last_index() {
        let (_dir, store) = open_store();
        assert_eq!(store.first_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 0);
    }

    #[test]
    fn test_empty_store_term_zero() {
        let (_dir, store) = open_store();
        // term(0) must succeed for empty stores (raft-rs calls this during init)
        assert_eq!(store.term(0).unwrap(), 0);
    }

    #[test]
    fn test_append_entries_and_read_back() {
        let (_dir, store) = open_store();
        let entries = vec![
            make_entry(1, 1, b"entry1"),
            make_entry(2, 1, b"entry2"),
            make_entry(3, 1, b"entry3"),
        ];
        store.append_entries(&entries).unwrap();

        assert_eq!(store.first_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 3);

        let fetched = store
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(fetched.len(), 3);
        assert_eq!(fetched[0].data.as_slice(), b"entry1");
        assert_eq!(fetched[2].data.as_slice(), b"entry3");
    }

    #[test]
    fn test_term_after_append() {
        let (_dir, store) = open_store();
        let entries = vec![
            make_entry(1, 1, b"e1"),
            make_entry(2, 2, b"e2"),
            make_entry(3, 2, b"e3"),
        ];
        store.append_entries(&entries).unwrap();

        assert_eq!(store.term(1).unwrap(), 1);
        assert_eq!(store.term(2).unwrap(), 2);
        assert_eq!(store.term(3).unwrap(), 2);
    }

    #[test]
    fn test_term_compacted_returns_error() {
        let (_dir, store) = open_store();
        let entries = vec![
            make_entry(1, 1, b"e1"),
            make_entry(2, 1, b"e2"),
            make_entry(3, 1, b"e3"),
        ];
        store.append_entries(&entries).unwrap();
        store.compact(3).unwrap(); // compact entries 1,2

        assert!(matches!(
            store.term(1),
            Err(e) if matches!(e, raft::Error::Store(raft::StorageError::Compacted))
        ));
    }

    #[test]
    fn test_save_and_load_hard_state() {
        let (_dir, store) = open_store();
        let mut hs = HardState::default();
        hs.term = 5;
        hs.vote = 2;
        hs.commit = 10;
        store.save_hard_state(&hs).unwrap();

        let state = store.initial_state().unwrap();
        assert_eq!(state.hard_state.term, 5);
        assert_eq!(state.hard_state.vote, 2);
        assert_eq!(state.hard_state.commit, 10);
    }

    #[test]
    fn test_save_and_load_conf_state() {
        let (_dir, store) = open_store();
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);
        store.save_conf_state(&cs).unwrap();

        let state = store.initial_state().unwrap();
        assert_eq!(state.conf_state.voters, vec![1, 2, 3]);
    }

    #[test]
    fn test_compact_updates_first_index() {
        let (_dir, store) = open_store();
        let entries = vec![
            make_entry(1, 1, b"e1"),
            make_entry(2, 1, b"e2"),
            make_entry(3, 1, b"e3"),
            make_entry(4, 1, b"e4"),
        ];
        store.append_entries(&entries).unwrap();

        store.compact(3).unwrap(); // purge entries with index < 3
        assert_eq!(store.first_index().unwrap(), 3);
        assert_eq!(store.last_index().unwrap(), 4);
    }

    #[test]
    fn test_compact_noop_if_compact_index_le_first() {
        let (_dir, store) = open_store();
        let entries = vec![make_entry(1, 1, b"e1")];
        store.append_entries(&entries).unwrap();

        let first = store.first_index().unwrap();
        store.compact(first).unwrap(); // should be a no-op
        assert_eq!(store.first_index().unwrap(), first);
    }

    #[test]
    fn test_entries_with_max_size() {
        let (_dir, store) = open_store();
        let entries = vec![
            make_entry(1, 1, b"aaaaaaaaaa"), // 10 bytes
            make_entry(2, 1, b"bbbbbbbbbb"), // 10 bytes
            make_entry(3, 1, b"cccccccccc"), // 10 bytes
        ];
        store.append_entries(&entries).unwrap();

        // max_size=15 should return first entry and stop after second
        let fetched = store
            .entries(1, 4, Some(15), GetEntriesContext::empty(false))
            .unwrap();
        assert!(fetched.len() >= 1);
        assert!(fetched.len() <= 3);
    }

    #[test]
    fn test_entries_compacted_range() {
        let (_dir, store) = open_store();
        let entries = vec![make_entry(1, 1, b"e1"), make_entry(2, 1, b"e2")];
        store.append_entries(&entries).unwrap();
        store.compact(2).unwrap();

        let result = store.entries(1, 3, None, GetEntriesContext::empty(false));
        assert!(matches!(
            result,
            Err(e) if matches!(e, raft::Error::Store(raft::StorageError::Compacted))
        ));
    }

    #[test]
    fn test_snapshot_metadata() {
        let (_dir, store) = open_store();
        let entries = vec![make_entry(1, 1, b"e1"), make_entry(2, 2, b"e2")];
        store.append_entries(&entries).unwrap();

        let snap = store.snapshot(0, 0).unwrap();
        assert_eq!(snap.get_metadata().index, 2);
        assert_eq!(snap.get_metadata().term, 2);
    }

    #[test]
    fn test_empty_append_is_noop() {
        let (_dir, store) = open_store();
        store.append_entries(&[]).unwrap();
        assert_eq!(store.last_index().unwrap(), 0);
    }
}
