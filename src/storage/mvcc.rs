use std::collections::HashMap;

use crate::error::StorageError;

/// MVCC key encoding: [key_len: u64 BE][key bytes][revision: u64 BE]
///
/// Big-endian encoding ensures lexicographic order matches logical order:
/// - Same user key sorted by revision ascending
/// - Different user keys sorted by key bytes
pub fn encode_mvcc_key(key: &[u8], revision: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + key.len() + 8);
    encoded.extend_from_slice(&(key.len() as u64).to_be_bytes());
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(&revision.to_be_bytes());
    encoded
}

/// Decode an MVCC key into (user_key, revision).
pub fn decode_mvcc_key(encoded: &[u8]) -> Result<(&[u8], u64), StorageError> {
    if encoded.len() < 16 {
        return Err(StorageError::Codec("mvcc key too short".to_string()));
    }
    let key_len = u64::from_be_bytes(encoded[0..8].try_into().unwrap()) as usize;
    if encoded.len() < 8 + key_len + 8 {
        return Err(StorageError::Codec("mvcc key truncated".to_string()));
    }
    let key = &encoded[8..8 + key_len];
    let revision = u64::from_be_bytes(encoded[8 + key_len..16 + key_len].try_into().unwrap());
    Ok((key, revision))
}

/// Extract just the user key from an encoded MVCC key (no allocation for the key part).
pub fn mvcc_user_key(encoded: &[u8]) -> Result<&[u8], StorageError> {
    if encoded.len() < 16 {
        return Err(StorageError::Codec("mvcc key too short".to_string()));
    }
    let key_len = u64::from_be_bytes(encoded[0..8].try_into().unwrap()) as usize;
    if encoded.len() < 8 + key_len + 8 {
        return Err(StorageError::Codec("mvcc key truncated".to_string()));
    }
    Ok(&encoded[8..8 + key_len])
}

/// Encode a key-only MVCC key (for seek operations).
/// Uses revision 0 as the prefix for seeking the latest revision of a key.
pub fn encode_mvcc_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + key.len());
    encoded.extend_from_slice(&(key.len() as u64).to_be_bytes());
    encoded.extend_from_slice(key);
    encoded
}

/// MVCC value stored alongside each versioned key.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct MvccValue {
    /// Revision when this key was first created.
    pub create_revision: i64,
    /// Revision of this specific modification.
    pub mod_revision: i64,
    /// Version counter (incremented on each update, starts at 1).
    pub version: i64,
    /// Attached lease ID (0 = no lease).
    pub lease: i64,
    /// The user value bytes.
    pub value: Vec<u8>,
}

/// A single generation in a key's version history.
/// A new generation starts after a delete (tombstone).
#[derive(Debug, Clone)]
pub struct KeyRevision {
    /// Revision at which this generation was created.
    pub created: u64,
    /// Version number for this generation (starts at 1).
    pub ver: u64,
    /// All revisions in this generation (in ascending order).
    pub revisions: Vec<u64>,
    /// If set, the revision at which this generation was tombstoned (deleted).
    /// A tombstoned generation is closed — the next put starts a new generation.
    pub tombstone_rev: Option<u64>,
}

/// Tracks the full version history of a single user key.
///
/// Generations represent the lifecycle of a key:
/// - Generation 0: key created at rev 10, updated at rev 15, 20
/// - Tombstone at rev 25 (generation closed)
/// - Generation 1: key re-created at rev 30
///
/// The current generation is always the last one in `generations`.
#[derive(Debug, Clone)]
pub struct KeyIndex {
    pub generations: Vec<KeyRevision>,
}

impl Default for KeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyIndex {
    pub fn new() -> Self {
        Self {
            generations: Vec::new(),
        }
    }

    /// Record a new put (create or update) at the given revision.
    pub fn put(&mut self, revision: u64) {
        match self.generations.last_mut() {
            Some(last) if last.tombstone_rev.is_none() => {
                // Active generation — append.
                last.ver += 1;
                last.revisions.push(revision);
            }
            _ => {
                // No generations or last generation is tombstoned — start new.
                self.generations.push(KeyRevision {
                    created: revision,
                    ver: 1,
                    revisions: vec![revision],
                    tombstone_rev: None,
                });
            }
        }
    }

    /// Record a tombstone (delete) at the given revision.
    /// Closes the current generation so the next put starts a new one.
    pub fn tombstone(&mut self, revision: u64) {
        if let Some(last) = self.generations.last_mut()
            && last.tombstone_rev.is_none()
        {
            last.tombstone_rev = Some(revision);
        }
    }

    /// Find the revision to read for a given target revision.
    ///
    /// `target_rev == 0` means "latest" — returns the last revision in the
    /// last non-empty generation (skipping empty sentinel generations).
    ///
    /// Returns `None` if the key does not exist at the target revision
    /// (never created, or was a tombstone at that revision).
    pub fn get(&self, target_rev: u64) -> Option<u64> {
        if self.generations.is_empty() {
            return None;
        }

        if target_rev == 0 {
            // Latest: walk backwards to find the last non-empty generation.
            for generation in self.generations.iter().rev() {
                if generation.tombstone_rev.is_some() {
                    // Tombstoned generation: return the tombstone revision.
                    return generation.tombstone_rev;
                }
                if !generation.revisions.is_empty() {
                    return generation.revisions.last().copied();
                }
            }
            return None;
        }

        // Specific revision: search generations in order.
        for generation in &self.generations {
            if generation.created > target_rev {
                break;
            }
            // Check if the tombstone itself is at the target revision.
            if generation.tombstone_rev == Some(target_rev) {
                return Some(target_rev);
            }
            // If this generation is tombstoned and the target is past the
            // tombstone, skip to the next generation.
            if let Some(ts) = generation.tombstone_rev
                && target_rev > ts
            {
                continue;
            }
            // Find the largest revision <= target_rev in this generation.
            for rev in generation.revisions.iter().rev() {
                if *rev <= target_rev {
                    return Some(*rev);
                }
            }
        }

        None
    }

    /// Returns true if the latest state of this key is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.generations
            .last()
            .is_some_and(|g| g.tombstone_rev.is_some())
    }

    /// Serialize the KeyIndex for persistence.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.generations.len() as u32).to_be_bytes());
        for generation in &self.generations {
            buf.extend_from_slice(&generation.created.to_be_bytes());
            buf.extend_from_slice(&generation.ver.to_be_bytes());
            // tombstone_rev: 0 = none, otherwise the revision.
            let ts = generation.tombstone_rev.unwrap_or(0);
            buf.extend_from_slice(&ts.to_be_bytes());
            buf.extend_from_slice(&(generation.revisions.len() as u32).to_be_bytes());
            for rev in &generation.revisions {
                buf.extend_from_slice(&rev.to_be_bytes());
            }
        }
        buf
    }

    /// Deserialize a KeyIndex from bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        if buf.len() < 4 {
            return Err(StorageError::Codec("key_index too short".to_string()));
        }
        let num_gens = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut generations = Vec::with_capacity(num_gens);
        for _ in 0..num_gens {
            if buf.len() < offset + 28 {
                return Err(StorageError::Codec("key_index gen truncated".to_string()));
            }
            let created = u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());
            let ver = u64::from_be_bytes(buf[offset + 8..offset + 16].try_into().unwrap());
            let ts_raw = u64::from_be_bytes(buf[offset + 16..offset + 24].try_into().unwrap());
            let tombstone_rev = if ts_raw == 0 { None } else { Some(ts_raw) };
            let num_revs =
                u32::from_be_bytes(buf[offset + 24..offset + 28].try_into().unwrap()) as usize;
            offset += 28;
            let mut revisions = Vec::with_capacity(num_revs);
            for _ in 0..num_revs {
                if buf.len() < offset + 8 {
                    return Err(StorageError::Codec(
                        "key_index revision truncated".to_string(),
                    ));
                }
                revisions.push(u64::from_be_bytes(
                    buf[offset..offset + 8].try_into().unwrap(),
                ));
                offset += 8;
            }
            generations.push(KeyRevision {
                created,
                ver,
                revisions,
                tombstone_rev,
            });
        }
        Ok(Self { generations })
    }
}

/// Load the global revision counter from the meta column family.
pub fn load_global_revision(
    db: &rocksdb::DB,
    meta_cf: &rocksdb::ColumnFamily,
) -> Result<u64, StorageError> {
    match db.get_cf(meta_cf, b"global_revision")? {
        Some(bytes) => {
            if bytes.len() != 8 {
                return Err(StorageError::Codec(
                    "global_revision value invalid length".to_string(),
                ));
            }
            Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
        }
        None => Ok(0),
    }
}

/// Save the global revision counter into a WriteBatch.
pub fn save_global_revision(
    batch: &mut rocksdb::WriteBatch,
    meta_cf: &rocksdb::ColumnFamily,
    revision: u64,
) {
    batch.put_cf(meta_cf, b"global_revision", revision.to_be_bytes());
}

/// Load all key indexes from the mvcc column family by scanning.
/// This is called once at startup to rebuild the in-memory index.
pub fn load_key_indexes(
    db: &rocksdb::DB,
    mvcc_cf: &rocksdb::ColumnFamily,
) -> Result<HashMap<Vec<u8>, KeyIndex>, StorageError> {
    use rocksdb::IteratorMode;

    let mut indexes: HashMap<Vec<u8>, KeyIndex> = HashMap::new();

    let iter = db.iterator_cf(mvcc_cf, IteratorMode::Start);
    for item in iter {
        let (encoded_key, value_bytes) = item.map_err(StorageError::RocksDb)?;
        let (user_key, revision) = decode_mvcc_key(&encoded_key)?;

        // Check if it's a tombstone (empty value).
        let is_tombstone = value_bytes.is_empty();

        let ki = indexes.entry(user_key.to_vec()).or_default();

        if is_tombstone {
            ki.tombstone(revision);
        } else {
            ki.put(revision);
        }
    }

    Ok(indexes)
}

/// Get the latest MvccValue for a user key using the key index.
/// Returns None if the key is deleted or doesn't exist.
pub fn get_latest(
    db: &rocksdb::DB,
    mvcc_cf: &rocksdb::ColumnFamily,
    key: &[u8],
    key_index: &KeyIndex,
) -> Result<Option<MvccValue>, StorageError> {
    match key_index.get(0) {
        Some(revision) => {
            let mvcc_key = encode_mvcc_key(key, revision);
            match db.get_cf(mvcc_cf, &mvcc_key)? {
                Some(bytes) => {
                    if bytes.is_empty() {
                        // Tombstone
                        Ok(None)
                    } else {
                        let mv: MvccValue =
                            rkyv::from_bytes::<MvccValue, rkyv::rancor::BoxedError>(&bytes)
                                .map_err(|e| {
                                    StorageError::Codec(format!("decode MvccValue failed: {e}"))
                                })?;
                        Ok(Some(mv))
                    }
                }
                None => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Get the MvccValue for a user key at a specific revision.
pub fn get_at_revision(
    db: &rocksdb::DB,
    mvcc_cf: &rocksdb::ColumnFamily,
    key: &[u8],
    key_index: &KeyIndex,
    revision: u64,
) -> Result<Option<MvccValue>, StorageError> {
    match key_index.get(revision) {
        Some(found_rev) => {
            let mvcc_key = encode_mvcc_key(key, found_rev);
            match db.get_cf(mvcc_cf, &mvcc_key)? {
                Some(bytes) => {
                    if bytes.is_empty() {
                        Ok(None)
                    } else {
                        let mv: MvccValue =
                            rkyv::from_bytes::<MvccValue, rkyv::rancor::BoxedError>(&bytes)
                                .map_err(|e| {
                                    StorageError::Codec(format!("decode MvccValue failed: {e}"))
                                })?;
                        Ok(Some(mv))
                    }
                }
                None => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Range scan at a specific revision (0 = latest).
/// Returns (user_key, MvccValue) pairs.
pub fn range_scan_rev(
    db: &rocksdb::DB,
    mvcc_cf: &rocksdb::ColumnFamily,
    start: &[u8],
    end: &[u8],
    revision: u64,
    limit: usize,
    key_indexes: &HashMap<Vec<u8>, KeyIndex>,
) -> Result<Vec<(Vec<u8>, MvccValue)>, StorageError> {
    let mut results = Vec::new();

    // Iterate key indexes in the range [start, end).
    let mut keys_in_range: Vec<&Vec<u8>> = key_indexes
        .keys()
        .filter(|k| k.as_slice() >= start && (end.is_empty() || k.as_slice() < end))
        .collect();
    keys_in_range.sort();

    for user_key in keys_in_range {
        if results.len() >= limit {
            break;
        }
        let ki = &key_indexes[user_key];
        if let Some(mv) = get_at_revision(db, mvcc_cf, user_key, ki, revision)? {
            results.push((user_key.clone(), mv));
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_key_encode_decode_roundtrip() {
        let key = b"hello";
        let revision: u64 = 42;
        let encoded = encode_mvcc_key(key, revision);
        let (decoded_key, decoded_rev) = decode_mvcc_key(&encoded).unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_rev, revision);
    }

    #[test]
    fn test_mvcc_key_ordering() {
        // Same key, different revisions: ascending revision order.
        let k1 = encode_mvcc_key(b"key", 1);
        let k2 = encode_mvcc_key(b"key", 2);
        let k3 = encode_mvcc_key(b"key", 100);
        assert!(k1 < k2);
        assert!(k2 < k3);

        // Different keys, same revision: key bytes order.
        let ka = encode_mvcc_key(b"a", 1);
        let kb = encode_mvcc_key(b"b", 1);
        assert!(ka < kb);
    }

    #[test]
    fn test_mvcc_key_empty_key() {
        let encoded = encode_mvcc_key(b"", 99);
        let (key, rev) = decode_mvcc_key(&encoded).unwrap();
        assert_eq!(key, b"");
        assert_eq!(rev, 99);
    }

    #[test]
    fn test_mvcc_key_too_short() {
        assert!(decode_mvcc_key(&[0u8; 15]).is_err());
        assert!(decode_mvcc_key(&[0u8; 3]).is_err());
    }

    #[test]
    fn test_mvcc_user_key() {
        let encoded = encode_mvcc_key(b"test", 1);
        assert_eq!(mvcc_user_key(&encoded).unwrap(), b"test");
    }

    #[test]
    fn test_key_index_put() {
        let mut ki = KeyIndex::new();
        ki.put(10);
        assert_eq!(ki.get(0), Some(10));
        assert_eq!(ki.get(10), Some(10));
        assert_eq!(ki.get(9), None);

        ki.put(15);
        assert_eq!(ki.get(0), Some(15));
        assert_eq!(ki.get(10), Some(10));
        assert_eq!(ki.get(15), Some(15));
        assert_eq!(ki.get(12), Some(10));
    }

    #[test]
    fn test_key_index_tombstone_and_revive() {
        let mut ki = KeyIndex::new();
        ki.put(10);
        ki.put(15);
        ki.tombstone(20);
        ki.put(30);

        // Latest should be 30 (revived).
        assert_eq!(ki.get(0), Some(30));

        // At revision 20, the tombstone itself is visible.
        assert_eq!(ki.get(20), Some(20));

        // At revision 19, the previous put is visible.
        assert_eq!(ki.get(19), Some(15));

        // At revision 25 (between tombstone and revive), nothing.
        assert_eq!(ki.get(25), None);

        // At revision 30, the new put is visible.
        assert_eq!(ki.get(30), Some(30));
    }

    #[test]
    fn test_key_index_tombstone_latest() {
        let mut ki = KeyIndex::new();
        ki.put(10);
        ki.tombstone(20);

        // Latest: tombstone is visible at its own revision.
        assert_eq!(ki.get(0), Some(20));
        assert!(ki.is_tombstone());
    }

    #[test]
    fn test_key_index_empty() {
        let ki = KeyIndex::new();
        assert_eq!(ki.get(0), None);
        assert_eq!(ki.get(100), None);
        assert!(!ki.is_tombstone());
    }

    #[test]
    fn test_key_index_encode_decode_roundtrip() {
        let mut ki = KeyIndex::new();
        ki.put(10);
        ki.put(15);
        ki.tombstone(20);
        ki.put(30);
        ki.put(35);

        let encoded = ki.encode();
        let decoded = KeyIndex::decode(&encoded).unwrap();

        assert_eq!(decoded.generations.len(), ki.generations.len());
        for (a, b) in ki.generations.iter().zip(decoded.generations.iter()) {
            assert_eq!(a.created, b.created);
            assert_eq!(a.ver, b.ver);
            assert_eq!(a.revisions, b.revisions);
        }
    }

    #[test]
    fn test_key_index_encode_decode_empty() {
        let ki = KeyIndex::new();
        let encoded = ki.encode();
        let decoded = KeyIndex::decode(&encoded).unwrap();
        assert!(decoded.generations.is_empty());
    }
}
