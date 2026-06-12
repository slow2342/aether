/// RegionEpoch tracks structural changes to a region.
/// `conf_ver` increments on replica set changes, `version` on splits/merges.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RegionEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

impl Default for RegionEpoch {
    fn default() -> Self {
        Self {
            conf_ver: 1,
            version: 1,
        }
    }
}

/// A contiguous key range `[start_key, end_key)` with replica placement.
///
/// Empty `start_key` means negative infinity; empty `end_key` means positive infinity.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub region_epoch: RegionEpoch,
    pub leader: u64,
    pub replicas: Vec<u64>,
}

impl Region {
    /// Returns true if `key` falls within this region's range.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        (self.start_key.is_empty() || key >= self.start_key.as_slice())
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }

    /// Returns the big-endian u64 key for storing this region in RocksDB.
    pub fn storage_key(id: u64) -> [u8; 8] {
        id.to_be_bytes()
    }
}
