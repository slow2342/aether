use std::collections::BTreeMap;

use rocksdb::WriteBatch;

use super::region::{Region, RegionEpoch};
use crate::storage::RocksStorage;

/// Default region ID assigned to the initial whole-keyspace region.
pub const DEFAULT_REGION_ID: u64 = 1;

/// In-memory index of all regions, owned by the state machine.
pub struct ShardManager {
    /// All regions by ID.
    regions: BTreeMap<u64, Region>,
    /// start_key → region_id for fast key lookup.
    key_to_region: BTreeMap<Vec<u8>, u64>,
    /// Monotonic ID generator.
    next_region_id: u64,
    /// Maximum number of regions allowed. 0 = unlimited.
    max_regions: usize,
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardManager {
    /// Create a new ShardManager with the default whole-keyspace region.
    pub fn new() -> Self {
        Self::with_max_regions(0)
    }

    /// Create with a maximum region limit. 0 = unlimited.
    pub fn with_max_regions(max_regions: usize) -> Self {
        let default_region = Region {
            id: DEFAULT_REGION_ID,
            start_key: Vec::new(),
            end_key: Vec::new(),
            region_epoch: RegionEpoch::default(),
            leader: 0,
            replicas: Vec::new(),
        };

        let mut regions = BTreeMap::new();
        regions.insert(DEFAULT_REGION_ID, default_region);

        let mut key_to_region = BTreeMap::new();
        key_to_region.insert(Vec::new(), DEFAULT_REGION_ID);

        Self {
            regions,
            key_to_region,
            next_region_id: DEFAULT_REGION_ID + 1,
            max_regions,
        }
    }

    /// Find the region containing `key`.
    pub fn find_region(&self, key: &[u8]) -> Option<&Region> {
        // Find the last start_key <= key. The empty start_key ("") acts as
        // negative infinity, so the range always returns at least one entry.
        self.key_to_region
            .range(..=key.to_vec())
            .next_back()
            .map(|(_, &id)| id)
            .and_then(|id| self.regions.get(&id))
            .filter(|r| r.contains_key(key))
    }

    /// Get a region by ID.
    pub fn get_region(&self, id: u64) -> Option<&Region> {
        self.regions.get(&id)
    }

    /// List all regions.
    pub fn list_regions(&self) -> Vec<&Region> {
        self.regions.values().collect()
    }

    /// Get the configured max_regions limit (0 = unlimited).
    pub fn max_regions(&self) -> usize {
        self.max_regions
    }

    /// Apply a split operation. Returns (parent, child) on success.
    pub fn apply_split(
        &mut self,
        region_id: u64,
        split_key: Vec<u8>,
    ) -> Result<(Region, Region), String> {
        // Enforce max regions limit.
        if self.max_regions > 0 && self.regions.len() >= self.max_regions {
            return Err(format!(
                "cannot split: region count {} would exceed max {}",
                self.regions.len(),
                self.max_regions
            ));
        }

        let parent = self
            .regions
            .get(&region_id)
            .ok_or_else(|| format!("region {region_id} not found"))?;

        // Validate split_key is within the region's range.
        if !split_key.is_empty() {
            if !parent.start_key.is_empty() && split_key <= parent.start_key {
                return Err("split key must be greater than start key".into());
            }
            if !parent.end_key.is_empty() && split_key >= parent.end_key {
                return Err("split key must be less than end key".into());
            }
        } else {
            return Err("split key must not be empty".into());
        }

        let child_id = self.next_region_id;
        self.next_region_id += 1;

        let old_end_key = parent.end_key.clone();

        // Update parent: end_key = split_key, bump epoch.
        let parent = self.regions.get_mut(&region_id).unwrap();
        parent.end_key = split_key.clone();
        parent.region_epoch.version += 1;
        let parent_clone = parent.clone();

        // Update key_to_region: parent's start_key stays, add child's start_key.
        // (parent start_key already in the map)

        // Create child region.
        let child = Region {
            id: child_id,
            start_key: split_key.clone(),
            end_key: old_end_key,
            region_epoch: RegionEpoch::default(),
            leader: parent_clone.leader,
            replicas: parent_clone.replicas.clone(),
        };

        self.regions.insert(child_id, child.clone());
        self.key_to_region.insert(split_key, child_id);

        Ok((parent_clone, child))
    }

    /// Apply a region update (leader/replica change).
    pub fn apply_update(&mut self, region: Region) -> Result<(), String> {
        let existing = self
            .regions
            .get(&region.id)
            .ok_or_else(|| format!("region {} not found", region.id))?;

        // Reject stale epoch.
        if region.region_epoch.version < existing.region_epoch.version
            || region.region_epoch.conf_ver < existing.region_epoch.conf_ver
        {
            return Err(format!(
                "stale epoch: got {:?}, existing {:?}",
                region.region_epoch, existing.region_epoch
            ));
        }

        // If start_key changed, update key_to_region index.
        if existing.start_key != region.start_key {
            self.key_to_region.remove(&existing.start_key);
            self.key_to_region
                .insert(region.start_key.clone(), region.id);
        }

        self.regions.insert(region.id, region);
        Ok(())
    }

    /// Load regions from the `region` column family in RocksDB.
    /// Returns a ShardManager rebuilt from persistent storage, or a fresh one
    /// with the default region if no data exists.
    pub fn load_from_storage(storage: &RocksStorage) -> Self {
        Self::load_from_storage_with_limit(storage, 0)
    }

    /// Load regions with a max_regions limit. 0 = unlimited.
    pub fn load_from_storage_with_limit(storage: &RocksStorage, max_regions: usize) -> Self {
        let cf = storage.region_cf();
        let iter = storage.db().iterator_cf(
            cf,
            rocksdb::IteratorMode::From(&[], rocksdb::Direction::Forward),
        );

        let mut regions = BTreeMap::new();
        let mut key_to_region = BTreeMap::new();
        let mut max_id = 0u64;

        for item in iter {
            let (_key, value) = match item {
                Ok(kv) => kv,
                Err(e) => {
                    tracing::error!(error = %e, "failed to read region CF entry");
                    continue;
                }
            };

            match rkyv::from_bytes::<Region, rkyv::rancor::BoxedError>(&value) {
                Ok(region) => {
                    if region.id > max_id {
                        max_id = region.id;
                    }
                    key_to_region.insert(region.start_key.clone(), region.id);
                    regions.insert(region.id, region);
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to deserialize region entry");
                }
            }
        }

        if regions.is_empty() {
            return Self::with_max_regions(max_regions);
        }

        Self {
            regions,
            key_to_region,
            next_region_id: max_id + 1,
            max_regions,
        }
    }

    /// Save a single region to a WriteBatch.
    pub fn save_region_to_batch(
        &self,
        batch: &mut WriteBatch,
        storage: &RocksStorage,
        region: &Region,
    ) -> Result<(), String> {
        let cf = storage.region_cf();
        let key = Region::storage_key(region.id);
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(region)
            .map_err(|e| format!("serialize region {}: {e}", region.id))?;
        batch.put_cf(cf, key, &bytes);
        Ok(())
    }
}
