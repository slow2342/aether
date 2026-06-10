#[allow(clippy::module_inception)]
pub mod lease;
pub mod store;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tokio::sync::watch;

pub use self::lease::{Lease, LeaseInfo};
pub use self::store::LeaseStore;

/// In-memory lease manager. Owned by the state machine.
pub struct LeaseManager {
    /// Active leases indexed by ID
    leases: BTreeMap<i64, Lease>,
    /// Expiry ordering (expiry_time, lease_id) — earliest first
    expiry: BTreeSet<(i64, i64)>,
    /// Reverse mapping: lease_id → set of keys
    lease_keys: HashMap<i64, HashSet<Vec<u8>>>,
    /// Next lease ID counter
    next_id: i64,
    /// Signal channel to wake expiry task (sends earliest expiry_time)
    expiry_tx: watch::Sender<i64>,
    /// Max number of leases
    max_leases: usize,
}

impl LeaseManager {
    pub fn new(max_leases: usize, next_id: i64) -> (Self, watch::Receiver<i64>) {
        let (expiry_tx, expiry_rx) = watch::channel(i64::MAX);
        let sm = Self {
            leases: BTreeMap::new(),
            expiry: BTreeSet::new(),
            lease_keys: HashMap::new(),
            next_id,
            expiry_tx,
            max_leases,
        };
        (sm, expiry_rx)
    }

    /// Rebuild from persisted data (on leader election).
    pub fn restore(&mut self, store: &LeaseStore) -> Result<(), crate::error::StorageError> {
        // Load all data FIRST — if any step fails, in-memory state is untouched.
        let counter = store.load_lease_counter()?;
        let infos = store.load_all_leases()?;
        let pairs = store.load_all_lease_key_pairs()?;

        self.leases.clear();
        self.expiry.clear();
        self.lease_keys.clear();

        if counter > self.next_id {
            self.next_id = counter;
        }

        let now_ms = now_millis();
        for info in infos {
            if info.expiry_time <= now_ms {
                continue; // skip already expired
            }
            let lease = info.to_lease();
            self.expiry.insert((lease.expiry_time, lease.id));
            self.leases.insert(lease.id, lease);
        }

        for (lease_id, key) in pairs {
            if self.leases.contains_key(&lease_id) {
                self.lease_keys.entry(lease_id).or_default().insert(key);
            }
        }

        self.notify_expiry_task();
        Ok(())
    }

    /// Get the next lease ID and increment counter.
    pub fn next_lease_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Grant a new lease. Returns the Lease.
    pub fn grant(&mut self, id: i64, ttl: i64, expiry_time: i64) -> Lease {
        let lease = Lease::new(id, ttl, expiry_time);
        self.leases.insert(id, lease.clone());
        self.expiry.insert((expiry_time, id));
        self.notify_expiry_task();
        lease
    }

    /// Revoke a lease. Returns the set of keys that were attached.
    pub fn revoke(&mut self, id: i64) -> Option<(Lease, HashSet<Vec<u8>>)> {
        let lease = self.leases.remove(&id)?;
        self.expiry.remove(&(lease.expiry_time, id));
        let keys = self.lease_keys.remove(&id).unwrap_or_default();
        self.notify_expiry_task();
        Some((lease, keys))
    }

    /// Keep-alive a lease. Returns the lease if found.
    /// Expiry is checked by the expiry task, not here — this runs inside the
    /// state machine apply path and must not call now_millis() for determinism.
    pub fn keep_alive(&mut self, id: i64, new_expiry_time: i64) -> Option<Lease> {
        let lease = self.leases.get_mut(&id)?;
        self.expiry.remove(&(lease.expiry_time, id));
        lease.expiry_time = new_expiry_time;
        self.expiry.insert((new_expiry_time, id));
        let result = lease.clone();
        self.notify_expiry_task();
        Some(result)
    }

    /// Get a lease by ID.
    pub fn get(&self, id: i64) -> Option<&Lease> {
        self.leases.get(&id)
    }

    /// Get keys attached to a lease.
    pub fn get_keys(&self, id: i64) -> Option<&HashSet<Vec<u8>>> {
        self.lease_keys.get(&id)
    }

    /// List all active lease IDs.
    pub fn list(&self) -> Vec<i64> {
        self.leases.keys().copied().collect()
    }

    /// Number of active leases.
    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }

    /// Attach a key to a lease.
    pub fn attach_key(&mut self, lease_id: i64, key: Vec<u8>) {
        self.lease_keys.entry(lease_id).or_default().insert(key);
    }

    /// Detach a key from a lease.
    pub fn detach_key(&mut self, lease_id: i64, key: &[u8]) {
        if let Some(keys) = self.lease_keys.get_mut(&lease_id) {
            keys.remove(key);
            if keys.is_empty() {
                self.lease_keys.remove(&lease_id);
            }
        }
    }

    /// Get IDs of expired leases without removing them.
    pub fn expired_ids(&self) -> Vec<i64> {
        let now_ms = now_millis();
        // Quick check: if nothing is expired, return empty without allocating
        match self.expiry.iter().next() {
            Some(&(expiry_time, _)) if expiry_time <= now_ms => {}
            _ => return Vec::new(),
        }
        self.expiry
            .iter()
            .take_while(|&&(expiry_time, _)| expiry_time <= now_ms)
            .map(|&(_, id)| id)
            .collect()
    }

    pub fn max_leases(&self) -> usize {
        self.max_leases
    }

    /// Get the current next lease ID counter without incrementing.
    pub fn next_id(&self) -> i64 {
        self.next_id
    }

    /// Notify expiry task of the current earliest expiry time.
    fn notify_expiry_task(&self) {
        let earliest = self
            .expiry
            .iter()
            .next()
            .map(|&(t, _)| t)
            .unwrap_or(i64::MAX);
        let _ = self.expiry_tx.send(earliest);
    }
}

/// Current time in milliseconds since Unix epoch.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as i64
}
