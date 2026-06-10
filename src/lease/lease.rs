/// In-memory representation of a lease.
#[derive(Debug, Clone)]
pub struct Lease {
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub expiry_time: i64,
}

/// Persistent lease information (for RocksDB serialization).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LeaseInfo {
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub expiry_time: i64,
}

impl Lease {
    pub fn new(id: i64, ttl: i64, expiry_time: i64) -> Self {
        Self {
            id,
            ttl,
            granted_ttl: ttl,
            expiry_time,
        }
    }

    pub fn to_info(&self) -> LeaseInfo {
        LeaseInfo {
            id: self.id,
            ttl: self.ttl,
            granted_ttl: self.granted_ttl,
            expiry_time: self.expiry_time,
        }
    }
}

impl LeaseInfo {
    pub fn to_lease(&self) -> Lease {
        Lease {
            id: self.id,
            ttl: self.ttl,
            granted_ttl: self.granted_ttl,
            expiry_time: self.expiry_time,
        }
    }
}
