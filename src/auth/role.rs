/// Permission type
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum PermissionType {
    Read,
    Write,
    Readwrite,
}

impl PermissionType {
    /// Check if this permission type includes the required type
    pub fn includes(self, required: PermissionType) -> bool {
        match self {
            PermissionType::Readwrite => true,
            PermissionType::Read => required == PermissionType::Read,
            PermissionType::Write => required == PermissionType::Write,
        }
    }
}

/// A permission granting access to a key range
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Permission {
    pub perm_type: PermissionType,
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
}

impl Permission {
    /// Check if this permission covers the given single key with the required type
    pub fn covers_key(&self, key: &[u8], required: PermissionType) -> bool {
        if !self.perm_type.includes(required) {
            return false;
        }
        if key < self.key.as_slice() {
            return false;
        }
        if self.range_end.is_empty() {
            // Single key match
            key == self.key.as_slice()
        } else if self.range_end == b"\0" {
            // All keys from self.key to end of keyspace
            true
        } else {
            // Range [self.key, self.range_end)
            key < self.range_end.as_slice()
        }
    }

    /// Check if this permission covers the entire requested range [req_key, req_range_end)
    pub fn covers_range(
        &self,
        req_key: &[u8],
        req_range_end: &[u8],
        required: PermissionType,
    ) -> bool {
        if !self.perm_type.includes(required) {
            return false;
        }
        if req_key < self.key.as_slice() {
            return false;
        }
        if self.range_end.is_empty() {
            // Single key permission can't cover a range
            return false;
        }
        if self.range_end == b"\0" {
            // Wildcard: covers everything from self.key to end of keyspace
            return true;
        }
        // Permission covers [self.key, self.range_end)
        // Request covers [req_key, req_range_end)
        // Need: req_range_end <= self.range_end
        if req_range_end.is_empty() {
            // Single key request, already checked req_key >= self.key
            req_key < self.range_end.as_slice()
        } else if req_range_end == b"\0" {
            // Request goes to end of keyspace, permission doesn't
            false
        } else {
            req_range_end <= self.range_end.as_slice()
        }
    }
}

/// Role stored in the auth system
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

impl Role {
    pub fn new(name: String) -> Self {
        Self {
            name,
            permissions: Vec::new(),
        }
    }
}

/// Extract all unique keys from a TxnRequest for permission checking
pub fn extract_txn_keys(
    compare: &[crate::raft::Compare],
    success: &[crate::raft::RequestOp],
    failure: &[crate::raft::RequestOp],
) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for cmp in compare {
        keys.push(cmp.key.clone());
    }
    for op in success.iter().chain(failure.iter()) {
        if let Some(request) = &op.request {
            match request {
                crate::raft::Request::Put(p) => keys.push(p.key.clone()),
                crate::raft::Request::Get(g) => keys.push(g.key.clone()),
                crate::raft::Request::Delete(d) => keys.push(d.key.clone()),
                crate::raft::Request::Range(r) => keys.push(r.key.clone()),
                crate::raft::Request::Txn(t) => {
                    keys.extend(extract_txn_keys(&t.compare, &t.success, &t.failure));
                }
            }
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_perm(key: &[u8], range_end: &[u8]) -> Permission {
        Permission {
            perm_type: PermissionType::Read,
            key: key.to_vec(),
            range_end: range_end.to_vec(),
        }
    }

    #[test]
    fn test_single_key_match() {
        let p = read_perm(b"/app/a", b"");
        assert!(p.covers_key(b"/app/a", PermissionType::Read));
        assert!(!p.covers_key(b"/app/b", PermissionType::Read));
    }

    #[test]
    fn test_range_match() {
        let p = read_perm(b"/app/", b"/app0");
        assert!(p.covers_key(b"/app/a", PermissionType::Read));
        assert!(p.covers_key(b"/app/b", PermissionType::Read));
        assert!(!p.covers_key(b"/other/", PermissionType::Read));
    }

    #[test]
    fn test_wildcard_range() {
        let p = read_perm(b"/app/", b"\0");
        assert!(p.covers_key(b"/app/a", PermissionType::Read));
        assert!(p.covers_key(b"/zzz", PermissionType::Read));
    }

    #[test]
    fn test_permission_type_includes() {
        assert!(PermissionType::Readwrite.includes(PermissionType::Read));
        assert!(PermissionType::Readwrite.includes(PermissionType::Write));
        assert!(PermissionType::Read.includes(PermissionType::Read));
        assert!(!PermissionType::Read.includes(PermissionType::Write));
    }

    #[test]
    fn test_covers_range() {
        let p = read_perm(b"/app/", b"/app0");
        assert!(p.covers_range(b"/app/a", b"/app/b", PermissionType::Read));
        assert!(!p.covers_range(b"/app/", b"/zzz", PermissionType::Read));
    }

    #[test]
    fn test_covers_range_wildcard() {
        let p = read_perm(b"/", b"\0");
        assert!(p.covers_range(b"/app/", b"/app0", PermissionType::Read));
    }

    #[test]
    fn test_single_key_perm_covers_range() {
        let p = read_perm(b"/app/a", b"");
        assert!(!p.covers_range(b"/app/a", b"/app/b", PermissionType::Read));
    }

    #[test]
    fn test_covers_key_wrong_type() {
        let p = read_perm(b"/app/", b"\0");
        assert!(!p.covers_key(b"/app/a", PermissionType::Write));
    }
}
