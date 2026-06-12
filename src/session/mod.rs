use std::collections::HashSet;

use crate::lease::LeaseManager;

/// In-memory session manager. Shared between API layer and state machine.
///
/// A session is a thin wrapper over a lease. The session ID equals the lease ID.
/// This manager provides session-specific validation and tracking.
pub struct SessionManager {
    /// Active session IDs (session_id == lease_id).
    sessions: HashSet<i64>,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: HashSet::new(),
        }
    }

    /// Create a session. session_id == lease_id.
    pub fn create(&mut self, session_id: i64) {
        self.sessions.insert(session_id);
    }

    /// Close a session. Returns true if the session existed.
    pub fn close(&mut self, session_id: i64) -> bool {
        self.sessions.remove(&session_id)
    }

    /// Check if a session exists.
    pub fn exists(&self, session_id: i64) -> bool {
        self.sessions.contains(&session_id)
    }

    /// Get the number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Restore sessions from lease manager (on startup).
    /// All active leases are treated as sessions.
    pub fn restore(&mut self, lease_manager: &LeaseManager) {
        self.sessions.clear();
        self.sessions.extend(lease_manager.list());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_manager_create_close() {
        let mut mgr = SessionManager::new();

        assert!(!mgr.exists(42));
        assert_eq!(mgr.session_count(), 0);

        mgr.create(42);
        assert!(mgr.exists(42));
        assert_eq!(mgr.session_count(), 1);

        assert!(mgr.close(42));
        assert!(!mgr.exists(42));
        assert_eq!(mgr.session_count(), 0);
    }

    #[test]
    fn test_session_manager_close_nonexistent() {
        let mut mgr = SessionManager::new();
        assert!(!mgr.close(99));
    }

    #[test]
    fn test_session_manager_multiple_sessions() {
        let mut mgr = SessionManager::new();

        mgr.create(1);
        mgr.create(2);
        mgr.create(3);

        assert_eq!(mgr.session_count(), 3);
        assert!(mgr.exists(1));
        assert!(mgr.exists(2));
        assert!(mgr.exists(3));

        mgr.close(2);
        assert_eq!(mgr.session_count(), 2);
        assert!(mgr.exists(1));
        assert!(!mgr.exists(2));
        assert!(mgr.exists(3));
    }
}
