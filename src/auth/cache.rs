use std::collections::HashMap;
use std::sync::RwLock;

use super::role::Role;
use super::user::User;

/// In-memory cache for auth data (users and roles).
/// Updated by the state machine on apply, read by the interceptor on every request.
pub struct AuthCache {
    users: RwLock<HashMap<String, User>>,
    roles: RwLock<HashMap<String, Role>>,
}

impl AuthCache {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            roles: RwLock::new(HashMap::new()),
        }
    }

    // --- User operations ---

    pub fn get_user(&self, name: &str) -> Option<User> {
        self.users.read().unwrap().get(name).cloned()
    }

    pub fn insert_user(&self, user: User) {
        self.users.write().unwrap().insert(user.name.clone(), user);
    }

    pub fn remove_user(&self, name: &str) {
        self.users.write().unwrap().remove(name);
    }

    pub fn list_users(&self) -> Vec<User> {
        self.users.read().unwrap().values().cloned().collect()
    }

    /// Check if any user references the given role
    pub fn is_role_in_use(&self, role_name: &str) -> bool {
        self.users
            .read()
            .unwrap()
            .values()
            .any(|u| u.roles.contains(&role_name.to_string()))
    }

    // --- Role operations ---

    pub fn get_role(&self, name: &str) -> Option<Role> {
        self.roles.read().unwrap().get(name).cloned()
    }

    pub fn insert_role(&self, role: Role) {
        self.roles.write().unwrap().insert(role.name.clone(), role);
    }

    pub fn remove_role(&self, name: &str) {
        self.roles.write().unwrap().remove(name);
    }

    pub fn list_roles(&self) -> Vec<Role> {
        self.roles.read().unwrap().values().cloned().collect()
    }

    // --- Bulk load ---

    pub fn load_users(&self, users: Vec<User>) {
        let mut map = self.users.write().unwrap();
        for user in users {
            map.insert(user.name.clone(), user);
        }
    }

    pub fn load_roles(&self, roles: Vec<Role>) {
        let mut map = self.roles.write().unwrap();
        for role in roles {
            map.insert(role.name.clone(), role);
        }
    }
}

impl Default for AuthCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_crud() {
        let cache = AuthCache::new();
        let user = User::new("alice".to_string(), "hash".to_string());
        cache.insert_user(user.clone());

        let got = cache.get_user("alice").unwrap();
        assert_eq!(got.name, "alice");

        let users = cache.list_users();
        assert_eq!(users.len(), 1);

        cache.remove_user("alice");
        assert!(cache.get_user("alice").is_none());
    }

    #[test]
    fn test_role_in_use() {
        let cache = AuthCache::new();
        let mut user = User::new("alice".to_string(), "hash".to_string());
        user.roles.push("admin".to_string());
        cache.insert_user(user);

        assert!(cache.is_role_in_use("admin"));
        assert!(!cache.is_role_in_use("other"));
    }
}
