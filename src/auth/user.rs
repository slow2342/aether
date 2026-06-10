use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};

/// User stored in the auth system
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct User {
    pub name: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub enabled: bool,
}

impl User {
    pub fn new(name: String, password_hash: String) -> Self {
        Self {
            name,
            password_hash,
            roles: Vec::new(),
            enabled: true,
        }
    }

    /// Hash a password using argon2id
    pub fn hash_password(password: &[u8]) -> Result<String, String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password, &salt)
            .map_err(|e| format!("password hash failed: {e}"))?;
        Ok(hash.to_string())
    }

    /// Verify a password against the stored hash
    pub fn verify_password(&self, password: &[u8]) -> bool {
        let parsed_hash = match PasswordHash::new(&self.password_hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password, &parsed_hash)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_verify() {
        let hash = User::hash_password(b"secret123").unwrap();
        let user = User::new("alice".to_string(), hash);
        assert!(user.verify_password(b"secret123"));
        assert!(!user.verify_password(b"wrong"));
    }

    #[test]
    fn test_different_hashes_for_same_password() {
        let h1 = User::hash_password(b"same").unwrap();
        let h2 = User::hash_password(b"same").unwrap();
        assert_ne!(h1, h2); // salt differs
    }
}
