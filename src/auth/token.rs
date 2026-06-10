use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // username
    pub exp: u64,    // expiry timestamp (seconds)
    pub iat: u64,    // issued at (seconds)
}

/// Validates and creates JWT tokens
pub struct TokenValidator {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    token_expiry_hours: u64,
}

impl TokenValidator {
    pub fn new(signing_key: &str, token_expiry_hours: u64) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(signing_key.as_bytes()),
            decoding_key: DecodingKey::from_secret(signing_key.as_bytes()),
            token_expiry_hours,
        }
    }

    /// Create a JWT token for the given username
    pub fn create_token(&self, username: &str) -> Result<String, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = Claims {
            sub: username.to_string(),
            exp: now + self.token_expiry_hours * 3600,
            iat: now,
        };
        encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| format!("token creation failed: {e}"))
    }

    /// Validate a JWT token and return the claims
    pub fn validate_token(&self, token: &str) -> Result<Claims, String> {
        let token_data = decode::<Claims>(token, &self.decoding_key, &Validation::default())
            .map_err(|e| format!("token validation failed: {e}"))?;
        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_validate() {
        let validator = TokenValidator::new("test-secret", 24);
        let token = validator.create_token("alice").unwrap();
        let claims = validator.validate_token(&token).unwrap();
        assert_eq!(claims.sub, "alice");
    }

    #[test]
    fn test_invalid_token() {
        let validator = TokenValidator::new("test-secret", 24);
        assert!(validator.validate_token("invalid-token").is_err());
    }

    #[test]
    fn test_wrong_secret() {
        let v1 = TokenValidator::new("secret-1", 24);
        let v2 = TokenValidator::new("secret-2", 24);
        let token = v1.create_token("alice").unwrap();
        assert!(v2.validate_token(&token).is_err());
    }
}
