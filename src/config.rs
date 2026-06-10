use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct AetherConfig {
    #[serde(default = "default_node_id")]
    pub node_id: u64,

    #[serde(default = "default_addr")]
    pub addr: String,

    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub log: LogConfig,

    #[serde(default)]
    pub lease: LeaseConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    #[serde(default)]
    pub peers: Vec<PeerConfig>,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_token_expiry")]
    pub token_expiry_hours: u64,

    #[serde(default = "default_signing_key")]
    pub signing_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default)]
    pub json: bool,
}

fn default_node_id() -> u64 {
    1
}

fn default_addr() -> String {
    "127.0.0.1:2379".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/tmp/aether")
}

fn default_heartbeat_interval() -> u64 {
    1000
}

fn default_election_timeout() -> u64 {
    10000
}

fn default_token_expiry() -> u64 {
    24
}

fn default_signing_key() -> String {
    // Insecure default for development only. All nodes in a cluster must share
    // the same signing key — configure it explicitly for production.
    String::new()
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            election_timeout_ms: default_election_timeout(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            token_expiry_hours: default_token_expiry(),
            signing_key: default_signing_key(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            json: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LeaseConfig {
    #[serde(default = "default_max_ttl")]
    pub max_ttl: i64,

    #[serde(default = "default_max_leases")]
    pub max_leases: usize,
}

fn default_max_ttl() -> i64 {
    86400
}

fn default_max_leases() -> usize {
    10000
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            max_ttl: default_max_ttl(),
            max_leases: default_max_leases(),
        }
    }
}

impl Default for AetherConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            addr: default_addr(),
            data_dir: default_data_dir(),
            cluster: ClusterConfig::default(),
            auth: AuthConfig::default(),
            log: LogConfig::default(),
            lease: LeaseConfig::default(),
        }
    }
}

impl AetherConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AetherConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
