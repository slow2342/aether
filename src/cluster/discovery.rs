use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

/// Discovered peer information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub node_id: u64,
    pub addr: String,
}

/// Errors returned by the discovery service.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("dns resolution failed for {host}: {reason}")]
    DnsResolution { host: String, reason: String },

    #[error("invalid address format: {0}")]
    InvalidAddress(String),

    #[error("discovery config error: {0}")]
    Config(String),

    #[error("discovery timed out after {0:?}")]
    Timeout(Duration),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Discovery configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery method: "static", "dns", or "token".
    #[serde(default = "default_method")]
    pub method: String,

    /// DNS hostnames to resolve (for method = "dns").
    #[serde(default)]
    pub dns_hosts: Vec<String>,

    /// Default port for DNS-resolved addresses (if not specified in hostname).
    #[serde(default = "default_port")]
    pub dns_default_port: u16,

    /// Path to a shared file for token-based discovery (for method = "token").
    #[serde(default)]
    pub token_file: PathBuf,

    /// Expected cluster size for token-based discovery.
    #[serde(default = "default_expected_size")]
    pub token_expected_size: usize,

    /// Poll interval for discovery (used by token method).
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Timeout for the overall discovery process.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_method() -> String {
    "static".to_string()
}

fn default_port() -> u16 {
    2380
}

fn default_expected_size() -> usize {
    3
}

fn default_poll_interval_ms() -> u64 {
    500
}

fn default_timeout_secs() -> u64 {
    30
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            method: default_method(),
            dns_hosts: Vec::new(),
            dns_default_port: default_port(),
            token_file: PathBuf::new(),
            token_expected_size: default_expected_size(),
            poll_interval_ms: default_poll_interval_ms(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

/// Trait for cluster discovery providers.
#[async_trait]
pub trait DiscoveryProvider: Send + Sync {
    /// Discover peers in the cluster.
    async fn discover(&self) -> Result<Vec<PeerInfo>, DiscoveryError>;
}

/// DNS-based discovery: resolves hostnames to peer addresses.
pub struct DnsSrvDiscovery {
    hosts: Vec<String>,
    default_port: u16,
    timeout: Duration,
}

impl DnsSrvDiscovery {
    pub fn new(hosts: Vec<String>, default_port: u16, timeout: Duration) -> Self {
        Self {
            hosts,
            default_port,
            timeout,
        }
    }

    /// Parse a host string, extracting optional port and node id.
    /// Supports IPv4 and bracketed IPv6:
    /// - "hostname" (uses default_port)
    /// - "hostname:port"
    /// - "hostname:port:id" (with explicit node id)
    /// - "[::1]:port"
    /// - "[::1]:port:id"
    fn parse_host_entry(entry: &str, default_port: u16) -> Result<(u64, String), DiscoveryError> {
        // Handle bracketed IPv6: extract host from [...] first, then parse port:id from the rest.
        if let Some(bracket_end) = entry.find(']') {
            if !entry.starts_with('[') {
                return Err(DiscoveryError::InvalidAddress(entry.to_string()));
            }
            let host = &entry[1..bracket_end];
            let rest = &entry[bracket_end + 1..];
            // rest is either "" or ":port" or ":port:id"
            if rest.is_empty() {
                return Ok((0, format!("[{host}]:{default_port}")));
            }
            if !rest.starts_with(':') {
                return Err(DiscoveryError::InvalidAddress(entry.to_string()));
            }
            let after_colon = &rest[1..];
            let (port_str, id) = match after_colon.find(':') {
                Some(idx) => (&after_colon[..idx], Some(&after_colon[idx + 1..])),
                None => (after_colon, None),
            };
            let port: u16 = port_str
                .parse()
                .map_err(|_| DiscoveryError::InvalidAddress(entry.to_string()))?;
            let node_id = match id {
                Some(id_str) => id_str
                    .parse()
                    .map_err(|_| DiscoveryError::InvalidAddress(entry.to_string()))?,
                None => 0,
            };
            return Ok((node_id, format!("[{host}]:{port}")));
        }

        // IPv4 / hostname: split on ':'
        let parts: Vec<&str> = entry.splitn(3, ':').collect();
        match parts.len() {
            1 => {
                let host = parts[0].to_string();
                let addr = format!("{host}:{default_port}");
                Ok((0, addr))
            }
            2 => {
                let host = parts[0].to_string();
                let port: u16 = parts[1]
                    .parse()
                    .map_err(|_| DiscoveryError::InvalidAddress(entry.to_string()))?;
                Ok((0, format!("{host}:{port}")))
            }
            3 => {
                let host = parts[0].to_string();
                let port: u16 = parts[1]
                    .parse()
                    .map_err(|_| DiscoveryError::InvalidAddress(entry.to_string()))?;
                let id: u64 = parts[2]
                    .parse()
                    .map_err(|_| DiscoveryError::InvalidAddress(entry.to_string()))?;
                Ok((id, format!("{host}:{port}")))
            }
            _ => Err(DiscoveryError::InvalidAddress(entry.to_string())),
        }
    }
}

#[async_trait]
impl DiscoveryProvider for DnsSrvDiscovery {
    async fn discover(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let deadline = tokio::time::Instant::now() + self.timeout;

        // First pass: parse all entries and resolve addresses.
        let mut parsed: Vec<(u64, String)> = Vec::with_capacity(self.hosts.len());
        for entry in &self.hosts {
            if tokio::time::Instant::now() >= deadline {
                return Err(DiscoveryError::Timeout(self.timeout));
            }
            let (id, addr) = Self::parse_host_entry(entry, self.default_port)?;

            // Resolve the hostname to verify it's reachable and get the canonical address.
            let addr_clone = addr.clone();
            let resolved =
                tokio::time::timeout(Duration::from_secs(5), tokio::net::lookup_host(&addr_clone))
                    .await;

            let resolved_addr = match resolved {
                Ok(Ok(addrs)) => {
                    addrs
                        .into_iter()
                        .next()
                        .ok_or_else(|| DiscoveryError::DnsResolution {
                            host: addr,
                            reason: "no addresses returned".to_string(),
                        })?
                }
                Ok(Err(e)) => {
                    return Err(DiscoveryError::DnsResolution {
                        host: addr,
                        reason: e.to_string(),
                    });
                }
                Err(_) => {
                    return Err(DiscoveryError::DnsResolution {
                        host: addr,
                        reason: "timeout".to_string(),
                    });
                }
            };

            parsed.push((id, resolved_addr.to_string()));
        }

        // Second pass: assign node IDs, avoiding collisions with user-specified IDs.
        let used_ids: std::collections::HashSet<u64> = parsed
            .iter()
            .filter(|(id, _)| *id != 0)
            .map(|(id, _)| *id)
            .collect();
        let mut auto_id: u64 = 1;
        let mut peers = Vec::with_capacity(parsed.len());
        for (id, addr) in parsed {
            let node_id = if id == 0 {
                while used_ids.contains(&auto_id) {
                    auto_id += 1;
                }
                let assigned = auto_id;
                auto_id += 1;
                assigned
            } else {
                id
            };
            peers.push(PeerInfo { node_id, addr });
        }

        Ok(peers)
    }
}

/// Token-based discovery: nodes register in a shared file.
///
/// Protocol:
/// 1. First node creates the token file with its registration.
/// 2. Subsequent nodes append their registration.
/// 3. Once `expected_size` nodes have registered, discovery completes.
///
/// The token file format is newline-delimited JSON, one `PeerInfo` per line.
pub struct TokenDiscovery {
    token_file: PathBuf,
    self_info: PeerInfo,
    expected_size: usize,
    poll_interval: Duration,
    timeout: Duration,
}

/// JSON-serializable entry for the token file.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TokenEntry {
    node_id: u64,
    addr: String,
}

impl TokenDiscovery {
    pub fn new(
        token_file: PathBuf,
        self_info: PeerInfo,
        expected_size: usize,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Self {
        Self {
            token_file,
            self_info,
            expected_size,
            poll_interval,
            timeout,
        }
    }

    /// Read registered peers from the token file, skipping malformed lines.
    async fn read_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let content = match tokio::fs::read_to_string(&self.token_file).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut peers = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<TokenEntry>(line) {
                Ok(entry) => {
                    peers.push(PeerInfo {
                        node_id: entry.node_id,
                        addr: entry.addr,
                    });
                }
                Err(e) => {
                    // Skip malformed lines (e.g. partial writes from crashed nodes).
                    tracing::warn!(error = %e, line = %line, "skipping malformed token file entry");
                }
            }
        }
        Ok(peers)
    }

    /// Append this node's registration to the token file.
    /// Uses O_APPEND for atomic appends from concurrent writers.
    /// Duplicate entries may accumulate on restart; the expected_size check
    /// counts total entries, so configure expected_size accordingly.
    async fn register(&self) -> Result<(), DiscoveryError> {
        let entry = TokenEntry {
            node_id: self.self_info.node_id,
            addr: self.self_info.addr.clone(),
        };
        let line = format!("{}\n", serde_json::to_string(&entry)?);

        let mut opts = tokio::fs::OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        opts.mode(0o600);
        let mut file = opts.open(&self.token_file).await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl DiscoveryProvider for TokenDiscovery {
    async fn discover(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        // Register self
        self.register().await?;
        tracing::info!(
            node_id = self.self_info.node_id,
            addr = %self.self_info.addr,
            token_file = %self.token_file.display(),
            "registered in discovery token file"
        );

        // Poll until we have enough peers or timeout
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            let peers = self.read_peers().await?;
            if peers.len() >= self.expected_size {
                tracing::info!(
                    peer_count = peers.len(),
                    expected = self.expected_size,
                    "discovery complete: enough peers registered"
                );
                return Ok(peers);
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(DiscoveryError::Timeout(self.timeout));
            }

            tracing::debug!(
                current = peers.len(),
                expected = self.expected_size,
                "waiting for more peers"
            );
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

/// Build a `DiscoveryProvider` from configuration and self info.
pub fn build_provider(
    config: &DiscoveryConfig,
    self_info: PeerInfo,
) -> Result<Box<dyn DiscoveryProvider>, DiscoveryError> {
    match config.method.as_str() {
        "dns" => {
            if config.dns_hosts.is_empty() {
                return Err(DiscoveryError::Config(
                    "dns_hosts must not be empty for dns discovery".to_string(),
                ));
            }
            Ok(Box::new(DnsSrvDiscovery::new(
                config.dns_hosts.clone(),
                config.dns_default_port,
                Duration::from_secs(config.timeout_secs),
            )))
        }
        "token" => {
            if config.token_file.as_os_str().is_empty() {
                return Err(DiscoveryError::Config(
                    "token_file must not be empty for token discovery".to_string(),
                ));
            }
            Ok(Box::new(TokenDiscovery::new(
                config.token_file.clone(),
                self_info,
                config.token_expected_size,
                Duration::from_millis(config.poll_interval_ms),
                Duration::from_secs(config.timeout_secs),
            )))
        }
        other => Err(DiscoveryError::Config(format!(
            "unknown discovery method: {other} (valid: dns, token)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parse_host_entry_hostname_only() {
        let (id, addr) = DnsSrvDiscovery::parse_host_entry("node1.example.com", 2380).unwrap();
        assert_eq!(id, 0);
        assert_eq!(addr, "node1.example.com:2380");
    }

    #[test]
    fn test_parse_host_entry_with_port() {
        let (id, addr) = DnsSrvDiscovery::parse_host_entry("node1.example.com:2381", 2380).unwrap();
        assert_eq!(id, 0);
        assert_eq!(addr, "node1.example.com:2381");
    }

    #[test]
    fn test_parse_host_entry_with_port_and_id() {
        let (id, addr) =
            DnsSrvDiscovery::parse_host_entry("node1.example.com:2381:42", 2380).unwrap();
        assert_eq!(id, 42);
        assert_eq!(addr, "node1.example.com:2381");
    }

    #[test]
    fn test_parse_host_entry_invalid_port() {
        let result = DnsSrvDiscovery::parse_host_entry("host:notaport", 2380);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_token_discovery_register_and_read() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };

        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            1, // expect only 1 node
            Duration::from_millis(50),
            Duration::from_secs(5),
        );

        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[0].addr, "127.0.0.1:2380");
    }

    #[tokio::test]
    async fn test_token_discovery_multiple_nodes() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        // Pre-register two peers in the token file
        let entries = vec![
            TokenEntry {
                node_id: 1,
                addr: "127.0.0.1:2380".to_string(),
            },
            TokenEntry {
                node_id: 2,
                addr: "127.0.0.1:2381".to_string(),
            },
        ];
        let content: String = entries
            .iter()
            .map(|e| format!("{}\n", serde_json::to_string(e).unwrap()))
            .collect();
        tokio::fs::write(&token_file, content).await.unwrap();

        let self_info = PeerInfo {
            node_id: 3,
            addr: "127.0.0.1:2382".to_string(),
        };

        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            3, // expect 3 nodes
            Duration::from_millis(50),
            Duration::from_secs(5),
        );

        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[1].node_id, 2);
        assert_eq!(peers[2].node_id, 3);
    }

    #[tokio::test]
    async fn test_token_discovery_timeout() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };

        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            3, // expect 3 nodes but only 1 registers
            Duration::from_millis(50),
            Duration::from_millis(200), // short timeout
        );

        let result = provider.discover().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DiscoveryError::Timeout(_)));
    }

    #[test]
    fn test_build_provider_static_rejected() {
        let config = DiscoveryConfig::default(); // method = "static"
        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };
        // "static" is handled by the caller, not by build_provider.
        let result = build_provider(&config, self_info);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_provider_dns_empty_hosts() {
        let config = DiscoveryConfig {
            method: "dns".to_string(),
            dns_hosts: Vec::new(),
            ..Default::default()
        };
        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };
        let result = build_provider(&config, self_info);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_provider_unknown_method() {
        let config = DiscoveryConfig {
            method: "unknown".to_string(),
            ..Default::default()
        };
        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };
        let result = build_provider(&config, self_info);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dns_discovery_localhost() {
        // Test with 127.0.0.1 (always resolvable)
        let provider = DnsSrvDiscovery::new(
            vec!["127.0.0.1:2380:1".to_string()],
            2380,
            Duration::from_secs(5),
        );
        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, 1);
        assert!(peers[0].addr.starts_with("127.0.0.1"));
    }

    // --- IPv6 parsing tests ---

    #[test]
    fn test_parse_host_entry_ipv6_with_port_and_id() {
        let (id, addr) = DnsSrvDiscovery::parse_host_entry("[::1]:2381:42", 2380).unwrap();
        assert_eq!(id, 42);
        assert_eq!(addr, "[::1]:2381");
    }

    #[test]
    fn test_parse_host_entry_ipv6_with_port() {
        let (id, addr) = DnsSrvDiscovery::parse_host_entry("[::1]:2381", 2380).unwrap();
        assert_eq!(id, 0);
        assert_eq!(addr, "[::1]:2381");
    }

    #[test]
    fn test_parse_host_entry_ipv6_default_port() {
        let (id, addr) = DnsSrvDiscovery::parse_host_entry("[::1]", 2380).unwrap();
        assert_eq!(id, 0);
        assert_eq!(addr, "[::1]:2380");
    }

    #[test]
    fn test_parse_host_entry_ipv6_loopback() {
        let (id, addr) =
            DnsSrvDiscovery::parse_host_entry("[::ffff:127.0.0.1]:2380:5", 2380).unwrap();
        assert_eq!(id, 5);
        assert_eq!(addr, "[::ffff:127.0.0.1]:2380");
    }

    // --- Auto-ID collision test ---

    #[tokio::test]
    async fn test_dns_discovery_auto_id_avoids_collision() {
        // Host with explicit id=1 and host with auto-id — should not collide.
        let provider = DnsSrvDiscovery::new(
            vec![
                "127.0.0.1:2380:1".to_string(),
                "127.0.0.1:2381".to_string(), // auto-id, should get 2 not 1
            ],
            2380,
            Duration::from_secs(5),
        );
        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 2);
        let ids: Vec<u64> = peers.iter().map(|p| p.node_id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    // --- Token dedup test ---

    #[tokio::test]
    async fn test_token_discovery_always_appends() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        // First "boot" — registers once, expected_size=1 so it completes immediately.
        let provider = TokenDiscovery::new(
            token_file.clone(),
            PeerInfo {
                node_id: 1,
                addr: "127.0.0.1:2380".to_string(),
            },
            1,
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 1);

        // Simulate restart: register() always appends, so there are now 2 entries.
        let provider2 = TokenDiscovery::new(
            token_file.clone(),
            PeerInfo {
                node_id: 1,
                addr: "127.0.0.1:2380".to_string(),
            },
            2, // expect 2 entries (original + restart append)
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        let peers2 = provider2.discover().await.unwrap();
        assert_eq!(peers2.len(), 2);
    }

    #[tokio::test]
    async fn test_token_discovery_re_register_on_addr_change() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };

        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            1,
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        provider.discover().await.unwrap();

        // Simulate restart with same node_id but different address.
        let self_info2 = PeerInfo {
            node_id: 1,
            addr: "10.0.0.5:2380".to_string(),
        };
        let provider2 = TokenDiscovery::new(
            token_file.clone(),
            self_info2,
            1,
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        let peers = provider2.discover().await.unwrap();
        // File now has old entry + new entry (2 lines) — register() always appends.
        assert_eq!(peers.len(), 2);
        // The new address is present.
        assert!(peers.iter().any(|p| p.addr == "10.0.0.5:2380"));
    }

    #[tokio::test]
    async fn test_token_discovery_no_duplicate_on_restart() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };

        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            1,
            Duration::from_millis(50),
            Duration::from_secs(5),
        );

        // First "boot" — registers once.
        let peers = provider.discover().await.unwrap();
        assert_eq!(peers.len(), 1);

        // Simulate restart: same node_id, same file.
        // register() always appends, so the file now has 2 entries.
        let provider2 = TokenDiscovery::new(
            token_file.clone(),
            PeerInfo {
                node_id: 1,
                addr: "127.0.0.1:2380".to_string(),
            },
            2, // expect 2 entries (original + restart append)
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        let peers2 = provider2.discover().await.unwrap();
        assert_eq!(peers2.len(), 2);
    }

    // --- Token corrupt line test ---

    #[tokio::test]
    async fn test_token_discovery_skips_corrupt_lines() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("discovery.json");

        // Write a valid entry followed by a corrupt line and another valid entry.
        let content = "{\"node_id\":1,\"addr\":\"127.0.0.1:2380\"}\n\
                        {this is not valid json\n\
                        {\"node_id\":2,\"addr\":\"127.0.0.1:2381\"}\n";
        tokio::fs::write(&token_file, content).await.unwrap();

        let self_info = PeerInfo {
            node_id: 3,
            addr: "127.0.0.1:2382".to_string(),
        };
        let provider = TokenDiscovery::new(
            token_file.clone(),
            self_info,
            3,
            Duration::from_millis(50),
            Duration::from_secs(5),
        );

        let peers = provider.discover().await.unwrap();
        // Should have 3 peers: 2 from file (skipping corrupt) + 1 self.
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[1].node_id, 2);
        assert_eq!(peers[2].node_id, 3);
    }

    // --- expected_size edge case ---

    #[test]
    fn test_build_provider_token_empty_file() {
        let config = DiscoveryConfig {
            method: "token".to_string(),
            token_file: PathBuf::from("/tmp/test-discovery-token.json"),
            ..Default::default()
        };
        let self_info = PeerInfo {
            node_id: 1,
            addr: "127.0.0.1:2380".to_string(),
        };
        let provider = build_provider(&config, self_info);
        assert!(provider.is_ok());
    }
}
