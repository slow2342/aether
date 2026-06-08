/// Snapshot manager for handling snapshot persistence on disk
pub struct SnapshotManager {
    /// Base path for snapshot storage
    base_path: String,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }

    /// Save a snapshot to disk
    pub async fn save_snapshot(
        &self,
        snapshot_id: &str,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        let path = format!("{}/{}", self.base_path, snapshot_id);
        tokio::fs::write(&path, data).await?;
        tracing::info!(path = %path, "saved snapshot");
        Ok(())
    }

    /// Load a snapshot from disk
    pub async fn load_snapshot(&self, snapshot_id: &str) -> Result<Vec<u8>, std::io::Error> {
        let path = format!("{}/{}", self.base_path, snapshot_id);
        let data = tokio::fs::read(&path).await?;
        tracing::info!(path = %path, "loaded snapshot");
        Ok(data)
    }

    /// List available snapshots
    pub async fn list_snapshots(&self) -> Result<Vec<String>, std::io::Error> {
        let mut snapshots = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str()
                && name.starts_with("snapshot-")
            {
                snapshots.push(name.to_string());
            }
        }

        Ok(snapshots)
    }

    /// Delete a snapshot
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), std::io::Error> {
        let path = format!("{}/{}", self.base_path, snapshot_id);
        tokio::fs::remove_file(&path).await?;
        tracing::info!(path = %path, "deleted snapshot");
        Ok(())
    }
}
