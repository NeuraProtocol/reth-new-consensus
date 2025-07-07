//! Storage adapter for Narwhal worker batches

use crate::consensus_storage::MdbxConsensusStorage;
use narwhal::BatchDigest;
use std::sync::Arc;
use tracing::{debug, error};

/// Adapter for storing and retrieving worker batches
pub struct BatchStorageAdapter {
    storage: Arc<MdbxConsensusStorage>,
}

impl BatchStorageAdapter {
    /// Create a new batch storage adapter
    pub fn new(storage: Arc<MdbxConsensusStorage>) -> Self {
        Self { storage }
    }

    /// Store a batch
    pub fn store_batch(&self, digest: &BatchDigest, data: &[u8]) -> anyhow::Result<()> {
        debug!(
            "Storing batch {} ({} bytes)",
            hex::encode(digest),
            data.len()
        );
        
        // Use the batch digest as key
        let key = digest.to_vec();
        
        // Store in MDBX
        self.storage.store_batch(&key, data)?;
        
        Ok(())
    }

    /// Retrieve a batch by digest
    pub fn get_batch(&self, digest: &BatchDigest) -> anyhow::Result<Option<Vec<u8>>> {
        let key = digest.to_vec();
        self.storage.get_batch(&key)
    }

    /// Delete a batch
    pub fn delete_batch(&self, digest: &BatchDigest) -> anyhow::Result<()> {
        let key = digest.to_vec();
        self.storage.delete_batch(&key)?;
        
        debug!("Deleted batch {}", hex::encode(digest));
        Ok(())
    }

    /// Check if a batch exists
    pub fn batch_exists(&self, digest: &BatchDigest) -> anyhow::Result<bool> {
        Ok(self.get_batch(digest)?.is_some())
    }

    /// Get the size of stored batches
    pub fn get_storage_size(&self) -> anyhow::Result<u64> {
        // This would need to be implemented in the storage layer
        Ok(0)
    }
}

/// Worker batch data structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerBatch {
    /// The batch digest
    pub digest: BatchDigest,
    /// The raw transactions in this batch
    pub transactions: Vec<Vec<u8>>,
    /// The worker ID that created this batch
    pub worker_id: u32,
    /// Timestamp when the batch was created
    pub timestamp: u64,
}

impl WorkerBatch {
    /// Create a new worker batch
    pub fn new(transactions: Vec<Vec<u8>>, worker_id: u32) -> Self {
        let data = bincode::serialize(&transactions).unwrap();
        let digest = BatchDigest::new(&data);
        
        Self {
            digest,
            transactions,
            worker_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Serialize the batch for storage
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(self).map_err(Into::into)
    }

    /// Deserialize a batch from storage
    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(data).map_err(Into::into)
    }
}