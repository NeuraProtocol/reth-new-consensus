//! Batch storage implementations for workers

use crate::{
    Batch, BatchDigest, DagError, DagResult,
    storage_trait::BatchStore,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};
use alloy_primitives;

/// Trait for batch storage operations in MDBX
pub trait BatchStorageOps: Send + Sync {
    /// Store a worker batch by digest
    fn store_worker_batch(&self, digest: alloy_primitives::B256, batch_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get a worker batch by digest
    fn get_worker_batch(&self, digest: alloy_primitives::B256) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Delete a worker batch by digest
    fn delete_worker_batch(&self, digest: alloy_primitives::B256) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get multiple worker batches by digests
    fn get_worker_batches(&self, digests: &[alloy_primitives::B256]) -> Result<Vec<Option<Vec<u8>>>, Box<dyn std::error::Error + Send + Sync>>;
}

/// In-memory implementation of batch storage
#[derive(Clone)]
pub struct InMemoryBatchStore {
    batches: Arc<RwLock<HashMap<BatchDigest, Batch>>>,
}

impl InMemoryBatchStore {
    pub fn new() -> Self {
        Self {
            batches: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryBatchStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchStore for InMemoryBatchStore {
    async fn write_batch(&self, digest: &BatchDigest, batch: &Batch) -> DagResult<()> {
        trace!("Writing batch {} to memory store", digest);
        self.batches.write().await.insert(*digest, batch.clone());
        Ok(())
    }
    
    async fn read_batch(&self, digest: &BatchDigest) -> DagResult<Option<Batch>> {
        trace!("Reading batch {} from memory store", digest);
        Ok(self.batches.read().await.get(digest).cloned())
    }
    
    async fn delete_batch(&self, digest: &BatchDigest) -> DagResult<()> {
        trace!("Deleting batch {} from memory store", digest);
        self.batches.write().await.remove(digest);
        Ok(())
    }
    
    async fn read_batches(&self, digests: &[BatchDigest]) -> DagResult<Vec<Option<Batch>>> {
        trace!("Reading {} batches from memory store", digests.len());
        let store = self.batches.read().await;
        Ok(digests.iter().map(|d| store.get(d).cloned()).collect())
    }
}

/// MDBX-backed batch storage using a generic storage backend
pub struct MdbxBatchStore<S: BatchStorageOps> {
    /// Reference to the storage backend
    storage: Arc<S>,
}

impl<S: BatchStorageOps> MdbxBatchStore<S> {
    pub fn new(storage: Arc<S>) -> Self {
        debug!("Creating MDBX batch store with real database backend");
        Self { storage }
    }
}

#[async_trait]
impl<S: BatchStorageOps + 'static> BatchStore for MdbxBatchStore<S> {
    async fn write_batch(&self, digest: &BatchDigest, batch: &Batch) -> DagResult<()> {
        // Convert BatchDigest to B256
        let digest_b256 = alloy_primitives::B256::from(digest.0);
        
        // Serialize the batch
        let batch_data = bincode::serialize(batch)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize batch: {}", e)))?;
        
        let batch_size = batch_data.len();
        
        // Store in MDBX
        self.storage.store_worker_batch(digest_b256, batch_data)
            .map_err(|e| DagError::StorageError(format!("Failed to store batch: {}", e)))?;
        
        trace!("Stored batch {} in MDBX ({} bytes)", digest, batch_size);
        Ok(())
    }
    
    async fn read_batch(&self, digest: &BatchDigest) -> DagResult<Option<Batch>> {
        // Convert BatchDigest to B256
        let digest_b256 = alloy_primitives::B256::from(digest.0);
        
        // Read from MDBX
        match self.storage.get_worker_batch(digest_b256) {
            Ok(Some(batch_data)) => {
                // Deserialize the batch
                let batch: Batch = bincode::deserialize(&batch_data)
                    .map_err(|e| DagError::StorageError(format!("Failed to deserialize batch: {}", e)))?;
                trace!("Retrieved batch {} from MDBX ({} bytes)", digest, batch_data.len());
                Ok(Some(batch))
            }
            Ok(None) => {
                trace!("Batch {} not found in MDBX", digest);
                Ok(None)
            }
            Err(e) => Err(DagError::StorageError(format!("Failed to read batch: {}", e)))
        }
    }
    
    async fn delete_batch(&self, digest: &BatchDigest) -> DagResult<()> {
        // Convert BatchDigest to B256
        let digest_b256 = alloy_primitives::B256::from(digest.0);
        
        // Delete from storage
        self.storage.delete_worker_batch(digest_b256)
            .map_err(|e| DagError::StorageError(format!("Failed to delete batch: {}", e)))?;
        
        trace!("Deleted batch {} from MDBX", digest);
        Ok(())
    }
    
    async fn read_batches(&self, digests: &[BatchDigest]) -> DagResult<Vec<Option<Batch>>> {
        // Convert BatchDigests to B256s
        let digest_b256s: Vec<alloy_primitives::B256> = digests
            .iter()
            .map(|d| alloy_primitives::B256::from(d.0))
            .collect();
        
        // Read from storage
        let batch_datas = self.storage.get_worker_batches(&digest_b256s)
            .map_err(|e| DagError::StorageError(format!("Failed to read batches: {}", e)))?;
        
        // Deserialize each batch
        let mut results = Vec::with_capacity(batch_datas.len());
        for (i, batch_data_opt) in batch_datas.into_iter().enumerate() {
            match batch_data_opt {
                Some(batch_data) => {
                    let batch: Batch = bincode::deserialize(&batch_data)
                        .map_err(|e| DagError::StorageError(format!("Failed to deserialize batch: {}", e)))?;
                    results.push(Some(batch));
                }
                None => {
                    trace!("Batch {} not found in MDBX", digests[i]);
                    results.push(None);
                }
            }
        }
        
        trace!("Retrieved {} batches from MDBX", results.iter().filter(|b| b.is_some()).count());
        Ok(results)
    }
}