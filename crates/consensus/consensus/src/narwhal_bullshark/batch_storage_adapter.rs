//! Adapter to connect Narwhal batch storage to MDBX consensus storage

use narwhal::batch_store::{BatchStorageOps, MdbxBatchStore};
use narwhal::storage_trait::BatchStore;
use crate::consensus_storage::MdbxConsensusStorage;
use std::sync::Arc;
use alloy_primitives::B256;

/// Adapter that implements BatchStorageOps for MdbxConsensusStorage
pub struct BatchStorageAdapter {
    storage: Arc<MdbxConsensusStorage>,
}

impl BatchStorageAdapter {
    pub fn new(storage: Arc<MdbxConsensusStorage>) -> Self {
        Self { storage }
    }
}

impl BatchStorageOps for BatchStorageAdapter {
    fn store_worker_batch(&self, digest: B256, batch_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.store_worker_batch(digest, batch_data)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn std::error::Error + Send + Sync>)
    }
    
    fn get_worker_batch(&self, digest: B256) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.storage.get_worker_batch(digest)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn std::error::Error + Send + Sync>)
    }
    
    fn delete_worker_batch(&self, digest: B256) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.delete_worker_batch(digest)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn std::error::Error + Send + Sync>)
    }
    
    fn get_worker_batches(&self, digests: &[B256]) -> Result<Vec<Option<Vec<u8>>>, Box<dyn std::error::Error + Send + Sync>> {
        self.storage.get_worker_batches(digests)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Create an MDBX-backed batch store for Narwhal workers
pub fn create_mdbx_batch_store(storage: Arc<MdbxConsensusStorage>) -> Arc<dyn BatchStore> {
    let adapter = Arc::new(BatchStorageAdapter::new(storage));
    Arc::new(MdbxBatchStore::new(adapter))
}