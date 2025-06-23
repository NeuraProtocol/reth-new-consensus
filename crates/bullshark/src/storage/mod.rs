pub mod tables;

use alloy_primitives::B256;
use anyhow::Result;

pub use tables::*;

/// Trait for consensus storage operations
/// 
/// This trait will be implemented by Reth using its MDBX database.
/// The consensus engine receives an implementation of this trait
/// rather than depending on database crates directly.
pub trait ConsensusStorage: Send + Sync {
    /// Store a certificate
    fn store_certificate(&self, id: u64, certificate: Certificate) -> Result<()>;
    
    /// Get a certificate by ID
    fn get_certificate(&self, id: u64) -> Result<Option<Certificate>>;
    
    /// Store a consensus batch
    fn store_batch(&self, batch: ConsensusBatch) -> Result<()>;
    
    /// Get a consensus batch by ID
    fn get_batch(&self, id: u64) -> Result<Option<ConsensusBatch>>;
    
    /// Mark a batch as finalized with a block hash
    fn finalize_batch(&self, batch_id: u64, block_hash: B256) -> Result<()>;
    
    /// Get the block hash for a finalized batch
    fn get_finalized_batch(&self, batch_id: u64) -> Result<Option<B256>>;
    
    /// Store a DAG vertex (certificate by its hash)
    fn store_dag_vertex(&self, hash: B256, certificate: Certificate) -> Result<()>;
    
    /// Get a DAG vertex by hash
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Certificate>>;
    
    /// Update the latest finalized certificate ID
    fn set_latest_finalized(&self, certificate_id: u64) -> Result<()>;
    
    /// Get the latest finalized certificate ID
    fn get_latest_finalized(&self) -> Result<Option<u64>>;
}

/// In-memory implementation for testing
#[derive(Debug, Default)]
pub struct InMemoryConsensusStorage {
    certificates: std::sync::Mutex<std::collections::HashMap<u64, Certificate>>,
    batches: std::sync::Mutex<std::collections::HashMap<u64, ConsensusBatch>>,
    finalized_batches: std::sync::Mutex<std::collections::HashMap<u64, B256>>,
    dag_vertices: std::sync::Mutex<std::collections::HashMap<B256, Certificate>>,
    latest_finalized: std::sync::Mutex<Option<u64>>,
}

impl InMemoryConsensusStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConsensusStorage for InMemoryConsensusStorage {
    fn store_certificate(&self, id: u64, certificate: Certificate) -> Result<()> {
        let mut certs = self.certificates.lock().unwrap();
        certs.insert(id, certificate);
        Ok(())
    }
    
    fn get_certificate(&self, id: u64) -> Result<Option<Certificate>> {
        let certs = self.certificates.lock().unwrap();
        Ok(certs.get(&id).cloned())
    }
    
    fn store_batch(&self, batch: ConsensusBatch) -> Result<()> {
        let mut batches = self.batches.lock().unwrap();
        batches.insert(batch.id, batch);
        Ok(())
    }
    
    fn get_batch(&self, id: u64) -> Result<Option<ConsensusBatch>> {
        let batches = self.batches.lock().unwrap();
        Ok(batches.get(&id).cloned())
    }
    
    fn finalize_batch(&self, batch_id: u64, block_hash: B256) -> Result<()> {
        let mut finalized = self.finalized_batches.lock().unwrap();
        finalized.insert(batch_id, block_hash);
        Ok(())
    }
    
    fn get_finalized_batch(&self, batch_id: u64) -> Result<Option<B256>> {
        let finalized = self.finalized_batches.lock().unwrap();
        Ok(finalized.get(&batch_id).copied())
    }
    
    fn store_dag_vertex(&self, hash: B256, certificate: Certificate) -> Result<()> {
        let mut vertices = self.dag_vertices.lock().unwrap();
        vertices.insert(hash, certificate);
        Ok(())
    }
    
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Certificate>> {
        let vertices = self.dag_vertices.lock().unwrap();
        Ok(vertices.get(&hash).cloned())
    }
    
    fn set_latest_finalized(&self, certificate_id: u64) -> Result<()> {
        let mut latest = self.latest_finalized.lock().unwrap();
        *latest = Some(certificate_id);
        Ok(())
    }
    
    fn get_latest_finalized(&self) -> Result<Option<u64>> {
        let latest = self.latest_finalized.lock().unwrap();
        Ok(*latest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_storage() {
        let storage = InMemoryConsensusStorage::new();
        
        let certificate = Certificate {
            batch_id: 1,
            transactions: vec![b"tx1".to_vec(), b"tx2".to_vec()],
            block_hash: B256::from([1u8; 32]),
            timestamp: 1234567890,
            signature: b"signature".to_vec(),
        };
        
        // Store certificate
        storage.store_certificate(1, certificate.clone()).unwrap();
        
        // Retrieve certificate
        let retrieved = storage.get_certificate(1).unwrap();
        assert_eq!(retrieved, Some(certificate));
        
        // Non-existent certificate
        let missing = storage.get_certificate(999).unwrap();
        assert_eq!(missing, None);
    }

    #[test]
    fn test_batch_finalization() {
        let storage = InMemoryConsensusStorage::new();
        
        let batch = ConsensusBatch {
            id: 1,
            transactions: vec![b"tx1".to_vec()],
            timestamp: 1234567890,
        };
        
        let block_hash = B256::from([42u8; 32]);
        
        // Store batch
        storage.store_batch(batch.clone()).unwrap();
        
        // Finalize batch
        storage.finalize_batch(1, block_hash).unwrap();
        
        // Check finalization
        let finalized_hash = storage.get_finalized_batch(1).unwrap();
        assert_eq!(finalized_hash, Some(block_hash));
    }
    
    #[test]
    fn test_latest_finalized() {
        let storage = InMemoryConsensusStorage::new();
        
        // Initially no latest finalized
        assert_eq!(storage.get_latest_finalized().unwrap(), None);
        
        // Set latest finalized
        storage.set_latest_finalized(42).unwrap();
        assert_eq!(storage.get_latest_finalized().unwrap(), Some(42));
        
        // Update latest finalized
        storage.set_latest_finalized(100).unwrap();
        assert_eq!(storage.get_latest_finalized().unwrap(), Some(100));
    }
}

/*
Usage example:

The MDBX storage implementation is provided by reth-consensus crate to avoid circular dependencies.
At node startup, the storage is injected into the consensus engine:

use bullshark::{BullsharkConsensus, ConsensusStorage};
use std::sync::Arc;

fn setup_consensus_with_storage(storage: Arc<dyn ConsensusStorage>) -> anyhow::Result<()> {
    // Create consensus engine with injected storage
    let consensus = BullsharkConsensus::with_storage(committee, config, storage);
    Ok(())
}
*/

 