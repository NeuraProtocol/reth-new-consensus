//! Implementation of ConsensusDbOps for integrating Narwhal storage with MDBX

use narwhal::storage_mdbx::ConsensusDbOps;
use crate::consensus_storage::{MdbxConsensusStorage, DatabaseOps};
use alloy_primitives::B256;
use std::sync::Arc;
use tracing::warn;

/// Implementation of ConsensusDbOps that delegates to MdbxConsensusStorage
#[derive(Debug)]
pub struct ConsensusDbOpsImpl {
    storage: Arc<MdbxConsensusStorage>,
}

impl ConsensusDbOpsImpl {
    /// Create new instance with storage reference
    pub fn new(storage: Arc<MdbxConsensusStorage>) -> Self {
        Self { storage }
    }
}

impl ConsensusDbOps for ConsensusDbOpsImpl {
    fn store_certificate(&self, cert_id: u64, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.store_certificate(cert_id, data)
            .map_err(|e| e.into())
    }
    
    fn get_certificate(&self, cert_id: u64) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.storage.get_certificate(cert_id)
            .map_err(|e| e.into())
    }
    
    fn store_dag_vertex(&self, hash: B256, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.store_dag_vertex(hash, data)
            .map_err(|e| e.into())
    }
    
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.storage.get_dag_vertex(hash)
            .map_err(|e| e.into())
    }
    
    fn get_certificate_count(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Get stats and return certificate count
        self.storage.get_stats()
            .map(|stats| stats.total_certificates)
            .map_err(|e| e.into())
    }
    
    fn store_vote(&self, header_digest: B256, vote_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Store vote using the proper ConsensusVotes table
        self.storage.store_vote(header_digest, vote_data)
            .map_err(|e| e.into())
    }
    
    fn get_votes(&self, header_digest: B256) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        // Get votes from the ConsensusVotes table
        self.storage.get_votes(header_digest)
            .map_err(|e| e.into())
    }
    
    fn remove_votes(&self, header_digest: B256) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Remove votes from the ConsensusVotes table
        self.storage.remove_votes(header_digest)
            .map_err(|e| e.into())
    }
    
    fn index_certificate_by_round(&self, round: u64, cert_digest: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Store certificate index using the proper ConsensusCertificatesByRound table
        self.storage.index_certificate_by_round(round, cert_digest)
            .map_err(|e| e.into())
    }
    
    fn get_certificates_by_round(&self, round: u64) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        // Get certificates from the ConsensusCertificatesByRound table
        self.storage.get_certificates_by_round(round)
            .map_err(|e| e.into())
    }
    
    fn remove_certificates_before_round(&self, round: u64) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Remove old certificates using the proper indexed tables
        self.storage.remove_certificates_before_round(round)
            .map_err(|e| e.into())
    }
}

/// Create a new MDBX storage adapter that uses the real database
pub fn create_mdbx_dag_storage(storage: Arc<MdbxConsensusStorage>) -> narwhal::storage_trait::DagStorageRef {
    let db_ops = Arc::new(ConsensusDbOpsImpl::new(storage));
    narwhal::storage_mdbx::MdbxDagStorage::new_ref(db_ops)
}