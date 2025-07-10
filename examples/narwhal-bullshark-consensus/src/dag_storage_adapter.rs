//! Storage adapter for Narwhal DAG persistence

use crate::consensus_storage::MdbxConsensusStorage;
use narwhal::types::{Certificate, CertificateDigest, Vote, HeaderDigest, PublicKey};
use narwhal::storage_trait::DagStorageInterface;
use fastcrypto::Hash;
type Round = u64;
use bullshark::dag::Dag;
use alloy_primitives::B256;
use std::sync::Arc;
use tracing::{debug, error};
use async_trait::async_trait;

/// Adapter for storing and retrieving DAG data
pub struct DagStorageAdapter {
    storage: Arc<MdbxConsensusStorage>,
}

impl DagStorageAdapter {
    /// Create a new DAG storage adapter
    pub fn new(storage: Arc<MdbxConsensusStorage>) -> Self {
        Self { storage }
    }

    /// Store a certificate in the DAG
    pub fn store_certificate(&self, certificate: &Certificate) -> anyhow::Result<()> {
        let digest = certificate.digest();
        let round = certificate.header.round;
        
        debug!(
            "Storing certificate {} at round {}",
            hex::encode(digest.as_bytes()),
            round
        );
        
        // Serialize certificate
        let data = bincode::serialize(certificate)?;
        
        // Store in MDBX via consensus storage (using DAG vertex table)
        let digest_b256 = B256::from_slice(digest.as_bytes());
        self.storage.store_dag_vertex(digest_b256, data)?;
        
        // Also index by round for efficient queries
        self.storage.index_certificate_by_round(round, digest.as_bytes().to_vec())?;
        
        Ok(())
    }

    /// Retrieve a certificate by digest
    pub fn get_certificate(&self, digest: &CertificateDigest) -> anyhow::Result<Option<Certificate>> {
        let digest_b256 = B256::from_slice(digest.as_bytes());
        match self.storage.get_dag_vertex(digest_b256)? {
            Some(data) => {
                let cert = bincode::deserialize(&data)?;
                Ok(Some(cert))
            }
            None => Ok(None),
        }
    }

    /// Get all certificates for a given round
    pub fn get_certificates_by_round(&self, round: Round) -> anyhow::Result<Vec<Certificate>> {
        let digests = self.storage.get_certificates_by_round(round)?;
        let mut certificates = Vec::new();
        
        for digest in digests {
            if digest.len() == 32 {
                let mut digest_array = [0u8; 32];
                digest_array.copy_from_slice(&digest);
                let cert_digest = CertificateDigest::new(digest_array);
                if let Some(cert) = self.get_certificate(&cert_digest)? {
                    certificates.push(cert);
                }
            }
        }
        
        Ok(certificates)
    }

    /// Store a vote
    pub fn store_vote(&self, certificate_digest: &CertificateDigest, vote_data: &[u8]) -> anyhow::Result<()> {
        let digest_b256 = B256::from_slice(certificate_digest.as_bytes());
        self.storage.store_vote(digest_b256, vote_data.to_vec())
    }

    /// Get votes for a certificate
    pub fn get_votes(&self, certificate_digest: &CertificateDigest) -> anyhow::Result<Vec<Vec<u8>>> {
        let digest_b256 = B256::from_slice(certificate_digest.as_bytes());
        self.storage.get_votes(digest_b256)
    }

    /// Perform garbage collection on old DAG data
    pub fn garbage_collect(&self, keep_rounds: u64) -> anyhow::Result<()> {
        debug!("Running DAG garbage collection, keeping {} rounds", keep_rounds);
        
        // Get current round from storage
        let current_round = self.storage.get_latest_round()?;
        
        if current_round <= keep_rounds {
            return Ok(());
        }
        
        let cutoff_round = current_round - keep_rounds;
        
        // Remove old certificates (batch operation)
        let removed_count = self.storage.remove_certificates_before_round(cutoff_round)?;
        debug!("Removed {} certificates before round {}", removed_count, cutoff_round);
        
        Ok(())
    }
}

/// Extension trait for DAG operations
pub trait DagExt {
    /// Get the latest round in the DAG
    fn latest_round(&self) -> Round;
    
    /// Get all certificates at a specific round
    fn certificates_at_round(&self, round: Round) -> Vec<&Certificate>;
}

impl DagExt for Dag {
    fn latest_round(&self) -> Round {
        // Implementation would depend on actual Dag structure
        0
    }
    
    fn certificates_at_round(&self, _round: Round) -> Vec<&Certificate> {
        // Implementation would depend on actual Dag structure
        vec![]
    }
}

#[async_trait]
impl DagStorageInterface for DagStorageAdapter {
    async fn store_certificate(&self, certificate: Certificate) -> narwhal::DagResult<()> {
        self.store_certificate(&certificate)
            .map_err(|e| narwhal::DagError::StorageError(format!("Failed to store certificate: {}", e)))
    }

    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        self.get_certificate(digest).unwrap_or(None)
    }

    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> narwhal::DagResult<()> {
        let vote_data = bincode::serialize(&vote)
            .map_err(|e| narwhal::DagError::StorageError(format!("Failed to serialize vote: {}", e)))?;
        
        // Convert HeaderDigest to B256 for storage
        let digest_b256 = {
            use fastcrypto::Hash;
            let bytes: [u8; 32] = header_digest.to_bytes();
            alloy_primitives::B256::from(bytes)
        };
        
        self.storage.store_vote(digest_b256, vote_data)
            .map_err(|e| narwhal::DagError::StorageError(format!("Failed to store vote: {}", e)))
    }

    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        // Convert HeaderDigest to B256 for storage lookup
        let digest_b256 = {
            use fastcrypto::Hash;
            let bytes: [u8; 32] = header_digest.to_bytes();
            alloy_primitives::B256::from(bytes)
        };
        
        match self.storage.get_votes(digest_b256) {
            Ok(vote_data_vec) => {
                vote_data_vec.into_iter()
                    .filter_map(|data| bincode::deserialize(&data).ok())
                    .collect()
            }
            Err(_) => vec![]
        }
    }

    async fn remove_votes(&self, _header_digest: &HeaderDigest) -> narwhal::DagResult<()> {
        // Implementation would need to be added to storage layer
        Ok(())
    }

    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        self.get_certificates_by_round(round).unwrap_or_default()
    }

    async fn store_latest_certificate(&self, _authority: PublicKey, certificate: Certificate) -> narwhal::DagResult<()> {
        // Store the certificate (latest tracking could be added later)
        self.store_certificate(&certificate)
            .map_err(|e| narwhal::DagError::StorageError(format!("Failed to store latest certificate: {}", e)))
    }

    async fn get_latest_certificate(&self, _authority: &PublicKey) -> Option<Certificate> {
        // Implementation would need authority->certificate mapping
        None
    }

    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        if round == 0 {
            return vec![];
        }
        
        let parent_round = round - 1;
        self.get_certificates_by_round(parent_round)
            .unwrap_or_default()
            .into_iter()
            .map(|cert| cert.digest())
            .collect()
    }

    async fn garbage_collect(&self, cutoff_round: Round) -> narwhal::DagResult<()> {
        self.garbage_collect(cutoff_round)
            .map_err(|e| narwhal::DagError::StorageError(format!("Garbage collection failed: {}", e)))
    }
}