//! Storage adapter for Narwhal DAG persistence

use crate::consensus_storage::MdbxConsensusStorage;
use narwhal::types::{Certificate, CertificateDigest};
use fastcrypto::Hash;
type Round = u64;
use bullshark::dag::Dag;
use alloy_primitives::B256;
use std::sync::Arc;
use tracing::{debug, error};

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
        let round = certificate.round();
        
        debug!(
            "Storing certificate {} at round {}",
            hex::encode(digest.as_bytes()),
            round
        );
        
        // Serialize certificate
        let data = bincode::serialize(certificate)?;
        
        // Store in MDBX via consensus storage
        self.storage.store_certificate(digest.as_bytes(), data)?;
        
        // Also index by round for efficient queries
        self.storage.index_certificate_by_round(round, digest.as_bytes())?;
        
        Ok(())
    }

    /// Retrieve a certificate by digest
    pub fn get_certificate(&self, digest: &CertificateDigest) -> anyhow::Result<Option<Certificate>> {
        match self.storage.get_certificate(digest.as_ref())? {
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
            if let Some(cert) = self.get_certificate(&CertificateDigest::from(digest))? {
                certificates.push(cert);
            }
        }
        
        Ok(certificates)
    }

    /// Store a vote
    pub fn store_vote(&self, certificate_digest: &CertificateDigest, vote_data: &[u8]) -> anyhow::Result<()> {
        self.storage.store_vote(certificate_digest.as_ref(), vote_data)
    }

    /// Get votes for a certificate
    pub fn get_votes(&self, certificate_digest: &CertificateDigest) -> anyhow::Result<Vec<Vec<u8>>> {
        self.storage.get_votes(certificate_digest.as_ref())
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
        
        // Remove old certificates
        for round in 0..cutoff_round {
            let digests = self.storage.get_certificates_by_round(round)?;
            for digest in digests {
                self.storage.remove_certificate(&digest)?;
            }
        }
        
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