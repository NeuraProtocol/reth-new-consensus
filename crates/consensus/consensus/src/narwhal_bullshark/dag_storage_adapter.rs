//! Adapter to connect MdbxConsensusStorage to Narwhal's DagStorageInterface

use narwhal::{
    storage_trait::{DagStorageInterface, DagStorageRef},
    types::*,
    Round, DagResult, DagError,
};
use crate::consensus_storage::MdbxConsensusStorage;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use fastcrypto::Hash;
use anyhow::anyhow;

/// Adapter that wraps MdbxConsensusStorage to implement DagStorageInterface
pub struct MdbxDagStorageAdapter {
    /// Reference to the consensus storage
    consensus_storage: Arc<MdbxConsensusStorage>,
    /// Temporary in-memory cache for latest certificates per authority
    /// This is an optimization to avoid repeated DB lookups
    latest_certificates_cache: Arc<RwLock<HashMap<narwhal::types::PublicKey, narwhal::types::Certificate>>>,
}

impl MdbxDagStorageAdapter {
    /// Create new adapter
    pub fn new(consensus_storage: Arc<MdbxConsensusStorage>) -> Self {
        Self {
            consensus_storage,
            latest_certificates_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create as Arc reference
    pub fn new_ref(consensus_storage: Arc<MdbxConsensusStorage>) -> DagStorageRef {
        Arc::new(Self::new(consensus_storage))
    }
}

#[async_trait]
impl DagStorageInterface for MdbxDagStorageAdapter {
    async fn store_certificate(&self, certificate: Certificate) -> DagResult<()> {
        // Convert to bytes for storage
        let cert_bytes = bincode::serialize(&certificate)
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to serialize certificate: {}", e)))?;
        
        // Use certificate digest as B256 for storage
        let cert_digest = certificate.digest();
        let digest_bytes: [u8; 32] = cert_digest.to_bytes();
        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
        
        // Store in MDBX DAG vertices table
        self.consensus_storage.store_dag_vertex(digest_b256, cert_bytes.clone())
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to store certificate: {}", e)))?;
        
        // Also index by round for retrieval
        let round = certificate.round();
        self.consensus_storage.index_certificate_by_round(round, digest_bytes.to_vec())
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to index certificate by round: {}", e)))?;
        
        Ok(())
    }
    
    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        // Convert certificate digest to B256 for MDBX lookup
        let digest_bytes: [u8; 32] = digest.to_bytes();
        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
        
        // Retrieve from MDBX storage
        match self.consensus_storage.get_dag_vertex(digest_b256) {
            Ok(Some(cert_bytes)) => {
                // Deserialize certificate from bytes
                match bincode::deserialize::<Certificate>(&cert_bytes) {
                    Ok(certificate) => Some(certificate),
                    Err(e) => {
                        tracing::warn!("Failed to deserialize certificate: {:?}", e);
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to retrieve certificate from storage: {:?}", e);
                None
            }
        }
    }
    
    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        // Get certificate digests for this round from the index
        match self.consensus_storage.get_certificates_by_round(round) {
            Ok(digest_vecs) => {
                let mut certificates = Vec::new();
                
                for digest_vec in digest_vecs {
                    if digest_vec.len() == 32 {
                        // Convert Vec<u8> to B256 for MDBX lookup
                        let mut digest_bytes = [0u8; 32];
                        digest_bytes.copy_from_slice(&digest_vec);
                        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
                        
                        // Retrieve certificate from storage
                        match self.consensus_storage.get_dag_vertex(digest_b256) {
                            Ok(Some(cert_bytes)) => {
                                match bincode::deserialize::<Certificate>(&cert_bytes) {
                                    Ok(certificate) => certificates.push(certificate),
                                    Err(e) => {
                                        tracing::warn!("Failed to deserialize certificate: {:?}", e);
                                    }
                                }
                            }
                            Ok(None) => {
                                tracing::warn!("Certificate digest indexed but certificate not found");
                            }
                            Err(e) => {
                                tracing::warn!("Failed to retrieve certificate: {:?}", e);
                            }
                        }
                    }
                }
                
                certificates
            }
            Err(e) => {
                tracing::warn!("Failed to get certificates by round: {:?}", e);
                Vec::new()
            }
        }
    }
    
    async fn get_latest_certificate(&self, authority: &PublicKey) -> Option<Certificate> {
        let cache = self.latest_certificates_cache.read().await;
        cache.get(authority).cloned()
    }
    
    async fn store_latest_certificate(&self, authority: PublicKey, certificate: Certificate) -> DagResult<()> {
        let mut cache = self.latest_certificates_cache.write().await;
        cache.insert(authority, certificate);
        Ok(())
    }
    
    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> DagResult<()> {
        // Convert header digest to B256 for storage
        let digest_bytes: [u8; 32] = header_digest.to_bytes();
        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
        
        // Serialize vote to bytes
        let vote_bytes = bincode::serialize(&vote)
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to serialize vote: {}", e)))?;
        
        // Store in MDBX using consensus storage
        self.consensus_storage.store_vote(digest_b256, vote_bytes)
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to store vote: {}", e)))?;
        
        Ok(())
    }
    
    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        // Convert header digest to B256 for lookup
        let digest_bytes: [u8; 32] = header_digest.to_bytes();
        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
        
        // Retrieve from MDBX
        match self.consensus_storage.get_votes(digest_b256) {
            Ok(vote_bytes_vec) => {
                let mut votes = Vec::new();
                
                for vote_bytes in vote_bytes_vec {
                    match bincode::deserialize::<Vote>(&vote_bytes) {
                        Ok(vote) => votes.push(vote),
                        Err(e) => {
                            tracing::warn!("Failed to deserialize vote: {:?}", e);
                        }
                    }
                }
                
                votes
            }
            Err(e) => {
                tracing::warn!("Failed to get votes: {:?}", e);
                Vec::new()
            }
        }
    }
    
    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()> {
        // Convert header digest to B256
        let digest_bytes: [u8; 32] = header_digest.to_bytes();
        let digest_b256 = alloy_primitives::B256::from(digest_bytes);
        
        // Remove from MDBX
        self.consensus_storage.remove_votes(digest_b256)
            .map_err(|e| DagError::Storage(anyhow::anyhow!("Failed to remove votes: {}", e)))?;
        
        Ok(())
    }
    
    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        // For getting parents, we need certificates from round - 1
        if round == 0 {
            return Vec::new();
        }
        
        let parent_round = round - 1;
        
        // Get all certificates from parent round
        match self.consensus_storage.get_certificates_by_round(parent_round) {
            Ok(digest_vecs) => {
                let mut parent_digests = Vec::new();
                
                for digest_vec in digest_vecs {
                    if digest_vec.len() == 32 {
                        // Convert Vec<u8> back to CertificateDigest
                        let mut digest_bytes = [0u8; 32];
                        digest_bytes.copy_from_slice(&digest_vec);
                        
                        // Create CertificateDigest from bytes
                        let cert_digest = CertificateDigest::new(digest_bytes);
                        parent_digests.push(cert_digest);
                    }
                }
                
                parent_digests
            }
            Err(e) => {
                tracing::warn!("Failed to get parents for round {}: {:?}", round, e);
                Vec::new()
            }
        }
    }
    
    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
        // Remove certificates and votes before the cutoff round
        match self.consensus_storage.remove_certificates_before_round(cutoff_round) {
            Ok(removed_count) => {
                tracing::info!("Garbage collected {} certificates before round {}", removed_count, cutoff_round);
                Ok(())
            }
            Err(e) => {
                Err(DagError::Storage(anyhow::anyhow!("Failed to garbage collect: {}", e)))
            }
        }
    }
}