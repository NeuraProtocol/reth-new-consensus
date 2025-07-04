//! MDBX database operations adapter for Narwhal DAG storage
//!
//! This module provides the glue between Narwhal's DagStorage trait
//! and our MDBX-based consensus storage implementation.

use narwhal::storage_trait::{DagStorageInterface, DagStorageRef};
use narwhal::types::{Certificate, CertificateDigest, HeaderDigest, Vote, PublicKey, Header};
use narwhal::{DagResult, Round, DagError};
use crate::consensus_storage::MdbxConsensusStorage;
use std::sync::Arc;
use std::collections::HashMap;
use alloy_primitives::B256;
use async_trait::async_trait;
use tracing::{debug, warn, error};
use tokio::sync::RwLock;
use fastcrypto::{Hash, traits::ToFromBytes};

/// Create MDBX-backed DAG storage adapter
pub fn create_mdbx_dag_storage(storage: Arc<MdbxConsensusStorage>) -> DagStorageRef {
    Arc::new(MdbxDagStorageAdapter { 
        storage,
        // In-memory cache for latest certificates per authority
        latest_certificates: Arc::new(RwLock::new(HashMap::new())),
    })
}

/// Adapter that implements Narwhal's DagStorageInterface using MDBX
pub struct MdbxDagStorageAdapter {
    storage: Arc<MdbxConsensusStorage>,
    /// Cache for latest certificates per authority (not stored in DB yet)
    latest_certificates: Arc<RwLock<HashMap<PublicKey, Certificate>>>,
}

impl MdbxDagStorageAdapter {
    /// Convert certificate digest to B256 for storage
    fn cert_digest_to_b256(digest: &CertificateDigest) -> B256 {
        let bytes: [u8; 32] = digest.to_bytes();
        B256::from(bytes)
    }
    
    /// Convert header digest to B256 for storage
    fn header_digest_to_b256(digest: &HeaderDigest) -> B256 {
        let bytes: [u8; 32] = digest.to_bytes();
        B256::from(bytes)
    }
    
    /// Store certificate in MDBX using the DAG vertices table
    async fn store_certificate_internal(&self, certificate: &Certificate) -> DagResult<()> {
        // Serialize certificate
        let cert_bytes = bincode::serialize(certificate)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize certificate: {}", e)))?;
        
        let digest = certificate.digest();
        let digest_b256 = Self::cert_digest_to_b256(&digest);
        
        // Store in DAG vertices table
        self.storage.store_dag_vertex(digest_b256, cert_bytes)
            .map_err(|e| DagError::StorageError(format!("Failed to store certificate: {}", e)))?;
        
        // Also index by round
        let round_digest_bytes = bincode::serialize(&digest)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize digest: {}", e)))?;
        
        // Try to index by round but ignore errors for now since the table might not exist yet
        if let Err(e) = self.storage.index_certificate_by_round(certificate.header.round, round_digest_bytes) {
            debug!("Warning: Failed to index certificate by round (table may not exist yet): {}", e);
        }
        
        debug!("✅ Stored certificate {} for round {} in MDBX", digest, certificate.header.round);
        Ok(())
    }
    
    /// Get certificate from MDBX
    async fn get_certificate_internal(&self, digest: &CertificateDigest) -> Option<Certificate> {
        let digest_b256 = Self::cert_digest_to_b256(digest);
        
        match self.storage.get_dag_vertex(digest_b256) {
            Ok(Some(cert_bytes)) => {
                match bincode::deserialize::<Certificate>(&cert_bytes) {
                    Ok(cert) => {
                        debug!("✅ Retrieved certificate {} from MDBX", digest);
                        Some(cert)
                    }
                    Err(e) => {
                        error!("Failed to deserialize certificate: {}", e);
                        None
                    }
                }
            }
            Ok(None) => {
                debug!("Certificate {} not found in MDBX", digest);
                None
            }
            Err(e) => {
                error!("Failed to get certificate from MDBX: {}", e);
                None
            }
        }
    }
    
    /// Store vote in MDBX
    async fn store_vote_internal(&self, header_digest: HeaderDigest, vote: &Vote) -> DagResult<()> {
        // Serialize vote
        let vote_bytes = bincode::serialize(vote)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize vote: {}", e)))?;
        
        let digest_b256 = Self::header_digest_to_b256(&header_digest);
        
        // Store vote (this uses the DAG vertices table as a workaround)
        self.storage.store_vote(digest_b256, vote_bytes)
            .map_err(|e| DagError::StorageError(format!("Failed to store vote: {}", e)))?;
        
        debug!("✅ Stored vote for header {} in MDBX", header_digest);
        Ok(())
    }
    
    /// Get votes from MDBX
    async fn get_votes_internal(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        let digest_b256 = Self::header_digest_to_b256(header_digest);
        
        match self.storage.get_votes(digest_b256) {
            Ok(vote_bytes_vec) => {
                let mut votes = Vec::new();
                for vote_bytes in vote_bytes_vec {
                    match bincode::deserialize::<Vote>(&vote_bytes) {
                        Ok(vote) => votes.push(vote),
                        Err(e) => warn!("Failed to deserialize vote: {}", e),
                    }
                }
                debug!("✅ Retrieved {} votes for header {} from MDBX", votes.len(), header_digest);
                votes
            }
            Err(e) => {
                error!("Failed to get votes from MDBX: {}", e);
                Vec::new()
            }
        }
    }
    
    /// Get certificates by round from MDBX
    async fn get_certificates_by_round_internal(&self, round: Round) -> Vec<Certificate> {
        match self.storage.get_certificates_by_round(round) {
            Ok(digest_bytes_vec) => {
                let mut certificates = Vec::new();
                
                for digest_bytes in digest_bytes_vec {
                    // Deserialize the digest
                    match bincode::deserialize::<CertificateDigest>(&digest_bytes) {
                        Ok(digest) => {
                            // Fetch the actual certificate
                            if let Some(cert) = self.get_certificate_internal(&digest).await {
                                certificates.push(cert);
                            }
                        }
                        Err(e) => warn!("Failed to deserialize certificate digest: {}", e),
                    }
                }
                
                debug!("✅ Retrieved {} certificates for round {} from MDBX", certificates.len(), round);
                certificates
            }
            Err(e) => {
                error!("Failed to get certificates by round from MDBX: {}", e);
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl DagStorageInterface for MdbxDagStorageAdapter {
    /// Store a certificate in MDBX
    async fn store_certificate(&self, certificate: Certificate) -> DagResult<()> {
        debug!("MdbxDagStorageAdapter: Storing certificate in MDBX");
        self.store_certificate_internal(&certificate).await
    }

    /// Get a certificate by digest from MDBX
    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        debug!("MdbxDagStorageAdapter: Getting certificate from MDBX");
        self.get_certificate_internal(digest).await
    }

    /// Store a vote in MDBX
    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> DagResult<()> {
        debug!("MdbxDagStorageAdapter: Storing vote in MDBX");
        self.store_vote_internal(header_digest, &vote).await
    }

    /// Get votes for a header from MDBX
    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        debug!("MdbxDagStorageAdapter: Getting votes from MDBX");
        self.get_votes_internal(header_digest).await
    }

    /// Remove votes for a header from MDBX
    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()> {
        debug!("MdbxDagStorageAdapter: Removing votes from MDBX");
        let digest_b256 = Self::header_digest_to_b256(header_digest);
        
        self.storage.remove_votes(digest_b256)
            .map_err(|e| DagError::StorageError(format!("Failed to remove votes: {}", e)))?;
        
        debug!("✅ Removed votes for header {} from MDBX", header_digest);
        Ok(())
    }

    /// Get certificates by round from MDBX
    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        debug!("MdbxDagStorageAdapter: Getting certificates by round {} from MDBX", round);
        self.get_certificates_by_round_internal(round).await
    }

    /// Store latest certificate for an authority (in-memory cache + MDBX)
    async fn store_latest_certificate(&self, authority: PublicKey, certificate: Certificate) -> DagResult<()> {
        debug!("MdbxDagStorageAdapter: Storing latest certificate for authority");
        
        // Store in cache
        self.latest_certificates.write().await.insert(authority.clone(), certificate.clone());
        
        // Also store the certificate itself in MDBX
        self.store_certificate_internal(&certificate).await?;
        
        debug!("✅ Stored latest certificate for authority {} in cache and MDBX", authority);
        Ok(())
    }

    /// Get latest certificate for an authority (from cache)
    async fn get_latest_certificate(&self, authority: &PublicKey) -> Option<Certificate> {
        debug!("MdbxDagStorageAdapter: Getting latest certificate for authority");
        let result = self.latest_certificates.read().await.get(authority).cloned();
        
        if result.is_some() {
            debug!("✅ Found latest certificate for authority {} in cache", authority);
        } else {
            debug!("Latest certificate for authority {} not found in cache", authority);
        }
        
        result
    }

    /// Get certificates from previous round for parent tracking
    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        debug!("MdbxDagStorageAdapter: Getting parents for round {} from MDBX", round);
        
        if round == 0 {
            return Vec::new();
        }
        
        let parent_round = round - 1;
        let certificates = self.get_certificates_by_round_internal(parent_round).await;
        
        let digests: Vec<CertificateDigest> = certificates.into_iter()
            .map(|cert| cert.digest())
            .collect();
        
        debug!("✅ Found {} parent certificates for round {} from MDBX", digests.len(), round);
        digests
    }

    /// Garbage collect old data from MDBX
    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
        debug!("MdbxDagStorageAdapter: Garbage collecting data before round {} in MDBX", cutoff_round);
        
        match self.storage.remove_certificates_before_round(cutoff_round) {
            Ok(removed_count) => {
                debug!("✅ Garbage collected {} certificates before round {} from MDBX", removed_count, cutoff_round);
                
                // Also clean up the latest certificates cache
                let mut cache = self.latest_certificates.write().await;
                cache.retain(|_, cert| cert.header.round >= cutoff_round);
                
                Ok(())
            }
            Err(e) => {
                error!("Failed to garbage collect from MDBX: {}", e);
                Err(DagError::StorageError(format!("Garbage collection failed: {}", e)))
            }
        }
    }
}