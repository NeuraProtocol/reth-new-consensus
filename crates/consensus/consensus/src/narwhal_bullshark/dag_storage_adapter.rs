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
    /// Temporary in-memory cache for data not yet in consensus tables
    /// TODO: Extend consensus tables to include all needed DAG data
    votes_cache: Arc<RwLock<HashMap<narwhal::types::HeaderDigest, Vec<narwhal::types::Vote>>>>,
    latest_certificates_cache: Arc<RwLock<HashMap<narwhal::types::PublicKey, narwhal::types::Certificate>>>,
}

impl MdbxDagStorageAdapter {
    /// Create new adapter
    pub fn new(consensus_storage: Arc<MdbxConsensusStorage>) -> Self {
        Self {
            consensus_storage,
            votes_cache: Arc::new(RwLock::new(HashMap::new())),
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
        
        // Store in consensus storage
        // TODO: Use proper certificate ID from consensus tables
        let cert_id = 0u64; // Placeholder - need proper ID generation
        
        // For now, just cache in memory
        // TODO: Implement actual storage using consensus_storage
        
        Ok(())
    }
    
    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        // TODO: Implement retrieval from consensus storage
        None
    }
    
    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        // TODO: Implement retrieval from consensus storage
        Vec::new()
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
        let mut votes = self.votes_cache.write().await;
        votes.entry(header_digest).or_insert_with(Vec::new).push(vote);
        Ok(())
    }
    
    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        let votes = self.votes_cache.read().await;
        votes.get(header_digest).cloned().unwrap_or_default()
    }
    
    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()> {
        let mut votes = self.votes_cache.write().await;
        votes.remove(header_digest);
        Ok(())
    }
    
    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        // TODO: Implement using consensus storage
        Vec::new()
    }
    
    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
        // TODO: Implement garbage collection
        Ok(())
    }
}