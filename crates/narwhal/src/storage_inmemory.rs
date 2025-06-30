//! In-memory implementation of DagStorageInterface
//! This wraps the existing DagStorage for testing and development

use crate::{
    storage_trait::{DagStorageInterface, DagStorageRef},
    storage::DagStorage,
    types::*, 
    Round, DagResult, DagError,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use fastcrypto::Hash;

/// In-memory implementation of DAG storage
pub struct InMemoryDagStorage {
    /// Underlying storage
    storage: Arc<DagStorage>,
    /// Track latest certificates per authority
    latest_certificates: Arc<RwLock<HashMap<PublicKey, Certificate>>>,
}

impl InMemoryDagStorage {
    /// Create new in-memory storage
    pub fn new() -> Self {
        Self {
            storage: Arc::new(DagStorage::new()),
            latest_certificates: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create as Arc reference
    pub fn new_ref() -> DagStorageRef {
        Arc::new(Self::new())
    }
}

#[async_trait]
impl DagStorageInterface for InMemoryDagStorage {
    async fn store_certificate(&self, certificate: Certificate) -> DagResult<()> {
        self.storage.store_certificate(certificate).await
    }
    
    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        self.storage.get_certificate(digest).await
    }
    
    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        self.storage.get_certificates_by_round(round).await
    }
    
    async fn get_latest_certificate(&self, authority: &PublicKey) -> Option<Certificate> {
        let latest = self.latest_certificates.read().await;
        latest.get(authority).cloned()
    }
    
    async fn store_latest_certificate(&self, authority: PublicKey, certificate: Certificate) -> DagResult<()> {
        let mut latest = self.latest_certificates.write().await;
        latest.insert(authority, certificate);
        Ok(())
    }
    
    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> DagResult<()> {
        self.storage.store_vote(vote).await
    }
    
    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        self.storage.get_votes(header_digest).await
    }
    
    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()> {
        // The existing DagStorage doesn't have remove_votes, so we'll need to add it
        // For now, return Ok(()) as a placeholder
        Ok(())
    }
    
    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        // Get certificates from previous round
        let prev_round = round.saturating_sub(1);
        let certs = self.storage.get_certificates_by_round(prev_round).await;
        certs.into_iter().map(|c| c.digest()).collect()
    }
    
    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
        // The existing storage has garbage_collect but with different signature
        // We'll adapt it
        let last_round = self.storage.get_last_committed_round().await;
        let gc_depth = last_round.saturating_sub(cutoff_round);
        self.storage.garbage_collect(last_round, gc_depth).await;
        Ok(())
    }
}