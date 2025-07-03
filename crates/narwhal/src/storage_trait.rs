//! Storage trait for Narwhal DAG
//! This allows us to have different implementations (in-memory, MDBX, etc.)

use crate::{DagError, DagResult, types::*, Round, Batch, BatchDigest};
use async_trait::async_trait;
use std::sync::Arc;

/// Trait for DAG storage operations
#[async_trait]
pub trait DagStorageInterface: Send + Sync {
    /// Store a certificate
    async fn store_certificate(&self, certificate: Certificate) -> DagResult<()>;
    
    /// Get a certificate by digest
    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate>;
    
    /// Get all certificates for a given round
    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate>;
    
    /// Get latest certificate for a given authority
    async fn get_latest_certificate(&self, authority: &PublicKey) -> Option<Certificate>;
    
    /// Store latest certificate for an authority
    async fn store_latest_certificate(&self, authority: PublicKey, certificate: Certificate) -> DagResult<()>;
    
    /// Store a pending vote
    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> DagResult<()>;
    
    /// Get all votes for a header
    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote>;
    
    /// Remove votes for a header
    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()>;
    
    /// Get certificates from previous round for parent tracking
    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest>;
    
    /// Clean up storage older than specified round
    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()>;
}

/// Type alias for storage instances
pub type DagStorageRef = Arc<dyn DagStorageInterface>;

/// Batch storage interface for workers
#[async_trait]
pub trait BatchStore: Send + Sync {
    /// Write a batch to storage
    async fn write_batch(&self, digest: &BatchDigest, batch: &Batch) -> DagResult<()>;
    
    /// Read a batch from storage
    async fn read_batch(&self, digest: &BatchDigest) -> DagResult<Option<Batch>>;
    
    /// Delete a batch from storage
    async fn delete_batch(&self, digest: &BatchDigest) -> DagResult<()>;
    
    /// Read multiple batches
    async fn read_batches(&self, digests: &[BatchDigest]) -> DagResult<Vec<Option<Batch>>>;
}