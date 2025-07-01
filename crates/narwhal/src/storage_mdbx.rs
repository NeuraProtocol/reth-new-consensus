//! MDBX-backed implementation of DAG storage for Narwhal
//! 
//! This provides persistent storage for Narwhal DAG structures using Reth's MDBX database

use crate::{
    storage_trait::{DagStorageInterface, DagStorageRef},
    types::{Certificate, CertificateDigest, HeaderDigest, Vote, PublicKey},
    DagError, DagResult, Round,
};
use async_trait::async_trait;
use bincode;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use tracing::{debug, info, warn, error};
use alloy_primitives::B256;
use fastcrypto::{Hash, blake2b_256};

/// Trait for database operations that will be implemented by the consensus layer
pub trait ConsensusDbOps: Send + Sync {
    /// Store a certificate by ID
    fn store_certificate(&self, cert_id: u64, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get a certificate by ID
    fn get_certificate(&self, cert_id: u64) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Store a DAG vertex by hash
    fn store_dag_vertex(&self, hash: B256, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get a DAG vertex by hash
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get total number of certificates
    fn get_certificate_count(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Store a vote for a header
    fn store_vote(&self, header_digest: B256, vote_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get all votes for a header
    fn get_votes(&self, header_digest: B256) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Remove all votes for a header
    fn remove_votes(&self, header_digest: B256) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Add certificate to round index
    fn index_certificate_by_round(&self, round: u64, cert_digest: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get all certificate digests for a round
    fn get_certificates_by_round(&self, round: u64) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Remove certificates older than a round
    fn remove_certificates_before_round(&self, round: u64) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>;
}

/// MDBX-backed implementation of DAG storage
pub struct MdbxDagStorage {
    /// Database operations provided by the consensus layer
    db_ops: Arc<dyn ConsensusDbOps>,
    /// Certificate ID counter for mapping digests to IDs
    next_cert_id: Arc<RwLock<u64>>,
    /// Cache of certificate digest to ID mappings
    cert_id_cache: Arc<RwLock<HashMap<CertificateDigest, u64>>>,
    /// Cache of ID to certificate digest mappings
    id_to_digest_cache: Arc<RwLock<HashMap<u64, CertificateDigest>>>,
}

impl MdbxDagStorage {
    /// Create new MDBX-backed storage with injected database operations
    pub fn new(db_ops: Arc<dyn ConsensusDbOps>) -> Self {
        info!("Creating MDBX-backed DAG storage with real database operations");
        
        // Try to determine the next cert ID from existing data
        let next_cert_id = match db_ops.get_certificate_count() {
            Ok(count) => count + 1,
            Err(_) => 1,
        };
        
        Self {
            db_ops,
            next_cert_id: Arc::new(RwLock::new(next_cert_id)),
            cert_id_cache: Arc::new(RwLock::new(HashMap::new())),
            id_to_digest_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create as Arc reference
    pub fn new_ref(db_ops: Arc<dyn ConsensusDbOps>) -> DagStorageRef {
        Arc::new(Self::new(db_ops))
    }

    /// Get or create certificate ID for a digest
    async fn get_or_create_cert_id(&self, digest: CertificateDigest) -> DagResult<u64> {
        // Check cache first
        {
            let cache = self.cert_id_cache.read().await;
            if let Some(&id) = cache.get(&digest) {
                return Ok(id);
            }
        }

        // Not in cache, create new ID
        let mut next_id = self.next_cert_id.write().await;
        let mut cert_cache = self.cert_id_cache.write().await;
        let mut id_cache = self.id_to_digest_cache.write().await;
        
        let id = *next_id;
        *next_id += 1;
        
        cert_cache.insert(digest, id);
        id_cache.insert(id, digest);
        
        Ok(id)
    }

    /// Get certificate ID from digest
    async fn get_cert_id(&self, digest: CertificateDigest) -> Option<u64> {
        let cache = self.cert_id_cache.read().await;
        cache.get(&digest).copied()
    }

    /// Get certificate digest from ID
    async fn get_cert_digest(&self, id: u64) -> Option<CertificateDigest> {
        let cache = self.id_to_digest_cache.read().await;
        cache.get(&id).copied()
    }
}

#[async_trait]
impl DagStorageInterface for MdbxDagStorage {
    async fn store_certificate(&self, certificate: Certificate) -> DagResult<()> {
        let digest = certificate.digest();
        let cert_id = self.get_or_create_cert_id(digest).await?;
        
        // Serialize certificate
        let cert_data = bincode::serialize(&certificate)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize certificate: {}", e)))?;
        
        // Store in MDBX using database operations
        self.db_ops.store_certificate(cert_id, cert_data.clone())
            .map_err(|e| DagError::StorageError(format!("Failed to store certificate: {}", e)))?;
        
        // Also store in DAG vertices table by digest
        let digest_bytes: B256 = digest.into();
        self.db_ops.store_dag_vertex(digest_bytes, cert_data)
            .map_err(|e| DagError::StorageError(format!("Failed to store DAG vertex: {}", e)))?;
        
        // Index certificate by round for efficient round-based queries
        let round = certificate.round();
        let digest_bytes_vec = bincode::serialize(&digest)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize digest: {}", e)))?;
        self.db_ops.index_certificate_by_round(round, digest_bytes_vec)
            .map_err(|e| DagError::StorageError(format!("Failed to index certificate by round: {}", e)))?;
        
        debug!("Stored certificate {} with ID {} in MDBX and indexed for round {}", digest, cert_id, round);
        Ok(())
    }

    async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        // Try to get from DAG vertices first (faster lookup by digest)
        let digest_bytes: B256 = (*digest).into();
        
        match self.db_ops.get_dag_vertex(digest_bytes) {
            Ok(Some(data)) => {
                match bincode::deserialize::<Certificate>(&data) {
                    Ok(cert) => {
                        debug!("Retrieved certificate {} from MDBX DAG vertices", digest);
                        return Some(cert);
                    }
                    Err(e) => {
                        error!("Failed to deserialize certificate from DAG vertices: {}", e);
                    }
                }
            }
            Ok(None) => {
                // Not in DAG vertices, try by ID
                if let Some(cert_id) = self.get_cert_id(*digest).await {
                    match self.db_ops.get_certificate(cert_id) {
                        Ok(Some(data)) => {
                            match bincode::deserialize::<Certificate>(&data) {
                                Ok(cert) => {
                                    debug!("Retrieved certificate {} with ID {} from MDBX", digest, cert_id);
                                    return Some(cert);
                                }
                                Err(e) => {
                                    error!("Failed to deserialize certificate: {}", e);
                                }
                            }
                        }
                        Ok(None) => debug!("Certificate {} not found in MDBX", digest),
                        Err(e) => error!("Failed to get certificate from MDBX: {}", e),
                    }
                }
            }
            Err(e) => error!("Failed to get DAG vertex from MDBX: {}", e),
        }
        
        None
    }

    async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        // Use the round index to efficiently retrieve certificates
        match self.db_ops.get_certificates_by_round(round) {
            Ok(digest_list) => {
                let mut certificates = Vec::new();
                
                for digest_bytes in digest_list {
                    // Deserialize the digest
                    match bincode::deserialize::<CertificateDigest>(&digest_bytes) {
                        Ok(digest) => {
                            // Retrieve the certificate by digest
                            if let Some(cert) = self.get_certificate(&digest).await {
                                certificates.push(cert);
                            }
                        }
                        Err(e) => error!("Failed to deserialize certificate digest: {}", e),
                    }
                }
                
                debug!("Retrieved {} certificates for round {} from MDBX using round index", certificates.len(), round);
                certificates
            }
            Err(e) => {
                error!("Failed to get certificates by round from MDBX: {}", e);
                Vec::new()
            }
        }
    }

    async fn store_latest_certificate(&self, authority: PublicKey, certificate: Certificate) -> DagResult<()> {
        // Store the certificate first
        self.store_certificate(certificate.clone()).await?;
        
        // For latest certificates, we use a special key in the DAG vertices table
        let key = format!("latest_cert_{:?}", authority);
        let key_hash = B256::from_slice(&blake2b_256(|hasher| {
            use blake2::digest::Update;
            hasher.update(key.as_bytes());
        }));
        
        let cert_data = bincode::serialize(&certificate)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize latest certificate: {}", e)))?;
        
        self.db_ops.store_dag_vertex(key_hash, cert_data)
            .map_err(|e| DagError::StorageError(format!("Failed to store latest certificate: {}", e)))?;
        
        debug!("Stored latest certificate for authority {:?} in MDBX", authority);
        Ok(())
    }

    async fn get_latest_certificate(&self, authority: &PublicKey) -> Option<Certificate> {
        let key = format!("latest_cert_{:?}", authority);
        let key_hash = B256::from_slice(&blake2b_256(|hasher| {
            use blake2::digest::Update;
            hasher.update(key.as_bytes());
        }));
        
        match self.db_ops.get_dag_vertex(key_hash) {
            Ok(Some(data)) => {
                match bincode::deserialize::<Certificate>(&data) {
                    Ok(cert) => {
                        debug!("Retrieved latest certificate for authority {:?} from MDBX", authority);
                        Some(cert)
                    }
                    Err(e) => {
                        error!("Failed to deserialize latest certificate: {}", e);
                        None
                    }
                }
            }
            Ok(None) => {
                debug!("No latest certificate found for authority {:?}", authority);
                None
            }
            Err(e) => {
                error!("Failed to get latest certificate from MDBX: {}", e);
                None
            }
        }
    }

    async fn store_vote(&self, header_digest: HeaderDigest, vote: Vote) -> DagResult<()> {
        // Serialize the vote
        let vote_data = bincode::serialize(&vote)
            .map_err(|e| DagError::StorageError(format!("Failed to serialize vote: {}", e)))?;
        
        // Convert header digest to B256 for storage
        let digest_bytes: B256 = header_digest.into();
        
        // Store vote in the votes table indexed by header digest
        self.db_ops.store_vote(digest_bytes, vote_data)
            .map_err(|e| DagError::StorageError(format!("Failed to store vote: {}", e)))?;
        
        debug!("Stored vote from {:?} for header {:?} in MDBX", vote.author, header_digest);
        Ok(())
    }

    async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        // Convert header digest to B256 for storage
        let digest_bytes: B256 = (*header_digest).into();
        
        // Get all votes for this header from the votes table
        match self.db_ops.get_votes(digest_bytes) {
            Ok(vote_data_list) => {
                let mut votes = Vec::new();
                for vote_data in vote_data_list {
                    match bincode::deserialize::<Vote>(&vote_data) {
                        Ok(vote) => votes.push(vote),
                        Err(e) => error!("Failed to deserialize vote: {}", e),
                    }
                }
                debug!("Retrieved {} votes for header {:?} from MDBX", votes.len(), header_digest);
                votes
            }
            Err(e) => {
                error!("Failed to get votes from MDBX: {}", e);
                Vec::new()
            }
        }
    }

    async fn remove_votes(&self, header_digest: &HeaderDigest) -> DagResult<()> {
        // Convert header digest to B256 for storage
        let digest_bytes: B256 = (*header_digest).into();
        
        // Remove all votes for this header from the votes table
        self.db_ops.remove_votes(digest_bytes)
            .map_err(|e| DagError::StorageError(format!("Failed to remove votes: {}", e)))?;
        
        debug!("Removed votes for header {:?} from MDBX", header_digest);
        Ok(())
    }

    async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
        // Get all certificates from the previous round
        let prev_round = round.saturating_sub(1);
        let certificates = self.get_certificates_by_round(prev_round).await;
        certificates.into_iter().map(|cert| Hash::digest(&cert)).collect()
    }

    async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
        // Remove certificates older than the cutoff round
        match self.db_ops.remove_certificates_before_round(cutoff_round) {
            Ok(removed_count) => {
                // Also clean up our in-memory caches
                let mut cert_cache = self.cert_id_cache.write().await;
                let mut id_cache = self.id_to_digest_cache.write().await;
                
                // Get all certificates before cutoff to remove from caches
                for round in 0..cutoff_round {
                    if let Ok(digest_list) = self.db_ops.get_certificates_by_round(round) {
                        for digest_bytes in digest_list {
                            if let Ok(digest) = bincode::deserialize::<CertificateDigest>(&digest_bytes) {
                                // Remove from caches
                                if let Some(cert_id) = cert_cache.remove(&digest) {
                                    id_cache.remove(&cert_id);
                                }
                            }
                        }
                    }
                }
                
                info!("Garbage collected {} certificates older than round {} from MDBX", removed_count, cutoff_round);
                Ok(())
            }
            Err(e) => {
                error!("Failed to garbage collect certificates: {}", e);
                Err(DagError::StorageError(format!("Garbage collection failed: {}", e)))
            }
        }
    }
}