//! Redb-based consensus storage implementation
//! 
//! This provides a pure-Rust storage backend for consensus data,
//! completely isolated from Reth's MDBX database to avoid lock contention.

use crate::consensus_storage::{ConsensusStorage, DatabaseOps};
use narwhal::storage::{
    BatchStore, CertificateStore, HeaderStore, PayloadStore, VoteStore,
    BatchStoreError, CertificateStoreError, HeaderStoreError, PayloadStoreError, VoteStoreError,
};
use narwhal::types::{
    Batch, BatchDigest, Certificate, CertificateDigest, Header, HeaderDigest,
    Vote, PublicKey as NarwhalPublicKey, Round,
};
use std::sync::Arc;
use redb::{Database, TableDefinition, ReadableTable};
use alloy_primitives::B256;
use tracing::{debug, error, info};

// Define table schemas
const HEADERS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("headers");
const CERTIFICATES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("certificates");
const BATCHES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("batches");
const VOTES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("votes");
const CERTIFICATE_BY_ROUND_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("certificates_by_round");
const CONSENSUS_STATE_TABLE: TableDefinition<&str, u64> = TableDefinition::new("consensus_state");
const FINALIZED_BATCHES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("finalized_batches");

/// Redb-based implementation of consensus storage
pub struct RedbConsensusStorage {
    /// The Redb database instance
    db: Arc<Database>,
}

impl RedbConsensusStorage {
    /// Create a new Redb consensus storage instance
    pub fn new(db_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        info!("Opening Redb consensus database at: {}", db_path);
        
        let db = Database::create(db_path)?;
        
        // Create tables if they don't exist
        let write_tx = db.begin_write()?;
        {
            write_tx.open_table(HEADERS_TABLE)?;
            write_tx.open_table(CERTIFICATES_TABLE)?;
            write_tx.open_table(BATCHES_TABLE)?;
            write_tx.open_table(VOTES_TABLE)?;
            write_tx.open_table(CERTIFICATE_BY_ROUND_TABLE)?;
            write_tx.open_table(CONSENSUS_STATE_TABLE)?;
            write_tx.open_table(FINALIZED_BATCHES_TABLE)?;
        }
        write_tx.commit()?;
        
        info!("✅ Redb consensus storage initialized successfully");
        
        Ok(Self {
            db: Arc::new(db),
        })
    }
    
    /// Get the last consensus index from storage
    pub fn get_last_consensus_index(&self) -> u64 {
        match self.db.begin_read() {
            Ok(read_tx) => {
                match read_tx.open_table(CONSENSUS_STATE_TABLE) {
                    Ok(table) => {
                        match table.get("last_consensus_index") {
                            Ok(Some(value)) => value.value(),
                            _ => 0,
                        }
                    }
                    Err(e) => {
                        error!("Failed to open consensus state table: {}", e);
                        0
                    }
                }
            }
            Err(e) => {
                error!("Failed to begin read transaction: {}", e);
                0
            }
        }
    }
    
    /// Update the last consensus index
    pub fn update_last_consensus_index(&self, index: u64) -> Result<(), Box<dyn std::error::Error>> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(CONSENSUS_STATE_TABLE)?;
            table.insert("last_consensus_index", index)?;
        }
        write_tx.commit()?;
        Ok(())
    }
}

// Implement ConsensusStorage trait
impl ConsensusStorage for RedbConsensusStorage {
    fn set_db_ops(&mut self, _db_ops: Box<dyn DatabaseOps>) {
        // Redb doesn't need external database operations
        debug!("Redb storage doesn't require external database operations");
    }
    
    fn can_store_batches(&self) -> bool {
        true
    }
}

// Implement HeaderStore
impl HeaderStore for RedbConsensusStorage {
    type Error = HeaderStoreError;
    
    fn write(&self, header: &Header) -> Result<(), Self::Error> {
        let digest = header.digest();
        let key = digest.as_ref();
        let value = bincode::serialize(header)
            .map_err(|e| HeaderStoreError::SerializationError(e.to_string()))?;
        
        let write_tx = self.db.begin_write()
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        {
            let mut table = write_tx.open_table(HEADERS_TABLE)
                .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
            table.insert(key, value.as_slice())
                .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        }
        write_tx.commit()
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        
        debug!("Stored header with digest: {:?}", digest);
        Ok(())
    }
    
    fn read(&self, digest: &HeaderDigest) -> Result<Option<Header>, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(HEADERS_TABLE)
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(value)) => {
                let header = bincode::deserialize(value.value())
                    .map_err(|e| HeaderStoreError::SerializationError(e.to_string()))?;
                Ok(Some(header))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(HeaderStoreError::StoreError(e.to_string())),
        }
    }
    
    fn contains(&self, digest: &HeaderDigest) -> Result<bool, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(HEADERS_TABLE)
            .map_err(|e| HeaderStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(HeaderStoreError::StoreError(e.to_string())),
        }
    }
}

// Implement CertificateStore
impl CertificateStore for RedbConsensusStorage {
    type Error = CertificateStoreError;
    
    fn write(&self, certificate: &Certificate) -> Result<(), Self::Error> {
        let digest = certificate.digest();
        let key = digest.as_ref();
        let value = bincode::serialize(certificate)
            .map_err(|e| CertificateStoreError::SerializationError(e.to_string()))?;
        
        let write_tx = self.db.begin_write()
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        {
            // Store in main certificates table
            let mut cert_table = write_tx.open_table(CERTIFICATES_TABLE)
                .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
            cert_table.insert(key, value.as_slice())
                .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
            
            // Also store by round for round-based queries
            let mut round_table = write_tx.open_table(CERTIFICATE_BY_ROUND_TABLE)
                .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
            round_table.insert(certificate.round(), key)
                .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        }
        write_tx.commit()
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        
        debug!("Stored certificate with digest: {:?} for round {}", digest, certificate.round());
        Ok(())
    }
    
    fn read(&self, digest: &CertificateDigest) -> Result<Option<Certificate>, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(CERTIFICATES_TABLE)
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(value)) => {
                let certificate = bincode::deserialize(value.value())
                    .map_err(|e| CertificateStoreError::SerializationError(e.to_string()))?;
                Ok(Some(certificate))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CertificateStoreError::StoreError(e.to_string())),
        }
    }
    
    fn contains(&self, digest: &CertificateDigest) -> Result<bool, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(CERTIFICATES_TABLE)
            .map_err(|e| CertificateStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(CertificateStoreError::StoreError(e.to_string())),
        }
    }
}

// Implement BatchStore
impl BatchStore for RedbConsensusStorage {
    type Error = BatchStoreError;
    
    fn write(&self, batch: &Batch) -> Result<(), Self::Error> {
        let digest = batch.digest();
        let key = digest.as_ref();
        let value = bincode::serialize(batch)
            .map_err(|e| BatchStoreError::SerializationError(e.to_string()))?;
        
        let write_tx = self.db.begin_write()
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        {
            let mut table = write_tx.open_table(BATCHES_TABLE)
                .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
            table.insert(key, value.as_slice())
                .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        }
        write_tx.commit()
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        
        debug!("Stored batch with digest: {:?}", digest);
        Ok(())
    }
    
    fn read(&self, digest: &BatchDigest) -> Result<Option<Batch>, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(BATCHES_TABLE)
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(value)) => {
                let batch = bincode::deserialize(value.value())
                    .map_err(|e| BatchStoreError::SerializationError(e.to_string()))?;
                Ok(Some(batch))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(BatchStoreError::StoreError(e.to_string())),
        }
    }
    
    fn contains(&self, digest: &BatchDigest) -> Result<bool, Self::Error> {
        let key = digest.as_ref();
        
        let read_tx = self.db.begin_read()
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(BATCHES_TABLE)
            .map_err(|e| BatchStoreError::StoreError(e.to_string()))?;
        
        match table.get(key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(BatchStoreError::StoreError(e.to_string())),
        }
    }
}

// Implement VoteStore
impl VoteStore for RedbConsensusStorage {
    type Error = VoteStoreError;
    
    fn write(&self, vote: &Vote) -> Result<(), Self::Error> {
        // Use a composite key: author + header digest
        let mut key = Vec::new();
        key.extend_from_slice(&vote.author.to_bytes());
        key.extend_from_slice(vote.header_digest.as_ref());
        
        let value = bincode::serialize(vote)
            .map_err(|e| VoteStoreError::SerializationError(e.to_string()))?;
        
        let write_tx = self.db.begin_write()
            .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
        {
            let mut table = write_tx.open_table(VOTES_TABLE)
                .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
            table.insert(key.as_slice(), value.as_slice())
                .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
        }
        write_tx.commit()
            .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
        
        debug!("Stored vote from {:?} for header {:?}", vote.author, vote.header_digest);
        Ok(())
    }
    
    fn read_by_author_and_digest(
        &self,
        author: &NarwhalPublicKey,
        digest: &HeaderDigest,
    ) -> Result<Option<Vote>, Self::Error> {
        let mut key = Vec::new();
        key.extend_from_slice(&author.to_bytes());
        key.extend_from_slice(digest.as_ref());
        
        let read_tx = self.db.begin_read()
            .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
        let table = read_tx.open_table(VOTES_TABLE)
            .map_err(|e| VoteStoreError::StoreError(e.to_string()))?;
        
        match table.get(key.as_slice()) {
            Ok(Some(value)) => {
                let vote = bincode::deserialize(value.value())
                    .map_err(|e| VoteStoreError::SerializationError(e.to_string()))?;
                Ok(Some(vote))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(VoteStoreError::StoreError(e.to_string())),
        }
    }
}

// Implement finalized batch storage
impl RedbConsensusStorage {
    /// Store a finalized batch
    pub fn store_finalized_batch(&self, block_number: u64, batch: &crate::types::FinalizedBatch) -> Result<(), Box<dyn std::error::Error>> {
        let value = bincode::serialize(batch)?;
        
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(FINALIZED_BATCHES_TABLE)?;
            table.insert(block_number, value.as_slice())?;
        }
        write_tx.commit()?;
        
        debug!("Stored finalized batch for block {}", block_number);
        Ok(())
    }
    
    /// Read a finalized batch
    pub fn read_finalized_batch(&self, block_number: u64) -> Result<Option<crate::types::FinalizedBatch>, Box<dyn std::error::Error>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(FINALIZED_BATCHES_TABLE)?;
        
        match table.get(block_number)? {
            Some(value) => {
                let batch = bincode::deserialize(value.value())?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
}