//! Redb-based consensus storage implementation
//! 
//! This provides a pure-Rust storage backend for consensus data,
//! completely isolated from Reth's MDBX database to avoid lock contention.

use crate::consensus_storage::{DatabaseOps, MdbxConsensusStorage};
use std::sync::{Arc, Mutex};
use redb::{Database, TableDefinition, ReadableTable};
use alloy_primitives::B256;
use tracing::{debug, error, info};
use anyhow::Result;

// Define table schemas
const CERTIFICATES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("certificates");
const BATCHES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("batches");
const FINALIZED_BATCHES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("finalized_batches");
const DAG_VERTICES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dag_vertices");
const VOTES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("votes");
const CONSENSUS_STATE_TABLE: TableDefinition<&str, u64> = TableDefinition::new("consensus_state");

/// Redb-based implementation of consensus storage
/// This wraps MdbxConsensusStorage but provides its own database operations
pub struct RedbConsensusStorage {
    /// The underlying storage implementation
    inner: MdbxConsensusStorage,
    /// The Redb database
    db: Arc<Database>,
}

impl RedbConsensusStorage {
    /// Create a new Redb consensus storage instance
    pub fn new(db_path: &str) -> Result<Self> {
        info!("Opening Redb consensus database at: {}", db_path);
        
        let db = Database::create(db_path)?;
        
        // Create tables if they don't exist
        let write_tx = db.begin_write()?;
        {
            write_tx.open_table(CERTIFICATES_TABLE)?;
            write_tx.open_table(BATCHES_TABLE)?;
            write_tx.open_table(FINALIZED_BATCHES_TABLE)?;
            write_tx.open_table(DAG_VERTICES_TABLE)?;
            write_tx.open_table(VOTES_TABLE)?;
            write_tx.open_table(CONSENSUS_STATE_TABLE)?;
        }
        write_tx.commit()?;
        
        info!("✅ Redb consensus storage initialized successfully");
        
        // Create the inner storage and inject our database operations
        let mut inner = MdbxConsensusStorage::new();
        let db_ops = Box::new(RedbDatabaseOps::new(Arc::new(db.clone())));
        inner.set_db_ops(db_ops);
        
        Ok(Self {
            inner,
            db: Arc::new(db),
        })
    }
    
    /// Get the inner storage for use with the bridge
    pub fn inner(&self) -> &MdbxConsensusStorage {
        &self.inner
    }
}

/// Redb implementation of DatabaseOps
struct RedbDatabaseOps {
    db: Arc<Database>,
}

impl RedbDatabaseOps {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl std::fmt::Debug for RedbDatabaseOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbDatabaseOps").finish()
    }
}

impl DatabaseOps for RedbDatabaseOps {
    fn get_finalized_batch(&self, batch_id: u64) -> Result<Option<B256>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(FINALIZED_BATCHES_TABLE)?;
        
        match table.get(batch_id)? {
            Some(value) => {
                let bytes = value.value();
                if bytes.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(bytes);
                    Ok(Some(B256::from(arr)))
                } else {
                    Err(anyhow::anyhow!("Invalid block hash length"))
                }
            }
            None => Ok(None),
        }
    }
    
    fn put_finalized_batch(&self, batch_id: u64, block_hash: B256) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(FINALIZED_BATCHES_TABLE)?;
            table.insert(batch_id, block_hash.as_slice())?;
        }
        write_tx.commit()?;
        debug!("Stored finalized batch {} with hash {}", batch_id, block_hash);
        Ok(())
    }
    
    fn get_certificate(&self, cert_id: u64) -> Result<Option<Vec<u8>>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(CERTIFICATES_TABLE)?;
        
        match table.get(cert_id)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put_certificate(&self, cert_id: u64, data: Vec<u8>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(CERTIFICATES_TABLE)?;
            table.insert(cert_id, data.as_slice())?;
        }
        write_tx.commit()?;
        debug!("Stored certificate {}", cert_id);
        Ok(())
    }
    
    fn get_batch(&self, batch_id: u64) -> Result<Option<Vec<u8>>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(BATCHES_TABLE)?;
        
        match table.get(batch_id)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put_batch(&self, batch_id: u64, data: Vec<u8>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(BATCHES_TABLE)?;
            table.insert(batch_id, data.as_slice())?;
        }
        write_tx.commit()?;
        debug!("Stored batch {}", batch_id);
        Ok(())
    }
    
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Vec<u8>>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(DAG_VERTICES_TABLE)?;
        
        match table.get(hash.as_slice())? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put_dag_vertex(&self, hash: B256, data: Vec<u8>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(DAG_VERTICES_TABLE)?;
            table.insert(hash.as_slice(), data.as_slice())?;
        }
        write_tx.commit()?;
        debug!("Stored DAG vertex {}", hash);
        Ok(())
    }
    
    fn get_latest_finalized(&self) -> Result<Option<u64>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(CONSENSUS_STATE_TABLE)?;
        
        match table.get("latest_finalized")? {
            Some(value) => Ok(Some(value.value())),
            None => Ok(None),
        }
    }
    
    fn put_latest_finalized(&self, cert_id: u64) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(CONSENSUS_STATE_TABLE)?;
            table.insert("latest_finalized", cert_id)?;
        }
        write_tx.commit()?;
        debug!("Updated latest finalized to {}", cert_id);
        Ok(())
    }
    
    fn get_vote(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(VOTES_TABLE)?;
        
        match table.get(key)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put_vote(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(VOTES_TABLE)?;
            table.insert(key.as_slice(), data.as_slice())?;
        }
        write_tx.commit()?;
        debug!("Stored vote");
        Ok(())
    }
}

