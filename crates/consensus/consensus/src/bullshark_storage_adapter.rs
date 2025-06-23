/// Adapter that implements bullshark's ConsensusStorage trait using MDBX
use std::sync::Arc;
use anyhow::Result;
use alloy_primitives::B256;
use bullshark::{ConsensusStorage, BullsharkCertificate, ConsensusBatch};
use crate::consensus_storage::MdbxConsensusStorage;
use bincode;

/// Adapter that implements bullshark's ConsensusStorage trait using MDBX
pub struct BullsharkMdbxAdapter {
    storage: Arc<MdbxConsensusStorage>,
}

impl BullsharkMdbxAdapter {
    pub fn new(storage: Arc<MdbxConsensusStorage>) -> Self {
        Self { storage }
    }
}

impl ConsensusStorage for BullsharkMdbxAdapter {
    fn store_certificate(&self, id: u64, certificate: BullsharkCertificate) -> Result<()> {
        let data = bincode::serialize(&certificate)?;
        self.storage.store_certificate(id, data)
    }
    
    fn get_certificate(&self, id: u64) -> Result<Option<BullsharkCertificate>> {
        match self.storage.get_certificate(id)? {
            Some(data) => {
                let certificate = bincode::deserialize(&data)?;
                Ok(Some(certificate))
            }
            None => Ok(None),
        }
    }
    
    fn store_batch(&self, batch: ConsensusBatch) -> Result<()> {
        let data = bincode::serialize(&batch)?;
        self.storage.store_batch(batch.id, data)
    }
    
    fn get_batch(&self, id: u64) -> Result<Option<ConsensusBatch>> {
        match self.storage.get_batch(id)? {
            Some(data) => {
                let batch = bincode::deserialize(&data)?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
    
    fn finalize_batch(&self, batch_id: u64, block_hash: B256) -> Result<()> {
        self.storage.record_finalized_batch(batch_id, block_hash)
    }
    
    fn get_finalized_batch(&self, batch_id: u64) -> Result<Option<B256>> {
        self.storage.get_finalized_batch(batch_id)
    }
    
    fn store_dag_vertex(&self, hash: B256, certificate: BullsharkCertificate) -> Result<()> {
        let data = bincode::serialize(&certificate)?;
        self.storage.store_dag_vertex(hash, data)
    }
    
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<BullsharkCertificate>> {
        match self.storage.get_dag_vertex(hash)? {
            Some(data) => {
                let certificate = bincode::deserialize(&data)?;
                Ok(Some(certificate))
            }
            None => Ok(None),
        }
    }
    
    fn set_latest_finalized(&self, certificate_id: u64) -> Result<()> {
        self.storage.set_latest_finalized(certificate_id)
    }
    
    fn get_latest_finalized(&self) -> Result<Option<u64>> {
        self.storage.get_latest_finalized()
    }
} 