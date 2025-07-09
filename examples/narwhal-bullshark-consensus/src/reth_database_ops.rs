//! Real MDBX database operations implementation that connects to Reth's database
//! This is copied from the working implementation in premoveversion

use crate::consensus_storage::DatabaseOps;
use reth_provider::{DatabaseProviderFactory, DBProvider};
use reth_db_api::{
    tables::{
        ConsensusFinalizedBatch, ConsensusCertificates, ConsensusBatches, ConsensusDagVertices,
        ConsensusLatestFinalized, WorkerBatches, ConsensusVotes, ConsensusCertificatesByRound
    },
    transaction::{DbTx, DbTxMut},
    cursor::{DbCursorRO, DbCursorRW},
};
use alloy_primitives::B256;
use anyhow::Result;
use std::sync::Arc;
use tracing::debug;

/// Concrete implementation of DatabaseOps that uses Reth's database provider
/// This bridges the consensus storage with Reth's MDBX database through the provider interface
#[derive(Debug)]
pub struct RethDatabaseOps<P> {
    provider: Arc<P>,
}

impl<P> RethDatabaseOps<P> 
where
    P: DatabaseProviderFactory + Send + Sync + std::fmt::Debug,
{
    pub fn new(provider: Arc<P>) -> Self {
        debug!("✅ REAL: Creating RethDatabaseOps with provider");
        Self { provider }
    }
}

impl<P> DatabaseOps for RethDatabaseOps<P>
where
    P: DatabaseProviderFactory + Send + Sync + std::fmt::Debug,
{
    fn get_finalized_batch(&self, batch_id: u64) -> Result<Option<B256>> {
        let provider = self.provider.database_provider_ro()?;
        Ok(provider.tx_ref().get::<ConsensusFinalizedBatch>(batch_id)?)
    }
    
    fn put_finalized_batch(&self, batch_id: u64, block_hash: B256) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().put::<ConsensusFinalizedBatch>(batch_id, block_hash)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_certificate(&self, cert_id: u64) -> Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        Ok(provider.tx_ref().get::<ConsensusCertificates>(cert_id)?)
    }
    
    fn put_certificate(&self, cert_id: u64, data: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().put::<ConsensusCertificates>(cert_id, data)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_batch(&self, batch_id: u64) -> Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        Ok(provider.tx_ref().get::<ConsensusBatches>(batch_id)?)
    }
    
    fn put_batch(&self, batch_id: u64, data: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().put::<ConsensusBatches>(batch_id, data)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_dag_vertex(&self, hash: B256) -> Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        Ok(provider.tx_ref().get::<ConsensusDagVertices>(hash)?)
    }
    
    fn put_dag_vertex(&self, hash: B256, data: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().put::<ConsensusDagVertices>(hash, data)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_latest_finalized(&self) -> Result<Option<u64>> {
        let provider = self.provider.database_provider_ro()?;
        // Using key=0 as singleton record
        Ok(provider.tx_ref().get::<ConsensusLatestFinalized>(0u8)?)
    }
    
    fn put_latest_finalized(&self, cert_id: u64) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        // Using key=0 as singleton record
        provider.tx_mut().put::<ConsensusLatestFinalized>(0u8, cert_id)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn list_finalized_batches(&self, limit: Option<usize>) -> Result<Vec<(u64, B256)>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_db_api::cursor::DbCursorRO;
        
        let mut cursor = provider.tx_ref().cursor_read::<ConsensusFinalizedBatch>()?;
        let mut results = Vec::new();
        let mut walker = cursor.walk(None)?;
        
        while let Some(entry) = walker.next() {
            let (key, value) = entry?;
            results.push((key, value));
            
            if let Some(limit) = limit {
                if results.len() >= limit {
                    break;
                }
            }
        }
        
        Ok(results)
    }
    
    fn get_table_stats(&self) -> Result<(u64, u64, u64)> {
        let provider = self.provider.database_provider_ro()?;
        use reth_db_api::cursor::DbCursorRO;
        
        // Count entries in each table
        let mut cert_cursor = provider.tx_ref().cursor_read::<ConsensusCertificates>()?;
        let cert_count = cert_cursor.walk(None)?.count() as u64;
        
        let mut batch_cursor = provider.tx_ref().cursor_read::<ConsensusBatches>()?;
        let batch_count = batch_cursor.walk(None)?.count() as u64;
        
        let mut vertex_cursor = provider.tx_ref().cursor_read::<ConsensusDagVertices>()?;
        let vertex_count = vertex_cursor.walk(None)?.count() as u64;
        
        Ok((cert_count, batch_count, vertex_count))
    }
    
    fn put_vote(&self, header_digest: B256, vote_data: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        
        // Get existing votes
        let existing_votes = provider.tx_ref().get::<ConsensusVotes>(header_digest)?;
        let mut votes: Vec<Vec<u8>> = existing_votes
            .map(|data| bincode::deserialize(&data).unwrap_or_default())
            .unwrap_or_default();
        
        votes.push(vote_data);
        
        // Serialize and store updated votes
        let serialized = bincode::serialize(&votes)?;
        provider.tx_mut().put::<ConsensusVotes>(header_digest, serialized)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_votes(&self, header_digest: B256) -> Result<Vec<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        let result = provider.tx_ref().get::<ConsensusVotes>(header_digest)?;
        
        match result {
            Some(data) => Ok(bincode::deserialize(&data)?),
            None => Ok(Vec::new())
        }
    }
    
    fn remove_votes(&self, header_digest: B256) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().delete::<ConsensusVotes>(header_digest, None)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn index_certificate_by_round(&self, round: u64, cert_digest: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        
        // Get existing certificates for this round
        let existing_certs = provider.tx_ref().get::<ConsensusCertificatesByRound>(round)?;
        let mut certificates: Vec<Vec<u8>> = existing_certs
            .map(|data| bincode::deserialize(&data).unwrap_or_default())
            .unwrap_or_default();
        
        certificates.push(cert_digest);
        
        // Serialize and store updated certificates
        let serialized = bincode::serialize(&certificates)?;
        provider.tx_mut().put::<ConsensusCertificatesByRound>(round, serialized)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_certificates_by_round(&self, round: u64) -> Result<Vec<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        let result = provider.tx_ref().get::<ConsensusCertificatesByRound>(round)?;
        
        match result {
            Some(data) => Ok(bincode::deserialize(&data)?),
            None => Ok(Vec::new())
        }
    }
    
    fn remove_certificates_before_round(&self, round: u64) -> Result<u64> {
        let mut provider = self.provider.database_provider_rw()?;
        use reth_db_api::cursor::DbCursorRW;
        
        let mut cursor = provider.tx_mut().cursor_write::<ConsensusCertificatesByRound>()?;
        let mut removed_count = 0u64;
        
        // Collect keys to delete first
        let mut keys_to_delete = Vec::new();
        let mut walker = cursor.walk(None)?;
        
        while let Some(entry) = walker.next() {
            let (key, value) = entry?;
            if key < round {
                // Count certificates in this round
                let certificates: Vec<Vec<u8>> = bincode::deserialize(&value)?;
                removed_count += certificates.len() as u64;
                keys_to_delete.push(key);
            }
        }
        
        // Now delete the entries
        for key in keys_to_delete {
            provider.tx_mut().delete::<ConsensusCertificatesByRound>(key, None)?;
        }
        
        DBProvider::commit(provider)?;
        Ok(removed_count)
    }
    
    fn put_worker_batch(&self, digest: B256, batch_data: Vec<u8>) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().put::<WorkerBatches>(digest, batch_data)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_worker_batch(&self, digest: B256) -> Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        Ok(provider.tx_ref().get::<WorkerBatches>(digest)?)
    }
    
    fn delete_worker_batch(&self, digest: B256) -> Result<()> {
        let mut provider = self.provider.database_provider_rw()?;
        provider.tx_mut().delete::<WorkerBatches>(digest, None)?;
        DBProvider::commit(provider)?;
        Ok(())
    }
    
    fn get_worker_batches(&self, digests: &[B256]) -> Result<Vec<Option<Vec<u8>>>> {
        let provider = self.provider.database_provider_ro()?;
        let mut results = Vec::with_capacity(digests.len());
        
        for digest in digests {
            let result = provider.tx_ref().get::<WorkerBatches>(*digest)?;
            results.push(result);
        }
        
        Ok(results)
    }
}