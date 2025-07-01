// Consensus extension tables for Narwhal + Bullshark

use reth_db_api::table::{Table, DupSort};
use alloy_primitives::B256;
use std;
use alloc::vec::Vec;

/// Table mapping finalized batch id to the block hash that was built from that batch.
#[derive(Debug)]
pub struct ConsensusFinalizedBatch {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusFinalizedBatch {
    const NAME: &'static str = "ConsensusFinalizedBatch";
    const DUPSORT: bool = false;
    
    type Key = u64;
    type Value = B256;
}

/// Table storing consensus certificates by ID
#[derive(Debug)]
pub struct ConsensusCertificates {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusCertificates {
    const NAME: &'static str = "ConsensusCertificates";
    const DUPSORT: bool = false;
    
    type Key = u64;
    type Value = Vec<u8>;
}

/// Table storing consensus batches by ID  
#[derive(Debug)]
pub struct ConsensusBatches {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusBatches {
    const NAME: &'static str = "ConsensusBatches";
    const DUPSORT: bool = false;
    
    type Key = u64;
    type Value = Vec<u8>;
}

/// Table storing DAG vertices by hash
#[derive(Debug)]
pub struct ConsensusDagVertices {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusDagVertices {
    const NAME: &'static str = "ConsensusDagVertices";
    const DUPSORT: bool = false;
    
    type Key = B256;
    type Value = Vec<u8>;
}

/// Table storing the latest finalized certificate ID
#[derive(Debug)]
pub struct ConsensusLatestFinalized {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusLatestFinalized {
    const NAME: &'static str = "ConsensusLatestFinalized";
    const DUPSORT: bool = false;
    
    type Key = u8;
    type Value = u64;
}

/// Table storing worker batch data by digest
#[derive(Debug)]
pub struct WorkerBatches {
    _private: std::marker::PhantomData<()>,
}

impl Table for WorkerBatches {
    const NAME: &'static str = "WorkerBatches";
    const DUPSORT: bool = false;
    
    type Key = B256; // BatchDigest
    type Value = Vec<u8>; // Serialized Batch
}

/// Table storing worker batch metadata
#[derive(Debug)]
pub struct WorkerBatchMetadata {
    _private: std::marker::PhantomData<()>,
}

impl Table for WorkerBatchMetadata {
    const NAME: &'static str = "WorkerBatchMetadata";
    const DUPSORT: bool = false;
    
    type Key = B256; // BatchDigest
    type Value = Vec<u8>; // Serialized metadata (timestamp, worker_id, etc)
}

/// Table storing pending transactions for workers
#[derive(Debug)]
pub struct WorkerPendingTransactions {
    _private: std::marker::PhantomData<()>,
}

impl Table for WorkerPendingTransactions {
    const NAME: &'static str = "WorkerPendingTransactions";
    const DUPSORT: bool = false;
    
    type Key = B256; // Transaction hash
    type Value = Vec<u8>; // Serialized transaction
}

/// Table storing batch acknowledgments from other workers
#[derive(Debug)]
pub struct WorkerBatchAcks {
    _private: std::marker::PhantomData<()>,
}

impl Table for WorkerBatchAcks {
    const NAME: &'static str = "WorkerBatchAcks";
    const DUPSORT: bool = true; // Multiple acks per batch
    
    type Key = B256; // BatchDigest
    type Value = Vec<u8>; // Serialized acknowledgment (worker_id, signature, etc)
}

impl DupSort for WorkerBatchAcks {
    type SubKey = Vec<u8>; // Worker ID as subkey
}

/// Table storing votes indexed by header digest
#[derive(Debug)]
pub struct ConsensusVotes {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusVotes {
    const NAME: &'static str = "ConsensusVotes";
    const DUPSORT: bool = true; // Multiple votes per header
    
    type Key = B256; // HeaderDigest
    type Value = Vec<u8>; // Serialized Vote
}

impl DupSort for ConsensusVotes {
    type SubKey = Vec<u8>; // Voter public key as subkey
}

/// Table indexing certificates by round for efficient round queries
#[derive(Debug)]
pub struct ConsensusCertificatesByRound {
    _private: std::marker::PhantomData<()>,
}

impl Table for ConsensusCertificatesByRound {
    const NAME: &'static str = "ConsensusCertificatesByRound";
    const DUPSORT: bool = true; // Multiple certificates per round
    
    type Key = u64; // Round number
    type Value = Vec<u8>; // Certificate digest
}

impl DupSort for ConsensusCertificatesByRound {
    type SubKey = Vec<u8>; // Certificate digest as subkey
} 