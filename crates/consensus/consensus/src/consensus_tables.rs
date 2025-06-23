// Consensus extension tables for Narwhal + Bullshark

use reth_db_api::table::Table;
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