//! Narwhal DAG consensus implementation
//! 
//! This crate implements the Narwhal DAG (Directed Acyclic Graph) consensus protocol,
//! which provides reliable broadcast and data availability for the Bullshark BFT consensus.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

// Note: moved test-only crate declarations to test modules to avoid issues

pub mod dag_service;
pub mod gossip;
pub mod storage;
pub mod types;
pub mod primary;
pub mod worker;
pub mod metrics;
pub mod error;
pub mod config;
pub mod network;
pub mod rpc;

// Re-export key types
pub use dag_service::{DagService, DagMessage};
pub use gossip::GossipProtocol;
pub use storage::DagStorage;
pub use types::*;
pub use primary::Primary;
pub use worker::Worker;
pub use error::{DagError, DagResult};
// pub use config::NarwhalConfig; // Using local definition instead
pub use network::{NarwhalNetwork, NetworkEvent};

use serde::{Deserialize, Serialize};
use alloy_primitives::B256;
use alloy_consensus::{Transaction as AlloyTransaction, TxEnvelope};
use fastcrypto::Hash;
use blake2::digest::Update;

/// A transaction in the Narwhal DAG - bridging to Reth transactions
#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Transaction(pub Vec<u8>);

impl Transaction {
    /// Create from raw transaction bytes
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Transaction(bytes)
    }
    
    /// Convert to alloy transaction envelope (placeholder implementation)
    pub fn to_alloy_transaction(&self) -> Result<TxEnvelope, DagError> {
        // For now, return an error - this would require proper RLP decoding
        // TODO: Implement proper RLP decoding for alloy transaction types
        Err(DagError::InvalidTransaction("Transaction decoding not yet implemented".to_string()))
    }
    
    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A batch of transactions in Narwhal
#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct Batch(pub Vec<Transaction>);

/// A batch digest for content addressing
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchDigest(pub [u8; 32]);

impl std::fmt::Debug for BatchDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl std::fmt::Display for BatchDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let encoded = base64::encode(self.0);
        let display_str = encoded.get(0..16).unwrap_or(&encoded);
        write!(f, "{}", display_str)
    }
}

impl From<BatchDigest> for fastcrypto::Digest {
    fn from(digest: BatchDigest) -> Self {
        fastcrypto::Digest::new(digest.0)
    }
}

impl Hash for Batch {
    type TypedDigest = BatchDigest;

    fn digest(&self) -> Self::TypedDigest {
        BatchDigest(fastcrypto::blake2b_256(|hasher| {
            self.0.iter().for_each(|tx| hasher.update(tx.as_bytes()))
        }))
    }
}

/// Round number in the DAG
pub type Round = u64;

/// Worker ID for sharding
pub type WorkerId = u32;

/// The result of finalized consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedOutput {
    /// The round number
    pub round: Round,
    /// The finalized batch
    pub batch: Batch,
    /// The block number this should become
    pub block_number: u64,
    /// Parent hash for the block
    pub parent_hash: B256,
}

/// Configuration for the Narwhal DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarwhalConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum delay before creating a batch
    pub max_batch_delay: std::time::Duration,
    /// Number of workers per authority
    pub num_workers: WorkerId,
    /// Garbage collection depth
    pub gc_depth: Round,
    /// Committee size
    pub committee_size: usize,
}

impl Default for NarwhalConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1024,
            max_batch_delay: std::time::Duration::from_millis(100),
            num_workers: 4,
            gc_depth: 50,
            committee_size: 4,
        }
    }
}
