//! Bullshark BFT consensus implementation
//! 
//! This crate implements the Bullshark Byzantine Fault Tolerant consensus protocol,
//! which provides finality on top of the Narwhal DAG.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod bft_service;
pub mod consensus;
pub mod finality;
pub mod dag;
pub mod utils;
pub mod config;
pub mod storage;

// Re-export key types
pub use bft_service::BftService;
pub use consensus::BullsharkConsensus;
pub use finality::FinalityEngine;
pub use config::BftConfig;
pub use storage::{ConsensusStorage, InMemoryConsensusStorage, Certificate as BullsharkCertificate, ConsensusBatch};

use serde::{Deserialize, Serialize};
use alloy_primitives::{B256};
use narwhal::{
    types::{Certificate as NarwhalCertificate},
    Transaction as NarwhalTransaction,
    Round,
};

/// A batch of transactions that has been finalized by Bullshark
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedBatchInternal {
    /// Block number this batch represents
    pub block_number: u64,
    /// Parent hash of the previous block
    pub parent_hash: B256,
    /// Transactions in this batch
    pub transactions: Vec<NarwhalTransaction>,
    /// Timestamp for the block
    pub timestamp: u64,
    /// Round this batch was finalized in
    pub round: Round,
    /// Certificates that led to this finalization
    pub certificates: Vec<NarwhalCertificate>,
}

/// Output from the consensus protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusOutput {
    /// The finalized certificate
    pub certificate: NarwhalCertificate,
    /// Global consensus index
    pub consensus_index: u64,
}

/// Sequence number for global ordering
pub type SequenceNumber = u64;

/// Result type for Bullshark operations
pub type BullsharkResult<T> = Result<T, BullsharkError>;

/// Errors that can occur in Bullshark consensus
#[derive(Debug, thiserror::Error)]
pub enum BullsharkError {
    /// Invalid certificate format or content
    #[error("Invalid certificate: {0}")]
    InvalidCertificate(String),
    
    /// Storage backend error
    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
    
    /// Consensus protocol error
    #[error("Consensus error: {0}")]
    Consensus(String),
    
    /// Network communication error
    #[error("Network error: {0}")]
    Network(String),
    
    /// Configuration parameter error
    #[error("Configuration error: {0}")]
    Configuration(String),
}
