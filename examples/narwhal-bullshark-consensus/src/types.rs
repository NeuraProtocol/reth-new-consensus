//! Type definitions for Narwhal+Bullshark consensus integration

use alloy_primitives::{B256, Address};
use reth_primitives::TransactionSigned;
use serde::{Serialize, Deserialize};

/// A batch of transactions that has been finalized by consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedBatch {
    /// The consensus round this batch was finalized in
    pub round: u64,
    /// The block number this batch should become
    pub block_number: u64,
    /// The transactions in this batch
    pub transactions: Vec<TransactionSigned>,
    /// The certificate digest from consensus
    pub certificate_digest: B256,
    /// The validator who proposed this batch
    pub proposer: Address,
    /// Timestamp when the batch was finalized
    pub timestamp: u64,
}

/// Configuration for the consensus service
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Network address to bind to
    pub network_addr: std::net::SocketAddr,
    /// Peer addresses to connect to
    pub peer_addresses: Vec<std::net::SocketAddr>,
    /// Path to validator key file
    pub validator_key_file: String,
    /// Directory containing validator configurations
    pub validator_config_dir: String,
    /// Maximum batch delay in milliseconds
    pub max_batch_delay_ms: u64,
    /// Maximum batch size in bytes
    pub max_batch_size: usize,
    /// Minimum block time in milliseconds
    pub min_block_time_ms: u64,
    /// RPC port for consensus API
    pub consensus_rpc_port: u16,
    /// Port for consensus P2P network
    pub consensus_port: u16,
    /// Whether to enable admin API
    pub enable_admin_api: bool,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            network_addr: "127.0.0.1:9000".parse().unwrap(),
            peer_addresses: vec![],
            validator_key_file: String::new(),
            validator_config_dir: String::new(),
            max_batch_delay_ms: 100,
            max_batch_size: 100_000,
            min_block_time_ms: 500,
            consensus_rpc_port: 10000,
            consensus_port: 9000,
            enable_admin_api: false,
        }
    }
}