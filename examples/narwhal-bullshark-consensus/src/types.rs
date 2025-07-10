//! Type definitions for Narwhal+Bullshark consensus integration

use alloy_primitives::{B256, Address};
use reth_ethereum_primitives::TransactionSigned;
use serde::{Serialize, Deserialize};

/// A batch of transactions that has been finalized by consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedBatch {
    /// The consensus round this batch was finalized in
    pub round: u64,
    /// The block number this batch should become
    pub block_number: u64,
    /// Parent hash of the previous block
    pub parent_hash: B256,
    /// The transactions in this batch
    pub transactions: Vec<TransactionSigned>,
    /// The certificate digest from consensus
    pub certificate_digest: B256,
    /// The validator who proposed this batch
    pub proposer: Address,
    /// Timestamp when the batch was finalized
    pub timestamp: u64,
    /// The consensus round (same as round, kept for compatibility)
    pub consensus_round: u64,
    /// Validator signatures for this batch
    pub validator_signatures: Vec<(narwhal::types::PublicKey, Vec<u8>)>,
}

/// Configuration for the consensus service
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Network address to bind to
    pub network_addr: std::net::SocketAddr,
    /// Path to validator key file
    pub validator_key_file: String,
    /// Directory containing validator configurations
    pub validator_config_dir: String,
    /// Optional path to committee configuration file (overrides directory scanning)
    pub committee_config_file: Option<String>,
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
    /// Node's public key for consensus (from pre-move version)
    pub node_public_key: narwhal::types::PublicKey,
    /// Node's private key bytes for consensus (from pre-move version)
    pub consensus_private_key_bytes: Vec<u8>,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        use fastcrypto::traits::EncodeDecodeBase64;
        use fastcrypto::bls12381::{BLS12381KeyPair, BLS12381PrivateKey};
        use fastcrypto::traits::KeyPair;
        
        // Generate a temporary keypair for default (will be overridden)
        let keypair = BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        let public_key = narwhal::types::PublicKey::from(keypair.public().clone());
        let private_key_bytes = keypair.private().as_ref().to_vec();
        
        Self {
            network_addr: "127.0.0.1:9000".parse().unwrap(),
            validator_key_file: String::new(),
            validator_config_dir: String::new(),
            committee_config_file: None,
            max_batch_delay_ms: 100,
            max_batch_size: 100_000,
            min_block_time_ms: 500,
            consensus_rpc_port: 10000,
            consensus_port: 9000,
            enable_admin_api: false,
            node_public_key: public_key,
            consensus_private_key_bytes: private_key_bytes,
        }
    }
}

/// Committee configuration from JSON file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeConfig {
    /// Committee epoch
    pub epoch: u64,
    /// List of validators in the committee
    pub validators: Vec<CommitteeValidator>,
}

/// Individual validator in committee configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeValidator {
    /// Validator name
    pub name: String,
    /// EVM address (hex string with 0x prefix)
    pub evm_address: String,
    /// Consensus public key (base64)
    pub consensus_public_key: String,
    /// Validator stake
    pub stake: u64,
    /// Network address for primary consensus
    pub network_address: String,
    /// Worker port range (e.g., "19000:19003")
    pub worker_port_range: String,
}

/// Configuration for consensus RPC services
#[derive(Debug, Clone)]
pub struct ConsensusRpcConfig {
    /// RPC listening address
    pub addr: std::net::SocketAddr,
    /// Enable admin APIs
    pub enable_admin: bool,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for ConsensusRpcConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:10000".parse().unwrap(),
            enable_admin: false,
            enable_metrics: true,
        }
    }
}