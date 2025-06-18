use reth_primitives::{TransactionSigned};
use alloy_primitives::{B256};
use narwhal::{NarwhalConfig, types::PublicKey};
use bullshark::BftConfig;
use serde::{Deserialize, Serialize};
use fastcrypto::traits::KeyPair;
use rand_08;

/// A finalized batch of transactions from Narwhal + Bullshark consensus
#[derive(Debug, Clone)]
pub struct FinalizedBatch {
    /// Block number this batch represents
    pub block_number: u64,
    /// Parent hash of the previous block
    pub parent_hash: B256,
    /// Transactions in this batch
    pub transactions: Vec<TransactionSigned>,
    /// Timestamp for the block
    pub timestamp: u64,
}

/// Configuration for Narwhal + Bullshark consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarwhalBullsharkConfig {
    /// This node's public key
    pub node_public_key: PublicKey,
    /// Narwhal DAG configuration
    pub narwhal: NarwhalConfig,
    /// Bullshark BFT configuration  
    pub bullshark: BftConfig,
}

impl Default for NarwhalBullsharkConfig {
    fn default() -> Self {
        // Generate a dummy key for testing
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        
        Self {
            node_public_key: keypair.public().clone(),
            narwhal: NarwhalConfig::default(),
            bullshark: BftConfig::default(),
        }
    }
}
