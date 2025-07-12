//! Configuration for Bullshark BFT consensus

use narwhal::{Round, types::PublicKey};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use fastcrypto::traits::KeyPair;
use rand_08;

/// Configuration for Bullshark BFT consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BftConfig {
    /// This node's public key
    pub node_key: PublicKey,
    /// Garbage collection depth in rounds
    pub gc_depth: Round,
    /// Finalization timeout
    pub finalization_timeout: Duration,
    /// Maximum number of certificates to process per round
    pub max_certificates_per_round: usize,
    /// Leader rotation frequency (rounds)
    pub leader_rotation_frequency: Round,
    /// Minimum round for leader election (must be even)
    /// In production this should be 2, but can be 0 for testing
    pub min_leader_round: Round,
    /// Minimum time between blocks (to prevent excessive block production)
    pub min_block_time: Duration,
    /// Maximum certificates to output per DAG traversal (prevents overwhelming system when catching up)
    pub max_certificates_per_dag: usize,
    /// Timeout for waiting for leader certificates in round completion
    pub round_completion_timeout: Duration,
}

impl Default for BftConfig {
    fn default() -> Self {
        // Generate a dummy key for testing
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        
        Self {
            node_key: keypair.public().clone(),
            gc_depth: 50,
            finalization_timeout: Duration::from_millis(5000),
            max_certificates_per_round: 1000,
            leader_rotation_frequency: 2, // Change leader every 2 rounds
            min_leader_round: 0, // Temporarily set to 0 for testing
            min_block_time: Duration::from_millis(100), // 100ms minimum between blocks for ultra-fast EVM
            max_certificates_per_dag: 50, // Reduce to process certificates more frequently
            round_completion_timeout: Duration::from_millis(200), // Wait up to 200ms for leader certificates
        }
    }
} 