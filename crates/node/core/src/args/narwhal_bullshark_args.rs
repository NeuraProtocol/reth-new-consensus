//! CLI arguments for Narwhal + Bullshark consensus

use clap::Args;
use std::net::SocketAddr;
use fastcrypto::traits::KeyPair;

/// Default network address for Narwhal
const NARWHAL_NETWORK_DEFAULT: &str = "127.0.0.1:9000";

/// Default committee size
const COMMITTEE_SIZE_DEFAULT: usize = 4;

/// Default batch size
const MAX_BATCH_SIZE_DEFAULT: usize = 1024;

/// Default GC depth  
const GC_DEPTH_DEFAULT: u64 = 50;

/// Default finality threshold
const FINALITY_THRESHOLD_DEFAULT: usize = 3;

/// Parameters for configuring Narwhal + Bullshark consensus
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "Narwhal + Bullshark Consensus")]
pub struct NarwhalBullsharkArgs {
    /// Enable Narwhal + Bullshark consensus instead of standard Ethereum consensus
    #[arg(long = "narwhal.enable", default_value_t = false)]
    pub enabled: bool,

    /// Network address for Narwhal networking
    #[arg(long = "narwhal.network-addr", default_value = NARWHAL_NETWORK_DEFAULT)]
    pub network_address: SocketAddr,

    /// Committee size (number of validators)
    #[arg(long = "narwhal.committee-size", default_value_t = COMMITTEE_SIZE_DEFAULT)]
    pub committee_size: usize,

    /// Maximum batch size for transaction batching
    #[arg(long = "narwhal.max-batch-size", default_value_t = MAX_BATCH_SIZE_DEFAULT)]
    pub max_batch_size: usize,

    /// Maximum batch delay in milliseconds
    #[arg(long = "narwhal.max-batch-delay-ms", default_value_t = 100)]
    pub max_batch_delay_ms: u64,

    /// Number of workers per authority
    #[arg(long = "narwhal.num-workers", default_value_t = 4)]
    pub num_workers: u32,

    /// Garbage collection depth in rounds
    #[arg(long = "narwhal.gc-depth", default_value_t = GC_DEPTH_DEFAULT)]
    pub gc_depth: u64,

    /// Finality threshold (minimum confirmations needed)
    #[arg(long = "bullshark.finality-threshold", default_value_t = FINALITY_THRESHOLD_DEFAULT)]
    pub finality_threshold: usize,

    /// Maximum pending rounds to keep
    #[arg(long = "bullshark.max-pending-rounds", default_value_t = 10)]
    pub max_pending_rounds: usize,

    /// Finalization timeout in seconds
    #[arg(long = "bullshark.finalization-timeout-secs", default_value_t = 5)]
    pub finalization_timeout_secs: u64,

    /// Maximum certificates per round
    #[arg(long = "bullshark.max-certificates-per-round", default_value_t = 1000)]
    pub max_certificates_per_round: usize,

    /// Leader rotation frequency (rounds)
    #[arg(long = "bullshark.leader-rotation-frequency", default_value_t = 2)]
    pub leader_rotation_frequency: u64,

    /// Enable metrics collection
    #[arg(long = "narwhal.enable-metrics", default_value_t = true)]
    pub enable_metrics: bool,
}

impl Default for NarwhalBullsharkArgs {
    fn default() -> Self {
        Self {
            enabled: false,
            network_address: NARWHAL_NETWORK_DEFAULT.parse().unwrap(),
            committee_size: COMMITTEE_SIZE_DEFAULT,
            max_batch_size: MAX_BATCH_SIZE_DEFAULT,
            max_batch_delay_ms: 100,
            num_workers: 4,
            gc_depth: GC_DEPTH_DEFAULT,
            finality_threshold: FINALITY_THRESHOLD_DEFAULT,
            max_pending_rounds: 10,
            finalization_timeout_secs: 5,
            max_certificates_per_round: 1000,
            leader_rotation_frequency: 2,
            enable_metrics: true,
        }
    }
}

impl NarwhalBullsharkArgs {
    /// Convert to Narwhal configuration
    pub fn to_narwhal_config(&self) -> narwhal::NarwhalConfig {
        narwhal::NarwhalConfig {
            max_batch_size: self.max_batch_size,
            max_batch_delay: std::time::Duration::from_millis(self.max_batch_delay_ms),
            num_workers: self.num_workers,
            gc_depth: self.gc_depth,
            committee_size: self.committee_size,
        }
    }

    /// Convert to Bullshark configuration
    pub fn to_bullshark_config(&self) -> bullshark::BftConfig {
        // Generate a dummy key for now (TODO: proper key management)
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        
        bullshark::BftConfig {
            node_key: keypair.public().clone(),
            gc_depth: self.gc_depth,
            finalization_timeout: std::time::Duration::from_secs(self.finalization_timeout_secs),
            max_certificates_per_round: self.max_certificates_per_round,
            leader_rotation_frequency: self.leader_rotation_frequency,
        }
    }

    /// Convert to Reth integration configuration
    pub fn to_integration_config(&self) -> reth_consensus::narwhal_bullshark::integration::RethIntegrationConfig {
        reth_consensus::narwhal_bullshark::integration::RethIntegrationConfig {
            network_address: self.network_address,
            enable_networking: true, // Enable networking for production use
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: self.enable_metrics,
        }
    }
} 