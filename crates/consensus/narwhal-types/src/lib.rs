//! Shared types for Narwhal+Bullshark consensus integration
//! 
//! This crate provides minimal shared types that are needed by multiple crates
//! to avoid circular dependencies.

use alloy_primitives::Address;
use serde::{Serialize, Deserialize};

/// Validator key configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidatorKeyConfig {
    /// Key management strategy
    pub key_strategy: KeyManagementStrategy,
    /// Directory containing validator keys (for FileSystem strategy)
    pub key_directory: Option<std::path::PathBuf>,
    /// Whether to use deterministic consensus keys from EVM private keys
    pub deterministic_consensus_keys: bool,
}

/// Key management strategy for validators
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyManagementStrategy {
    /// Generate random keys for testing
    Random,
    /// Load keys from the file system
    FileSystem,
    /// Use external key management (e.g., Vault)
    External,
}

impl Default for KeyManagementStrategy {
    fn default() -> Self {
        Self::Random
    }
}

/// Basic validator key pair information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorKeyPair {
    /// EVM address for this validator
    pub evm_address: Address,
    /// BLS public key for consensus (base64 encoded)
    pub consensus_public_key: String,
    /// Human-readable name
    pub name: String,
    /// Validator's stake
    pub stake: u64,
}

/// Reth integration configuration
#[derive(Debug, Clone, Default)]
pub struct RethIntegrationConfig {
    /// Maximum transactions per batch
    pub max_transactions_per_batch: usize,
    /// Batch creation interval in milliseconds
    pub batch_creation_interval_ms: u64,
}

/// Narwhal configuration
#[derive(Debug, Clone)]
pub struct NarwhalConfig {
    /// Maximum batch delay in milliseconds
    pub max_batch_delay_ms: u64,
    /// Maximum batch size in bytes
    pub max_batch_size: usize,
    /// Garbage collection depth
    pub gc_depth: u64,
    /// Network timeout in milliseconds
    pub timeout_ms: u64,
    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,
}

impl Default for NarwhalConfig {
    fn default() -> Self {
        Self {
            max_batch_delay_ms: 100,
            max_batch_size: 100_000,
            gc_depth: 50,
            timeout_ms: 30_000,
            max_concurrent_requests: 500,
        }
    }
}