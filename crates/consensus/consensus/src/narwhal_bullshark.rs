//! Narwhal + Bullshark consensus integration for Reth

/// Block builder for converting consensus batches to Reth blocks
pub mod block_builder;
/// Consensus engine implementation
pub mod engine;
/// Type definitions for the consensus integration
pub mod types;
/// Main service for running Narwhal + Bullshark consensus
pub mod service;
/// Integration bridge between consensus and Reth execution
pub mod integration;
/// Mempool bridge for connecting Reth transaction pool with consensus
pub mod mempool_bridge;
/// Validator key management for EVM-compatible consensus
pub mod validator_keys;

// Re-export key types for easier access
pub use types::{FinalizedBatch, NarwhalBullsharkConfig};
pub use service::{NarwhalBullsharkService, ServiceConfig};
pub use integration::{NarwhalRethBridge, RethIntegrationConfig};
pub use mempool_bridge::{MempoolBridge, PoolStats};
pub use engine::{NarwhalBullsharkConsensus, FinalizationStream};
pub use validator_keys::{ValidatorKeyPair, ValidatorRegistry, ValidatorIdentity, ValidatorMetadata, ValidatorKeyConfig, KeyManagementStrategy};
