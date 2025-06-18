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

// Re-export key types for easier access
pub use types::{FinalizedBatch, NarwhalBullsharkConfig};
pub use service::{NarwhalBullsharkService, ServiceConfig};
pub use integration::{NarwhalRethBridge, RethIntegrationConfig};
pub use engine::{NarwhalBullsharkConsensus, FinalizationStream};
