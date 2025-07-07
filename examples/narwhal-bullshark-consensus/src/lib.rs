//! Narwhal + Bullshark consensus implementation for Reth
//! 
//! This example shows how to integrate a custom BFT consensus algorithm (Narwhal+Bullshark)
//! with Reth's execution engine, replacing Ethereum's standard consensus.
//!
//! NOTE: This is a simplified implementation focusing on the architecture.
//! Many modules are placeholders that would need proper implementation.

pub mod consensus_engine;
pub mod types;
pub mod validator_keys;
pub mod chain_state;
pub mod block_builder;
pub mod engine_integration;
pub mod simple_integration;
pub mod simple_block_builder;
pub mod test_integration;

// Re-export key types
pub use consensus_engine::NarwhalBullsharkEngine;
pub use types::{FinalizedBatch, ConsensusConfig};
pub use validator_keys::{ValidatorKeyPair, ValidatorRegistry};
pub use chain_state::{ChainState, ChainStateTracker};
pub use test_integration::TestIntegration;

// These modules contain complex implementations that need significant work
// to adapt to the current Reth APIs. They are included to show the structure
// but are not fully functional.

#[cfg(feature = "full")]
pub mod integration;
#[cfg(feature = "full")]
pub mod payload_builder;
#[cfg(feature = "full")]
pub mod mempool_bridge;
#[cfg(feature = "full")]
pub mod service;
#[cfg(feature = "full")]
pub mod consensus_storage;
#[cfg(feature = "full")]
pub mod mdbx_database_ops;
#[cfg(feature = "full")]
pub mod rpc_impl;
#[cfg(feature = "full")]
pub mod rpc_config;
#[cfg(feature = "full")]
pub mod transaction_adapter;
#[cfg(feature = "full")]
pub mod dag_storage_adapter;
#[cfg(feature = "full")]
pub mod batch_storage_adapter;