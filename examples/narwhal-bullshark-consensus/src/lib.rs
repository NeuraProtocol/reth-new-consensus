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
pub mod block_executor;
pub mod engine_integration;
pub mod database_integration;
pub mod node_integration;
pub mod test_block_submission;
pub mod real_consensus_integration;
// Working components from pre-move version
pub mod narwhal_reth_bridge;
pub mod narwhal_bullshark_service;
pub mod working_validator_registry;
// Restored critical functionality from pre-move version
pub mod canonical_state_fix;
pub mod complete_integration;
pub mod reth_block_executor;
pub mod narwhal_bullshark_engine;
pub mod chain_state_adapter;

// Re-export key types
pub use consensus_engine::NarwhalBullsharkEngine;
pub use types::{FinalizedBatch, ConsensusConfig};
pub use validator_keys::{ValidatorKeyPair, ValidatorRegistry};
pub use chain_state::{ChainState, ChainStateTracker};

// These modules contain complex implementations that need significant work
// to adapt to the current Reth APIs. They are included to show the structure
// but are not fully functional.

#[cfg(feature = "full")]
pub mod integration;
#[cfg(feature = "full")]
pub mod payload_builder;
#[cfg(feature = "full")]
pub mod service;
// Required modules for working implementation
pub mod consensus_storage;
pub mod mdbx_database_ops;
pub mod transaction_adapter;
pub mod payload_job_generator;
pub mod reth_payload_builder_integration;
pub mod node_integration_v2;
#[cfg(test)]
mod test_payload_builder;
pub mod dag_storage_adapter;
pub mod batch_storage_adapter;
pub mod mempool_bridge;
pub mod reth_database_ops;
pub mod simple_consensus_db;
#[cfg(feature = "full")]
pub mod rpc_impl;
#[cfg(feature = "full")]
pub mod rpc_config;