//! Narwhal + Bullshark consensus implementation for Reth
//! 
//! This shows how to integrate a custom BFT consensus algorithm (Narwhal+Bullshark)
//! with Reth's execution engine, replacing Ethereum's standard consensus.
//!
//! This is the REAL implementation - no mocks or simplified versions.

pub mod consensus_engine;
pub mod types;
pub mod validator_keys;
pub mod chain_state;
pub mod block_builder; // Creates basic blocks from consensus batches
pub mod block_executor; // Full EVM execution to create complete blocks
pub mod engine_integration; // Engine API for canonical state updates
// pub mod database_integration; // Disabled - causes canonical state sync issues
pub mod node_integration;
pub mod real_consensus_integration;

// Re-export key types
pub use consensus_engine::NarwhalBullsharkEngine;
pub use types::{FinalizedBatch, ConsensusConfig};
pub use validator_keys::{ValidatorKeyPair, ValidatorRegistry};
pub use chain_state::{ChainState, ChainStateTracker};

// Supporting modules for full consensus implementation
pub mod integration;
pub mod mempool_bridge;
pub mod bls_consensus_seal;
pub mod service;
pub mod consensus_storage;
pub mod mdbx_database_ops;
pub mod rpc_impl;
pub mod rpc_config;
pub mod transaction_adapter;
// pub mod dag_storage_adapter; // Temporarily disabled for compilation
// pub mod batch_storage_adapter; // Temporarily disabled for compilation