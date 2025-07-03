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
/// Storage adapter for DAG service
pub mod dag_storage_adapter;
/// Transaction adapter for connecting Reth transaction pool to workers
pub mod transaction_adapter;
/// Implementation of ConsensusDbOps for Narwhal storage integration
pub mod consensus_db_ops_impl;
/// Batch storage adapter for workers
pub mod batch_storage_adapter;
/// Service-based RPC implementation for standalone consensus RPC server
pub mod service_rpc;
/// Chain state tracking for consensus integration
pub mod chain_state;
/// Chain state adapter for Bullshark
pub mod chain_state_adapter;

// Re-export key types for easier access
pub use types::{FinalizedBatch, NarwhalBullsharkConfig, ConsensusRpcConfig};
pub use service::NarwhalBullsharkService;
pub use integration::{NarwhalRethBridge, RethIntegrationConfig};
pub use mempool_bridge::{MempoolBridge, PoolStats};
pub use engine::{NarwhalBullsharkConsensus, FinalizationStream};
pub use validator_keys::{ValidatorKeyPair, ValidatorRegistry, ValidatorIdentity, ValidatorMetadata, ValidatorKeyConfig, KeyManagementStrategy};
pub use transaction_adapter::{TransactionAdapter, TransactionAdapterBuilder, encode_transaction};
pub use service_rpc::start_service_rpc_server;
pub use chain_state::{ChainState, ChainStateTracker};
