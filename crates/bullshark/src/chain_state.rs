//! Chain state interface for Bullshark consensus
//!
//! This module provides a trait for accessing blockchain state information
//! that Bullshark needs for creating finalized batches.

use alloy_primitives::B256;

/// Container for chain state information
#[derive(Debug, Clone)]
pub struct ChainState {
    /// Current block number
    pub block_number: u64,
    /// Current parent hash
    pub parent_hash: B256,
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            block_number: 0,
            parent_hash: B256::ZERO,
        }
    }
}

/// Trait for accessing chain state information
pub trait ChainStateProvider: Send + Sync {
    /// Get the current chain state
    fn get_chain_state(&self) -> ChainState;
}

/// Default implementation that returns genesis values
pub struct DefaultChainState;

impl ChainStateProvider for DefaultChainState {
    fn get_chain_state(&self) -> ChainState {
        ChainState::default()
    }
}