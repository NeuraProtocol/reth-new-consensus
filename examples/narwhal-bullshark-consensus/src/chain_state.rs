//! Chain state tracking for consensus integration

use alloy_primitives::{B256, Address};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug};

/// Current state of the blockchain
#[derive(Debug, Clone)]
pub struct ChainState {
    /// Current block number
    pub block_number: u64,
    /// Current block hash
    pub block_hash: B256,
    /// Parent block hash
    pub parent_hash: B256,
    /// Current state root
    pub state_root: B256,
    /// Current proposer
    pub proposer: Address,
    /// Timestamp of last block
    pub timestamp: u64,
    /// Current gas limit
    pub gas_limit: u64,
    /// Current base fee per gas
    pub base_fee_per_gas: u64,
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            block_number: 0,
            block_hash: B256::ZERO,
            parent_hash: B256::ZERO,
            state_root: B256::ZERO,
            proposer: Address::ZERO,
            timestamp: 0,
            gas_limit: 30_000_000,
            base_fee_per_gas: 875_000_000,
        }
    }
}

/// Thread-safe chain state tracker
#[derive(Clone)]
pub struct ChainStateTracker {
    state: Arc<RwLock<ChainState>>,
}

impl ChainStateTracker {
    /// Create a new chain state tracker with default state
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ChainState::default())),
        }
    }
    
    /// Create a new chain state tracker with initial state
    pub fn with_state(initial_state: ChainState) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
        }
    }

    /// Update the chain state after a new block
    pub async fn update_state(&self, new_state: ChainState) {
        let mut state = self.state.write().await;
        
        info!(
            "Updating chain state: block {} -> {}, hash {} -> {}",
            state.block_number,
            new_state.block_number,
            state.block_hash,
            new_state.block_hash
        );
        
        *state = new_state;
    }

    /// Get the current chain state
    pub async fn get_state(&self) -> ChainState {
        self.state.read().await.clone()
    }

    /// Get the current block number
    pub async fn get_block_number(&self) -> u64 {
        self.state.read().await.block_number
    }

    /// Get the current block hash
    pub async fn get_block_hash(&self) -> B256 {
        self.state.read().await.block_hash
    }

    /// Update just the block number and hash (synchronous version)
    pub fn update(&self, block_number: u64, block_hash: B256) {
        // Use try_write for non-blocking update
        if let Ok(mut state) = self.state.try_write() {
            state.parent_hash = state.block_hash;
            state.block_number = block_number;
            state.block_hash = block_hash;
            
            debug!(
                "Updated chain tip to block {} ({})",
                block_number,
                block_hash
            );
        }
    }
    
    /// Update just the block number and hash (async version)
    pub async fn update_tip(&self, block_number: u64, block_hash: B256) {
        let mut state = self.state.write().await;
        state.parent_hash = state.block_hash;
        state.block_number = block_number;
        state.block_hash = block_hash;
        
        debug!(
            "Updated chain tip to block {} ({})",
            block_number,
            block_hash
        );
    }
}

/// Provider interface for chain state (used by Bullshark)
pub trait ChainStateProvider: Send + Sync {
    /// Get the current block number
    fn block_number(&self) -> u64;
    
    /// Get the parent hash for a new block at the given height
    fn parent_hash(&self, block_number: u64) -> B256;
    
    /// Get the current state root
    fn state_root(&self) -> B256;
}

/// Implementation of ChainStateProvider for ChainStateTracker
impl ChainStateProvider for ChainStateTracker {
    fn block_number(&self) -> u64 {
        // This is a simplified sync implementation
        // In production, would need proper async handling
        futures::executor::block_on(self.get_block_number())
    }
    
    fn parent_hash(&self, block_number: u64) -> B256 {
        let state = futures::executor::block_on(self.get_state());
        
        if block_number == state.block_number + 1 {
            state.block_hash
        } else {
            // Would need to look up historical blocks
            B256::ZERO
        }
    }
    
    fn state_root(&self) -> B256 {
        futures::executor::block_on(self.get_state()).state_root
    }
}