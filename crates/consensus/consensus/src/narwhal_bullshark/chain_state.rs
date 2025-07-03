//! Chain state tracking for consensus integration
//!
//! This module provides the connection between Narwhal/Bullshark consensus
//! and Reth's blockchain state, ensuring consensus has access to the current
//! chain tip for proper parent hash tracking.

use alloy_primitives::B256;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Chain state information shared between consensus and execution
#[derive(Debug, Clone)]
pub struct ChainState {
    /// Current block number
    pub block_number: u64,
    /// Current block hash (parent for next block)
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

/// Thread-safe chain state tracker
#[derive(Clone)]
pub struct ChainStateTracker {
    state: Arc<RwLock<ChainState>>,
}

impl ChainStateTracker {
    /// Create a new chain state tracker
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ChainState::default())),
        }
    }
    
    /// Create with initial state
    pub fn with_initial_state(block_number: u64, parent_hash: B256) -> Self {
        Self {
            state: Arc::new(RwLock::new(ChainState {
                block_number,
                parent_hash,
            })),
        }
    }
    
    /// Update the chain state
    pub async fn update(&self, block_number: u64, parent_hash: B256) {
        let mut state = self.state.write().await;
        state.block_number = block_number;
        state.parent_hash = parent_hash;
        debug!("Updated chain state: block {} parent {}", block_number, parent_hash);
    }
    
    /// Get the current chain state
    pub async fn get(&self) -> ChainState {
        self.state.read().await.clone()
    }
    
    /// Get just the parent hash
    pub async fn parent_hash(&self) -> B256 {
        self.state.read().await.parent_hash
    }
    
    /// Get just the block number
    pub async fn block_number(&self) -> u64 {
        self.state.read().await.block_number
    }
    
    /// Advance to next block (used after block creation)
    pub async fn advance(&self, new_block_hash: B256) {
        let mut state = self.state.write().await;
        state.block_number += 1;
        state.parent_hash = new_block_hash;
        info!("Advanced chain state to block {} with parent {}", 
              state.block_number, state.parent_hash);
    }
}

impl Default for ChainStateTracker {
    fn default() -> Self {
        Self::new()
    }
}