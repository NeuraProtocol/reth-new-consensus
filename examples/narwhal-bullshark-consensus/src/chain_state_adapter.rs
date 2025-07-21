//! Adapter to connect consensus ChainStateTracker to Bullshark's ChainStateProvider trait

use crate::chain_state::ChainState;
use bullshark::{ChainStateProvider, chain_state::ChainState as BullsharkChainState};
use narwhal::{ChainStateProvider as NarwhalChainStateProvider, ChainState as NarwhalChainState};
use std::sync::{Arc, Mutex};

/// Adapter that implements Bullshark's ChainStateProvider using a shared chain state
pub struct ChainStateAdapter {
    state: Arc<Mutex<ChainState>>,
}

impl ChainStateAdapter {
    /// Create a new adapter with default state
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ChainState::default())),
        }
    }
    
    /// Create a new adapter with initial block number and hash
    pub fn with_initial_state(block_number: u64, parent_hash: alloy_primitives::B256) -> Self {
        let mut initial_state = ChainState::default();
        initial_state.block_number = block_number;
        initial_state.parent_hash = parent_hash;
        
        Self {
            state: Arc::new(Mutex::new(initial_state)),
        }
    }
    
    /// Update the chain state
    pub fn update(&self, block_number: u64, parent_hash: alloy_primitives::B256) {
        if let Ok(mut state) = self.state.lock() {
            state.block_number = block_number;
            state.parent_hash = parent_hash;
        }
    }
    
    /// Update the chain state with timestamp
    pub fn update_with_timestamp(&self, block_number: u64, parent_hash: alloy_primitives::B256, timestamp: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.block_number = block_number;
            state.parent_hash = parent_hash;
            state.timestamp = timestamp;
        }
    }
    
    /// Get a clone of the Arc for sharing
    pub fn clone_arc(&self) -> Arc<Mutex<ChainState>> {
        self.state.clone()
    }
}

impl ChainStateProvider for ChainStateAdapter {
    fn get_chain_state(&self) -> BullsharkChainState {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        
        BullsharkChainState {
            block_number: state.block_number,
            parent_hash: state.parent_hash,
            parent_timestamp: state.timestamp,
            gas_limit: state.gas_limit,
            base_fee_per_gas: state.base_fee_per_gas,
        }
    }
}

impl NarwhalChainStateProvider for ChainStateAdapter {
    fn get_chain_state(&self) -> NarwhalChainState {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        
        NarwhalChainState {
            block_number: state.block_number,
            parent_hash: state.parent_hash,
            parent_timestamp: state.timestamp,
        }
    }
}