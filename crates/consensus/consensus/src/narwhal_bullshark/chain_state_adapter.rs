//! Adapter to connect consensus ChainStateTracker to Bullshark's ChainStateProvider trait

use crate::narwhal_bullshark::ChainStateTracker;
use bullshark::{ChainStateProvider, chain_state::ChainState as BullsharkChainState};
use tokio::runtime::Handle;

/// Adapter that implements Bullshark's ChainStateProvider using consensus ChainStateTracker
pub struct ChainStateAdapter {
    tracker: ChainStateTracker,
}

impl ChainStateAdapter {
    /// Create a new adapter
    pub fn new(tracker: ChainStateTracker) -> Self {
        Self { tracker }
    }
}

impl ChainStateProvider for ChainStateAdapter {
    fn get_chain_state(&self) -> BullsharkChainState {
        // Use blocking to get the async value
        let handle = Handle::current();
        let chain_state = handle.block_on(self.tracker.get());
        
        BullsharkChainState {
            block_number: chain_state.block_number,
            parent_hash: chain_state.parent_hash,
        }
    }
}