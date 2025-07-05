//! Mock block executor for testing consensus without full Reth integration

use super::integration::BlockExecutor;
use reth_primitives::SealedBlock;
use alloy_primitives::B256;
use reth_execution_types::{BlockExecutionOutput, ExecutionOutcome};
use anyhow::Result;
use std::sync::Mutex;
use tracing::info;

/// A mock block executor that simulates blockchain behavior for testing
pub struct MockBlockExecutor {
    /// Current block number
    block_number: Mutex<u64>,
    /// Current block hash (parent for next block)
    block_hash: Mutex<B256>,
    /// Genesis block hash
    genesis_hash: B256,
}

impl MockBlockExecutor {
    /// Create a new mock executor with a specific genesis hash
    pub fn new(genesis_hash: B256) -> Self {
        info!("Created mock block executor with genesis hash: {}", genesis_hash);
        Self {
            block_number: Mutex::new(0), // Start at genesis
            block_hash: Mutex::new(genesis_hash),
            genesis_hash,
        }
    }
    
    /// Create a mock executor for the Neura test network
    pub fn neura_testnet() -> Self {
        // This is the actual genesis hash from the RPC query
        let genesis_hash = "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
            .parse::<B256>()
            .expect("Valid genesis hash");
        Self::new(genesis_hash)
    }
}

impl BlockExecutor for MockBlockExecutor {
    fn chain_tip(&self) -> Result<(u64, B256)> {
        let block_number = *self.block_number.lock().unwrap();
        let block_hash = *self.block_hash.lock().unwrap();
        info!("Mock executor returning chain tip: block {} hash {}", block_number, block_hash);
        Ok((block_number, block_hash))
    }
    
    fn execute_block(&self, block: &SealedBlock) -> Result<BlockExecutionOutput<ExecutionOutcome>> {
        // Simulate block execution
        let block_number = block.header.number;
        let block_hash = block.hash();
        
        info!("Mock executor 'executing' block {} with hash {}", block_number, block_hash);
        
        // Update our state
        *self.block_number.lock().unwrap() = block_number;
        *self.block_hash.lock().unwrap() = block_hash;
        
        // Return a dummy execution outcome
        Ok(BlockExecutionOutput {
            state: Default::default(),
            result: Default::default(),
        })
    }
    
    fn validate_block(&self, block: &SealedBlock) -> Result<()> {
        // Basic validation
        let expected_number = *self.block_number.lock().unwrap() + 1;
        if block.header.number != expected_number {
            return Err(anyhow::anyhow!(
                "Invalid block number: expected {}, got {}", 
                expected_number, 
                block.header.number
            ));
        }
        
        let expected_parent = *self.block_hash.lock().unwrap();
        if block.header.parent_hash != expected_parent {
            return Err(anyhow::anyhow!(
                "Invalid parent hash: expected {}, got {}", 
                expected_parent, 
                block.header.parent_hash
            ));
        }
        
        info!("Mock executor validated block {} successfully", block.header.number);
        Ok(())
    }
}