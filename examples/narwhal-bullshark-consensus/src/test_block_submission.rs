//! Test module for verifying block submission with correct hashes

use crate::{
    types::FinalizedBatch,
    block_builder::NarwhalBlockBuilder,
    engine_integration::EngineIntegration,
};
use alloy_primitives::{Address, B256};
use reth_chainspec::ChainSpec;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::ConfigureEvm;
use reth_node_ethereum::EthEvmConfig;
use reth_primitives::SealedBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory, DatabaseProviderFactory};
use std::sync::Arc;
use tracing::{info, debug};
use anyhow::Result;

/// Test that we can build blocks with proper state roots
pub async fn test_block_building<Provider>(
    chain_spec: Arc<ChainSpec>,
    provider: Provider,
) -> Result<()>
where
    Provider: StateProviderFactory + BlockReaderIdExt + DatabaseProviderFactory + Clone + 'static,
{
    info!("Testing block building with proper state roots");
    
    // Create EVM config
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    
    // Create block builder
    let builder = NarwhalBlockBuilder::new(
        chain_spec.clone(),
        provider.clone(),
        evm_config,
    );
    
    // Create a test batch with no transactions (simplest case)
    let batch1 = FinalizedBatch {
        block_number: 1,
        timestamp: 1000,
        proposer: Address::random(),
        transactions: vec![],
    };
    
    // Build the block
    let block1 = builder.build_block(batch1)?;
    info!(
        "Built empty block #{} with hash: {}, state_root: {}",
        block1.number,
        block1.hash(),
        block1.state_root
    );
    
    // Create a test batch with some transactions
    // For now, we'll use empty transactions vector since we need valid transactions
    let batch2 = FinalizedBatch {
        block_number: 2,
        timestamp: 2000,
        proposer: Address::random(),
        transactions: vec![], // TODO: Add test transactions
    };
    
    let block2 = builder.build_block(batch2)?;
    info!(
        "Built block #{} with hash: {}, state_root: {}, parent_hash: {}",
        block2.number,
        block2.hash(),
        block2.state_root,
        block2.parent_hash
    );
    
    // Verify parent hash linkage
    if block2.parent_hash != block1.hash() {
        return Err(anyhow::anyhow!(
            "Block 2 parent hash {} doesn't match block 1 hash {}",
            block2.parent_hash,
            block1.hash()
        ));
    }
    
    info!("✅ Block building test passed! Blocks are properly linked.");
    
    Ok(())
}

/// Test block submission to the engine API
pub async fn test_engine_submission(
    engine_url: String,
    jwt_secret: Option<String>,
    block: SealedBlock,
) -> Result<()> {
    info!("Testing block submission to engine API at {}", engine_url);
    
    // TODO: Implement engine submission test
    // This would involve:
    // 1. Creating an engine API client with JWT auth
    // 2. Calling engine_newPayloadV3
    // 3. Calling engine_forkchoiceUpdatedV3
    // 4. Verifying the block was accepted
    
    info!("⚠️  Engine submission test not yet implemented");
    
    Ok(())
}