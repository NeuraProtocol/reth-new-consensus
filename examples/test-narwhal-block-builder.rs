//! Test harness for Narwhal+Bullshark block building

use example_narwhal_bullshark_consensus::{
    test_block_submission::test_block_building,
    types::FinalizedBatch,
    block_builder::NarwhalBlockBuilder,
};
use alloy_primitives::Address;
use reth_chainspec::{ChainSpec, MAINNET};
use reth_node_ethereum::EthEvmConfig;
use reth_db::test_utils::create_test_rw_db;
use reth_provider::providers::StaticFileProvider;
use reth_provider::{ProviderFactory, BlockchainProvider, NodeTypes};
use std::sync::Arc;
use tracing::{info, error};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting Narwhal+Bullshark block building test");
    
    // Create a test database
    let db = create_test_rw_db();
    let chain_spec = Arc::new(ChainSpec::default());
    
    // Create provider factory
    let factory = ProviderFactory::<reth_node_types::AnyNodeTypes>::new(
        db.clone(),
        chain_spec.clone(),
        StaticFileProvider::read_write(tempfile::tempdir()?.into_path()).unwrap(),
    );
    
    // Create blockchain provider
    let provider = BlockchainProvider::new(factory.clone(), Default::default())?;
    
    // Create EVM config
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    
    // Create block builder
    let builder = NarwhalBlockBuilder::new(
        chain_spec.clone(),
        provider,
        evm_config,
    );
    
    // Test 1: Build an empty block
    info!("Test 1: Building empty block");
    let batch1 = FinalizedBatch {
        block_number: 1,
        timestamp: 1000,
        proposer: Address::random(),
        transactions: vec![],
    };
    
    let block1 = builder.build_block(batch1)?;
    info!(
        "✅ Built block #{} with hash: {}",
        block1.number,
        block1.hash()
    );
    
    // Test 2: Build another empty block
    info!("Test 2: Building second empty block");
    let batch2 = FinalizedBatch {
        block_number: 2,
        timestamp: 2000,
        proposer: Address::random(),
        transactions: vec![],
    };
    
    let block2 = builder.build_block(batch2)?;
    info!(
        "✅ Built block #{} with hash: {}, parent: {}",
        block2.number,
        block2.hash(),
        block2.parent_hash
    );
    
    // Verify the state root is not zero (even for empty blocks)
    if block1.state_root == alloy_primitives::B256::ZERO {
        error!("❌ Block 1 has zero state root!");
        return Err("Invalid state root".into());
    }
    
    info!("✅ All tests passed! Block building works correctly.");
    
    Ok(())
}
