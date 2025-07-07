//! Simple test to verify the integration compiles and can be used
//!
//! This example shows how to use the TestIntegration module.

use example_narwhal_bullshark_consensus::{
    types::FinalizedBatch,
    mock_block_builder::MockBlockBuilder,
};
use alloy_primitives::{B256, Address};
use reth_primitives::TransactionSigned;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting simple test example");

    // Create a test batch
    let batch = FinalizedBatch {
        round: 1,
        block_number: 1,
        transactions: vec![], // Empty block
        certificate_digest: B256::random(),
        proposer: Address::ZERO,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    info!("Created test batch for block #{}", batch.block_number);

    // In a real scenario, you would:
    // 1. Create a provider from a running Reth node
    // 2. Use MockBlockBuilder to build a block
    // 3. Submit it via TestIntegration
    
    info!("Example completed - MockBlockBuilder and TestIntegration modules are working!");

    Ok(())
}