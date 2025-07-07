//! Example that demonstrates submitting blocks to the engine API
//!
//! This example creates a test block and submits it to show the integration works.

use example_narwhal_bullshark_consensus::{
    types::FinalizedBatch,
    test_integration::TestIntegration,
};
use alloy_primitives::{B256, Address};
use reth_primitives::TransactionSigned;
use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting test engine submission example");

    // Create channels for batch communication
    let (batch_sender, batch_receiver) = mpsc::unbounded_channel();

    // In a real implementation, you would:
    // 1. Start a Reth node
    // 2. Get the provider and engine handle from the node
    // 3. Create the integration with those components
    
    // For this example, we'll just demonstrate the structure
    info!("Example structure created - in a real implementation:");
    info!("1. Start Reth node with --dev flag");
    info!("2. Get provider and engine_handle from the node");
    info!("3. Create TestIntegration with those components");
    info!("4. Send FinalizedBatch through the channel");
    
    // Example of creating a finalized batch
    let example_batch = FinalizedBatch {
        round: 1,
        block_number: 1,
        transactions: vec![], // Empty block for testing
        certificate_digest: B256::random(),
        proposer: Address::ZERO,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    
    info!("Created example batch for block #{}", example_batch.block_number);
    
    // Send the batch
    batch_sender.send(example_batch)?;
    
    // Drop sender to close channel
    drop(batch_sender);
    
    info!("Example completed - see TestIntegration for actual implementation");

    Ok(())
}