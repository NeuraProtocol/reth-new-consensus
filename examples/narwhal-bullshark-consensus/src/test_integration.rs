//! Test integration for Narwhal+Bullshark consensus
//!
//! This module provides a minimal test implementation to verify
//! we can submit blocks to the engine API.

use crate::{types::FinalizedBatch, mock_block_builder::MockBlockBuilder};
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_primitives::SealedBlock;
use reth_node_api::EngineApiMessageVersion;
use reth_payload_primitives::PayloadTypes;
use reth_provider::BlockReaderIdExt;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Test integration that submits simple blocks
pub struct TestIntegration<Provider> {
    /// Mock block builder
    block_builder: MockBlockBuilder<Provider>,
    /// Engine API handle
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<EthEngineTypes>,
    /// Channel for receiving finalized batches
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<Provider> TestIntegration<Provider>
where
    Provider: BlockReaderIdExt + Clone + Send + Sync + 'static,
{
    /// Create a new test integration
    pub fn new(
        provider: Provider,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<EthEngineTypes>,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        Self {
            block_builder: MockBlockBuilder::new(provider),
            engine_handle,
            batch_receiver,
        }
    }

    /// Run the integration loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting test integration");

        while let Some(batch) = self.batch_receiver.recv().await {
            if let Err(e) = self.process_batch(batch).await {
                error!("Failed to process batch: {}", e);
            }
        }

        Ok(())
    }

    /// Process a finalized batch
    async fn process_batch(&self, batch: FinalizedBatch) -> Result<()> {
        info!(
            "Processing batch for block #{} with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // Build block using mock builder
        let sealed_block = self.block_builder.build_block(batch)?;

        // Submit to engine
        self.submit_block(sealed_block).await
    }

    /// Submit a block to the engine API
    async fn submit_block(&self, block: SealedBlock) -> Result<()> {
        info!(
            "Submitting block #{} (hash: {}) to engine API",
            block.number,
            block.hash()
        );

        // Convert to payload
        let payload = EthPayloadTypes::block_to_payload(block.clone());

        // Submit new payload
        let status = self.engine_handle
            .new_payload(payload)
            .await?;

        match status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} accepted as VALID", block.number);

                // Update fork choice
                let forkchoice = ForkchoiceState {
                    head_block_hash: block.hash(),
                    safe_block_hash: block.hash(),
                    finalized_block_hash: block.hash(),
                };

                let fc_response = self.engine_handle
                    .fork_choice_updated(forkchoice, None, EngineApiMessageVersion::default())
                    .await?;

                match fc_response.payload_status.status {
                    PayloadStatusEnum::Valid => {
                        info!("Block #{} is now canonical", block.number);
                    }
                    _ => {
                        error!("Fork choice update failed");
                    }
                }
            }
            PayloadStatusEnum::Invalid { .. } => {
                error!("Block #{} rejected as INVALID", block.number);
                return Err(anyhow::anyhow!("Block rejected"));
            }
            _ => {
                warn!("Unexpected status for block #{}", block.number);
            }
        }

        Ok(())
    }
}