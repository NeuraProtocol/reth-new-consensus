//! Simplified integration for Narwhal+Bullshark consensus
//!
//! This module provides a straightforward way to build and submit blocks.

use crate::{
    types::FinalizedBatch,
    simple_block_builder::SimpleBlockBuilder,
};
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_node_api::EngineApiMessageVersion;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Simple integration that builds and submits blocks
pub struct SimpleIntegration<Provider> {
    /// Block builder
    block_builder: Arc<SimpleBlockBuilder<Provider>>,
    /// Engine API handle
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<EthEngineTypes>,
    /// Channel for receiving finalized batches
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<Provider> SimpleIntegration<Provider>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static,
{
    /// Create a new integration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<EthEngineTypes>,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        let block_builder = Arc::new(SimpleBlockBuilder::new(
            chain_spec,
            provider,
        ));

        Self {
            block_builder,
            engine_handle,
            batch_receiver,
        }
    }

    /// Run the integration loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting Narwhal+Bullshark integration");

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

        // Build the block
        let block = self.block_builder.build_block(batch)?;

        // Submit to engine API
        self.submit_block(block).await
    }

    /// Submit a block to the engine API
    async fn submit_block(&self, block: reth_primitives::SealedBlock) -> Result<()> {
        info!(
            "Submitting block #{} (hash: {}) to engine API",
            block.number,
            block.hash()
        );

        // Convert block to execution payload using the PayloadTypes trait
        use reth_payload_primitives::PayloadTypes;
        let payload = <EthEngineTypes as PayloadTypes>::block_to_payload(block.clone());

        // Submit new payload
        let status = self.engine_handle
            .new_payload(payload.clone())
            .await?;

        match status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} accepted as VALID", block.number);

                // Update fork choice to make it canonical
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
                    status => {
                        error!("Fork choice update failed with status: {:?}", status);
                    }
                }
            }
            PayloadStatusEnum::Invalid { .. } => {
                error!(
                    "Block #{} rejected as INVALID",
                    block.number
                );
                return Err(anyhow::anyhow!("Block rejected as invalid"));
            }
            PayloadStatusEnum::Syncing => {
                warn!("Engine is syncing, block #{} status pending", block.number);
            }
            PayloadStatusEnum::Accepted => {
                info!("Block #{} accepted, validation pending", block.number);
            }
        }

        Ok(())
    }
}