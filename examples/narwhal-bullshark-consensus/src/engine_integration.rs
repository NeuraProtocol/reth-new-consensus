//! Engine API integration for Narwhal+Bullshark consensus
//!
//! This module handles the submission of blocks to Reth's engine API.

use crate::{
    types::FinalizedBatch,
    block_builder::NarwhalBlockBuilder,
};
use alloy_primitives::B256;
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_node_api::EngineApiMessageVersion;
use reth_primitives::SealedBlock;
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_payload_primitives::PayloadTypes;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_evm::ConfigureEvm;
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Handles the integration with Reth's engine API for EVM-compatible chains
pub struct EngineIntegration<Provider, EvmConfig> {
    /// Block builder
    block_builder: Arc<NarwhalBlockBuilder<Provider, EvmConfig>>,
    /// Engine API handle for EVM-compatible chains
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Channel for receiving finalized batches
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<Provider, EvmConfig> EngineIntegration<Provider, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static,
    EvmConfig: ConfigureEvm + Clone + Send + Sync + 'static,
{
    /// Create a new engine integration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        evm_config: EvmConfig,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        let block_builder = Arc::new(NarwhalBlockBuilder::new(
            chain_spec,
            provider,
            evm_config,
        ));

        Self {
            block_builder,
            engine_handle,
            batch_receiver,
        }
    }

    /// Run the integration loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting engine integration");

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
    async fn submit_block(&self, block: SealedBlock) -> Result<()> {
        info!(
            "Submitting block #{} (hash: {}) to engine API",
            block.number,
            block.hash()
        );

        // Convert block to execution payload for EVM-compatible chain
        use reth_ethereum_engine_primitives::EthPayloadTypes;
        use reth_payload_primitives::PayloadTypes;
        let payload = EthPayloadTypes::block_to_payload(block.clone());

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
                    "Block #{} rejected as INVALID: {:?}",
                    block.number,
                    "invalid payload"
                );
                return Err(anyhow::anyhow!(
                    "Block rejected: {:?}",
                    "invalid payload"
                ));
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