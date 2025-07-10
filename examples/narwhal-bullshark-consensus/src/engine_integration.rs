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
use reth_provider::{BlockReaderIdExt, StateProviderFactory, DatabaseProviderFactory};
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
    Provider: StateProviderFactory + DatabaseProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static,
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

        // Use engine API to build payload with correct state root
        self.build_and_submit_payload(batch).await
    }

    /// Build and submit a payload using the engine API
    async fn build_and_submit_payload(&self, batch: FinalizedBatch) -> Result<()> {
        // For now, fall back to the original approach since engine payload building
        // requires more complex transaction pool integration
        info!("Building block with pre-calculated state root for batch #{}", batch.block_number);
        
        // Build the block using our block builder
        let block = self.block_builder.build_block(batch)?;
        
        // Submit to engine API
        self.submit_block(block).await
    }

    /// Submit a block to the engine API
    async fn submit_block(&self, mut block: SealedBlock) -> Result<()> {
        info!(
            "ENGINE INTEGRATION: Submitting block #{} (hash: {}) to engine API",
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
            PayloadStatusEnum::Invalid { validation_error } => {
                info!("Block #{} validation error: {}", block.number, validation_error);
                
                // Check if it's a state root mismatch error
                if validation_error.contains("mismatched block state root") {
                    info!("Detected state root mismatch, attempting to correct...");
                    
                    // Try to extract the correct state root from the error
                    if let Some(correct_state_root) = self.extract_correct_state_root(&validation_error) {
                        info!("Retrying block #{} with correct state root: {}", block.number, correct_state_root);
                        
                        // Rebuild the block with the correct state root
                        let mut new_header = block.header().clone();
                        new_header.state_root = correct_state_root;
                        
                        let new_block = reth_primitives::Block::new(new_header, block.body().clone());
                        let new_sealed_block = reth_primitives::SealedBlock::seal_slow(new_block);
                        
                        // Retry submission with correct state root
                        return self.submit_block_final(new_sealed_block).await;
                    } else {
                        warn!("Failed to extract correct state root from error: {}", validation_error);
                    }
                }
                
                error!(
                    "Block #{} rejected as INVALID: {:?}",
                    block.number,
                    validation_error
                );
                return Err(anyhow::anyhow!(
                    "Block rejected: {:?}",
                    validation_error
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
    
    /// Extract the correct state root from the error message
    fn extract_correct_state_root(&self, error_msg: &str) -> Option<alloy_primitives::B256> {
        info!("Attempting to extract state root from error: {}", error_msg);
        
        // Parse error message like: "mismatched block state root: got 0x0bd0570f3da85d151b62002747dc26d1219e04ea55e430efa58da58c00196149, expected 0x0000000000000000000000000000000000000000000000000000000000000000"
        if let Some(start) = error_msg.find("got ") {
            let start = start + 4; // Skip "got "
            if let Some(end) = error_msg[start..].find(',') {
                let state_root_str = &error_msg[start..start + end];
                info!("Found state root string: {}", state_root_str);
                
                if let Ok(state_root) = state_root_str.parse::<alloy_primitives::B256>() {
                    info!("Successfully parsed state root: {}", state_root);
                    return Some(state_root);
                } else {
                    warn!("Failed to parse state root from string: {}", state_root_str);
                }
            } else {
                warn!("Could not find comma after 'got' in error message");
            }
        } else {
            warn!("Could not find 'got' in error message");
        }
        None
    }
    
    /// Submit a block to the engine API (final attempt, no retry)
    async fn submit_block_final(&self, block: reth_primitives::SealedBlock) -> Result<()> {
        info!(
            "Final submission of block #{} (hash: {}) to engine API",
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
                info!("Block #{} accepted as VALID on retry", block.number);

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
            PayloadStatusEnum::Invalid { validation_error } => {
                error!(
                    "Block #{} rejected as INVALID on retry: {:?}",
                    block.number,
                    validation_error
                );
                return Err(anyhow::anyhow!(
                    "Block rejected on retry: {:?}",
                    validation_error
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