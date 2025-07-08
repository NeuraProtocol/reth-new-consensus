//! Engine API integration for Narwhal+Bullshark consensus
//!
//! This module handles the submission of execution payloads to Reth's engine API,
//! using Path B approach: letting Reth handle execution and state root calculation.

use crate::{
    types::FinalizedBatch,
    block_builder::NarwhalPayloadBuilder,
    block_executor::NarwhalBlockExecutor,
};
use alloy_primitives::B256;
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_node_api::EngineApiMessageVersion;
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_payload_primitives::PayloadTypes;
use reth_primitives::SealedBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Handles the integration with Reth's engine API for EVM-compatible chains
pub struct EngineIntegration<N> 
where
    N: reth_provider::providers::ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    /// Payload builder
    payload_builder: Arc<NarwhalPayloadBuilder<reth_provider::providers::BlockchainProvider<N>>>,
    /// Block executor for full EVM execution
    block_executor: Arc<NarwhalBlockExecutor<N>>,
    /// Engine API handle for EVM-compatible chains
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Channel for receiving finalized batches
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<N> EngineIntegration<N>
where
    N: reth_provider::providers::ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    /// Create a new engine integration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: reth_provider::providers::BlockchainProvider<N>,
        evm_config: reth_node_ethereum::EthEvmConfig,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        let payload_builder = Arc::new(NarwhalPayloadBuilder::new(
            chain_spec.clone(),
            provider.clone(),
        ));
        
        let block_executor = Arc::new(NarwhalBlockExecutor::new(
            provider,
            chain_spec,
            evm_config,
        ));

        Self {
            payload_builder,
            block_executor,
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

        // Build the basic block from consensus batch
        let basic_block = self.payload_builder.build_block(batch).await?;

        // Execute the block with full EVM execution
        let executed_block = self.block_executor.execute_block(basic_block).await?;

        // Submit the fully executed block to engine API for canonical updates
        self.submit_block(executed_block).await
    }

    /// Submit a sealed block to the engine API
    async fn submit_block(&self, block: SealedBlock) -> Result<()> {
        info!(
            "Submitting sealed block #{} (hash: {}) with {} transactions to engine API",
            block.number,
            block.hash(),
            block.body().transactions.len()
        );

        // Convert sealed block to execution payload
        let payload = EthPayloadTypes::block_to_payload(block.clone());

        // Submit new payload - this should properly execute and update canonical state
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
                        info!("Block #{} is now canonical and should update eth_blockNumber", block.number);
                    }
                    status => {
                        error!("Fork choice update failed with status: {:?}", status);
                    }
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
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
}