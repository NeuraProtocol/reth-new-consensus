//! Test integration for Narwhal+Bullshark consensus
//!
//! This module provides a minimal test implementation to verify
//! we can submit blocks to the engine API.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes};
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_primitives::{Block, SealedBlock};
use reth_ethereum_primitives::BlockBody;
use alloy_consensus::Header;
use alloy_consensus::Withdrawals;
use reth_primitives_traits::Block as BlockTrait;
use reth_node_api::EngineApiMessageVersion;
use reth_payload_primitives::PayloadTypes;
use reth_provider::BlockReaderIdExt;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Test integration that submits simple blocks
pub struct TestIntegration<Provider> {
    /// Provider for reading blockchain data
    provider: Provider,
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
            provider,
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

        // Get parent block info
        let parent_number = batch.block_number.saturating_sub(1);
        let parent_hash = self.provider
            .block_hash(parent_number)?
            .unwrap_or(B256::ZERO);

        // Create a minimal header
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::random(), // Placeholder
            transactions_root: B256::random(), // Placeholder
            receipts_root: B256::random(), // Placeholder
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 21000 * batch.transactions.len() as u64,
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: Default::default(),
            base_fee_per_gas: Some(1_000_000_000),
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // Create block
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
        };

        let block = Block { header, body };
        
        // Seal the block
        let sealed_block = block.seal_slow();

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