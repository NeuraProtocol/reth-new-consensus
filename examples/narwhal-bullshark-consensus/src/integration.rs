//! Main integration point between Narwhal+Bullshark consensus and Reth
//!
//! This module provides the bridge between the BFT consensus algorithm and
//! Reth's execution engine, handling block creation and submission.

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    block_builder::NarwhalPayloadBuilder,
};
use alloy_primitives::{B256, Address};
use reth_provider::{
    BlockReaderIdExt, StateProviderFactory, ChainSpecProvider,
    CanonStateNotificationSender, CanonStateNotifications,
};
use reth_node_api::{EngineValidator, BeaconConsensusEngineHandle};
use reth_payload_builder::{PayloadBuilderHandle, PayloadBuilderService};
// PayloadBuilder trait has been removed
use reth_transaction_pool::TransactionPool;
use reth_primitives::{SealedBlock, TransactionSigned};
use reth_tasks::TaskSpawner;
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use narwhal::types::{Committee, PublicKey as ConsensusPublicKey};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn, error, debug};
use anyhow::Result;

/// Main integration struct that bridges Narwhal+Bullshark with Reth
pub struct NarwhalRethIntegration<Provider, Pool, EvmConfig> {
    /// Chain specification
    chain_spec: Arc<ChainSpec>,
    /// Database provider
    provider: Provider,
    /// Transaction pool
    pool: Pool,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Engine API handle for submitting blocks
    engine_handle: BeaconConsensusEngineHandle<reth_node_ethereum::EthEngineTypes>,
    /// Channel for receiving finalized batches from consensus
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    /// Channel for sending finalized batches to consensus
    batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    /// Payload builder for creating blocks
    payload_builder: Arc<NarwhalPayloadBuilder<Provider>>,
    /// Current chain state
    chain_state: Arc<RwLock<ChainState>>,
}

#[derive(Debug, Clone)]
struct ChainState {
    pub block_number: u64,
    pub block_hash: B256,
}

impl<Provider, Pool, EvmConfig> NarwhalRethIntegration<Provider, Pool, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + ChainSpecProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + Unpin + 'static,
    EvmConfig: ConfigureEvm + Clone + Unpin + 'static,
{
    /// Create a new integration instance
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        engine_handle: BeaconConsensusEngineHandle<reth_node_ethereum::EthEngineTypes>,
    ) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        
        // Get current chain tip
        let tip = provider.last_block_number().unwrap_or(0);
        let hash = provider.block_hash(tip).unwrap().unwrap_or_default();
        
        let chain_state = Arc::new(RwLock::new(ChainState {
            block_number: tip,
            block_hash: hash,
        }));

        // Create payload builder
        let (_, builder_receiver): (mpsc::UnboundedSender<()>, mpsc::UnboundedReceiver<()>) = mpsc::unbounded_channel();
        let payload_builder = Arc::new(NarwhalPayloadBuilder::new(
            chain_spec.clone(),
            provider.clone(),
        ));

        Self {
            chain_spec,
            provider,
            pool,
            evm_config,
            engine_handle,
            batch_receiver,
            batch_sender,
            payload_builder,
            chain_state,
        }
    }

    /// Start the integration service
    pub async fn start(mut self, consensus_config: ConsensusConfig) -> Result<()> {
        info!("Starting Narwhal+Bullshark integration with Reth");

        // Start the consensus service in a separate task
        let consensus_handle = self.start_consensus_service(consensus_config).await?;

        // Main loop - process finalized batches
        while let Some(batch) = self.batch_receiver.recv().await {
            match self.process_finalized_batch(batch).await {
                Ok(block) => {
                    info!(
                        "Successfully created and submitted block #{} (hash: {})",
                        block.number,
                        block.hash()
                    );
                }
                Err(e) => {
                    error!("Failed to process finalized batch: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Process a finalized batch by creating and submitting a block
    async fn process_finalized_batch(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        info!(
            "Processing finalized batch #{} with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // Build the block using our payload builder
        let block = self.payload_builder.build_block(batch.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to build payload: {:?}", e))?;

        // Submit to engine API
        self.submit_block_to_engine(block.clone()).await?;

        // Update our chain state
        let mut state = self.chain_state.write().await;
        state.block_number = block.number;
        state.block_hash = block.hash();

        Ok(block)
    }

    /// Submit a block to the engine API
    async fn submit_block_to_engine(&self, block: SealedBlock) -> Result<()> {
        use reth_node_ethereum::EthEngineTypes;
        use reth_payload_primitives::PayloadTypes;
        use alloy_rpc_types::engine::{PayloadStatusEnum, ForkchoiceState};

        // Convert block to payload
        let payload = <EthEngineTypes as PayloadTypes>::block_to_payload(block.clone());

        // Submit new payload
        let status = self.engine_handle.new_payload(payload).await?;
        
        match status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} accepted by engine as valid", block.number);
                
                // Update fork choice to make it canonical
                let forkchoice = ForkchoiceState {
                    head_block_hash: block.hash(),
                    safe_block_hash: block.hash(),
                    finalized_block_hash: block.hash(),
                };
                
                let fc_response = self.engine_handle
                    .fork_choice_updated(forkchoice, None, reth_node_api::EngineApiMessageVersion::default())
                    .await?;
                
                if fc_response.payload_status.status == PayloadStatusEnum::Valid {
                    info!("Block #{} made canonical", block.number);
                } else {
                    error!("Fork choice update failed: {:?}", fc_response.payload_status);
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                error!("Block #{} rejected as invalid: {}", block.number, validation_error);
                return Err(anyhow::anyhow!("Block rejected: {}", validation_error));
            }
            _ => {
                warn!("Unexpected payload status: {:?}", status);
            }
        }

        Ok(())
    }

    /// Start the Narwhal+Bullshark consensus service
    async fn start_consensus_service(&self, config: ConsensusConfig) -> Result<()> {
        // This would start the actual Narwhal+Bullshark service
        // For now, we'll create a mock that sends test batches
        let sender = self.batch_sender.clone();
        let chain_state = self.chain_state.clone();

        tokio::spawn(async move {
            // Mock consensus - send a batch every 5 seconds
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                
                let state = chain_state.read().await;
                let next_block = state.block_number + 1;
                
                let batch = FinalizedBatch {
                    round: next_block,
                    block_number: next_block,
                    transactions: vec![], // Empty for now
                    certificate_digest: B256::random(),
                    proposer: Address::ZERO,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                
                if sender.send(batch).is_err() {
                    break;
                }
            }
        });

        Ok(())
    }

    /// Get a channel sender for submitting finalized batches
    pub fn batch_sender(&self) -> mpsc::UnboundedSender<FinalizedBatch> {
        self.batch_sender.clone()
    }
}