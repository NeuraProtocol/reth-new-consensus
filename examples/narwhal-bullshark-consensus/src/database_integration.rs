//! Direct database integration for Narwhal+Bullshark consensus
//!
//! This module handles the direct persistence of blocks to the database
//! without going through the engine API. This is useful for testing
//! and for cases where we want to bypass the engine.

use crate::{
    types::FinalizedBatch,
    block_builder::NarwhalPayloadBuilder,
    block_executor::NarwhalBlockExecutor,
};
use reth_provider::{providers::{BlockchainProvider, ProviderNodeTypes}, BlockReaderIdExt, StateProviderFactory, BlockNumReader, BlockHashReader, BlockWriter, DatabaseProviderFactory};
use reth_evm::ConfigureEvm;
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;

/// Handles direct database persistence of blocks from consensus
pub struct DatabaseIntegration<N> 
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    /// Payload builder for creating blocks from batches
    payload_builder: Arc<NarwhalPayloadBuilder<BlockchainProvider<N>>>,
    /// Block executor for direct database persistence
    block_executor: Arc<crate::block_executor::NarwhalBlockExecutor<N>>,
    /// Channel for receiving finalized batches
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<N> DatabaseIntegration<N>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    BlockchainProvider<N>: StateProviderFactory + DatabaseProviderFactory + BlockReaderIdExt + Clone,
{
    /// Create a new database integration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: BlockchainProvider<N>,
        evm_config: reth_node_ethereum::EthEvmConfig,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        let payload_builder = Arc::new(NarwhalPayloadBuilder::new(
            chain_spec.clone(),
            provider.clone(),
        ));

        let block_executor = Arc::new(crate::block_executor::NarwhalBlockExecutor::new(
            provider,
            chain_spec,
            evm_config,
        ));

        Self {
            payload_builder,
            block_executor,
            batch_receiver,
        }
    }

    /// Run the integration loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting database integration (direct persistence mode)");

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
            "Processing batch for block #{} with {} transactions (direct DB mode)",
            batch.block_number,
            batch.transactions.len()
        );

        // Build the sealed block
        let sealed_block = self.payload_builder.build_block(batch).await?;

        // Execute and persist directly to database
        self.block_executor.execute_and_persist_block(sealed_block).await?;

        Ok(())
    }
}

/// Simple wrapper to provide both engine and database integration options
pub enum ConsensusIntegration<Provider, Pool, EvmConfig> {
    /// Use engine API for block submission
    Engine(crate::engine_integration::EngineIntegration<Provider>),
    /// Use direct database writes - for providers that are BlockchainProvider<N>
    Database(Box<dyn std::any::Any + Send>),
}

impl<Provider, Pool, EvmConfig> ConsensusIntegration<Provider, Pool, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Send + Sync + Unpin + 'static,
    Pool: reth_transaction_pool::TransactionPool + Clone + Unpin + 'static,
    EvmConfig: reth_evm::ConfigureEvm + Clone + Unpin + 'static,
{
    /// Create a new integration based on configuration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
        use_engine_api: bool,
        engine_handle: Option<reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>>,
    ) -> Result<Self> {
        if use_engine_api {
            if let Some(engine) = engine_handle {
                Ok(Self::Engine(crate::engine_integration::EngineIntegration::new(
                    chain_spec,
                    provider,
                    engine,
                    batch_receiver,
                )))
            } else {
                Err(anyhow::anyhow!("Engine API requested but no engine handle provided"))
            }
        } else {
            // For database mode, we need a BlockchainProvider which we can't create generically
            // So we'll just use engine mode for now
            Err(anyhow::anyhow!("Direct database mode not supported with generic provider. Use --narwhal.use-engine-tree flag."))
        }
    }

    /// Run the integration
    pub async fn run(self) -> Result<()> {
        match self {
            Self::Engine(integration) => integration.run().await,
            Self::Database(_) => unreachable!("Database mode not supported"),
        }
    }
}