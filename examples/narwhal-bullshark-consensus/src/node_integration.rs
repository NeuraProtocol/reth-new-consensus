//! Integration with Reth node for Narwhal+Bullshark consensus
//!
//! This module provides the actual integration that connects to a running
//! Reth node and submits blocks from the BFT consensus.

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    test_integration::TestIntegration,
    real_consensus_integration::RealConsensusIntegration,
    validator_keys::ValidatorKeyPair,
};
use alloy_primitives::{B256, Address};
use reth_primitives::TransactionSigned;
use reth_provider::BlockReaderIdExt;
use reth_transaction_pool::TransactionPool;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{info, error, debug};
use anyhow::Result;

/// Integration that connects Narwhal+Bullshark consensus to a Reth node
pub struct NodeIntegration<Provider, Pool, EvmConfig> {
    /// Chain spec
    chain_spec: std::sync::Arc<reth_chainspec::ChainSpec>,
    /// Provider for reading blockchain data
    provider: Provider,
    /// Transaction pool
    pool: Pool,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Engine API handle
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Validator key
    validator_key: ValidatorKeyPair,
    /// Consensus configuration
    config: ConsensusConfig,
}

impl<Provider, Pool, EvmConfig> NodeIntegration<Provider, Pool, EvmConfig>
where
    Provider: reth_provider::StateProviderFactory + reth_provider::DatabaseProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static,
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    EvmConfig: reth_evm::ConfigureEvm + Clone + Send + Sync + 'static,
{
    /// Create a new node integration
    pub fn new(
        chain_spec: std::sync::Arc<reth_chainspec::ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
        validator_key: ValidatorKeyPair,
        config: ConsensusConfig,
    ) -> Self {
        Self {
            chain_spec,
            provider,
            pool,
            evm_config,
            engine_handle,
            validator_key,
            config,
        }
    }

    /// Run the integration
    pub async fn run(self) -> Result<()> {
        info!("Starting Narwhal+Bullshark node integration");
        
        // Check if we should use real consensus or mock
        let use_real_consensus = std::env::var("USE_REAL_CONSENSUS")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap_or(false);
            
        if use_real_consensus {
            info!("Using REAL Narwhal+Bullshark consensus");
            
            // Create and run real consensus integration
            let real_consensus = RealConsensusIntegration::new(
                self.chain_spec,
                self.provider,
                self.evm_config,
                self.validator_key,
                self.config,
                self.engine_handle,
            );
            
            real_consensus.run().await
        } else {
            info!("Using MOCK consensus for testing");
            
            // Create channels for batch communication
            let (batch_sender, batch_receiver) = mpsc::unbounded_channel();

            // Start the test integration that submits blocks
            let test_integration = TestIntegration::new(
                self.provider.clone(),
                self.engine_handle.clone(),
                batch_receiver,
            );

            // Spawn the integration task
            let integration_handle = tokio::spawn(async move {
                if let Err(e) = test_integration.run().await {
                    error!("Test integration failed: {}", e);
                }
            });

            // Start the mock consensus that produces batches
            let consensus_handle = tokio::spawn(self.run_mock_consensus(batch_sender));

            // Wait for tasks
            tokio::select! {
                res = integration_handle => {
                    if let Err(e) = res {
                        error!("Integration task failed: {}", e);
                    }
                }
                res = consensus_handle => {
                    if let Err(e) = res {
                        error!("Consensus task failed: {}", e);
                    }
                }
            }

            Ok(())
        }
    }

    /// Run a mock consensus that produces batches
    /// In a real implementation, this would be the actual Narwhal+Bullshark consensus
    async fn run_mock_consensus(
        self,
        batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    ) -> Result<()> {
        info!("Starting mock consensus (produces a block every {} ms)", self.config.min_block_time_ms);

        let mut ticker = interval(Duration::from_millis(self.config.min_block_time_ms));
        let mut round = 0u64;

        loop {
            ticker.tick().await;
            round += 1;

            // Get current block number
            let current_block = self.provider.best_block_number()?;
            let next_block = current_block + 1;

            // Get pending transactions from the pool
            // For now, create an empty block since we don't know the exact pool transaction type
            let transactions: Vec<TransactionSigned> = vec![];

            debug!(
                "Mock consensus round {} creating block #{} with {} transactions",
                round,
                next_block,
                transactions.len()
            );

            // Create a finalized batch
            let batch = FinalizedBatch {
                round,
                block_number: next_block,
                transactions,
                certificate_digest: B256::random(),
                proposer: self.validator_key.evm_address,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            // Send to integration
            if batch_sender.send(batch).is_err() {
                info!("Integration closed, stopping consensus");
                break;
            }
        }

        Ok(())
    }
}