//! Integration with Reth node for Narwhal+Bullshark consensus
//!
//! This module provides the actual integration that connects to a running
//! Reth node and submits blocks from the BFT consensus.

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    test_integration::TestIntegration,
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
pub struct NodeIntegration<Provider, Pool> {
    /// Provider for reading blockchain data
    provider: Provider,
    /// Transaction pool
    pool: Pool,
    /// Engine API handle
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Validator key
    validator_key: ValidatorKeyPair,
    /// Consensus configuration
    config: ConsensusConfig,
}

impl<Provider, Pool> NodeIntegration<Provider, Pool>
where
    Provider: BlockReaderIdExt + Clone + Send + Sync + 'static,
    Pool: TransactionPool + Clone + Send + Sync + 'static,
{
    /// Create a new node integration
    pub fn new(
        provider: Provider,
        pool: Pool,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
        validator_key: ValidatorKeyPair,
        config: ConsensusConfig,
    ) -> Self {
        Self {
            provider,
            pool,
            engine_handle,
            validator_key,
            config,
        }
    }

    /// Run the integration
    pub async fn run(self) -> Result<()> {
        info!("Starting Narwhal+Bullshark node integration");

        // Create channels for batch communication
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();

        // Start the test integration that submits blocks
        let test_integration = TestIntegration::new(
            self.provider.clone(),
            self.engine_handle,
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
            let pending_txs = self.pool.pooled_transactions();
            let transactions: Vec<TransactionSigned> = pending_txs
                .into_iter()
                .take(self.config.max_batch_size as usize / 1000) // Rough estimate
                .filter_map(|tx| {
                    // Extract the signed transaction from the pool transaction
                    match tx.transaction.clone().into_transaction() {
                        Some(signed) => Some(signed),
                        None => None,
                    }
                })
                .collect();

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