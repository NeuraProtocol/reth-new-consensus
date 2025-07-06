//! Block executor implementation for Narwhal + Bullshark consensus
//!
//! This module provides the real block execution integration with Reth's
//! execution engine, including state transitions and database updates.

use alloy_primitives::{Address, B256, U256};
use reth_chainspec::ChainSpec;
use reth_db::tables;
use reth_db_api::transaction::{DbTx, DbTxMut};
use reth_ethereum_primitives::{Block, BlockBody, TransactionSigned};
use alloy_consensus::Header;
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_execution_types::{BlockExecutionOutput, ExecutionOutcome};
use reth_primitives::{Receipt, SealedBlock, SealedHeader};
use reth_primitives_traits::{Block as _, RecoveredBlock};
use reth_provider::{
    BlockWriter, DatabaseProviderFactory, ExecutionDataProvider, ProviderError,
    StateProviderFactory, StateWriter,
};
use reth_revm::database::StateProviderDatabase;
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_storage_api::StateProvider;
use reth_storage_errors::provider::ProviderResult;
use reth_trie::{updates::TrieUpdates, StateRoot};
use revm::db::states::bundle_state::BundleRetention;
use revm::db::BundleState;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// A block executor that integrates with Reth's execution engine
pub struct RethBlockExecutor<Provider, EvmConfig> {
    /// The database provider factory
    provider: Provider,
    /// Chain specification
    chain_spec: Arc<ChainSpec>,
    /// EVM configuration
    evm_config: EvmConfig,
}

impl<Provider, EvmConfig> RethBlockExecutor<Provider, EvmConfig>
where
    Provider: DatabaseProviderFactory + StateProviderFactory + Clone,
    EvmConfig: ConfigureEvm<Header = Header>,
{
    /// Create a new block executor
    pub fn new(
        provider: Provider,
        chain_spec: Arc<ChainSpec>,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            provider,
            chain_spec,
            evm_config,
        }
    }

    /// Execute a block and return the execution outcome
    pub fn execute_block(
        &self,
        sealed_block: &SealedBlock,
    ) -> Result<BlockExecutionOutput<ExecutionOutcome>, ProviderError> {
        info!(
            "Executing block #{} with {} transactions",
            sealed_block.number,
            sealed_block.body().transactions.len()
        );

        // Get the current state provider
        let state_provider = self.provider.latest()?;
        
        // Create a state database for execution
        let state_db = StateProviderDatabase::new(state_provider);
        
        // Create the executor
        let mut executor = self.evm_config.batch_executor(state_db);

        // Convert sealed block to recovered block for execution
        // In Narwhal consensus, all transactions are pre-validated
        let transactions = sealed_block
            .body()
            .transactions
            .iter()
            .map(|tx| {
                // Transactions from consensus are already recovered
                tx.clone()
            })
            .collect::<Vec<_>>();

        let block = Block {
            header: sealed_block.header().clone(),
            body: BlockBody {
                transactions: sealed_block.body().transactions.clone(),
                ommers: vec![], // No uncles in Narwhal consensus
                withdrawals: sealed_block.body().withdrawals.clone(),
            },
        };

        // Create recovered block
        let recovered_block = RecoveredBlock::new_unhashed(block, transactions);

        // Execute the block
        let result = executor.execute_one(&recovered_block)?;
        
        // Get the state changes
        let state = executor.into_state();
        let bundle = state.take_bundle();

        Ok(BlockExecutionOutput {
            state: bundle,
            result,
        })
    }

    /// Execute and persist a block to the database
    pub async fn execute_and_persist_block(
        &self,
        sealed_block: SealedBlock,
    ) -> Result<(), ProviderError> {
        info!(
            "Executing and persisting block #{} (hash: {}, parent: {}, {} txs)",
            sealed_block.number,
            sealed_block.hash(),
            sealed_block.parent_hash,
            sealed_block.body().transactions.len()
        );

        // Execute the block
        let BlockExecutionOutput { state, result } = self.execute_block(&sealed_block)?;

        // Get a read-write database provider
        let provider_rw = self.provider.database_provider_rw()?;

        // Calculate state root from the execution result
        let state_root = {
            // Apply state changes to calculate new state root
            let state_provider = self.provider.latest()?;
            
            // In a real implementation, we would calculate the actual state root
            // For now, use a placeholder that includes block hash
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            sealed_block.hash().hash(&mut hasher);
            state.state().hash(&mut hasher);
            let hash_value = hasher.finish();
            let mut root_bytes = [0u8; 32];
            root_bytes[..8].copy_from_slice(&hash_value.to_le_bytes());
            B256::from(root_bytes)
        };

        // Update the header with execution results
        let mut executed_header = sealed_block.header().clone();
        executed_header.state_root = state_root;
        executed_header.receipts_root = result.receipts_root();
        executed_header.logs_bloom = result.logs_bloom();
        
        // Calculate gas used
        let gas_used = result
            .receipts
            .iter()
            .map(|r| r.cumulative_gas_used)
            .last()
            .unwrap_or(0);
        executed_header.gas_used = gas_used;

        let executed_block = SealedBlock::new(executed_header.seal_slow(), sealed_block.into_body());

        // Write header
        provider_rw.tx_ref().put::<tables::Headers>(
            executed_block.number,
            executed_block.header().clone(),
        )?;

        // Write header hash mapping
        provider_rw.tx_ref().put::<tables::HeaderNumbers>(
            executed_block.hash(),
            executed_block.number,
        )?;

        // Mark as canonical
        provider_rw.tx_ref().put::<tables::CanonicalHeaders>(
            executed_block.number,
            executed_block.hash(),
        )?;

        // Write block body
        if !executed_block.body().transactions.is_empty() {
            let first_tx_num = provider_rw.tx_ref().entries::<tables::Transactions>()? as u64;

            // Insert body indices
            let body_indices = reth_db_models::StoredBlockBodyIndices {
                first_tx_num,
                tx_count: executed_block.body().transactions.len() as u64,
            };
            provider_rw.tx_ref().put::<tables::BlockBodyIndices>(
                executed_block.number,
                body_indices,
            )?;

            // Insert transactions and receipts
            for (i, (tx, receipt)) in executed_block
                .body()
                .transactions
                .iter()
                .zip(result.receipts.iter())
                .enumerate()
            {
                let tx_id = first_tx_num + i as u64;
                
                // Insert transaction
                provider_rw.tx_ref().put::<tables::Transactions>(
                    tx_id,
                    tx.clone().into(),
                )?;

                // Insert transaction hash mapping
                provider_rw.tx_ref().put::<tables::TransactionHashNumbers>(
                    *tx.hash(),
                    tx_id,
                )?;

                // Insert receipt
                provider_rw.tx_ref().put::<tables::Receipts>(
                    tx_id,
                    receipt.clone(),
                )?;
            }
        }

        // Apply state changes
        let mut state_writer = provider_rw;
        state.write_to_db(&mut state_writer)?;

        // Commit all changes
        state_writer.commit()?;

        info!(
            "✅ Block #{} successfully executed and persisted (state_root: {}, gas_used: {})",
            executed_block.number, executed_block.state_root, executed_block.gas_used
        );

        Ok(())
    }

    /// Get the current chain tip
    pub fn chain_tip(&self) -> Result<(u64, B256), ProviderError> {
        let provider = self.provider.database_provider_ro()?;
        
        // Get the latest block number
        let latest_number = provider
            .tx_ref()
            .cursor_read::<tables::CanonicalHeaders>()?
            .last()?
            .map(|(num, _)| num)
            .unwrap_or(0);

        // Get the block hash
        let latest_hash = if latest_number == 0 {
            // Genesis hash for Neura
            "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
                .parse::<B256>()
                .unwrap_or(B256::ZERO)
        } else {
            provider
                .tx_ref()
                .get::<tables::CanonicalHeaders>(latest_number)?
                .unwrap_or(B256::ZERO)
        };

        info!("Chain tip: block {} hash {}", latest_number, latest_hash);
        Ok((latest_number, latest_hash))
    }
}

/// Reth block executor wrapper for consensus integration
pub struct ConsensusBlockExecutor<Provider, EvmConfig> {
    inner: Arc<RethBlockExecutor<Provider, EvmConfig>>,
}

impl<Provider, EvmConfig> ConsensusBlockExecutor<Provider, EvmConfig>
where
    Provider: DatabaseProviderFactory + StateProviderFactory + Clone + Send + Sync,
    EvmConfig: ConfigureEvm<Header = Header> + Send + Sync,
{
    /// Create a new consensus block executor
    pub fn new(
        provider: Provider,
        chain_spec: Arc<ChainSpec>,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            inner: Arc::new(RethBlockExecutor::new(provider, chain_spec, evm_config)),
        }
    }
}

impl<Provider, EvmConfig> reth_consensus::narwhal_bullshark::integration::BlockExecutor
    for ConsensusBlockExecutor<Provider, EvmConfig>
where
    Provider: DatabaseProviderFactory + StateProviderFactory + Clone + Send + Sync,
    EvmConfig: ConfigureEvm<Header = Header> + Send + Sync,
{
    fn chain_tip(&self) -> anyhow::Result<(u64, B256)> {
        self.inner
            .chain_tip()
            .map_err(|e| anyhow::anyhow!("Failed to get chain tip: {}", e))
    }

    fn execute_block(
        &self,
        block: &SealedBlock,
    ) -> anyhow::Result<BlockExecutionOutput<ExecutionOutcome>> {
        self.inner
            .execute_block(block)
            .map_err(|e| anyhow::anyhow!("Failed to execute block: {}", e))
    }

    fn validate_block(&self, block: &SealedBlock) -> anyhow::Result<()> {
        // Get current chain tip
        let (current_number, current_hash) = self.chain_tip()?;

        // Validate block number
        let expected_number = current_number + 1;
        if block.header().number != expected_number {
            return Err(anyhow::anyhow!(
                "Invalid block number: expected {}, got {}",
                expected_number,
                block.header().number
            ));
        }

        // Validate parent hash
        if block.header().parent_hash != current_hash {
            return Err(anyhow::anyhow!(
                "Invalid parent hash: expected {}, got {}",
                current_hash,
                block.header().parent_hash
            ));
        }

        // Additional validation could go here (e.g., timestamp checks)

        Ok(())
    }
}