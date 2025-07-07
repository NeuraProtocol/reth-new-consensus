//! Batch-based block executor for Narwhal + Bullshark consensus
//!
//! This implements real EVM execution using Reth's batch executor pattern.

use alloy_primitives::B256;
use reth_chainspec::ChainSpec;
use std::fmt;
use reth_evm::{execute::{BlockExecutionOutput, ExecutionOutcome, Executor}, ConfigureEvm};
use reth_evm::block::BlockExecutionResult;
use reth_primitives::{SealedBlock, SealedHeader};
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes}, 
    ProviderError, DatabaseProviderFactory, StateProviderFactory,
    BlockWriter, BlockNumReader, BlockHashReader, DBProvider, BlockReader, StorageLocation,
};
use reth_revm::database::StateProviderDatabase;
use reth_trie_common::{HashedPostState, KeccakKeyHasher};
use reth_trie::{StateRoot, updates::TrieUpdates};
use reth_trie_db::DatabaseStateRoot;
use std::sync::Arc;
use tracing::info;

/// Simple error wrapper for block execution
#[derive(Debug)]
struct BlockExecutionError(String);

impl fmt::Display for BlockExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BlockExecutionError {}

/// A batch-based block executor using Reth's execution framework
pub struct BatchBlockExecutor<N, EvmConfig> 
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    provider: BlockchainProvider<N>,
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
}

impl<N, EvmConfig> BatchBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm<Primitives = reth_ethereum_primitives::EthPrimitives> + Clone,
{
    /// Create a new block executor
    pub fn new(
        provider: BlockchainProvider<N>,
        chain_spec: Arc<ChainSpec>,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            provider,
            chain_spec,
            evm_config,
        }
    }

    /// Execute a block using batch executor
    pub fn execute_block_with_batch_executor(
        &self,
        block: &SealedBlock,
    ) -> Result<reth_evm::execute::BlockExecutionOutput<ExecutionOutcome>, ProviderError> {
        info!(
            "Executing block #{} with batch executor (parent: {}, {} txs)",
            block.number,
            block.parent_hash,
            block.body().transactions.len()
        );

        // Create a state provider using the blockchain provider
        let state_provider = self.provider.latest()?;
        let db = StateProviderDatabase::new(state_provider);
        
        // Create a batch executor
        let executor = self.evm_config.batch_executor(db);
        
        // Convert sealed block to recovered block for execution
        let block_with_senders = block.clone().unseal();
        let senders = vec![alloy_primitives::Address::ZERO; block_with_senders.body.transactions.len()];
        let recovered = reth_primitives::RecoveredBlock::new_unhashed(block_with_senders, senders);
        
        // Execute the block and get the full output
        let output = executor.execute(&recovered)
            .map_err(|e| ProviderError::other(BlockExecutionError(format!("Block execution failed: {:?}", e))))?;
        
        // Extract the result and state
        let block_result = output.result;
        let state = output.state;
        
        // Create ExecutionOutcome from the block result and state
        let execution_outcome = ExecutionOutcome::from_blocks(
            block.number,
            state.clone(),
            vec![block_result.clone()],
        );
        
        info!(
            "✅ Block #{} executed: gas used: {}",
            block.number,
            block_result.gas_used
        );
        
        // Return BlockExecutionOutput where T = ExecutionOutcome
        // This is a bit unusual but matches what the trait expects
        Ok(BlockExecutionOutput {
            state, // Return the actual state changes
            result: BlockExecutionResult {
                gas_used: block_result.gas_used,
                receipts: vec![execution_outcome], // ExecutionOutcome wrapped in Vec
                requests: Default::default(), // No EIP-7685 requests in Narwhal consensus
            },
        })
    }

    /// Execute and persist a block with full state updates
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

        // Get a read-write database provider for the entire operation
        let provider_rw = self.provider.database_provider_rw()?;
        
        // Create a state provider from the current state
        let state_provider = self.provider.latest()?;
        let db = StateProviderDatabase::new(state_provider);
        
        // Create a batch executor
        let executor = self.evm_config.batch_executor(db);
        
        // Convert sealed block to recovered block for execution
        let block_with_senders = sealed_block.clone().unseal();
        let senders = vec![alloy_primitives::Address::ZERO; block_with_senders.body.transactions.len()];
        let recovered = reth_primitives::RecoveredBlock::new_unhashed(block_with_senders.clone(), senders.clone());
        
        // Special handling for empty blocks
        if sealed_block.body().transactions.is_empty() {
            info!("Block #{} has no transactions, using simplified persistence", sealed_block.number);
            
            // Check if this block already exists
            if let Ok(Some(existing_block)) = provider_rw.block_by_number(sealed_block.number) {
                // Block already exists - check if it's the same block
                let existing_hash = SealedHeader::seal_slow(existing_block.header).hash();
                if existing_hash == sealed_block.hash() {
                    info!("Block #{} already exists with same hash, skipping", sealed_block.number);
                    return Ok(());
                } else {
                    // Different block at same height - this is a problem
                    return Err(ProviderError::other(BlockExecutionError(
                        format!("Block #{} already exists with different hash: existing={:?}, new={:?}", 
                                sealed_block.number, existing_hash, sealed_block.hash())
                    )));
                }
            }
            
            // For empty blocks, just insert the block without executing
            // Create the recovered block for persistence
            let recovered_block = reth_primitives::RecoveredBlock::new_unhashed(
                block_with_senders.clone(),
                senders.clone(),
            );
            
            // Insert just the block
            provider_rw.insert_block(recovered_block, StorageLocation::Database)?;
            
            // Commit the transaction
            DBProvider::commit(provider_rw)?;
            
            info!(
                "✅ Empty block #{} successfully persisted",
                sealed_block.number
            );
            
            return Ok(());
        }
        
        // Execute the block and get the full output
        let output = executor.execute(&recovered)
            .map_err(|e| ProviderError::other(BlockExecutionError(format!("Block execution failed: {:?}", e))))?;
        
        // Extract the execution results
        let state_bundle = output.state;
        let block_result = output.result;
        
        // Create ExecutionOutcome from the block execution
        let execution_outcome = ExecutionOutcome::from_blocks(
            sealed_block.number,
            state_bundle.clone(),
            vec![block_result.clone()],
        );
        
        // Generate hashed post state from the execution state
        let hashed_post_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state_bundle.state());
        let hashed_post_state_sorted = hashed_post_state.clone().into_sorted();
        
        // Calculate the state root and trie updates
        let (computed_state_root, trie_updates) = StateRoot::overlay_root_with_updates(
            provider_rw.tx_ref(),
            hashed_post_state.clone(),
        ).map_err(|e| ProviderError::other(BlockExecutionError(format!("State root calculation failed: {:?}", e))))?;
        
        info!(
            "Computed state root for block #{}: {} (header state root: {})",
            sealed_block.number,
            computed_state_root,
            sealed_block.state_root
        );
        
        // Check if this block already exists before persisting
        if let Ok(Some(existing_block)) = provider_rw.block_by_number(sealed_block.number) {
            // Block already exists - check if it's the same block
            let existing_hash = existing_block.header.hash_slow();
            if existing_hash == sealed_block.hash() {
                info!("Block #{} already exists with same hash, skipping", sealed_block.number);
                return Ok(());
            } else {
                // Different block at same height - this is a problem
                return Err(ProviderError::other(BlockExecutionError(
                    format!("Block #{} already exists with different hash: existing={:?}, new={:?}", 
                            sealed_block.number, existing_hash, sealed_block.hash())
                )));
            }
        }
        
        // Create the recovered block for persistence
        let recovered_block = reth_primitives::RecoveredBlock::new_unhashed(
            block_with_senders,
            senders,
        );
        
        // Use append_blocks_with_state to atomically persist everything
        BlockWriter::append_blocks_with_state(
            &provider_rw,
            vec![recovered_block],
            &execution_outcome,
            hashed_post_state_sorted,
            trie_updates,
        )?;
        
        // Commit the transaction
        DBProvider::commit(provider_rw)?;

        info!(
            "✅ Block #{} successfully executed and persisted with full state (state_root: {})",
            sealed_block.number,
            computed_state_root
        );

        Ok(())
    }

    /// Get the current chain tip
    pub fn chain_tip(&self) -> Result<(u64, B256), ProviderError> {
        let provider = self.provider.database_provider_ro()?;
        
        // Get the latest block number  
        let latest_number = BlockNumReader::last_block_number(&provider)?;

        // Get the block hash
        let latest_hash = if latest_number == 0 {
            // Genesis hash for Neura
            "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
                .parse::<B256>()
                .unwrap_or(B256::ZERO)
        } else {
            BlockHashReader::block_hash(&provider, latest_number)?.unwrap_or(B256::ZERO)
        };

        info!("Chain tip: block {} hash {}", latest_number, latest_hash);
        Ok((latest_number, latest_hash))
    }
}

/// Batch consensus block executor wrapper
pub struct BatchConsensusBlockExecutor<N, EvmConfig> 
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    inner: Arc<BatchBlockExecutor<N, EvmConfig>>,
}

impl<N, EvmConfig> BatchConsensusBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm<Primitives = reth_ethereum_primitives::EthPrimitives> + Clone + Send + Sync,
{
    /// Create a new consensus block executor
    pub fn new(
        provider: BlockchainProvider<N>,
        chain_spec: Arc<ChainSpec>,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            inner: Arc::new(BatchBlockExecutor::new(provider, chain_spec, evm_config)),
        }
    }
}

impl<N, EvmConfig> reth_consensus::narwhal_bullshark::integration::BlockExecutor
    for BatchConsensusBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm<Primitives = reth_ethereum_primitives::EthPrimitives> + Clone + Send + Sync,
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
        // Execute using batch executor which now returns the correct type
        self.inner
            .execute_block_with_batch_executor(block)
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

        Ok(())
    }
}