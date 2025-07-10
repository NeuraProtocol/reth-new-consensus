//! Block executor for Narwhal+Bullshark consensus
//!
//! This module handles the execution and persistence of blocks.

use alloy_primitives::B256;
use reth_chainspec::ChainSpec;
use reth_evm::{execute::{BlockExecutionOutput, ExecutionOutcome, Executor}, ConfigureEvm};
use reth_execution_types::BlockExecutionResult;
use reth_primitives::SealedBlock;
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes}, 
    ProviderError, DatabaseProviderFactory, StateProviderFactory, BlockReaderIdExt,
    BlockWriter, BlockNumReader, BlockHashReader, DBProvider, StorageLocation,
};
use reth_revm::database::StateProviderDatabase;
use reth_trie_common::{HashedPostState, KeccakKeyHasher};
use reth_trie::{updates::TrieUpdates, StateRoot};
use reth_trie_db::{DatabaseStateRoot, DatabaseTrieCursorFactory, DatabaseHashedCursorFactory};
use std::sync::Arc;
use tracing::{info, debug, warn};
use anyhow::Result;

/// Block executor that handles EVM execution and persistence
pub struct NarwhalBlockExecutor<Provider, EvmConfig> {
    provider: Provider,
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
}


impl<Provider, EvmConfig> NarwhalBlockExecutor<Provider, EvmConfig>
where
    Provider: StateProviderFactory + DatabaseProviderFactory + BlockReaderIdExt + Clone,
    Provider::Provider: reth_provider::BlockNumReader + reth_provider::BlockHashReader,
    Provider::ProviderRW: reth_provider::BlockWriter<Block = alloy_consensus::Block<alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844>>, Receipt = reth_ethereum_primitives::Receipt>,
    EvmConfig: reth_evm::ConfigureEvm<Primitives = reth_ethereum_primitives::EthPrimitives> + Clone,
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

    /// Get the current chain tip
    pub fn chain_tip(&self) -> Result<(u64, B256)> {
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

    /// Validate a block before execution
    pub fn validate_block(&self, block: &SealedBlock) -> Result<()> {
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

    /// Execute a block and return the execution output
    pub fn execute_block(
        &self,
        block: &SealedBlock,
    ) -> Result<BlockExecutionOutput<ExecutionOutcome>> {
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
        
        // Convert sealed block to recovered block for execution with proper type constraints
        let block_with_senders = block.clone().unseal();
        let senders = vec![alloy_primitives::Address::ZERO; block_with_senders.body.transactions.len()];
        let recovered = reth_primitives_traits::RecoveredBlock::new_unhashed(
            block_with_senders,
            senders
        );
        
        // Execute the block and get the full output
        let output = executor.execute(&recovered)?;
        
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
        
        // Return BlockExecutionOutput
        Ok(BlockExecutionOutput {
            state,
            result: BlockExecutionResult {
                gas_used: block_result.gas_used,
                receipts: vec![execution_outcome],
                requests: Default::default(),
            },
        })
    }

    /// Execute and persist a block with full state updates
    pub async fn execute_and_persist_block(
        &self,
        sealed_block: reth_primitives::SealedBlock,
    ) -> Result<()> {
        info!(
            "Executing and persisting block #{} (hash: {}, parent: {}, {} txs)",
            sealed_block.number,
            sealed_block.hash(),
            sealed_block.parent_hash,
            sealed_block.body().transactions.len()
        );

        // Validate the block first
        self.validate_block(&sealed_block)?;

        // Get a read-write database provider for the entire operation
        let provider_rw = self.provider.database_provider_rw()?;
        
        // Convert sealed block to recovered block for execution with proper type constraints  
        let block_with_senders = sealed_block.clone().unseal();
        let senders = vec![alloy_primitives::Address::ZERO; block_with_senders.body.transactions.len()];
        let recovered = reth_primitives_traits::RecoveredBlock::new_unhashed(
            block_with_senders.clone(),
            senders.clone()
        );
        
        // Special handling for empty blocks
        if sealed_block.body().transactions.is_empty() {
            info!("Block #{} has no transactions, using simplified persistence", sealed_block.number);
            
            // For empty blocks, we still need to use append_blocks_with_state
            // Create an empty execution outcome
            let empty_outcome = ExecutionOutcome::default();
            let empty_hashed_state = HashedPostState::default();
            let empty_trie_updates = TrieUpdates::default();
            
            provider_rw.append_blocks_with_state(
                vec![recovered],
                &empty_outcome,
                empty_hashed_state.into_sorted(),
                empty_trie_updates,
            )?;
            
            // Commit the transaction
            DBProvider::commit(provider_rw)?;
            
            info!(
                "✅ Empty block #{} successfully persisted",
                sealed_block.number
            );
            
            return Ok(());
        }
        
        // Execute the block
        let output = self.execute_block(&sealed_block)?;
        
        // Extract the execution results
        let state_bundle = output.state;
        let block_result = output.result;
        
        // The receipts contain ExecutionOutcome objects
        let execution_outcome = if let Some(outcome) = block_result.receipts.first() {
            outcome.clone()
        } else {
            return Err(anyhow::anyhow!("No execution outcome in block result"));
        };
        
        // Generate hashed post state from the execution state
        let hashed_post_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state_bundle.state());
        let hashed_post_state_sorted = hashed_post_state.clone().into_sorted();
        
        // Calculate the state root and trie updates
        let (computed_state_root, trie_updates) = StateRoot::overlay_root_with_updates(
            provider_rw.tx_ref(),
            hashed_post_state.clone(),
        )?;
        
        info!(
            "Computed state root for block #{}: {} (header state root: {})",
            sealed_block.number,
            computed_state_root,
            sealed_block.state_root
        );
        
        // Use append_blocks_with_state to atomically persist everything
        // Create ExecutionOutcome with proper structure based on Reth patterns
        let provider_execution_outcome = reth_execution_types::ExecutionOutcome {
            bundle: execution_outcome.state().clone(),
            receipts: execution_outcome.receipts().clone(),
            first_block: sealed_block.number,
            requests: vec![Default::default()], // No requests for now
        };
        
        BlockWriter::append_blocks_with_state(
            &provider_rw,
            vec![recovered],
            &provider_execution_outcome,
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
}