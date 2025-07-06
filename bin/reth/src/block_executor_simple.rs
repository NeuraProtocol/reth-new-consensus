//! Simplified block executor for Narwhal + Bullshark consensus
//!
//! This uses the high-level BlockWriter trait to persist blocks.

use alloy_primitives::B256;
use reth_chainspec::ChainSpec;
use reth_evm::{execute::{BlockExecutionOutput, ExecutionOutcome}, ConfigureEvm};
use reth_primitives::SealedBlock;
use reth_provider::{providers::{BlockchainProvider, ProviderNodeTypes}, ProviderError, DatabaseProviderFactory};
use std::sync::Arc;
use tracing::info;

/// A simplified block executor that uses BlockchainProvider directly
pub struct SimpleBlockExecutor<N, EvmConfig> 
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    provider: BlockchainProvider<N>,
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
}

impl<N, EvmConfig> SimpleBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm,
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

    /// Execute and persist a block
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

        // For now, we'll use the simple approach - just write the block
        // Real execution would happen here with the EVM
        let provider_rw = self.provider.database_provider_rw()?;
        
        // Convert sealed block to recovered block
        // For consensus blocks, we don't need sender recovery, so we'll use empty senders
        let block = reth_ethereum_primitives::Block {
            header: sealed_block.header().clone(),
            body: reth_ethereum_primitives::BlockBody {
                transactions: sealed_block.body().transactions.clone(),
                ommers: vec![],
                withdrawals: sealed_block.body().withdrawals.clone(),
            },
        };
        
        // Create empty senders list matching the number of transactions
        let senders = vec![alloy_primitives::Address::ZERO; sealed_block.body().transactions.len()];
        
        let recovered_block = reth_primitives::RecoveredBlock::new_unhashed(
            block,
            senders,
        );
        
        // Insert the block using BlockWriter trait
        use reth_provider::{BlockWriter, StorageLocation, DBProvider};
        BlockWriter::insert_block(&provider_rw, recovered_block, StorageLocation::Database)?;
        
        // Commit the transaction
        DBProvider::commit(provider_rw)?;

        info!(
            "✅ Block #{} successfully persisted",
            sealed_block.number
        );

        Ok(())
    }

    /// Get the current chain tip
    pub fn chain_tip(&self) -> Result<(u64, B256), ProviderError> {
        use reth_provider::{BlockNumReader, BlockHashReader};
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

/// Simple consensus block executor wrapper
pub struct SimpleConsensusBlockExecutor<N, EvmConfig> 
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
{
    inner: Arc<SimpleBlockExecutor<N, EvmConfig>>,
}

impl<N, EvmConfig> SimpleConsensusBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm + Send + Sync,
{
    /// Create a new consensus block executor
    pub fn new(
        provider: BlockchainProvider<N>,
        chain_spec: Arc<ChainSpec>,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            inner: Arc::new(SimpleBlockExecutor::new(provider, chain_spec, evm_config)),
        }
    }
}

impl<N, EvmConfig> reth_consensus::narwhal_bullshark::integration::BlockExecutor
    for SimpleConsensusBlockExecutor<N, EvmConfig>
where
    N: ProviderNodeTypes<Primitives = reth_ethereum_primitives::EthPrimitives>,
    EvmConfig: ConfigureEvm + Send + Sync,
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
        // For now, return a dummy execution output
        // Real execution would use the EVM here
        info!("Mock executing block #{}", block.number);
        
        Ok(BlockExecutionOutput {
            state: Default::default(),
            result: Default::default(),
        })
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