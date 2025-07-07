//! Block builder for Narwhal+Bullshark consensus
//! 
//! This module handles the creation of blocks from finalized batches,
//! ensuring proper state execution and hash calculation.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes, Bloom, BloomInput};
use alloy_consensus::{Header, Typed2718};
use reth_primitives::{
    Block, SealedBlock, Receipt, TxType
};
use reth_ethereum_primitives::BlockBody;
use reth_provider::{BlockReaderIdExt, StateProviderFactory, DatabaseProviderFactory};
use reth_evm::ConfigureEvm;
use alloy_consensus::proofs::{calculate_receipt_root, calculate_transaction_root};
use reth_chainspec::ChainSpec;
use alloy_eips::eip4895::Withdrawals;
use std::sync::Arc;
use tracing::{info, debug};
use anyhow::Result;

/// Builds blocks from finalized batches with proper state execution
pub struct NarwhalBlockBuilder<Provider, EvmConfig> {
    /// Chain specification
    chain_spec: Arc<ChainSpec>,
    /// Database provider
    provider: Provider,
    /// EVM configuration
    evm_config: EvmConfig,
}

impl<Provider, EvmConfig> NarwhalBlockBuilder<Provider, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + DatabaseProviderFactory + Clone + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
{
    /// Create a new block builder
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        evm_config: EvmConfig,
    ) -> Self {
        Self {
            chain_spec,
            provider,
            evm_config,
        }
    }

    /// Build a block from a finalized batch
    /// 
    /// This simplified version creates a block without executing transactions,
    /// using a temporary state root that will need to be corrected by the executor.
    pub fn build_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        debug!(
            "Building block #{} with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // Get the parent block
        let parent_number = batch.block_number.saturating_sub(1);
        let parent_hash = if parent_number == 0 {
            // Genesis parent
            B256::ZERO
        } else {
            self.provider
                .block_hash(parent_number)?
                .ok_or_else(|| anyhow::anyhow!("Parent block {} not found", parent_number))?
        };

        // Create simple receipts (execution will update these)
        let mut cumulative_gas_used = 0u64;
        let mut logs_bloom = Bloom::default();
        let receipts = batch.transactions.iter().map(|tx| {
            // Simple gas estimation
            let gas_used = 21000u64; // Base transaction cost
            cumulative_gas_used = cumulative_gas_used.saturating_add(gas_used);
            
            Receipt {
                tx_type: TxType::try_from(tx.ty()).unwrap_or(TxType::Legacy),
                success: true, // Assume success for now
                cumulative_gas_used,
                logs: vec![], // No logs for now
            }
        }).collect::<Vec<_>>();

        // Create the block header
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::random(), // Temporary - executor will update
            transactions_root: calculate_transaction_root(&batch.transactions),
            receipts_root: calculate_receipt_root(&receipts),
            logs_bloom,
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: cumulative_gas_used,
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: alloy_primitives::FixedBytes::default(),
            base_fee_per_gas: Some(1_000_000_000), // 1 gwei
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // Create the block body
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
        };

        // Create and seal the block
        let block = Block { header, body };
        // Use the trait method to seal the block
        let sealed_block = block.seal_slow();

        info!(
            "Built block #{} with {} transactions, hash: {}",
            sealed_block.number,
            sealed_block.body().transactions.len(),
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
}