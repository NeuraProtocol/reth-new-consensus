//! Simple block builder that creates blocks without full state execution
//! 
//! This is a temporary solution to test block submission while we work
//! on proper state execution integration.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes, FixedBytes};
use alloy_consensus::Header;
use reth_primitives::{Block, SealedBlock};
use reth_ethereum_primitives::BlockBody;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_chainspec::ChainSpec;
use reth_primitives_traits::Block as BlockTrait;
use alloy_eips::eip4895::Withdrawals;
use std::sync::Arc;
use tracing::{info, debug};
use anyhow::Result;

/// Builds blocks without full state execution
/// 
/// Note: This builder creates blocks with empty state roots and receipts.
/// It's meant for testing the integration layer, not for production use.
pub struct SimpleBlockBuilder<Provider> {
    /// Chain specification
    chain_spec: Arc<ChainSpec>,
    /// Database provider
    provider: Provider,
}

impl<Provider> SimpleBlockBuilder<Provider>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + 'static,
{
    /// Create a new simple block builder
    pub fn new(chain_spec: Arc<ChainSpec>, provider: Provider) -> Self {
        Self { chain_spec, provider }
    }

    /// Build a block from a finalized batch
    pub fn build_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        debug!(
            "Building block #{} with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // Get the parent block
        let parent_number = batch.block_number.saturating_sub(1);
        let parent_hash = self.provider
            .block_hash(parent_number)?
            .ok_or_else(|| anyhow::anyhow!("Parent block {} not found", parent_number))?;

        let parent_block = self.provider
            .block_by_hash(parent_hash)?
            .ok_or_else(|| anyhow::anyhow!("Parent block not found"))?;
        let parent_header = parent_block.header();

        // Calculate basic block properties
        let gas_used: u64 = batch.transactions.iter()
            .map(|_tx| 21000u64) // Basic transaction cost
            .sum();

        // Create the header
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::random(), // Placeholder - would be calculated by EVM
            transactions_root: alloy_consensus::proofs::calculate_transaction_root(&batch.transactions),
            receipts_root: alloy_consensus::constants::EMPTY_RECEIPTS,
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used,
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::default(),
            base_fee_per_gas: Some(1_000_000_000), // 1 gwei
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // Create the block
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
        };

        let block = Block { header, body };
        let sealed_block = block.seal_slow();

        info!(
            "Built block #{} with {} transactions, gas used: {}, hash: {}",
            sealed_block.number,
            sealed_block.body().transactions.len(),
            sealed_block.gas_used,
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
}