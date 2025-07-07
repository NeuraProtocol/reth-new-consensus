//! Mock block builder for testing
//! 
//! This creates blocks without actual state execution, suitable for testing
//! the engine API integration.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes, FixedBytes};
use alloy_consensus::Header;
use reth_primitives::{Block, SealedBlock};
use reth_ethereum_primitives::BlockBody;
use reth_provider::BlockReaderIdExt;
use reth_primitives_traits::Block as BlockTrait;
use alloy_eips::eip4895::Withdrawals;
use tracing::{info, debug};
use anyhow::Result;

/// Mock block builder that creates blocks without state execution
pub struct MockBlockBuilder<Provider> {
    /// Database provider
    provider: Provider,
}

impl<Provider> MockBlockBuilder<Provider>
where
    Provider: BlockReaderIdExt + Clone + 'static,
{
    /// Create a new mock block builder
    pub fn new(provider: Provider) -> Self {
        Self { provider }
    }

    /// Build a block from a finalized batch
    pub fn build_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        debug!(
            "Mock building block #{} with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // Get parent info
        let parent_number = batch.block_number.saturating_sub(1);
        let parent_hash = self.provider
            .block_hash(parent_number)?
            .unwrap_or(B256::ZERO);

        // Calculate basic properties
        let gas_used: u64 = batch.transactions.iter()
            .map(|_| 21000u64) // Basic gas cost
            .sum();

        // Create header with mock values
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::random(), // Mock state root
            transactions_root: alloy_consensus::proofs::calculate_transaction_root(&batch.transactions),
            receipts_root: B256::random(), // Mock receipts root
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

        // Create block body
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
        };

        // Create and seal block
        let block = Block { header, body };
        let sealed_block = block.seal_slow();

        info!(
            "Mock built block #{} with {} transactions, hash: {}",
            sealed_block.number,
            sealed_block.transaction_count(),
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
}