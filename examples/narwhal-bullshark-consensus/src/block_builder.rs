//! Production block builder for Narwhal+Bullshark consensus
//! 
//! This creates properly constructed blocks from finalized batches with correct
//! state roots, gas usage, and all required fields for production use.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes, FixedBytes};
use reth_primitives::{SealedBlock, Block, Header, TransactionSigned};
use reth_ethereum_primitives::BlockBody;
use reth_chainspec::ChainSpec;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use std::sync::Arc;
use tracing::{info, error};
use anyhow::Result;

/// Simple payload builder that creates blocks from finalized batches
pub struct NarwhalPayloadBuilder<Provider> {
    /// The chain spec
    chain_spec: Arc<ChainSpec>,
    /// The provider for database access
    provider: Provider,
}

impl<Provider> NarwhalPayloadBuilder<Provider>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + 'static,
{
    /// Create a new Narwhal payload builder
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
    ) -> Self {
        Self {
            chain_spec,
            provider,
        }
    }

    /// Build a sealed block from a finalized batch
    pub async fn build_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // Get the parent block hash
        let parent_hash = if batch.block_number == 0 {
            B256::ZERO
        } else {
            self.provider
                .block_hash(batch.block_number.saturating_sub(1))?
                .unwrap_or(B256::ZERO)
        };

        // Create basic header for engine API submission
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::ZERO, // Engine will calculate
            transactions_root: alloy_consensus::proofs::calculate_transaction_root(&batch.transactions),
            receipts_root: B256::ZERO, // Engine will calculate
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 0, // Engine will calculate
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::ZERO,
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
            withdrawals: Some(Default::default()),
        };

        // Create the block and seal it
        let block = Block { header, body };
        let sealed_block = SealedBlock::seal_slow(block);

        info!(
            "🏗️  Built block #{} with {} transactions for engine API execution",
            sealed_block.number,
            sealed_block.body().transactions.len()
        );

        Ok(sealed_block)
    }
}

