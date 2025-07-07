//! Block builder for Narwhal+Bullshark consensus
//! 
//! This module handles the creation of blocks from finalized batches,
//! ensuring proper state execution and hash calculation.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes};
use alloy_consensus::Header;
use reth_primitives::{
    Block, SealedBlock, TransactionSigned, Receipt
};
use reth_ethereum_primitives::BlockBody;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_evm::ConfigureEvm;
use alloy_consensus::proofs::{calculate_receipt_root, calculate_transaction_root};
use reth_chainspec::ChainSpec;
use reth_execution_types::Chain;
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
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + 'static,
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
        let parent_header = &parent_block.header;

        // Create the new block header
        let mut header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::ZERO, // Will be set after execution
            transactions_root: B256::ZERO, // Will be set after execution
            receipts_root: B256::ZERO, // Will be set after execution
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: alloy_primitives::FixedBytes::default(),
            base_fee_per_gas: Some(parent_header.next_block_base_fee(self.chain_spec.base_fee_params_at_timestamp(batch.timestamp))?),
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(parent_header.next_block_excess_blob_gas()),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // Create state provider at parent
        let state_provider = self.provider.state_by_block_hash(parent_hash)?;
        let mut db = reth_revm::db::State::builder()
            .with_database(state_provider)
            .with_bundle_update()
            .build();

        // Configure EVM
        let mut evm = self.evm_config.evm_with_env(&mut db, Default::default());
        self.evm_config.fill_block_env(
            evm.block_mut(),
            &self.chain_spec,
            &header,
            U256::ZERO,
        );

        // Execute transactions
        let mut receipts = Vec::new();
        let mut cumulative_gas_used = 0u64;

        for tx in &batch.transactions {
            // Get sender
            let sender = tx.recover_signer()
                .ok_or_else(|| anyhow::anyhow!("Failed to recover transaction sender"))?;

            // Configure transaction environment
            self.evm_config.fill_tx_env(evm.tx_mut(), tx, sender);

            // Execute transaction
            let result = evm.transact()
                .map_err(|e| anyhow::anyhow!("Transaction execution failed: {:?}", e))?;

            // Update gas used
            cumulative_gas_used = cumulative_gas_used.saturating_add(result.result.gas_used());

            // Create receipt
            let receipt = Receipt {
                status: result.result.is_success(),
                cumulative_gas_used,
                logs: result.result.logs().to_vec(),
            };

            receipts.push(receipt);
        }

        // Take the state bundle
        let bundle = db.take_bundle();

        // Update header with execution results
        header.gas_used = cumulative_gas_used;
        header.logs_bloom = receipts.iter()
            .flat_map(|r| r.logs.iter())
            .fold(Default::default(), |bloom, log| bloom | log.bloom());
        
        // Calculate roots
        header.state_root = bundle.state_root();
        header.transactions_root = calculate_transaction_root(&batch.transactions);
        header.receipts_root = calculate_receipt_root(&receipts);

        // Create the block
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(vec![]),
        };

        let block = Block { header, body };
        let sealed_block = block.seal_slow();

        info!(
            "Built block #{} with {} transactions, gas used: {}, hash: {}",
            sealed_block.number,
            sealed_block.body.transactions.len(),
            sealed_block.gas_used,
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
}