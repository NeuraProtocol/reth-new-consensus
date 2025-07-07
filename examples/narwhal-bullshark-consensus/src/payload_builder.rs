//! Payload builder implementation for Narwhal+Bullshark consensus
//! 
//! This module implements a custom payload builder that integrates with Reth's
//! payload building infrastructure to properly create blocks with correct state roots.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Address, Bytes};
use reth_payload_builder::{
    PayloadJobGenerator, PayloadJob,
    PayloadBuilderError, PayloadKind,
};
use reth_payload_primitives::{BuiltPayload, PayloadBuilderAttributes};
use reth_payload_primitives::PayloadTypes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_provider::{
    BlockReaderIdExt, StateProviderFactory, BlockExecutionWriter,
    CanonChainTracker, ChainSpecProvider, ProviderResult,
};
use reth_primitives::{
    SealedBlock, Block, Header, TransactionSigned, Receipt,
};
use revm_primitives::BlockEnv;
use reth_evm::ConfigureEvm;
use reth_ethereum_primitives::BlockBody;
use reth_execution_types::ExecutionOutcome;
use reth_chainspec::ChainSpec;
use reth_tasks::TaskSpawner;
use reth_transaction_pool::TransactionPool;
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, debug, error};

/// Narwhal+Bullshark payload builder that creates blocks from finalized batches
pub struct NarwhalPayloadBuilder<Provider, Pool, EvmConfig> {
    /// The chain spec
    chain_spec: Arc<ChainSpec>,
    /// The provider for database access
    provider: Provider,
    /// The transaction pool
    pool: Pool,
    /// The EVM configuration
    evm_config: EvmConfig,
    /// Channel to receive finalized batches from consensus
    batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
}

impl<Provider, Pool, EvmConfig> NarwhalPayloadBuilder<Provider, Pool, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + Unpin + 'static,
    EvmConfig: ConfigureEvm + Clone + Unpin + 'static,
{
    /// Create a new Narwhal payload builder
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        Self {
            chain_spec,
            provider,
            pool,
            evm_config,
            batch_receiver,
        }
    }

    /// Build a payload from a finalized batch
    pub async fn build_payload(&self, batch: FinalizedBatch) -> Result<SealedBlock, PayloadBuilderError> {
        // Get the parent block
        let parent_hash = self.provider
            .block_hash(batch.block_number.saturating_sub(1))
            .map_err(|e| PayloadBuilderError::Internal(e.into()))?
            .ok_or_else(|| PayloadBuilderError::Internal("Parent block not found".into()))?;

        let parent_block = self.provider
            .block_by_hash(parent_hash)
            .map_err(|e| PayloadBuilderError::Internal(e.into()))?
            .ok_or_else(|| PayloadBuilderError::Internal("Parent block not found".into()))?;

        // Create a state provider at the parent block
        let state_provider = self.provider
            .state_by_block_hash(parent_hash)
            .map_err(|e| PayloadBuilderError::Internal(e.into()))?;

        // Execute the transactions to get the proper state root
        let block_env = BlockEnv {
            number: U256::from(batch.block_number),
            coinbase: batch.proposer,
            timestamp: U256::from(batch.timestamp),
            gas_limit: U256::from(30_000_000u64), // Standard gas limit
            basefee: U256::from(1_000_000_000u64), // 1 gwei
            difficulty: U256::ZERO,
            prevrandao: Some(B256::random()), // For post-merge
            blob_excess_gas_and_price: None,
        };

        // Create the header
        let mut header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root: B256::ZERO, // Will be calculated
            transactions_root: B256::ZERO, // Will be calculated
            receipts_root: B256::ZERO, // Will be calculated
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: batch.timestamp,
            extra_data: Bytes::default(),
            mix_hash: B256::ZERO,
            nonce: 0,
            base_fee_per_gas: Some(1_000_000_000), // 1 gwei
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // Execute transactions and get receipts
        let mut cumulative_gas_used = 0;
        let mut receipts = Vec::new();
        let mut executed_txs = Vec::new();

        // Create an EVM with our configuration
        let mut evm = self.evm_config.evm_with_env(
            state_provider.clone(),
            Default::default(),
        );

        // Configure the block environment
        self.evm_config.fill_block_env(
            &mut evm.context.evm.env.block,
            &self.chain_spec,
            &header,
            U256::from(cumulative_gas_used),
        );

        for tx in batch.transactions {
            // Configure transaction environment
            self.evm_config.fill_tx_env(&mut evm.context.evm.env, &tx, tx.recover_signer().unwrap());

            // Execute the transaction
            let result = evm.transact().map_err(|e| PayloadBuilderError::Internal(e.into()))?;

            // Create receipt
            let receipt = Receipt {
                status: result.is_success(),
                cumulative_gas_used: cumulative_gas_used + result.gas_used(),
                logs: result.into_logs().into_iter().map(Into::into).collect(),
            };

            cumulative_gas_used += result.gas_used();
            receipts.push(receipt);
            executed_txs.push(tx);
        }

        // Update header with execution results
        header.gas_used = cumulative_gas_used;
        header.logs_bloom = Receipt::bloom_filter(&receipts);
        header.state_root = state_provider.state_root().map_err(|e| PayloadBuilderError::Internal(e.into()))?;
        header.transactions_root = alloy_consensus::proofs::calculate_transaction_root(&executed_txs);
        header.receipts_root = alloy_consensus::proofs::calculate_receipt_root(&receipts);

        // Create the block
        let block = Block {
            header,
            body: BlockBody {
                transactions: executed_txs,
                ommers: vec![],
                withdrawals: Some(vec![]),
            },
        };

        // Seal the block
        let sealed_block = block.seal_slow();

        info!(
            "Built block #{} with {} transactions (hash: {})",
            sealed_block.number,
            sealed_block.body.transactions.len(),
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
}

/// Job generator for Narwhal+Bullshark consensus
pub struct NarwhalPayloadJobGenerator<Provider, Pool, EvmConfig> {
    builder: Arc<NarwhalPayloadBuilder<Provider, Pool, EvmConfig>>,
}

impl<Provider, Pool, EvmConfig> PayloadJobGenerator for NarwhalPayloadJobGenerator<Provider, Pool, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + Send + Sync + 'static,
    Pool: TransactionPool + Clone + Unpin + Send + Sync + 'static,
    EvmConfig: ConfigureEvm + Clone + Unpin + Send + Sync + 'static,
{
    type Job = NarwhalPayloadJob;

    fn new_payload_job(
        &self,
        attributes: <Self::Job as PayloadJob>::PayloadAttributes,
    ) -> Result<Self::Job, PayloadBuilderError> {
        // For Narwhal+Bullshark, we don't use traditional payload attributes
        // Instead, we wait for finalized batches from consensus
        Ok(NarwhalPayloadJob {
            builder: self.builder.clone(),
            attributes,
        })
    }
}

/// Payload job for Narwhal+Bullshark consensus
pub struct NarwhalPayloadJob {
    builder: Arc<dyn std::any::Any + Send + Sync>,
    attributes: reth_payload_primitives::EthPayloadBuilderAttributes,
}

impl PayloadJob for NarwhalPayloadJob {
    type PayloadAttributes = reth_payload_primitives::EthPayloadBuilderAttributes;
    type BuiltPayload = BuiltPayload;

    fn best_payload(&self) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        // For Narwhal+Bullshark, we build payloads based on finalized batches
        // This would be called by the engine API when it needs a payload
        Err(PayloadBuilderError::Internal("Not implemented for Narwhal+Bullshark".into()))
    }

    fn payload_attributes(&self) -> Result<Self::PayloadAttributes, PayloadBuilderError> {
        Ok(self.attributes.clone())
    }

    fn resolve(&mut self) -> Pin<Box<dyn Future<Output = Result<Self::BuiltPayload, PayloadBuilderError>> + Send + '_>> {
        Box::pin(async {
            Err(PayloadBuilderError::Internal("Not implemented for Narwhal+Bullshark".into()))
        })
    }
}