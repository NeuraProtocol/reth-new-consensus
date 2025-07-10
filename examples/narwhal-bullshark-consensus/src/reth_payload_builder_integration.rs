//! Direct integration with Reth's payload builder for Narwhal+Bullshark consensus
//! 
//! This module implements the approach where we let Reth handle all block construction
//! by providing it with ordered transactions from consensus.

use crate::types::FinalizedBatch;
use alloy_primitives::{Address, B256, U256};
use alloy_consensus::constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS};
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use reth_chainspec::{ChainSpec, ChainSpecProvider};
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes, execute::BlockBuilder};
use reth_revm::State;
use alloy_eips::calc_next_block_base_fee;
use reth_execution_types::ExecutionOutcome;
use reth_primitives_traits::{Block as BlockTrait, SignerRecoverable};
use alloy_consensus::BlockHeader;
use reth_payload_builder::EthBuiltPayload;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives::{Block, BlockBody, Header, SealedBlock as EthSealedBlock, TransactionSigned, RecoveredTx, EthPrimitives};
use reth_provider::{BlockReaderIdExt, StateProviderFactory, HeaderProvider};
use reth_revm::database::StateProviderDatabase;
use std::sync::Arc;
use tracing::{debug, info};

/// Integration that uses Reth's block executor to build blocks from consensus output
pub struct RethPayloadBuilderIntegration<Provider, EvmConfig> {
    /// Provider for blockchain data
    provider: Provider,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Chain spec
    chain_spec: Arc<ChainSpec>,
}

impl<Provider, EvmConfig> RethPayloadBuilderIntegration<Provider, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + reth_provider::HeaderProvider<Header = alloy_consensus::Header> + Clone,
    EvmConfig: ConfigureEvm<NextBlockEnvCtx = NextBlockEnvAttributes, Primitives = reth_primitives::EthPrimitives> + Clone,
{
    /// Create a new integration
    pub fn new(
        provider: Provider,
        evm_config: EvmConfig,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            provider,
            evm_config,
            chain_spec,
        }
    }

    /// Build a block from a finalized batch using Reth's execution engine
    /// 
    /// This follows the exact pattern from your pseudocode:
    /// 1. Receive ordered txs from Narwhal + Bullshark
    /// 2. Build ExecutionPayload params
    /// 3. Call payload builder
    /// 4. Get built block with correct state_root, receipts_root, gas_used, etc.
    pub async fn build_block_from_batch(
        &self,
        batch: FinalizedBatch,
    ) -> Result<EthSealedBlock, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Building block #{} from batch with {} transactions",
            batch.block_number,
            batch.transactions.len()
        );

        // 1. We already have ordered txs from Narwhal + Bullshark in the batch
        let ordered_txs: Vec<TransactionSigned> = batch.transactions;

        // 2. Get parent block info
        let parent_number = batch.block_number.saturating_sub(1);
        let parent_hash = self.provider
            .block_hash(parent_number)?
            .ok_or("Parent block not found")?;
        
        let parent_header = self.provider
            .sealed_header(parent_number)?
            .ok_or("Parent header not found")?;

        // 3. Create state provider at parent block
        let state_provider = self.provider.state_by_block_hash(parent_hash)?;
        let state = StateProviderDatabase::new(&state_provider);
        
        // 4. Create EVM block builder with proper configuration
        let mut db_state = State::builder()
            .with_database(state)
            .with_bundle_update()
            .build();
            
        let mut builder = self.evm_config
            .builder_for_next_block(
                &mut db_state,
                &parent_header,
                NextBlockEnvAttributes {
                    timestamp: batch.timestamp,
                    suggested_fee_recipient: batch.proposer,
                    prev_randao: B256::random(), // For post-merge
                    gas_limit: 30_000_000, // Standard gas limit
                    parent_beacon_block_root: Some(B256::ZERO),
                    withdrawals: Some(vec![].into()),
                },
            )?;

        // 5. Apply pre-execution changes (withdrawals, etc.)
        builder.apply_pre_execution_changes()?;

        // 6. Execute all transactions
        let mut cumulative_gas_used = 0;
        let mut executed_txs = Vec::new();
        
        for tx in ordered_txs {
            // Recover the transaction with sender for execution
            let recovered = reth_primitives::RecoveredTx::new_unchecked(
                tx.clone(), 
                tx.recover_signer_unchecked().unwrap_or(alloy_primitives::Address::ZERO)
            );
            match builder.execute_transaction(recovered) {
                Ok(gas_used) => {
                    cumulative_gas_used += gas_used;
                    executed_txs.push(tx);
                    debug!("Executed transaction: gas_used={}", gas_used);
                }
                Err(e) => {
                    // Skip failed transactions
                    debug!("Transaction execution failed: {:?}", e);
                }
            }
        }

        // 7. Finish block building - this computes state_root, receipts_root, etc.
        let outcome = builder.finish(&state_provider)?;
        
        // 8. Extract the built block with all correct roots
        let recovered_block = outcome.block;
        
        // Get the block and seal it
        let block = recovered_block.into_block();
        let sealed_block = block.seal_slow();
        
        info!(
            "Built block #{} with {} transactions (state_root: {}, receipts_root: {})",
            sealed_block.header().number(),
            executed_txs.len(),
            sealed_block.header().state_root(),
            sealed_block.header().receipts_root()
        );

        Ok(sealed_block)
    }

    /// Alternative: Build using Reth's payload builder service
    /// This would be used if you want to go through the full payload builder infrastructure
    pub async fn build_using_payload_service(
        &self,
        batch: FinalizedBatch,
        payload_builder_handle: &reth_payload_builder::PayloadBuilderHandle<reth_ethereum_engine_primitives::EthPayloadTypes>,
    ) -> Result<EthBuiltPayload, Box<dyn std::error::Error + Send + Sync>> {
        use reth_payload_builder::EthPayloadBuilderAttributes;
        use alloy_rpc_types::engine::PayloadAttributes;
        
        // Create payload attributes
        let attributes = PayloadAttributes {
            timestamp: batch.timestamp,
            prev_randao: B256::random(),
            suggested_fee_recipient: batch.proposer,
            withdrawals: Some(vec![]),
            parent_beacon_block_root: Some(B256::ZERO),
        };
        
        // Get parent hash
        let parent_hash = self.provider
            .block_hash(batch.block_number.saturating_sub(1))?
            .ok_or("Parent block not found")?;
        
        // Create builder attributes
        let builder_attributes = EthPayloadBuilderAttributes::new(parent_hash, attributes);
        
        // Send to payload builder service
        let payload_id = payload_builder_handle
            .send_new_payload(builder_attributes)
            .await??;
        
        // Wait for payload to be built
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Get the built payload
        let payload = payload_builder_handle
            .best_payload(payload_id)
            .await
            .ok_or("No payload built")??;
        
        Ok(payload)
    }
}

/// Create a minimal header for a new block
fn create_header_from_batch(
    parent: &Header,
    batch: &FinalizedBatch,
    chain_spec: &ChainSpec,
) -> Header {
    // Calculate base fee using EIP-1559
    let params = chain_spec.base_fee_params_at_timestamp(batch.timestamp);
    let base_fee = calc_next_block_base_fee(
        parent.gas_used,
        parent.gas_limit,
        parent.base_fee_per_gas.unwrap_or(1_000_000_000),
        params,
    );

    Header {
        parent_hash: parent.hash_slow(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: batch.proposer,
        state_root: B256::ZERO, // Will be calculated by executor
        transactions_root: B256::ZERO, // Will be calculated by executor
        receipts_root: B256::ZERO, // Will be calculated by executor
        logs_bloom: Default::default(),
        difficulty: U256::ZERO,
        number: batch.block_number,
        gas_limit: 30_000_000,
        gas_used: 0, // Will be calculated by executor
        timestamp: batch.timestamp,
        extra_data: Default::default(),
        mix_hash: B256::ZERO,
        nonce: alloy_primitives::FixedBytes::default(),
        base_fee_per_gas: Some(base_fee),
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        requests_hash: Some(EMPTY_REQUESTS_HASH),
    }
}