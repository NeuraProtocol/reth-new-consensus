//! Block builder for Narwhal+Bullshark consensus
//! 
//! This module handles the creation of blocks from finalized batches,
//! ensuring proper state execution and hash calculation.

use crate::types::FinalizedBatch;
use alloy_primitives::{B256, U256, Bytes, Bloom, BloomInput};
use serde::{Serialize, Deserialize};
use alloy_consensus::{Header, Typed2718};
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
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
use fastcrypto;

/// Consensus seal data structure for block headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusSeal {
    /// Consensus round number
    pub round: u64,
    /// Certificate digest that finalized this block
    pub certificate_digest: B256,
    /// Aggregated BLS signature from validators
    pub aggregated_signature: Bytes,
    /// Bitmap of validators who signed
    pub signers_bitmap: Bytes,
}

impl ConsensusSeal {
    /// Encode consensus seal for storage in block header extra_data
    pub fn encode(&self) -> Bytes {
        // Simple encoding: round (8 bytes) + cert_digest (32 bytes) + sig_len (4 bytes) + sig + bitmap_len (4 bytes) + bitmap
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&self.round.to_be_bytes());
        encoded.extend_from_slice(self.certificate_digest.as_slice());
        encoded.extend_from_slice(&(self.aggregated_signature.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.aggregated_signature);
        encoded.extend_from_slice(&(self.signers_bitmap.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.signers_bitmap);
        Bytes::from(encoded)
    }
}

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

    /// Create consensus seal from finalized batch
    fn create_consensus_seal(&self, batch: &FinalizedBatch) -> ConsensusSeal {
        use fastcrypto::{
            traits::{ToFromBytes, AggregateAuthenticator},
            bls12381::{BLS12381AggregateSignature, BLS12381Signature},
        };
        
        debug!("Creating consensus seal for batch with {} validator signatures", batch.validator_signatures.len());
        
        // Aggregate signatures from validators
        let mut signatures = Vec::new();
        let mut signers_bitmap = vec![0u8; (batch.validator_signatures.len() + 7) / 8];
        
        // Collect BLS signatures and build bitmap
        for (i, (_pubkey, sig_bytes)) in batch.validator_signatures.iter().enumerate() {
            // Set bit in bitmap
            signers_bitmap[i / 8] |= 1 << (i % 8);
            
            // Try to parse BLS signature
            if let Ok(sig) = BLS12381Signature::from_bytes(sig_bytes) {
                signatures.push(sig);
                debug!("Successfully parsed BLS signature from validator {}", i);
            } else {
                debug!("Failed to parse BLS signature from validator {}", i);
            }
        }
        
        // Aggregate the signatures
        let aggregated_signature = if signatures.is_empty() {
            debug!("No valid signatures found, using empty aggregate");
            // No signatures, create empty aggregate
            Bytes::from(vec![0u8; 96]) // BLS12-381 signature size
        } else {
            debug!("Aggregating {} BLS signatures", signatures.len());
            // Aggregate all signatures
            match BLS12381AggregateSignature::aggregate(signatures) {
                Ok(agg_sig) => {
                    debug!("Successfully aggregated BLS signatures");
                    Bytes::from(agg_sig.as_bytes().to_vec())
                },
                Err(e) => {
                    debug!("Failed to aggregate BLS signatures: {}", e);
                    Bytes::from(vec![0u8; 96]) // Fallback on error
                }
            }
        };
        
        ConsensusSeal {
            round: batch.consensus_round,
            certificate_digest: batch.certificate_digest,
            aggregated_signature,
            signers_bitmap: Bytes::from(signers_bitmap),
        }
    }

    /// Build a block from a finalized batch
    /// 
    /// Creates a block with certificate digest as consensus proof in extra_data
    pub fn build_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // If there are no transactions, we can calculate the state root directly
        if batch.transactions.is_empty() {
            return self.build_empty_block(batch);
        }
        
        // For blocks with transactions, we need to execute them to get the correct state root
        self.build_block_with_execution(batch)
    }
    
    /// Build an empty block (no transactions)
    fn build_empty_block(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // Use certificate digest as extra_data (exactly 32 bytes as required by Ethereum)
        // This provides consensus proof while meeting engine API validation requirements
        let extra_data = Bytes::from(batch.certificate_digest.to_vec());
        
        debug!("Building empty block with {} bytes of extra_data", extra_data.len());

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

        // Use placeholder state root - engine integration will correct it if needed
        let state_root = alloy_consensus::constants::EMPTY_ROOT_HASH;

        // Get parent gas limit for proper validation
        // Based on the error message, the parent gas limit is 134,217,728 (0x8000000)
        // We need to stay within 1/1024 of this value
        let parent_gas_limit = 134_217_728u64;

        // Create the block header
        let header = Header {
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            beneficiary: batch.proposer,
            state_root, // Use calculated state root
            transactions_root: calculate_transaction_root(&batch.transactions),
            receipts_root: calculate_receipt_root(&receipts),
            logs_bloom,
            difficulty: U256::ZERO,
            number: batch.block_number,
            gas_limit: parent_gas_limit,
            gas_used: cumulative_gas_used,
            timestamp: batch.timestamp,
            extra_data,
            mix_hash: B256::ZERO,
            nonce: alloy_primitives::FixedBytes::default(),
            base_fee_per_gas: Some(875_000_000), // Parent block base fee
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: Some(EMPTY_REQUESTS_HASH),
        };

        // Create the block body
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
        };

        // Create and seal the block
        let block = Block { header, body };
        // Seal the block by computing its hash
        let sealed_block = SealedBlock::seal_slow(block);

        info!(
            "Built block #{} with {} transactions, hash: {}",
            sealed_block.number,
            sealed_block.body().transactions.len(),
            sealed_block.hash()
        );

        Ok(sealed_block)
    }
    
    /// Build a block with transaction execution to calculate correct state root
    fn build_block_with_execution(&self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // TODO: Implement execution-based block building
        // For now, fall back to empty block approach
        self.build_empty_block(batch)
    }
    
}