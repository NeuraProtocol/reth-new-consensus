use crate::narwhal_bullshark::types::{FinalizedBatch, ConsensusSeal};
use reth_primitives::{SealedBlock, Header};
use reth_ethereum_primitives::{Block, BlockBody};
use alloy_primitives::{B256, U256, Address, Bloom, Bytes};
use fastcrypto::{
    traits::{ToFromBytes, AggregateAuthenticator},
    bls12381::{BLS12381AggregateSignature, BLS12381Signature},
};

/// Builder for converting finalized consensus batches into Reth blocks
#[derive(Debug)]
pub struct BlockBuilder;

impl BlockBuilder {
    /// Create a new block builder
    pub fn new() -> Self { 
        Self 
    }
    
    /// Build a block from a finalized consensus batch
    pub async fn build_block(&self, batch: FinalizedBatch) -> SealedBlock {
        // Create consensus seal from batch data
        let consensus_seal = self.create_consensus_seal(&batch);
        
        // Encode consensus seal into extra_data field
        let extra_data = consensus_seal.encode();
        
        let header = Header {
            parent_hash: batch.parent_hash,
            ommers_hash: B256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: batch.timestamp,
            difficulty: U256::ZERO,
            nonce: Default::default(),
            // Use certificate digest as mix_hash for consensus proof
            mix_hash: batch.certificate_digest,
            beneficiary: Address::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            extra_data,
            base_fee_per_gas: Some(1_000_000_000),
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            withdrawals_root: None,
            requests_hash: None,
        };
        
        let body = BlockBody {
            transactions: batch.transactions,
            ommers: vec![],
            withdrawals: None,
        };
        
        let block = Block::new(header, body);
        
        SealedBlock::seal_slow(block)
    }
    
    /// Create consensus seal from finalized batch
    fn create_consensus_seal(&self, batch: &FinalizedBatch) -> ConsensusSeal {
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
            }
        }
        
        // Aggregate the signatures
        let aggregated_signature = if signatures.is_empty() {
            // No signatures, create empty aggregate
            Bytes::from(vec![0u8; 96]) // BLS12-381 signature size
        } else {
            // Aggregate all signatures
            match BLS12381AggregateSignature::aggregate(signatures) {
                Ok(agg_sig) => Bytes::from(agg_sig.as_bytes().to_vec()),
                Err(_) => Bytes::from(vec![0u8; 96]), // Fallback on error
            }
        };
        
        ConsensusSeal {
            round: batch.consensus_round,
            certificate_digest: batch.certificate_digest,
            aggregated_signature,
            signers_bitmap: Bytes::from(signers_bitmap),
        }
    }
}
