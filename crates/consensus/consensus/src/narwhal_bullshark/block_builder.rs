use crate::narwhal_bullshark::types::FinalizedBatch;
use reth_primitives::{SealedBlock, Header};
use reth_ethereum_primitives::{Block, BlockBody};
use alloy_primitives::{B256, U256, Address, Bloom};

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
        let header = Header {
            parent_hash: batch.parent_hash,
            ommers_hash: B256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: batch.timestamp,
            difficulty: U256::ZERO,
            nonce: Default::default(),
            mix_hash: B256::ZERO,
            beneficiary: Address::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            extra_data: Default::default(),
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
}
