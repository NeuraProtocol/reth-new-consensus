//! Canonical Block Metadata for deterministic EVM block construction
//! 
//! This module ensures all validators create identical blocks by having
//! the round leader specify all non-deterministic fields.

use alloy_primitives::{B256, U256, Bytes, Address};
use serde::{Serialize, Deserialize};
use anyhow::Result;

/// Canonical metadata for deterministic block construction
/// The round leader publishes this to ensure all validators create identical blocks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalBlockMetadata {
    /// Sequential block height
    pub block_number: u64,
    
    /// Hash of previous block
    pub parent_hash: B256,
    
    /// Fixed timestamp (not from local clock)
    pub timestamp: u64,
    
    /// Fixed metadata (empty or protocol-defined)
    pub extra_data: Bytes,
    
    /// Fixed to zero if unused
    pub mix_hash: B256,
    
    /// Fixed to zero if unused  
    pub nonce: u64,
    
    /// For EIP-1559, set by protocol or fixed
    pub base_fee_per_gas: U256,
    
    /// Gas limit (must be consistent)
    pub gas_limit: u64,
    
    /// Transaction ordering method
    pub tx_ordering: TxOrdering,
    
    /// Transaction hashes in final execution order
    pub tx_hashes: Vec<B256>,
    
    /// Expected block hash (optional, for validation)
    pub expected_block_hash: Option<B256>,
    
    /// Expected state root (optional, for validation)
    pub expected_state_root: Option<B256>,
    
    /// Expected receipts root (optional, for validation)
    pub expected_receipts_root: Option<B256>,
}

/// Transaction ordering method
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxOrdering {
    /// Sort by (sender_address, nonce)
    SenderNonce,
    /// Topological order from DAG, then by sender+nonce
    TopoSenderNonce,
    /// Use exact order from tx_hashes
    Explicit,
}

impl CanonicalBlockMetadata {
    /// Create new metadata for a block
    pub fn new(
        block_number: u64,
        parent_hash: B256,
        timestamp: u64,
        base_fee_per_gas: U256,
        gas_limit: u64,
        tx_hashes: Vec<B256>,
    ) -> Self {
        Self {
            block_number,
            parent_hash,
            timestamp,
            extra_data: Bytes::new(), // Empty by default
            mix_hash: B256::ZERO,
            nonce: 0,
            base_fee_per_gas,
            gas_limit,
            tx_ordering: TxOrdering::SenderNonce,
            tx_hashes,
            expected_block_hash: None,
            expected_state_root: None,
            expected_receipts_root: None,
        }
    }
    
    /// Validate that actual execution matches expected values
    pub fn validate_execution(
        &self,
        actual_block_hash: B256,
        actual_state_root: B256,
        actual_receipts_root: B256,
    ) -> Result<()> {
        if let Some(expected) = self.expected_block_hash {
            if expected != actual_block_hash {
                return Err(anyhow::anyhow!(
                    "Block hash mismatch: expected {} but got {}",
                    expected, actual_block_hash
                ));
            }
        }
        
        if let Some(expected) = self.expected_state_root {
            if expected != actual_state_root {
                return Err(anyhow::anyhow!(
                    "State root mismatch: expected {} but got {}",
                    expected, actual_state_root
                ));
            }
        }
        
        if let Some(expected) = self.expected_receipts_root {
            if expected != actual_receipts_root {
                return Err(anyhow::anyhow!(
                    "Receipts root mismatch: expected {} but got {}",
                    expected, actual_receipts_root
                ));
            }
        }
        
        Ok(())
    }
    
    /// Encode to bytes for inclusion in consensus messages
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| anyhow::anyhow!("Failed to encode metadata: {}", e))
    }
    
    /// Decode from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| anyhow::anyhow!("Failed to decode metadata: {}", e))
    }
}

/// Builder for creating canonical metadata with proper calculations
pub struct CanonicalMetadataBuilder {
    block_number: u64,
    parent_hash: B256,
    parent_gas_limit: u64,
    parent_gas_used: u64,
    parent_base_fee: u64,
}

impl CanonicalMetadataBuilder {
    pub fn new(block_number: u64, parent_hash: B256) -> Self {
        Self {
            block_number,
            parent_hash,
            parent_gas_limit: 134_217_728, // Default
            parent_gas_used: 0,
            parent_base_fee: 875_000_000,
        }
    }
    
    /// Set parent block info for proper gas calculations
    pub fn with_parent_info(mut self, gas_limit: u64, gas_used: u64, base_fee: u64) -> Self {
        self.parent_gas_limit = gas_limit;
        self.parent_gas_used = gas_used;
        self.parent_base_fee = base_fee;
        self
    }
    
    /// Build metadata with deterministic calculations
    pub fn build(self, round: u64, tx_hashes: Vec<B256>) -> CanonicalBlockMetadata {
        // Deterministic timestamp based on round
        let base_timestamp = 1700000000u64; // Fixed base
        let timestamp = base_timestamp + (round * 12); // 12 second blocks
        
        // Calculate base fee using EIP-1559
        let base_fee = self.calculate_base_fee();
        
        CanonicalBlockMetadata::new(
            self.block_number,
            self.parent_hash,
            timestamp,
            U256::from(base_fee),
            self.parent_gas_limit, // Keep parent's gas limit
            tx_hashes,
        )
    }
    
    /// Calculate base fee per EIP-1559
    fn calculate_base_fee(&self) -> u64 {
        use alloy_eips::eip1559::calc_next_block_base_fee;
        
        calc_next_block_base_fee(
            self.parent_gas_used,
            self.parent_gas_limit,
            self.parent_base_fee,
            alloy_eips::eip1559::BaseFeeParams::ethereum(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_serialization() {
        let metadata = CanonicalBlockMetadata::new(
            1,
            B256::ZERO,
            1700000012,
            U256::from(1_000_000_000u64),
            8_000_000,
            vec![],
        );
        
        let encoded = metadata.encode().unwrap();
        let decoded = CanonicalBlockMetadata::decode(&encoded).unwrap();
        
        assert_eq!(metadata, decoded);
    }
    
    #[test]
    fn test_deterministic_timestamp() {
        let builder = CanonicalMetadataBuilder::new(1, B256::ZERO);
        let metadata1 = builder.build(100, vec![]);
        
        let builder2 = CanonicalMetadataBuilder::new(1, B256::ZERO);
        let metadata2 = builder2.build(100, vec![]);
        
        assert_eq!(metadata1.timestamp, metadata2.timestamp);
        assert_eq!(metadata1.timestamp, 1700000000 + (100 * 12));
    }
}