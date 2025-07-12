//! Canonical metadata injection for deterministic block construction

use crate::{BullsharkResult, Round};
use alloy_primitives::B256;
use narwhal::types::PublicKey;

/// Trait for injecting canonical metadata during consensus
pub trait CanonicalMetadataInjector: Send + Sync {
    /// Check if we are the leader for this round
    fn is_leader(&self, round: Round, committee_size: usize) -> bool;
    
    /// Create canonical metadata for a round
    fn create_canonical_metadata(
        &self,
        round: Round,
        block_number: u64,
        parent_hash: B256,
        transaction_hashes: Vec<B256>,
    ) -> BullsharkResult<Vec<u8>>;
    
    /// Get our public key
    fn our_public_key(&self) -> &PublicKey;
}

/// Default implementation that doesn't inject metadata
pub struct NoOpMetadataInjector;

impl CanonicalMetadataInjector for NoOpMetadataInjector {
    fn is_leader(&self, _round: Round, _committee_size: usize) -> bool {
        false
    }
    
    fn create_canonical_metadata(
        &self,
        _round: Round,
        _block_number: u64,
        _parent_hash: B256,
        _transaction_hashes: Vec<B256>,
    ) -> BullsharkResult<Vec<u8>> {
        Ok(vec![])
    }
    
    fn our_public_key(&self) -> &PublicKey {
        panic!("NoOpMetadataInjector doesn't have a public key")
    }
}