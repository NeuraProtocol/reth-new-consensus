//! Canonical hash tracker for multi-validator consensus
//! 
//! This module tracks block hashes from all validators and maintains
//! the canonical hash (from the leader) for each block number.

use alloy_primitives::{B256, Address};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{info, debug, warn};

/// Entry tracking all validator hashes for a block
#[derive(Debug, Clone)]
pub struct BlockHashEntry {
    /// Block number
    pub block_number: u64,
    /// Round number that produced this block
    pub round: u64,
    /// Leader's hash (canonical)
    pub canonical_hash: B256,
    /// Leader's address
    pub leader: Address,
    /// All validator hashes (address -> hash)
    pub validator_hashes: HashMap<Address, B256>,
}

/// Tracks canonical hashes across validators
#[derive(Debug, Clone)]
pub struct CanonicalHashTracker {
    /// Block number -> hash entry
    entries: Arc<RwLock<HashMap<u64, BlockHashEntry>>>,
}

impl CanonicalHashTracker {
    /// Create a new tracker
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Record a block hash from a validator
    pub fn record_validator_hash(
        &self,
        block_number: u64,
        round: u64,
        validator: Address,
        hash: B256,
        is_leader: bool,
    ) -> Result<()> {
        let mut entries = self.entries.write().unwrap();
        
        let entry = entries.entry(block_number).or_insert_with(|| {
            debug!("Creating new entry for block {}", block_number);
            BlockHashEntry {
                block_number,
                round,
                canonical_hash: B256::ZERO,
                leader: Address::ZERO,
                validator_hashes: HashMap::new(),
            }
        });
        
        // Record the validator's hash
        entry.validator_hashes.insert(validator, hash);
        
        // If this is the leader, mark as canonical
        if is_leader {
            entry.canonical_hash = hash;
            entry.leader = validator;
            info!(
                "Block {} canonical hash set to {} from leader {}",
                block_number, hash, validator
            );
        }
        
        debug!(
            "Recorded hash {} from validator {} for block {} (leader: {})",
            hash, validator, block_number, is_leader
        );
        
        Ok(())
    }
    
    /// Get the canonical hash for a block
    pub fn get_canonical_hash(&self, block_number: u64) -> Option<B256> {
        let entries = self.entries.read().unwrap();
        entries.get(&block_number).map(|e| e.canonical_hash)
    }
    
    /// Get all hashes for a block
    pub fn get_block_hashes(&self, block_number: u64) -> Option<BlockHashEntry> {
        let entries = self.entries.read().unwrap();
        entries.get(&block_number).cloned()
    }
    
    /// Check if all validators agree on a block hash
    pub fn check_consensus(&self, block_number: u64) -> (bool, usize, usize) {
        let entries = self.entries.read().unwrap();
        
        if let Some(entry) = entries.get(&block_number) {
            let canonical = entry.canonical_hash;
            let agreeing = entry.validator_hashes.values()
                .filter(|&&h| h == canonical)
                .count();
            let total = entry.validator_hashes.len();
            
            (agreeing == total && total > 0, agreeing, total)
        } else {
            (false, 0, 0)
        }
    }
    
    /// Get a summary of hash agreement for recent blocks
    pub fn get_consensus_summary(&self, last_n_blocks: u64) -> Vec<(u64, bool, usize, usize)> {
        let entries = self.entries.read().unwrap();
        let mut blocks: Vec<_> = entries.keys().cloned().collect();
        blocks.sort_by(|a, b| b.cmp(a)); // Sort descending
        
        blocks.into_iter()
            .take(last_n_blocks as usize)
            .map(|block_num| {
                let (agreed, agreeing, total) = self.check_consensus(block_num);
                (block_num, agreed, agreeing, total)
            })
            .collect()
    }
    
    /// Clean up old entries
    pub fn cleanup_old_entries(&self, keep_last_n: u64) {
        let mut entries = self.entries.write().unwrap();
        
        if entries.len() <= keep_last_n as usize {
            return;
        }
        
        let mut block_numbers: Vec<_> = entries.keys().cloned().collect();
        block_numbers.sort();
        
        let cutoff = block_numbers.len() - keep_last_n as usize;
        for &block_num in &block_numbers[..cutoff] {
            entries.remove(&block_num);
            debug!("Cleaned up hash entry for block {}", block_num);
        }
    }
}

/// Storage trait for persisting canonical hash mappings
pub trait CanonicalHashStorage: Send + Sync {
    /// Store a block hash entry
    fn store_hash_entry(&self, entry: &BlockHashEntry) -> Result<()>;
    
    /// Get hash entry for a block
    fn get_hash_entry(&self, block_number: u64) -> Result<Option<BlockHashEntry>>;
    
    /// Get canonical hash for a block
    fn get_canonical_hash(&self, block_number: u64) -> Result<Option<B256>>;
    
    /// Get latest entries
    fn get_latest_entries(&self, limit: u64) -> Result<Vec<BlockHashEntry>>;
}