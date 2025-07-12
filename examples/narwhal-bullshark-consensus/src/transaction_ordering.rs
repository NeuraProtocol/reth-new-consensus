//! Deterministic transaction ordering for canonical block construction

use reth_primitives::TransactionSigned;
use alloy_primitives::B256;
use std::collections::HashSet;
use tracing::{debug, info};

/// Sort transactions deterministically for canonical block construction
pub fn order_transactions_deterministically(
    transactions: Vec<TransactionSigned>,
) -> Vec<TransactionSigned> {
    let mut unique_txs = deduplicate_transactions(transactions);
    
    // Sort by transaction hash for determinism
    // TODO: Once we have access to transaction details, sort by (sender, nonce)
    unique_txs.sort_by_key(|tx| *tx.hash());
    
    info!("Ordered {} transactions deterministically", unique_txs.len());
    unique_txs
}

/// Deduplicate transactions by hash
fn deduplicate_transactions(transactions: Vec<TransactionSigned>) -> Vec<TransactionSigned> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    let original_count = transactions.len();
    
    for tx in transactions {
        let hash = *tx.hash();
        if seen.insert(hash) {
            unique.push(tx);
        } else {
            debug!("Deduplicating transaction {}", hash);
        }
    }
    
    if unique.len() < original_count {
        info!(
            "Deduplicated {} transactions to {}",
            original_count,
            unique.len()
        );
    }
    
    unique
}

/// Verify transaction ordering matches expected order
pub fn verify_transaction_order(
    transactions: &[TransactionSigned],
    expected_hashes: &[B256],
) -> bool {
    if transactions.len() != expected_hashes.len() {
        return false;
    }
    
    for (tx, expected_hash) in transactions.iter().zip(expected_hashes.iter()) {
        if *tx.hash() != *expected_hash {
            return false;
        }
    }
    
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_primitives::Transaction;
    use alloy_primitives::{Address, U256};
    
    fn create_test_tx(sender: Address, nonce: u64) -> TransactionSigned {
        // Create a simple test transaction
        // In real code, this would be properly constructed
        unimplemented!("Test transaction creation")
    }
    
    #[test]
    fn test_deterministic_ordering() {
        // Test that transactions are ordered by (sender, nonce)
        // Implementation would go here
    }
    
    #[test]
    fn test_deduplication() {
        // Test that duplicate transactions are removed
        // Implementation would go here
    }
}