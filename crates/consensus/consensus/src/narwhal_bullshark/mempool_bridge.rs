//! Mempool bridge traits for integrating transaction pools with Narwhal + Bullshark consensus

use crate::narwhal_bullshark::FinalizedBatch;
use reth_primitives::{TransactionSigned as RethTransaction};
use alloy_primitives::{Address, B256, U256, TxHash};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, error, debug};
use std::sync::Arc;
use std::collections::HashSet;
use anyhow::Result;

/// Trait for mempool operations that consensus system needs
#[async_trait::async_trait]
pub trait MempoolOperations: Send + Sync {
    /// Subscribe to new transactions from the mempool
    async fn subscribe_new_transactions(&self) -> Result<mpsc::UnboundedReceiver<RethTransaction>>;
    
    /// Remove finalized transactions from the mempool
    async fn remove_transactions(&self, tx_hashes: Vec<TxHash>) -> Result<usize>;
    
    /// Update the mempool with new block information
    async fn update_block_info(&self, block_number: u64, block_hash: B256, base_fee: u64) -> Result<()>;
    
    /// Get current mempool statistics
    fn get_pool_stats(&self) -> PoolStats;
}

/// Bridge that connects mempool operations with the consensus system
pub struct MempoolBridge {
    /// Mempool operations provider (injected at runtime)
    mempool_ops: Option<Box<dyn MempoolOperations>>,
    /// Channel to send transactions to consensus system
    consensus_tx_sender: mpsc::UnboundedSender<RethTransaction>,
    /// Channel to receive finalized batches from consensus
    finalized_batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    /// Current block number for tracking
    current_block_number: u64,
    /// Current parent hash
    current_parent_hash: B256,
    /// Recently processed transaction hashes to avoid duplicates
    processed_hashes: HashSet<TxHash>,
    /// Maximum processed hashes to keep in memory
    max_processed_history: usize,
}

impl MempoolBridge {
    /// Create a new mempool bridge without mempool operations (will be injected later)
    pub fn new(
        consensus_tx_sender: mpsc::UnboundedSender<RethTransaction>,
        finalized_batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    ) -> Self {
        Self {
            mempool_ops: None,
            consensus_tx_sender,
            finalized_batch_receiver,
            current_block_number: 1,
            current_parent_hash: B256::ZERO,
            processed_hashes: HashSet::new(),
            max_processed_history: 10000, // Keep track of last 10k transactions
        }
    }
    
    /// Set the mempool operations provider (dependency injection)
    pub fn set_mempool_operations(&mut self, mempool_ops: Box<dyn MempoolOperations>) {
        self.mempool_ops = Some(mempool_ops);
    }

    /// Start the mempool bridge (requires mempool operations to be set)
    pub fn spawn(mut self) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mempool_ops = self.mempool_ops.take()
            .ok_or_else(|| anyhow::anyhow!("Mempool operations not set - call set_mempool_operations() first"))?;
        
        let mut handles = Vec::new();

        // Spawn task to listen for new transactions from pool and forward to consensus
        let pool_to_consensus_handle = {
            let consensus_sender = self.consensus_tx_sender.clone();
            
            tokio::spawn(async move {
                Self::run_pool_to_consensus_bridge(mempool_ops, consensus_sender).await
            })
        };
        handles.push(pool_to_consensus_handle);

        // Spawn task to listen for finalized batches and update pool  
        let consensus_to_pool_handle = tokio::spawn(async move {
            self.run_consensus_to_pool_bridge().await
        });
        handles.push(consensus_to_pool_handle);

        Ok(handles)
    }

    /// Forward new transactions from pool to consensus system
    async fn run_pool_to_consensus_bridge(
        mempool_ops: Box<dyn MempoolOperations>,
        consensus_sender: mpsc::UnboundedSender<RethTransaction>,
    ) -> Result<()> {
        info!("Starting pool → consensus bridge");
        
        // Subscribe to new transactions from the mempool
        let mut new_tx_receiver = mempool_ops.subscribe_new_transactions().await?;
        let mut tx_count = 0u64;

        while let Some(transaction) = new_tx_receiver.recv().await {
            tx_count += 1;
            let tx_hash = transaction.tx_hash();
            
            debug!(
                "Forwarding transaction to consensus: {} (count: {})",
                tx_hash, tx_count
            );

            // Forward to consensus system
            if consensus_sender.send(transaction).is_err() {
                warn!("Consensus system is shutting down, stopping pool bridge");
                break;
            }

            // Log progress periodically
            if tx_count % 100 == 0 {
                info!("Forwarded {} transactions from pool to consensus", tx_count);
            }
        }

        info!("Pool → consensus bridge terminated");
        Ok(())
    }

    /// Update pool with finalized batches from consensus
    async fn run_consensus_to_pool_bridge(&mut self) -> Result<()> {
        info!("Starting consensus → pool bridge");
        let mut batch_count = 0u64;

        while let Some(batch) = self.finalized_batch_receiver.recv().await {
            batch_count += 1;
            
            info!(
                "Processing finalized batch {} with {} transactions (batch count: {})",
                batch.block_number, batch.transactions.len(), batch_count
            );

            // Process the finalized batch
            if let Err(e) = self.process_finalized_batch(batch).await {
                error!("Failed to process finalized batch: {}", e);
                // Continue processing other batches
            }
        }

        info!("Consensus → pool bridge terminated");
        Ok(())
    }

    /// Process a finalized batch by updating the pool
    async fn process_finalized_batch(&mut self, batch: FinalizedBatch) -> Result<()> {
        // Extract transaction hashes from the batch
        let finalized_tx_hashes: Vec<TxHash> = batch.transactions
            .iter()
            .map(|tx| *tx.tx_hash())
            .collect();

        debug!("Removing {} finalized transactions from pool", finalized_tx_hashes.len());
        
        // Remove finalized transactions from pool using mempool operations
        let removed_count = if let Some(ref mempool_ops) = self.mempool_ops {
            mempool_ops.remove_transactions(finalized_tx_hashes.clone()).await?
        } else {
            0
        };
        
        info!(
            "Removed {} transactions from pool for block {}",
            removed_count,
            batch.block_number
        );

        // Update block info in the pool using mempool operations
        if let Some(ref mempool_ops) = self.mempool_ops {
            let block_hash = self.calculate_block_hash(&batch);
            let base_fee = 1_000_000_000u64; // 1 gwei base fee (should be calculated)
            
            debug!("Updating pool block info to block {}", batch.block_number);
            mempool_ops.update_block_info(batch.block_number, block_hash, base_fee).await?;
        }

        // Update our tracking
        self.current_block_number = batch.block_number;
        self.current_parent_hash = batch.parent_hash;

        // Track processed transactions to avoid reprocessing
        for tx_hash in finalized_tx_hashes {
            self.processed_hashes.insert(tx_hash);
        }

        // Limit the size of processed history
        if self.processed_hashes.len() > self.max_processed_history {
            // Remove oldest entries (this is a simple approach, could be optimized)
            let excess = self.processed_hashes.len() - self.max_processed_history / 2;
            let hashes_to_remove: Vec<_> = self.processed_hashes.iter().take(excess).cloned().collect();
            for hash in hashes_to_remove {
                self.processed_hashes.remove(&hash);
            }
        }

        debug!("Finalized batch {} processed successfully", batch.block_number);
        Ok(())
    }

    /// Calculate block hash for a finalized batch
    /// TODO: This should compute the actual block hash based on the block header
    fn calculate_block_hash(&self, batch: &FinalizedBatch) -> B256 {
        // For now, use a simple hash based on block number and parent hash
        // In a real implementation, this would build the actual block header and hash it
        use alloy_primitives::keccak256;
        
        let mut data = Vec::new();
        data.extend_from_slice(&batch.block_number.to_be_bytes());
        data.extend_from_slice(batch.parent_hash.as_slice());
        data.extend_from_slice(&batch.timestamp.to_be_bytes());
        
        keccak256(data).into()
    }

    /// Get current pool statistics
    pub fn get_pool_stats(&self) -> PoolStats {
        if let Some(ref mempool_ops) = self.mempool_ops {
            mempool_ops.get_pool_stats()
        } else {
            // Return default stats if no mempool operations are set
            PoolStats {
                pending_transactions: 0,
                queued_transactions: 0,
                total_transactions: 0,
                current_block_number: self.current_block_number,
                current_block_hash: self.current_parent_hash,
                processed_hashes_count: self.processed_hashes.len(),
            }
        }
    }
}

/// Statistics about the mempool bridge
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Number of pending transactions in pool
    pub pending_transactions: usize,
    /// Number of queued transactions in pool
    pub queued_transactions: usize,
    /// Total transactions in pool
    pub total_transactions: usize,
    /// Current block number tracked by pool
    pub current_block_number: u64,
    /// Current block hash tracked by pool
    pub current_block_hash: B256,
    /// Number of processed transaction hashes being tracked
    pub processed_hashes_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_mempool_bridge_creation() {
        let (consensus_tx_sender, _consensus_tx_receiver) = mpsc::unbounded_channel();
        let (_finalized_batch_sender, finalized_batch_receiver) = mpsc::unbounded_channel();

        let bridge = MempoolBridge::new(
            consensus_tx_sender,
            finalized_batch_receiver,
        );

        // Bridge should start without mempool operations
        assert!(bridge.mempool_ops.is_none());
    }
} 