//! Bridge between Reth's transaction pool and Narwhal+Bullshark consensus

use reth_transaction_pool::{TransactionPool, TransactionPoolExt, PoolTransaction};
use reth_primitives::TransactionSigned;
use tokio::sync::mpsc;
use futures::StreamExt;
use tracing::{info, debug, warn};
use std::sync::Arc;

/// Operations for interacting with mempool (compatibility trait from pre-move version)
pub trait MempoolOperations: Send + Sync {
    /// Remove transactions from the mempool
    fn remove_transactions(&self, tx_hashes: Vec<alloy_primitives::TxHash>);
    /// Get pending transaction count
    fn pending_count(&self) -> usize;
}

/// Bridge that forwards transactions from Reth's pool to consensus
pub struct MempoolBridge<Pool> {
    pool: Arc<Pool>,
    tx_sender: mpsc::UnboundedSender<TransactionSigned>,
}

impl<Pool> MempoolBridge<Pool>
where
    Pool: TransactionPool + TransactionPoolExt + Send + Sync + 'static,
{
    /// Create a new mempool bridge
    pub fn new(pool: Arc<Pool>) -> (Self, mpsc::UnboundedReceiver<TransactionSigned>) {
        let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
        
        Self {
            pool,
            tx_sender,
        }
        .into_parts()
    }

    /// Split into bridge and receiver
    fn into_parts(self) -> (Self, mpsc::UnboundedReceiver<TransactionSigned>) {
        let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
        
        let bridge = Self {
            pool: self.pool,
            tx_sender,
        };
        
        (bridge, tx_receiver)
    }

    /// Start the bridge
    pub fn start(self) -> mpsc::UnboundedReceiver<<Pool::Transaction as PoolTransaction>::Consensus> {
        let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
        let pool = self.pool;

        // Spawn task to forward transactions
        tokio::spawn(async move {
            // First, forward all existing pending transactions
            let pending = pool.pending_transactions();
            info!("Forwarding {} existing pending transactions to consensus", pending.len());
            
            for tx in pending {
                // Extract the TransactionSigned from the ValidPoolTransaction
                // Use to_consensus() to get Recovered<Consensus> then extract the signed transaction
                let tx_signed = tx.to_consensus().into_inner();
                if tx_sender.send(tx_signed).is_err() {
                    warn!("Failed to forward transaction - consensus stopped");
                    return;
                }
            }

            // Then listen for new transactions
            let mut new_txs = pool.new_transactions_listener();
            
            // Use while let with recv() for mpsc receiver
            while let Some(event) = new_txs.recv().await {
                debug!("New transaction in pool: {}", event.transaction.hash());
                
                // Extract the TransactionSigned from the pool transaction
                // Use to_consensus() to get Recovered<Consensus> then extract the signed transaction
                let tx_signed = event.transaction.to_consensus().into_inner();
                if tx_sender.send(tx_signed).is_err() {
                    warn!("Failed to forward transaction - consensus stopped");
                    break;
                }
            }
        });

        tx_receiver
    }

    /// Remove transactions from the pool after they're included in a block
    pub async fn remove_transactions(&self, tx_hashes: &[alloy_primitives::TxHash]) {
        if !tx_hashes.is_empty() {
            info!("Removing {} transactions from pool after block inclusion", tx_hashes.len());
            self.pool.remove_transactions(tx_hashes.to_vec());
        }
    }
}

impl<Pool> MempoolOperations for MempoolBridge<Pool>
where
    Pool: TransactionPool + TransactionPoolExt + Send + Sync + 'static,
{
    fn remove_transactions(&self, tx_hashes: Vec<alloy_primitives::TxHash>) {
        self.pool.remove_transactions(tx_hashes);
    }
    
    fn pending_count(&self) -> usize {
        self.pool.pending_transactions().len()
    }
}