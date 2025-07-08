//! Bridge between Reth's transaction pool and Narwhal+Bullshark consensus

use reth_transaction_pool::{TransactionPool, TransactionPoolExt, PoolTransaction};
use reth_primitives::TransactionSigned;
use tokio::sync::mpsc;
use tracing::{info, debug, warn};
use std::sync::Arc;

/// Bridge that forwards transactions from Reth's pool to consensus
pub struct MempoolBridge<Pool> {
    pool: Arc<Pool>,
}

impl<Pool> MempoolBridge<Pool>
where
    Pool: TransactionPool + TransactionPoolExt + Send + Sync + 'static,
{
    /// Create a new mempool bridge
    pub fn new(pool: Arc<Pool>) -> Self {
        Self {
            pool,
        }
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
                // to_consensus() returns Recovered<TransactionSigned>
                // We need to extract the TransactionSigned from the Recovered wrapper
                let recovered_tx = tx.to_consensus();
                let (tx_signed, _signer) = recovered_tx.into_parts();
                if tx_sender.send(tx_signed).is_err() {
                    warn!("Failed to forward transaction - consensus stopped");
                    return;
                }
            }

            // Then listen for new transactions
            let mut new_txs = pool.new_transactions_listener();
            
            while let Some(event) = new_txs.recv().await {
                debug!("New transaction in pool: {}", event.transaction.hash());
                
                // to_consensus() returns Recovered<TransactionSigned>
                // We need to extract the TransactionSigned from the Recovered wrapper
                let recovered_tx = event.transaction.to_consensus();
                let (tx_signed, _signer) = recovered_tx.into_parts();
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