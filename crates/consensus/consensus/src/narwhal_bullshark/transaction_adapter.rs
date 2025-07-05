//! Transaction adapter for connecting Reth transaction pool to Narwhal workers
//! 
//! This component receives transactions from Reth's transaction pool and
//! distributes them to Narwhal workers for batching

use alloy_consensus::TxEnvelope;
use alloy_rlp::Encodable;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{debug, info, warn, error};

/// Adapter that forwards transactions to Narwhal workers
pub struct TransactionAdapter {
    /// Channel to receive encoded transactions
    rx_transactions: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Channels to send transactions to workers
    tx_to_workers: Vec<mpsc::UnboundedSender<narwhal::Transaction>>,
    /// Current worker index for round-robin distribution
    current_worker: std::sync::atomic::AtomicUsize,
}

impl TransactionAdapter {
    /// Create a new transaction adapter
    pub fn new(
        rx_transactions: mpsc::UnboundedReceiver<Vec<u8>>,
        tx_to_workers: Vec<mpsc::UnboundedSender<narwhal::Transaction>>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_to_workers,
            current_worker: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    
    /// Spawn the adapter task
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }
    
    /// Main run loop
    async fn run(mut self) {
        info!("TransactionAdapter started with {} workers", self.tx_to_workers.len());
        
        if self.tx_to_workers.is_empty() {
            error!("No workers configured, shutting down");
            return;
        }
        
        // Listen for transactions
        while let Some(tx_bytes) = self.rx_transactions.recv().await {
            info!("TransactionAdapter received transaction ({} bytes)", tx_bytes.len());
            
            // Convert to Narwhal transaction
            let narwhal_tx = narwhal::Transaction::from_bytes(tx_bytes);
            
            // Select worker using round-robin
            let num_workers = self.tx_to_workers.len();
            let worker_idx = self.current_worker.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % num_workers;
            
            // Send to selected worker
            if let Err(e) = self.tx_to_workers[worker_idx].send(narwhal_tx) {
                warn!("Failed to send to worker {}: {}", worker_idx, e);
            } else {
                info!("TransactionAdapter sent transaction to worker {}", worker_idx);
            }
        }
        
        info!("TransactionAdapter shutting down");
    }
}

/// Builder for creating transaction adapter with worker connections
pub struct TransactionAdapterBuilder {
    /// Channels to workers
    tx_to_workers: Vec<mpsc::UnboundedSender<narwhal::Transaction>>,
}

impl TransactionAdapterBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            tx_to_workers: Vec::new(),
        }
    }
    
    /// Add a worker channel
    pub fn add_worker(mut self, tx: mpsc::UnboundedSender<narwhal::Transaction>) -> Self {
        self.tx_to_workers.push(tx);
        self
    }
    
    /// Add multiple worker channels
    pub fn add_workers(mut self, txs: Vec<mpsc::UnboundedSender<narwhal::Transaction>>) -> Self {
        self.tx_to_workers.extend(txs);
        self
    }
    
    /// Build the adapter with a transaction receiver
    pub fn build(self) -> (TransactionAdapter, mpsc::UnboundedSender<Vec<u8>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let adapter = TransactionAdapter::new(rx, self.tx_to_workers);
        (adapter, tx)
    }
}

/// Helper to convert various transaction types to bytes
pub fn encode_transaction(tx: TxEnvelope) -> Vec<u8> {
    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    encoded
}