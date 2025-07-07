//! Transaction adapter for converting between Reth and Narwhal transaction formats

use reth_primitives::TransactionSigned;
use alloy_primitives::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, error};

/// Adapter for converting and routing transactions between Reth and Narwhal
pub struct TransactionAdapter {
    /// Channel for sending transactions to workers
    tx_sender: mpsc::UnboundedSender<Vec<u8>>,
}

impl TransactionAdapter {
    /// Create a new transaction adapter
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
        (Self { tx_sender }, tx_receiver)
    }

    /// Process a Reth transaction and forward to Narwhal
    pub fn process_transaction(&self, tx: TransactionSigned) -> anyhow::Result<()> {
        let encoded = encode_transaction(&tx)?;
        
        if self.tx_sender.send(encoded).is_err() {
            error!("Failed to send transaction to worker - channel closed");
            return Err(anyhow::anyhow!("Worker channel closed"));
        }
        
        debug!("Forwarded transaction {} to worker", tx.hash());
        Ok(())
    }
}

/// Encode a Reth transaction for Narwhal
pub fn encode_transaction(tx: &TransactionSigned) -> anyhow::Result<Vec<u8>> {
    // Use RLP encoding for compatibility
    Ok(alloy_rlp::encode(tx))
}

/// Decode a transaction from Narwhal format
pub fn decode_transaction(data: &[u8]) -> anyhow::Result<TransactionSigned> {
    use alloy_rlp::Decodable;
    TransactionSigned::decode(&mut &data[..])
        .map_err(|e| anyhow::anyhow!("Failed to decode transaction: {}", e))
}

/// Builder for creating transaction adapters with configuration
pub struct TransactionAdapterBuilder {
    buffer_size: Option<usize>,
}

impl TransactionAdapterBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { buffer_size: None }
    }

    /// Set the buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Build the adapter
    pub fn build(self) -> (TransactionAdapter, mpsc::UnboundedReceiver<Vec<u8>>) {
        TransactionAdapter::new()
    }
}