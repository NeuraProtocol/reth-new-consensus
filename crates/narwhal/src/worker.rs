//! Worker node implementation for Narwhal
//! 
//! The worker is responsible for:
//! - Receiving transactions from clients
//! - Creating batches of transactions
//! - Replicating batches to other workers
//! - Notifying the primary of batch digests

use crate::{
    NarwhalConfig, WorkerId, types::*, Transaction, Batch, BatchDigest,
    batch_maker::{BatchMaker, BatchMakerConfig},
    quorum_waiter::{QuorumWaiter, BatchAck},
    DagError, DagResult,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{info, debug};
use std::time::Duration;

/// Channels for worker components
pub struct WorkerChannels {
    /// Send transactions to the worker
    pub tx_transaction: mpsc::UnboundedSender<Transaction>,
    /// Receive batch digests from the worker
    pub rx_batch_digest: mpsc::UnboundedReceiver<(BatchDigest, WorkerId)>,
    /// Send batch acknowledgments to the worker
    pub tx_batch_ack: mpsc::UnboundedSender<BatchAck>,
    /// Committee updates
    pub tx_reconfigure: watch::Sender<Committee>,
}

impl WorkerChannels {
    /// Create new worker channels
    pub fn new(committee: Committee) -> (Self, WorkerChannelReceivers) {
        let (tx_transaction, rx_transaction) = mpsc::unbounded_channel();
        let (tx_batch, rx_batch) = mpsc::unbounded_channel();
        let (tx_batch_digest, rx_batch_digest) = mpsc::unbounded_channel();
        let (tx_batch_ack, rx_batch_ack) = mpsc::unbounded_channel();
        let (tx_reconfigure, rx_reconfigure) = watch::channel(committee);
        
        let channels = Self {
            tx_transaction,
            rx_batch_digest,
            tx_batch_ack,
            tx_reconfigure,
        };
        
        let receivers = WorkerChannelReceivers {
            rx_transaction,
            tx_batch,
            rx_batch,
            tx_batch_digest,
            rx_batch_ack,
            rx_reconfigure,
        };
        
        (channels, receivers)
    }
}

/// Internal channel receivers for worker components
struct WorkerChannelReceivers {
    rx_transaction: mpsc::UnboundedReceiver<Transaction>,
    tx_batch: mpsc::UnboundedSender<Batch>,
    rx_batch: mpsc::UnboundedReceiver<Batch>,
    tx_batch_digest: mpsc::UnboundedSender<(BatchDigest, WorkerId)>,
    rx_batch_ack: mpsc::UnboundedReceiver<BatchAck>,
    rx_reconfigure: watch::Receiver<Committee>,
}

/// A worker node in the Narwhal network
#[derive(Debug)]
pub struct Worker {
    /// Primary's public key this worker belongs to
    pub primary_name: PublicKey,
    /// Worker ID
    pub id: WorkerId,
    /// Committee information
    pub committee: Committee,
    /// Configuration
    pub config: NarwhalConfig,
    /// Batch maker configuration
    pub batch_config: BatchMakerConfig,
}

impl Worker {
    /// Create a new worker node
    pub fn new(
        primary_name: PublicKey,
        id: WorkerId,
        committee: Committee,
        config: NarwhalConfig,
    ) -> Self {
        let batch_config = BatchMakerConfig {
            max_batch_size: config.max_batch_size,
            max_batch_delay: config.max_batch_delay,
        };
        
        Self {
            primary_name,
            id,
            committee,
            config,
            batch_config,
        }
    }

    /// Spawn the worker node with the provided channels
    pub fn spawn(self, channels: WorkerChannelReceivers) -> Vec<JoinHandle<DagResult<()>>> {
        info!(
            "Starting Narwhal Worker {} for primary {}",
            self.id, self.primary_name
        );
        
        let mut handles = vec![];
        
        // Spawn batch maker
        let batch_maker = BatchMaker::new(
            self.id,
            self.batch_config.clone(),
            self.committee.clone(),
            channels.rx_transaction,
            channels.tx_batch,
            channels.rx_reconfigure.clone(),
        );
        handles.push(batch_maker.spawn());
        
        // Spawn quorum waiter
        let quorum_waiter = QuorumWaiter::new(
            self.id,
            self.primary_name.clone(),
            self.committee.clone(),
            channels.rx_batch,
            channels.tx_batch_digest,
            channels.rx_batch_ack,
            channels.rx_reconfigure,
            Duration::from_secs(10), // TODO: Make configurable
        );
        handles.push(quorum_waiter.spawn());
        
        // TODO: Spawn additional components:
        // - Batch synchronizer
        // - Network handler
        // - Storage manager
        
        info!(
            "Worker {} spawned {} tasks",
            self.id,
            handles.len()
        );
        
        handles
    }
    
    /// Create channels and spawn the worker
    pub fn create_and_spawn(self) -> (WorkerChannels, Vec<JoinHandle<DagResult<()>>>) {
        let (channels, receivers) = WorkerChannels::new(self.committee.clone());
        let handles = self.spawn(receivers);
        (channels, handles)
    }
}