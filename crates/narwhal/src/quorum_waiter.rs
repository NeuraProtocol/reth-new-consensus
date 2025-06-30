//! Quorum waiter for the Narwhal worker
//! 
//! This component waits for acknowledgment that batches have been
//! replicated to enough other workers before notifying the primary

use crate::{
    DagError, DagResult, Batch, BatchDigest, WorkerId,
    types::{Committee, PublicKey},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{timeout, Duration},
};
use tracing::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use fastcrypto::Hash;

/// Message from other workers acknowledging a batch
#[derive(Debug, Clone)]
pub struct BatchAck {
    /// The batch digest being acknowledged
    pub digest: BatchDigest,
    /// The worker that sent the acknowledgment
    pub from_worker: WorkerId,
    /// The primary that owns the worker
    pub from_primary: PublicKey,
}

/// Waits for quorum of batch acknowledgments
pub struct QuorumWaiter {
    /// Worker ID
    worker_id: WorkerId,
    /// Primary name this worker belongs to
    primary_name: PublicKey,
    /// Committee information
    committee: Committee,
    /// Channel to receive batches from batch maker
    rx_batch: mpsc::UnboundedReceiver<Batch>,
    /// Channel to send batch digests to primary
    tx_primary: mpsc::UnboundedSender<(BatchDigest, WorkerId)>,
    /// Channel to receive batch acknowledgments
    rx_batch_ack: mpsc::UnboundedReceiver<BatchAck>,
    /// Channel for reconfiguration
    rx_reconfigure: watch::Receiver<Committee>,
    /// Pending batches waiting for quorum
    pending_batches: HashMap<BatchDigest, PendingBatch>,
    /// Timeout for batch acknowledgment
    batch_timeout: Duration,
}

struct PendingBatch {
    /// The batch itself
    batch: Batch,
    /// Workers that have acknowledged
    acknowledged_workers: HashSet<(PublicKey, WorkerId)>,
    /// Total stake of acknowledging primaries
    acknowledged_stake: u64,
}

impl QuorumWaiter {
    /// Create a new quorum waiter
    pub fn new(
        worker_id: WorkerId,
        primary_name: PublicKey,
        committee: Committee,
        rx_batch: mpsc::UnboundedReceiver<Batch>,
        tx_primary: mpsc::UnboundedSender<(BatchDigest, WorkerId)>,
        rx_batch_ack: mpsc::UnboundedReceiver<BatchAck>,
        rx_reconfigure: watch::Receiver<Committee>,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            worker_id,
            primary_name,
            committee,
            rx_batch,
            tx_primary,
            rx_batch_ack,
            rx_reconfigure,
            pending_batches: HashMap::new(),
            batch_timeout,
        }
    }

    /// Spawn the quorum waiter task
    pub fn spawn(self) -> JoinHandle<DagResult<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }

    /// Main run loop
    async fn run(mut self) -> DagResult<()> {
        info!("QuorumWaiter for worker {} started", self.worker_id);

        loop {
            tokio::select! {
                // Receive new batch from batch maker
                Some(batch) = self.rx_batch.recv() => {
                    self.handle_new_batch(batch).await?;
                }
                
                // Receive batch acknowledgment from other workers
                Some(ack) = self.rx_batch_ack.recv() => {
                    self.handle_batch_ack(ack).await?;
                }
                
                // Handle reconfiguration
                Ok(()) = self.rx_reconfigure.changed() => {
                    let new_committee = self.rx_reconfigure.borrow().clone();
                    info!("QuorumWaiter reconfiguring to epoch {}", new_committee.epoch);
                    self.committee = new_committee;
                    
                    // Clear pending batches on reconfiguration
                    self.pending_batches.clear();
                }
                
                else => {
                    debug!("QuorumWaiter shutting down");
                    break;
                }
            }
        }
        
        Ok(())
    }

    /// Handle a new batch from the batch maker
    async fn handle_new_batch(&mut self, batch: Batch) -> DagResult<()> {
        let digest = batch.digest();
        
        debug!(
            "Worker {} handling new batch {} with {} transactions",
            self.worker_id,
            digest,
            batch.0.len()
        );
        
        // Create pending batch entry
        let pending = PendingBatch {
            batch: batch.clone(),
            acknowledged_workers: HashSet::new(),
            acknowledged_stake: 0,
        };
        
        self.pending_batches.insert(digest, pending);
        
        // TODO: Broadcast batch to other workers
        // For now, we'll simulate immediate local acknowledgment
        let local_ack = BatchAck {
            digest,
            from_worker: self.worker_id,
            from_primary: self.primary_name.clone(),
        };
        self.handle_batch_ack(local_ack).await?;
        
        Ok(())
    }
    
    /// Handle batch acknowledgment from a worker
    async fn handle_batch_ack(&mut self, ack: BatchAck) -> DagResult<()> {
        let pending = match self.pending_batches.get_mut(&ack.digest) {
            Some(p) => p,
            None => {
                // Batch not found - might have already achieved quorum
                return Ok(());
            }
        };
        
        // Check if we already have ack from this worker
        let worker_key = (ack.from_primary.clone(), ack.from_worker);
        if !pending.acknowledged_workers.insert(worker_key) {
            // Already acknowledged by this worker
            return Ok(());
        }
        
        // Update stake
        let stake = self.committee.stake(&ack.from_primary);
        pending.acknowledged_stake += stake;
        
        debug!(
            "Worker {} received ack for batch {} from worker {}/{} (total stake: {})",
            self.worker_id, ack.digest, ack.from_primary, ack.from_worker, pending.acknowledged_stake
        );
        
        // Check if we have quorum
        if pending.acknowledged_stake >= self.committee.validity_threshold() {
            info!(
                "Worker {} achieved quorum for batch {} with stake {}",
                self.worker_id, ack.digest, pending.acknowledged_stake
            );
            
            // Send digest to primary
            if let Err(e) = self.tx_primary.send((ack.digest, self.worker_id)) {
                warn!("Failed to send batch digest to primary: {}", e);
                return Err(DagError::ShuttingDown);
            }
            
            // Remove from pending
            self.pending_batches.remove(&ack.digest);
        }
        
        Ok(())
    }
}