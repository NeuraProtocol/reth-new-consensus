//! Batch maker for the Narwhal worker
//! 
//! This component collects transactions and assembles them into batches

use crate::{
    DagError, DagResult, Transaction, Batch, WorkerId,
    types::{Committee, Epoch},
    metrics_collector::{metrics, MetricTimer},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::{debug, info, warn};
use std::time::SystemTime;

/// Configuration for the batch maker
#[derive(Debug, Clone)]
pub struct BatchMakerConfig {
    /// Maximum batch size in bytes
    pub max_batch_size: usize,
    /// Maximum delay before sealing a batch
    pub max_batch_delay: Duration,
}

impl Default for BatchMakerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 500_000, // 500KB default
            max_batch_delay: Duration::from_millis(100),
        }
    }
}

/// Assembles transactions into batches
pub struct BatchMaker {
    /// Worker ID
    worker_id: WorkerId,
    /// Configuration
    config: BatchMakerConfig,
    /// Committee information
    committee: Committee,
    /// Channel to receive transactions
    rx_transaction: mpsc::UnboundedReceiver<Transaction>,
    /// Channel to send completed batches
    tx_batch: mpsc::UnboundedSender<Batch>,
    /// Channel for reconfiguration
    rx_reconfigure: watch::Receiver<Committee>,
    /// Current batch being assembled
    current_batch: Vec<Transaction>,
    /// Current batch size in bytes
    current_batch_size: usize,
}

impl BatchMaker {
    /// Create a new batch maker
    pub fn new(
        worker_id: WorkerId,
        config: BatchMakerConfig,
        committee: Committee,
        rx_transaction: mpsc::UnboundedReceiver<Transaction>,
        tx_batch: mpsc::UnboundedSender<Batch>,
        rx_reconfigure: watch::Receiver<Committee>,
    ) -> Self {
        Self {
            worker_id,
            config,
            committee,
            rx_transaction,
            tx_batch,
            rx_reconfigure,
            current_batch: Vec::new(),
            current_batch_size: 0,
        }
    }

    /// Spawn the batch maker task
    pub fn spawn(self) -> JoinHandle<DagResult<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }

    /// Main run loop
    async fn run(mut self) -> DagResult<()> {
        info!("BatchMaker for worker {} started", self.worker_id);
        
        let mut timer = sleep(self.config.max_batch_delay);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Receive new transaction
                Some(transaction) = self.rx_transaction.recv() => {
                    let tx_size = transaction.as_bytes().len();
                    
                    // Record metrics
                    if let Some(m) = metrics() {
                        m.record_transaction_received(&format!("worker_{}", self.worker_id));
                        m.set_transactions_in_flight("worker_batching", self.current_batch.len() as i64);
                    }
                    
                    // Check if adding this transaction would exceed max size
                    if self.current_batch_size + tx_size > self.config.max_batch_size && !self.current_batch.is_empty() {
                        // Seal current batch first
                        self.seal_batch().await?;
                        
                        // Reset timer
                        timer.as_mut().reset(Instant::now() + self.config.max_batch_delay);
                    }
                    
                    // Add transaction to batch
                    self.current_batch.push(transaction);
                    self.current_batch_size += tx_size;
                    
                    // Check if batch is full
                    if self.current_batch_size >= self.config.max_batch_size {
                        self.seal_batch().await?;
                        
                        // Reset timer
                        timer.as_mut().reset(Instant::now() + self.config.max_batch_delay);
                    }
                }
                
                // Timer expired - seal batch if not empty
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal_batch().await?;
                    }
                    
                    // Reset timer
                    timer.as_mut().reset(Instant::now() + self.config.max_batch_delay);
                }
                
                // Handle reconfiguration
                Ok(()) = self.rx_reconfigure.changed() => {
                    let new_committee = self.rx_reconfigure.borrow().clone();
                    info!("BatchMaker reconfiguring to epoch {}", new_committee.epoch);
                    self.committee = new_committee;
                    
                    // Seal any pending batch before reconfiguration
                    if !self.current_batch.is_empty() {
                        self.seal_batch().await?;
                    }
                }
                
                else => {
                    debug!("BatchMaker shutting down");
                    break;
                }
            }
        }
        
        Ok(())
    }

    /// Seal the current batch and send it
    async fn seal_batch(&mut self) -> DagResult<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }
        
        let _timer = metrics().map(|m| MetricTimer::new(
            m.batch_creation_duration.clone(), 
            vec![&self.worker_id.to_string()]
        ));
        
        let batch = Batch(std::mem::take(&mut self.current_batch));
        let batch_size = self.current_batch_size;
        let transaction_count = batch.0.len();
        self.current_batch_size = 0;
        
        debug!(
            "Worker {} sealed batch with {} transactions ({} bytes)",
            self.worker_id,
            transaction_count,
            batch_size
        );
        
        // Record metrics
        if let Some(m) = metrics() {
            m.record_transactions_batched(&self.worker_id.to_string(), transaction_count as u64);
            m.record_batch_size(&self.worker_id.to_string(), transaction_count as f64);
        }
        
        // Send batch to the next stage
        if let Err(e) = self.tx_batch.send(batch) {
            warn!("Failed to send batch: {}", e);
            return Err(DagError::ShuttingDown);
        }
        
        Ok(())
    }
}