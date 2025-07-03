//! RPC handlers for worker services
//!
//! This module implements the server-side handlers for both WorkerToWorker
//! and PrimaryToWorker RPC services.

use crate::{
    Batch, BatchDigest, DagError, DagResult,
    rpc::{
        WorkerMessage, WorkerBatchRequest, WorkerBatchResponse,
        PrimaryWorkerMessage, WorkerSynchronizeMessage,
        RequestBatchRequest, RequestBatchResponse,
    },
    storage_trait::BatchStore,
};
use fastcrypto::Hash;
use async_trait::async_trait;
use anemo::{Request, Response};
use tokio::sync::mpsc;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Handler for WorkerToWorker RPC service
#[derive(Clone)]
pub struct WorkerReceiverHandler {
    /// Channel to send batches to the processor
    pub tx_processor: mpsc::UnboundedSender<Batch>,
    /// Batch storage
    pub store: Arc<dyn BatchStore>,
}

impl WorkerReceiverHandler {
    /// Create a new handler
    pub fn new(
        tx_processor: mpsc::UnboundedSender<Batch>,
        store: Arc<dyn BatchStore>,
    ) -> Self {
        Self {
            tx_processor,
            store,
        }
    }
}

#[async_trait]
impl crate::rpc::worker::worker_to_worker_server::WorkerToWorker for WorkerReceiverHandler {
    /// Handle incoming batch from another worker
    async fn send_message(
        &self,
        request: Request<WorkerMessage>,
    ) -> Result<Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        
        match message {
            WorkerMessage::Batch(batch) => {
                let digest = batch.digest();
                debug!("Received batch {} from another worker", digest);
                
                // Store the batch
                if let Err(e) = self.store.write_batch(&digest, &batch).await {
                    warn!("Failed to store batch {}: {:?}", digest, e);
                    return Err(anemo::rpc::Status::internal(format!("Storage error: {}", e)));
                }
                
                // Forward to processor
                if self.tx_processor.send(batch).is_err() {
                    return Err(anemo::rpc::Status::internal("Processor channel closed"));
                }
                
                Ok(Response::new(()))
            }
        }
    }
    
    /// Handle batch request from another worker
    async fn request_batches(
        &self,
        request: Request<WorkerBatchRequest>,
    ) -> Result<Response<WorkerBatchResponse>, anemo::rpc::Status> {
        let message = request.into_body();
        debug!("Received request for {} batches", message.digests.len());
        
        let mut batches = Vec::new();
        for digest in message.digests {
            match self.store.read_batch(&digest).await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => {
                    debug!("Batch {} not found in store", digest);
                }
                Err(e) => {
                    warn!("Error reading batch {}: {:?}", digest, e);
                }
            }
        }
        
        debug!("Returning {} batches", batches.len());
        Ok(Response::new(WorkerBatchResponse { batches }))
    }
}

/// Handler for PrimaryToWorker RPC service
#[derive(Clone)]
pub struct PrimaryReceiverHandler {
    /// Worker ID
    pub worker_id: u32,
    /// Batch storage
    pub store: Arc<dyn BatchStore>,
    /// Channel to synchronizer
    pub tx_synchronizer: Option<mpsc::UnboundedSender<WorkerSynchronizeMessage>>,
    /// Channel to primary for batch requests
    pub tx_primary: Option<mpsc::UnboundedSender<(BatchDigest, u32)>>,
}

impl PrimaryReceiverHandler {
    /// Create a new handler
    pub fn new(
        worker_id: u32,
        store: Arc<dyn BatchStore>,
        tx_synchronizer: Option<mpsc::UnboundedSender<WorkerSynchronizeMessage>>,
        tx_primary: Option<mpsc::UnboundedSender<(BatchDigest, u32)>>,
    ) -> Self {
        Self {
            worker_id,
            store,
            tx_synchronizer,
            tx_primary,
        }
    }
}

#[async_trait]
impl crate::rpc::primary::primary_to_worker_server::PrimaryToWorker for PrimaryReceiverHandler {
    /// Handle message from primary
    async fn send_message(
        &self,
        request: Request<PrimaryWorkerMessage>,
    ) -> Result<Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        
        match message {
            PrimaryWorkerMessage::DeleteBatches(digests) => {
                info!("Primary requested deletion of {} batches", digests.len());
                
                for digest in digests {
                    if let Err(e) = self.store.delete_batch(&digest).await {
                        warn!("Failed to delete batch {}: {:?}", digest, e);
                    }
                }
                
                Ok(Response::new(()))
            }
        }
    }
    
    /// Handle synchronization request from primary
    async fn synchronize(
        &self,
        request: Request<WorkerSynchronizeMessage>,
    ) -> Result<Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        info!("Primary requested synchronization of {} batches", message.digests.len());
        
        // Forward to synchronizer if available
        if let Some(ref tx) = self.tx_synchronizer {
            if tx.send(message).is_err() {
                return Err(anemo::rpc::Status::internal("Synchronizer channel closed"));
            }
        } else {
            warn!("No synchronizer channel configured");
        }
        
        Ok(Response::new(()))
    }
    
    /// Handle batch request from primary
    async fn request_batch(
        &self,
        request: Request<RequestBatchRequest>,
    ) -> Result<Response<RequestBatchResponse>, anemo::rpc::Status> {
        let message = request.into_body();
        debug!("Primary requested batch {}", message.digest);
        
        match self.store.read_batch(&message.digest).await {
            Ok(batch) => {
                if batch.is_none() {
                    debug!("Batch {} not found in store", message.digest);
                }
                Ok(Response::new(RequestBatchResponse { batch }))
            }
            Err(e) => {
                warn!("Error reading batch {}: {:?}", message.digest, e);
                Err(anemo::rpc::Status::internal(format!("Storage error: {}", e)))
            }
        }
    }
}