//! Worker network implementation for batch replication
//!
//! This module provides the networking layer for worker-to-worker
//! and primary-to-worker communication.

use crate::{
    Batch, BatchDigest, DagError, DagResult, WorkerId,
    rpc::{WorkerMessage, WorkerBatchRequest, WorkerBatchResponse},
    types::PublicKey,
};
use fastcrypto::Hash;
use anemo::{Network, PeerId, Request, Response};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Information about a worker's network addresses
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// The primary this worker belongs to
    pub primary: PublicKey,
    /// Worker ID within the primary
    pub worker_id: WorkerId,
    /// Network address for worker-to-worker communication
    pub worker_address: SocketAddr,
}

/// Worker network handle for batch replication
#[derive(Clone)]
pub struct WorkerNetwork {
    /// Our worker info
    our_info: WorkerInfo,
    /// Anemo network instance
    network: Network,
    /// Mapping from (primary, worker_id) to peer ID
    peer_map: Arc<RwLock<HashMap<(PublicKey, WorkerId), PeerId>>>,
    /// Worker information registry
    worker_registry: Arc<RwLock<HashMap<(PublicKey, WorkerId), WorkerInfo>>>,
}

impl WorkerNetwork {
    /// Create a new worker network
    pub fn new(
        our_info: WorkerInfo,
        network: Network,
    ) -> Self {
        Self {
            our_info,
            network,
            peer_map: Arc::new(RwLock::new(HashMap::new())),
            worker_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Register worker information
    pub async fn register_worker(&self, info: WorkerInfo) -> DagResult<()> {
        let key = (info.primary.clone(), info.worker_id);
        self.worker_registry.write().await.insert(key, info);
        Ok(())
    }
    
    /// Connect to a worker
    pub async fn connect_to_worker(&self, info: &WorkerInfo) -> DagResult<PeerId> {
        info!("Connecting to worker {} of primary {}", info.worker_id, info.primary);
        
        match self.network.connect(info.worker_address).await {
            Ok(peer_id) => {
                let key = (info.primary.clone(), info.worker_id);
                self.peer_map.write().await.insert(key, peer_id);
                info!("Connected to worker at {} with peer ID {}", info.worker_address, peer_id);
                Ok(peer_id)
            }
            Err(e) => {
                warn!("Failed to connect to worker at {}: {}", info.worker_address, e);
                Err(DagError::Network(format!("Connection failed: {}", e)))
            }
        }
    }
    
    /// Send a batch to another worker
    pub async fn send_batch_to_worker(
        &self,
        target_primary: &PublicKey,
        target_worker: WorkerId,
        batch: Batch,
    ) -> DagResult<()> {
        let digest = batch.digest();
        debug!("Sending batch {} to worker {} of {}", digest, target_worker, target_primary);
        
        // Get peer ID for the target worker
        let peer_id = {
            let peer_map = self.peer_map.read().await;
            peer_map.get(&(target_primary.clone(), target_worker)).copied()
        };
        
        let peer_id = match peer_id {
            Some(id) => id,
            None => {
                // Try to connect if not already connected
                let registry = self.worker_registry.read().await;
                let info = registry.get(&(target_primary.clone(), target_worker))
                    .ok_or_else(|| DagError::Network(format!(
                        "No info for worker {} of {}", target_worker, target_primary
                    )))?;
                self.connect_to_worker(info).await?
            }
        };
        
        // Send the batch
        if let Some(peer) = self.network.peer(peer_id) {
            let mut client = crate::rpc::worker::worker_to_worker_client::WorkerToWorkerClient::new(peer);
            let message = WorkerMessage::Batch(batch);
            
            match client.send_message(message).await {
                Ok(_) => {
                    debug!("Successfully sent batch {} to worker", digest);
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to send batch {} to worker: {:?}", digest, e);
                    Err(DagError::Network(format!("RPC failed: {:?}", e)))
                }
            }
        } else {
            Err(DagError::Network("Peer not found".to_string()))
        }
    }
    
    /// Broadcast a batch to multiple workers
    pub async fn broadcast_batch(
        &self,
        targets: Vec<(PublicKey, WorkerId)>,
        batch: Batch,
    ) -> Vec<DagResult<()>> {
        let mut results = Vec::new();
        
        for (primary, worker_id) in targets {
            let batch_clone = batch.clone();
            let result = self.send_batch_to_worker(&primary, worker_id, batch_clone).await;
            results.push(result);
        }
        
        results
    }
    
    /// Request batches from another worker
    pub async fn request_batches_from_worker(
        &self,
        target_primary: &PublicKey,
        target_worker: WorkerId,
        digests: Vec<BatchDigest>,
    ) -> DagResult<Vec<Batch>> {
        debug!("Requesting {} batches from worker {} of {}", 
               digests.len(), target_worker, target_primary);
        
        // Get peer ID for the target worker
        let peer_id = {
            let peer_map = self.peer_map.read().await;
            peer_map.get(&(target_primary.clone(), target_worker)).copied()
        };
        
        let peer_id = match peer_id {
            Some(id) => id,
            None => {
                // Try to connect if not already connected
                let registry = self.worker_registry.read().await;
                let info = registry.get(&(target_primary.clone(), target_worker))
                    .ok_or_else(|| DagError::Network(format!(
                        "No info for worker {} of {}", target_worker, target_primary
                    )))?;
                self.connect_to_worker(info).await?
            }
        };
        
        // Request the batches
        if let Some(peer) = self.network.peer(peer_id) {
            let mut client = crate::rpc::worker::worker_to_worker_client::WorkerToWorkerClient::new(peer);
            let request = WorkerBatchRequest { digests };
            
            match client.request_batches(request).await {
                Ok(response) => {
                    let batches = response.into_inner().batches;
                    debug!("Received {} batches from worker", batches.len());
                    Ok(batches)
                }
                Err(e) => {
                    warn!("Failed to request batches from worker: {:?}", e);
                    Err(DagError::Network(format!("RPC failed: {:?}", e)))
                }
            }
        } else {
            Err(DagError::Network("Peer not found".to_string()))
        }
    }
    
    /// Get network statistics
    pub async fn get_stats(&self) -> WorkerNetworkStats {
        WorkerNetworkStats {
            connected_workers: self.peer_map.read().await.len(),
            known_workers: self.worker_registry.read().await.len(),
            our_worker_id: self.our_info.worker_id,
            our_primary: self.our_info.primary.clone(),
        }
    }
}

/// Worker network statistics
#[derive(Debug, Clone)]
pub struct WorkerNetworkStats {
    pub connected_workers: usize,
    pub known_workers: usize,
    pub our_worker_id: WorkerId,
    pub our_primary: PublicKey,
}