//! Worker cache for managing worker information across the network

use crate::{WorkerId, types::PublicKey, Epoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use arc_swap::ArcSwap;

/// Information about a worker node
#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq, Debug)]
pub struct WorkerInfo {
    /// The network public key of this worker
    pub name: PublicKey,
    /// Address to receive client transactions
    pub transactions_address: SocketAddr,
    /// Address to receive messages from other workers and primary
    pub worker_address: SocketAddr,
}

/// Index of workers for a single authority
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerIndex(pub BTreeMap<WorkerId, WorkerInfo>);

/// Cache of all worker information in the network
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerCache {
    /// Mapping from authority public key to their workers
    pub workers: BTreeMap<PublicKey, WorkerIndex>,
    /// The epoch number for this worker configuration
    pub epoch: Epoch,
}

pub type SharedWorkerCache = Arc<ArcSwap<WorkerCache>>;

impl WorkerCache {
    /// Create a new worker cache
    pub fn new(epoch: Epoch) -> Self {
        Self {
            workers: BTreeMap::new(),
            epoch,
        }
    }
    
    /// Get the epoch
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    
    /// Get worker info for a specific authority and worker ID
    pub fn get_worker(&self, authority: &PublicKey, worker_id: WorkerId) -> Option<&WorkerInfo> {
        self.workers
            .get(authority)
            .and_then(|index| index.0.get(&worker_id))
    }
    
    /// Get all workers for a specific authority
    pub fn get_authority_workers(&self, authority: &PublicKey) -> Option<&WorkerIndex> {
        self.workers.get(authority)
    }
    
    /// Add or update worker information
    pub fn add_worker(&mut self, authority: PublicKey, worker_id: WorkerId, info: WorkerInfo) {
        self.workers
            .entry(authority)
            .or_insert_with(|| WorkerIndex(BTreeMap::new()))
            .0
            .insert(worker_id, info);
    }
    
    /// Get all workers except those belonging to a specific authority
    pub fn get_other_workers(&self, exclude_authority: &PublicKey, worker_id: WorkerId) -> Vec<(PublicKey, WorkerId, &WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(auth, _)| *auth != exclude_authority)
            .flat_map(|(auth, index)| {
                index.0
                    .get(&worker_id)
                    .map(|info| (auth.clone(), worker_id, info))
            })
            .collect()
    }
    
    /// Get all workers in the network
    pub fn all_workers(&self) -> Vec<(&PublicKey, WorkerId, &WorkerInfo)> {
        self.workers
            .iter()
            .flat_map(|(auth, index)| {
                index.0.iter().map(move |(id, info)| (auth, *id, info))
            })
            .collect()
    }
}

impl From<WorkerCache> for SharedWorkerCache {
    fn from(cache: WorkerCache) -> Self {
        Arc::new(ArcSwap::from_pointee(cache))
    }
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerIndex: {} workers",
            self.0.len()
        )
    }
}

impl std::fmt::Display for WorkerCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerCache E{}: {} authorities, {} total workers",
            self.epoch,
            self.workers.len(),
            self.all_workers().len()
        )
    }
}