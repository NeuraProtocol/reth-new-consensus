//! Configuration for Narwhal

use crate::{Round, WorkerId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the Narwhal DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarwhalConfig {
    /// Maximum batch size in bytes
    pub max_batch_size: usize,
    /// Maximum delay before creating a batch
    pub max_batch_delay: Duration,
    /// Number of workers per authority
    pub num_workers: WorkerId,
    /// Garbage collection depth
    pub gc_depth: Round,
    /// Committee size
    pub committee_size: usize,
    /// Maximum header size
    pub max_header_size: usize,
    /// Maximum header delay
    pub max_header_delay: Duration,
    /// Sync retry delay
    pub sync_retry_delay: Duration,
    /// Number of nodes to sync from
    pub sync_retry_nodes: usize,
}

impl Default for NarwhalConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1024 * 1024,  // 1MB
            max_batch_delay: Duration::from_millis(100),
            num_workers: 4,
            gc_depth: 50,
            committee_size: 4,
            max_header_size: 1024 * 1024,  // 1MB
            max_header_delay: Duration::from_millis(100),
            sync_retry_delay: Duration::from_millis(5000),
            sync_retry_nodes: 3,
        }
    }
} 