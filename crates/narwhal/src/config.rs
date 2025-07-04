//! Configuration for Narwhal consensus
//!
//! This module provides comprehensive configuration options for all aspects
//! of the Narwhal DAG consensus system, replacing hardcoded values throughout
//! the codebase with configurable parameters.

use crate::{Round, WorkerId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Complete configuration for Narwhal consensus
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
    /// Use in-memory batch storage (for testing)
    pub batch_storage_memory: bool,
    
    // Extended configuration options
    /// Worker configuration
    pub worker: WorkerConfig,
    /// Network configuration
    pub network: NetworkConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
    /// Byzantine fault tolerance configuration
    pub byzantine: ByzantineConfig,
}

/// Worker-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Worker batch replication timeout
    pub batch_timeout: Duration,
    /// Quorum waiter timeout for batch replication
    pub quorum_timeout: Duration,
    /// Maximum batch requests in flight
    pub max_batch_requests: usize,
    /// Worker cache size
    pub cache_size: usize,
    /// Worker batch expiration time
    pub batch_expiration: Duration,
    /// Worker batch request retry attempts
    pub batch_retry_attempts: u32,
    /// Worker batch request retry delay
    pub batch_retry_delay: Duration,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum concurrent connections per peer
    pub max_connections_per_peer: usize,
    /// Maximum total concurrent connections
    pub max_total_connections: usize,
    /// Network health check interval
    pub health_check_interval: Duration,
    /// Peer connection retry interval
    pub peer_retry_interval: Duration,
    /// Maximum message size
    pub max_message_size: usize,
    /// Send buffer size
    pub send_buffer_size: usize,
    /// Receive buffer size
    pub recv_buffer_size: usize,
    /// Retry configuration
    pub retry: RetryConfig,
}

/// Retry configuration for network operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Initial retry interval
    pub initial_interval: Duration,
    /// Maximum retry interval
    pub max_interval: Duration,
    /// Retry interval multiplier
    pub multiplier: f64,
    /// Randomization factor
    pub randomization_factor: f64,
    /// Maximum elapsed time for retries
    pub max_elapsed_time: Option<Duration>,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum database size
    pub max_db_size: usize,
    /// Database sync mode
    pub sync_mode: SyncMode,
    /// Compression enabled
    pub compression_enabled: bool,
    /// Cache size for frequently accessed data
    pub cache_size: usize,
    /// Batch write size
    pub batch_write_size: usize,
    /// Storage pruning interval
    pub pruning_interval: Duration,
    /// Maximum storage operation retries
    pub max_retries: u32,
}

/// Database sync mode
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncMode {
    /// Synchronous writes (slower but safer)
    Synchronous,
    /// Asynchronous writes (faster but less safe)
    Asynchronous,
    /// Write-through cache
    WriteThrough,
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Transaction pre-allocation size
    pub tx_prealloc_size: usize,
    /// Batch pre-allocation size
    pub batch_prealloc_size: usize,
    /// Channel buffer sizes
    pub channel_buffer_size: usize,
    /// Yield interval for long-running tasks
    pub yield_interval: Duration,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: usize,
    /// CPU affinity settings
    pub cpu_affinity: Option<Vec<usize>>,
    /// Memory pool size
    pub memory_pool_size: usize,
}

/// Byzantine fault tolerance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ByzantineConfig {
    /// Maximum tolerated Byzantine faults
    pub max_byzantine_faults: usize,
    /// Equivocation detection timeout
    pub equivocation_timeout: Duration,
    /// Invalid signature penalty duration
    pub penalty_duration: Duration,
    /// Maximum penalty score before disconnection
    pub max_penalty_score: u32,
    /// Byzantine behavior detection window
    pub detection_window: Duration,
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
            batch_storage_memory: true, // Default to in-memory for now
            worker: WorkerConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            performance: PerformanceConfig::default(),
            byzantine: ByzantineConfig::default(),
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            batch_timeout: Duration::from_secs(10),
            quorum_timeout: Duration::from_secs(5),
            max_batch_requests: 1000,
            cache_size: 10_000,
            batch_expiration: Duration::from_secs(60),
            batch_retry_attempts: 3,
            batch_retry_delay: Duration::from_millis(500),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_connections_per_peer: 100,
            max_total_connections: 1000,
            health_check_interval: Duration::from_secs(30),
            peer_retry_interval: Duration::from_secs(5),
            max_message_size: 5 * 1024 * 1024, // 5MB
            send_buffer_size: 1024 * 1024, // 1MB
            recv_buffer_size: 1024 * 1024, // 1MB
            retry: RetryConfig::default(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_interval: Duration::from_millis(500),
            max_interval: Duration::from_secs(15),
            multiplier: 1.5,
            randomization_factor: 0.3,
            max_elapsed_time: Some(Duration::from_secs(30)),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_db_size: 10 * 1024 * 1024 * 1024, // 10GB
            sync_mode: SyncMode::WriteThrough,
            compression_enabled: true,
            cache_size: 100 * 1024 * 1024, // 100MB
            batch_write_size: 1000,
            pruning_interval: Duration::from_secs(3600), // 1 hour
            max_retries: 3,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            tx_prealloc_size: 1000,
            batch_prealloc_size: 100,
            channel_buffer_size: 10_000,
            yield_interval: Duration::from_millis(100),
            max_concurrent_tasks: 1000,
            cpu_affinity: None,
            memory_pool_size: 100 * 1024 * 1024, // 100MB
        }
    }
}

impl Default for ByzantineConfig {
    fn default() -> Self {
        Self {
            max_byzantine_faults: 1, // f = 1 for 4 validators
            equivocation_timeout: Duration::from_secs(60),
            penalty_duration: Duration::from_secs(300), // 5 minutes
            max_penalty_score: 100,
            detection_window: Duration::from_secs(600), // 10 minutes
        }
    }
}

impl NarwhalConfig {
    /// Create a new configuration with custom values
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Basic configuration from env
        if let Ok(val) = std::env::var("NARWHAL_MAX_BATCH_SIZE") {
            if let Ok(size) = val.parse() {
                config.max_batch_size = size;
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_MAX_BATCH_DELAY_MS") {
            if let Ok(ms) = val.parse() {
                config.max_batch_delay = Duration::from_millis(ms);
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_NUM_WORKERS") {
            if let Ok(num) = val.parse() {
                config.num_workers = num;
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_GC_DEPTH") {
            if let Ok(depth) = val.parse() {
                config.gc_depth = depth;
            }
        }

        // Worker configuration from env
        if let Ok(val) = std::env::var("NARWHAL_WORKER_BATCH_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse() {
                config.worker.batch_timeout = Duration::from_secs(secs);
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_WORKER_CACHE_SIZE") {
            if let Ok(size) = val.parse() {
                config.worker.cache_size = size;
            }
        }

        // Network configuration from env
        if let Ok(val) = std::env::var("NARWHAL_CONNECTION_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse() {
                config.network.connection_timeout = Duration::from_secs(secs);
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_MAX_CONNECTIONS_PER_PEER") {
            if let Ok(max) = val.parse() {
                config.network.max_connections_per_peer = max;
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_MAX_MESSAGE_SIZE") {
            if let Ok(size) = val.parse() {
                config.network.max_message_size = size;
            }
        }

        // Storage configuration from env
        if let Ok(val) = std::env::var("NARWHAL_STORAGE_CACHE_SIZE") {
            if let Ok(size) = val.parse() {
                config.storage.cache_size = size;
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_STORAGE_COMPRESSION") {
            config.storage.compression_enabled = val.to_lowercase() == "true";
        }

        // Performance configuration from env
        if let Ok(val) = std::env::var("NARWHAL_CHANNEL_BUFFER_SIZE") {
            if let Ok(size) = val.parse() {
                config.performance.channel_buffer_size = size;
            }
        }
        if let Ok(val) = std::env::var("NARWHAL_MAX_CONCURRENT_TASKS") {
            if let Ok(max) = val.parse() {
                config.performance.max_concurrent_tasks = max;
            }
        }

        config
    }

    /// Convert retry config to the retry crate's config
    pub fn to_retry_config(&self) -> crate::retry::RetryConfig {
        crate::retry::RetryConfig {
            initial_retry_interval: self.network.retry.initial_interval,
            max_retry_interval: self.network.retry.max_interval,
            retry_delay_multiplier: self.network.retry.multiplier,
            retry_delay_rand_factor: self.network.retry.randomization_factor,
            retrying_max_elapsed_time: self.network.retry.max_elapsed_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NarwhalConfig::default();
        assert_eq!(config.max_batch_size, 1024 * 1024);
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.worker.cache_size, 10_000);
        assert_eq!(config.network.max_connections_per_peer, 100);
    }

    #[test]
    fn test_serialization() {
        let config = NarwhalConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NarwhalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.max_batch_size, deserialized.max_batch_size);
    }
}