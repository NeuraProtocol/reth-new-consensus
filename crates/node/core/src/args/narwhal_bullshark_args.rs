//! CLI arguments for Narwhal + Bullshark consensus

use clap::{Args, ArgAction};
use std::net::SocketAddr;
use fastcrypto::traits::KeyPair;
use serde::{Deserialize, Serialize};

/// Default network address for Narwhal
const NARWHAL_NETWORK_DEFAULT: &str = "127.0.0.1:9000";

/// Default committee size
const COMMITTEE_SIZE_DEFAULT: usize = 4;

/// Default batch size
const MAX_BATCH_SIZE_DEFAULT: usize = 1024;

/// Default GC depth  
const GC_DEPTH_DEFAULT: u64 = 50;

/// Default finality threshold
const FINALITY_THRESHOLD_DEFAULT: usize = 3;

/// Parameters for configuring Narwhal + Bullshark consensus
#[derive(Debug, Clone, Args, PartialEq, Eq, Serialize, Deserialize)]
#[command(next_help_heading = "Narwhal + Bullshark Consensus")]
#[serde(default)]
pub struct NarwhalBullsharkArgs {
    /// Enable Narwhal + Bullshark consensus instead of standard Ethereum consensus
    #[arg(long = "narwhal.enable", action = ArgAction::SetTrue)]
    pub narwhal_enabled: bool,

    /// Network address for Narwhal networking
    #[arg(long = "narwhal.network-addr", default_value = NARWHAL_NETWORK_DEFAULT)]
    pub network_address: SocketAddr,

    /// Committee size (number of validators)
    #[arg(long = "narwhal.committee-size", default_value_t = COMMITTEE_SIZE_DEFAULT)]
    pub committee_size: usize,
    
    // ===== VALIDATOR KEY MANAGEMENT =====
    
    /// Validator private key (hex string, 0x-prefixed or raw)
    #[arg(long = "validator.private-key", env = "VALIDATOR_PRIVATE_KEY")]
    pub validator_private_key: Option<String>,
    
    /// Path to validator key file (JSON format with evm_private_key field)
    #[arg(long = "validator.key-file", env = "VALIDATOR_KEY_FILE")]
    pub validator_key_file: Option<std::path::PathBuf>,
    
    /// Directory containing validator configuration files
    #[arg(long = "validator.config-dir", env = "VALIDATOR_CONFIG_DIR")]
    pub validator_config_dir: Option<std::path::PathBuf>,
    
    /// Committee configuration file with validator public keys
    #[arg(long = "validator.committee-config", env = "COMMITTEE_CONFIG_FILE")]
    pub committee_config_file: Option<std::path::PathBuf>,
    
    /// Use deterministic consensus key derivation from EVM key
    #[arg(long = "validator.deterministic-consensus-key", action = ArgAction::SetTrue)]
    pub deterministic_consensus_key: bool,
    
    /// Validator index in committee (0-based, can also use VALIDATOR_INDEX env var)
    #[arg(long = "validator.index", env = "VALIDATOR_INDEX")]
    pub validator_index: Option<usize>,
    
    // ===== HASHICORP VAULT INTEGRATION =====
    
    /// Enable HashiCorp Vault for key management
    #[arg(long = "vault.enable", action = ArgAction::SetTrue)]
    pub vault_enabled: bool,
    
    /// Vault server address (e.g., https://vault.example.com:8200)
    #[arg(long = "vault.addr", env = "VAULT_ADDR")]
    pub vault_address: Option<String>,
    
    /// Vault mount path for validator keys (default: secret)
    #[arg(long = "vault.mount-path", default_value = "secret")]
    pub vault_mount_path: String,
    
    /// Vault key path for this validator's private key
    #[arg(long = "vault.key-path", env = "VAULT_KEY_PATH")]
    pub vault_key_path: Option<String>,
    
    /// Vault authentication token (not recommended, use VAULT_TOKEN env var)
    #[arg(long = "vault.token", env = "VAULT_TOKEN")]
    pub vault_token: Option<String>,

    /// Maximum batch size for transaction batching
    #[arg(long = "narwhal.max-batch-size", default_value_t = MAX_BATCH_SIZE_DEFAULT)]
    pub max_batch_size: usize,

    /// Maximum batch delay in milliseconds
    #[arg(long = "narwhal.max-batch-delay-ms", default_value_t = 2000)]
    pub max_batch_delay_ms: u64,

    /// Number of workers per authority
    #[arg(long = "narwhal.num-workers", default_value_t = 4)]
    pub num_workers: u32,

    /// Garbage collection depth in rounds
    #[arg(long = "narwhal.gc-depth", default_value_t = GC_DEPTH_DEFAULT)]
    pub gc_depth: u64,

    /// Finality threshold (minimum confirmations needed)
    #[arg(long = "bullshark.finality-threshold", default_value_t = FINALITY_THRESHOLD_DEFAULT)]
    pub finality_threshold: usize,

    /// Maximum pending rounds to keep
    #[arg(long = "bullshark.max-pending-rounds", default_value_t = 10)]
    pub max_pending_rounds: usize,

    /// Finalization timeout in seconds
    #[arg(long = "bullshark.finalization-timeout-secs", default_value_t = 5)]
    pub finalization_timeout_secs: u64,

    /// Maximum certificates per round
    #[arg(long = "bullshark.max-certificates-per-round", default_value_t = 1000)]
    pub max_certificates_per_round: usize,

    /// Leader rotation frequency (rounds)
    #[arg(long = "bullshark.leader-rotation-frequency", default_value_t = 2)]
    pub leader_rotation_frequency: u64,

    /// Minimum round for leader election (must be even)
    /// Default is 2 for production, but can be set to 0 for testing
    #[arg(long = "bullshark.min-leader-round", default_value_t = 0)]
    pub min_leader_round: u64,

    /// Disable metrics collection
    #[arg(long = "narwhal.disable-metrics", action = ArgAction::SetTrue)]
    pub disable_metrics: bool,

    /// Peer addresses for other validators (comma-separated)
    #[arg(long = "narwhal.peers", value_delimiter = ',')]
    pub peer_addresses: Vec<SocketAddr>,

    /// Bootstrap mode - start without waiting for peers
    #[arg(long = "narwhal.bootstrap", action = ArgAction::SetTrue)]
    pub bootstrap_mode: bool,
    
    /// Port for standalone consensus RPC server (0 = disabled)
    #[arg(long = "consensus-rpc-port", default_value_t = 0)]
    pub consensus_rpc_port: u16,
    
    /// Enable admin endpoints for consensus RPC
    #[arg(long = "consensus-rpc-enable-admin", action = ArgAction::SetTrue)]
    pub consensus_rpc_enable_admin: bool,
    
    // ===== NETWORK CONFIGURATION =====
    
    /// Certificate cache size
    #[arg(long = "narwhal.cache-size", default_value_t = 1000)]
    pub cache_size: usize,
    
    /// Maximum concurrent network requests
    #[arg(long = "narwhal.max-concurrent-requests", default_value_t = 200)]
    pub max_concurrent_requests: usize,
    
    /// Connection timeout in milliseconds
    #[arg(long = "narwhal.connection-timeout-ms", default_value_t = 5000)]
    pub connection_timeout_ms: u64,
    
    /// Request timeout in milliseconds
    #[arg(long = "narwhal.request-timeout-ms", default_value_t = 10000)]
    pub request_timeout_ms: u64,
    
    /// Number of retry attempts for failed requests
    #[arg(long = "narwhal.retry-attempts", default_value_t = 3)]
    pub retry_attempts: u32,
    
    /// Base delay for exponential backoff in milliseconds
    #[arg(long = "narwhal.retry-base-delay-ms", default_value_t = 100)]
    pub retry_base_delay_ms: u64,
    
    /// Maximum delay for exponential backoff in milliseconds
    #[arg(long = "narwhal.retry-max-delay-ms", default_value_t = 10000)]
    pub retry_max_delay_ms: u64,
    
    /// Sync retry delay in milliseconds
    #[arg(long = "narwhal.sync-retry-delay-ms", default_value_t = 5000)]
    pub sync_retry_delay_ms: u64,
    
    // ===== PERFORMANCE CONFIGURATION =====
    
    /// Pre-allocated certificate buffer size
    #[arg(long = "narwhal.certificate-buffer-size", default_value_t = 1000)]
    pub certificate_buffer_size: usize,
    
    /// Maximum transactions per batch
    #[arg(long = "narwhal.max-transactions-per-batch", default_value_t = 100)]
    pub max_transactions_per_batch: usize,
    
    /// Batch creation interval in milliseconds
    #[arg(long = "narwhal.batch-creation-interval-ms", default_value_t = 50)]
    pub batch_creation_interval_ms: u64,
    
    // ===== BULLSHARK ADVANCED CONFIGURATION =====
    
    /// Maximum DAG walk depth for consensus
    #[arg(long = "bullshark.max-dag-walk-depth", default_value_t = 10)]
    pub max_dag_walk_depth: usize,
    
    /// Enable detailed consensus metrics
    #[arg(long = "bullshark.enable-detailed-metrics", action = ArgAction::SetTrue)]
    pub enable_detailed_metrics: bool,
    
    /// Minimum time between blocks in milliseconds
    #[arg(long = "bullshark.min-block-time-ms", default_value_t = 2000)]
    pub min_block_time_ms: u64,
    
    // ===== WORKER CONFIGURATION =====
    
    /// Base port for worker services (workers use sequential ports from this base)
    #[arg(long = "narwhal.worker-base-port", default_value_t = 19000)]
    pub worker_base_port: u16,
    
    /// Worker bind address (default: same as primary network address)
    #[arg(long = "narwhal.worker-bind-address")]
    pub worker_bind_address: Option<String>,
}

impl Default for NarwhalBullsharkArgs {
    fn default() -> Self {
        Self {
            narwhal_enabled: false,
            network_address: NARWHAL_NETWORK_DEFAULT.parse().unwrap(),
            committee_size: COMMITTEE_SIZE_DEFAULT,
            validator_private_key: None,
            validator_key_file: None,
            validator_config_dir: None,
            committee_config_file: None,
            deterministic_consensus_key: false,
            validator_index: None,
            vault_enabled: false,
            vault_address: None,
            vault_mount_path: "secret".to_string(),
            vault_key_path: None,
            vault_token: None,
            max_batch_size: MAX_BATCH_SIZE_DEFAULT,
            max_batch_delay_ms: 2000,
            num_workers: 4,
            gc_depth: GC_DEPTH_DEFAULT,
            finality_threshold: FINALITY_THRESHOLD_DEFAULT,
            max_pending_rounds: 10,
            finalization_timeout_secs: 5,
            max_certificates_per_round: 1000,
            leader_rotation_frequency: 2,
            min_leader_round: 0,
            disable_metrics: false,
            peer_addresses: Vec::new(),
            bootstrap_mode: false,
            consensus_rpc_port: 0,
            consensus_rpc_enable_admin: false,
            cache_size: 1000,
            max_concurrent_requests: 200,
            connection_timeout_ms: 5000,
            request_timeout_ms: 10000,
            retry_attempts: 3,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 10000,
            sync_retry_delay_ms: 5000,
            certificate_buffer_size: 1000,
            max_transactions_per_batch: 100,
            batch_creation_interval_ms: 50,
            max_dag_walk_depth: 10,
            enable_detailed_metrics: false,
            min_block_time_ms: 2000,
            worker_base_port: 19000,
            worker_bind_address: None,
        }
    }
}

impl NarwhalBullsharkArgs {
    /// Convert CLI arguments to ValidatorKeyConfig
    pub fn to_validator_key_config(&self) -> reth_consensus::narwhal_bullshark::validator_keys::ValidatorKeyConfig {
        use reth_consensus::narwhal_bullshark::validator_keys::{ValidatorKeyConfig, KeyManagementStrategy};
        
        let strategy = if self.vault_enabled {
            KeyManagementStrategy::External
        } else if self.validator_key_file.is_some() || self.validator_config_dir.is_some() {
            KeyManagementStrategy::FileSystem  
        } else if self.validator_private_key.is_some() {
            KeyManagementStrategy::FileSystem // Use in-memory key as file strategy
        } else {
            KeyManagementStrategy::Random // Fallback for testing
        };
        
        ValidatorKeyConfig {
            key_strategy: strategy,
            key_directory: self.validator_config_dir.clone(),
            deterministic_consensus_keys: self.deterministic_consensus_key,
        }
    }
    
    /// Get validator index for this node
    pub fn get_validator_index(&self) -> Option<usize> {
        // Priority: CLI arg > env var > None
        self.validator_index
            .or_else(|| std::env::var("VALIDATOR_INDEX").ok().and_then(|s| s.parse().ok()))
    }
    
    /// Get validator private key from CLI args or environment
    pub fn get_validator_private_key(&self) -> Option<String> {
        self.validator_private_key.clone()
            .or_else(|| std::env::var("VALIDATOR_PRIVATE_KEY").ok())
    }
    
    /// Check if vault-based key management is configured
    pub fn is_vault_configured(&self) -> bool {
        self.vault_enabled && 
        self.vault_address.is_some() && 
        self.vault_key_path.is_some()
    }
    
    /// Get vault configuration for key management
    pub fn get_vault_config(&self) -> Option<VaultConfig> {
        if !self.is_vault_configured() {
            return None;
        }
        
        Some(VaultConfig {
            address: self.vault_address.clone()?,
            mount_path: self.vault_mount_path.clone(),
            key_path: self.vault_key_path.clone()?,
            token: self.vault_token.clone(),
        })
    }

    /// Convert to Narwhal configuration
    pub fn to_narwhal_config(&self) -> narwhal::NarwhalConfig {
        let mut config = narwhal::NarwhalConfig::default();
        
        // Basic configuration
        config.max_batch_size = self.max_batch_size;
        config.max_batch_delay = std::time::Duration::from_millis(self.max_batch_delay_ms);
        config.num_workers = self.num_workers;
        config.gc_depth = self.gc_depth;
        config.committee_size = self.committee_size;
        config.batch_storage_memory = false; // Use persistent storage in production
        config.sync_retry_delay = std::time::Duration::from_millis(self.sync_retry_delay_ms);
        
        // Worker configuration
        config.worker.cache_size = self.cache_size;
        config.worker.batch_timeout = std::time::Duration::from_millis(self.request_timeout_ms);
        config.worker.max_batch_requests = self.max_concurrent_requests;
        config.worker.batch_retry_attempts = self.retry_attempts;
        config.worker.batch_retry_delay = std::time::Duration::from_millis(self.retry_base_delay_ms);
        
        // Network configuration
        config.network.connection_timeout = std::time::Duration::from_millis(self.connection_timeout_ms);
        config.network.request_timeout = std::time::Duration::from_millis(self.request_timeout_ms);
        config.network.retry.initial_interval = std::time::Duration::from_millis(self.retry_base_delay_ms);
        config.network.retry.max_interval = std::time::Duration::from_millis(self.retry_max_delay_ms);
        
        // Storage configuration
        config.storage.cache_size = self.certificate_buffer_size;
        
        // Performance configuration
        config.performance.tx_prealloc_size = self.max_transactions_per_batch;
        config.performance.yield_interval = std::time::Duration::from_millis(self.batch_creation_interval_ms);
        
        config
    }

    /// Convert to Bullshark configuration
    /// ✅ FIX: Now uses real validator key instead of dummy random key
    pub fn to_bullshark_config(&self, validator_keypair: &reth_consensus::narwhal_bullshark::validator_keys::ValidatorKeyPair) -> bullshark::BftConfig {
        bullshark::BftConfig {
            node_key: validator_keypair.consensus_keypair.public().clone(),
            gc_depth: self.gc_depth,
            finalization_timeout: std::time::Duration::from_secs(self.finalization_timeout_secs),
            max_certificates_per_round: self.max_certificates_per_round,
            leader_rotation_frequency: self.leader_rotation_frequency,
            min_leader_round: self.min_leader_round,
            min_block_time: std::time::Duration::from_millis(self.min_block_time_ms),
        }
    }

    /// Convert to Reth integration configuration
    pub fn to_integration_config(&self) -> reth_consensus::narwhal_bullshark::integration::RethIntegrationConfig {
        reth_consensus::narwhal_bullshark::integration::RethIntegrationConfig {
            network_address: self.network_address,
            enable_networking: true, // Enable networking for production use
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: !self.disable_metrics,
            peer_addresses: self.peer_addresses.clone(),
        }
    }

    /// Get peer addresses for committee setup
    pub fn get_peer_addresses(&self) -> &[SocketAddr] {
        &self.peer_addresses
    }

    /// Check if this node should wait for peers before starting
    pub fn should_wait_for_peers(&self) -> bool {
        !self.bootstrap_mode && !self.peer_addresses.is_empty()
    }
    
    /// Get worker bind address (defaults to the primary bind address)
    pub fn get_worker_bind_address(&self) -> String {
        self.worker_bind_address.clone()
            .unwrap_or_else(|| {
                // Extract the IP from the network address
                let ip = self.network_address.ip().to_string();
                ip
            })
    }
    
    /// Get worker configuration for committee creation
    pub fn get_worker_configuration(&self) -> narwhal::types::WorkerConfiguration {
        narwhal::types::WorkerConfiguration {
            num_workers: self.num_workers,
            base_port: self.worker_base_port,
            base_address: self.get_worker_bind_address(),
            worker_ports: None, // Will be set explicitly from validator config if needed
        }
    }
}

/// Vault configuration extracted from CLI arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    pub address: String,
    pub mount_path: String,
    pub key_path: String,
    pub token: Option<String>,
} 