//! Configuration for consensus RPC
//!
//! This module provides configuration for the consensus RPC endpoints,
//! replacing hardcoded values with configurable parameters.

use serde::{Deserialize, Serialize};
use std::{time::Duration, net::SocketAddr};

/// Complete configuration for consensus RPC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusRpcConfig {
    /// RPC server bind address
    pub bind_address: SocketAddr,
    /// Maximum request size
    pub max_request_size: u32,
    /// Maximum response size
    pub max_response_size: u32,
    /// Request timeout
    pub request_timeout: Duration,
    /// Validator configuration
    pub validator: ValidatorConfig,
    /// Network configuration
    pub network: NetworkRpcConfig,
    /// Performance configuration
    pub performance: PerformanceRpcConfig,
}

/// Validator-specific RPC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// Default stake amount for validators (when actual stake not available)
    pub default_stake: u64,
    /// Whether to show inactive validators
    pub show_inactive: bool,
    /// Maximum validators to return in lists
    pub max_validators: usize,
}

/// Network-specific RPC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkRpcConfig {
    /// Default listen address for validators
    pub default_listen_address: String,
    /// Default consensus port
    pub default_consensus_port: u16,
    /// Default worker base port
    pub default_worker_base_port: u16,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
}

/// Performance-specific RPC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceRpcConfig {
    /// Header cache size
    pub header_cache_size: u64,
    /// Vote cache size
    pub vote_cache_size: u64,
    /// Certificate cache size
    pub certificate_cache_size: u64,
    /// Max concurrent requests
    pub max_concurrent_requests: u64,
    /// Worker threads
    pub worker_threads: u64,
}

impl Default for ConsensusRpcConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9651".parse().unwrap(),
            max_request_size: 10 * 1024 * 1024, // 10MB
            max_response_size: 10 * 1024 * 1024, // 10MB
            request_timeout: Duration::from_secs(30),
            validator: ValidatorConfig::default(),
            network: NetworkRpcConfig::default(),
            performance: PerformanceRpcConfig::default(),
        }
    }
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            default_stake: 1000, // Default stake amount
            show_inactive: false,
            max_validators: 1000,
        }
    }
}

impl Default for NetworkRpcConfig {
    fn default() -> Self {
        Self {
            default_listen_address: "0.0.0.0".to_string(),
            default_consensus_port: 8000,
            default_worker_base_port: 8100,
            connection_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

impl Default for PerformanceRpcConfig {
    fn default() -> Self {
        Self {
            header_cache_size: 10000,
            vote_cache_size: 50000,
            certificate_cache_size: 10000,
            max_concurrent_requests: 1000,
            worker_threads: 4,
        }
    }
}

impl ConsensusRpcConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // RPC server configuration
        if let Ok(addr) = std::env::var("CONSENSUS_RPC_BIND_ADDRESS") {
            if let Ok(parsed) = addr.parse() {
                config.bind_address = parsed;
            }
        }
        if let Ok(val) = std::env::var("CONSENSUS_RPC_MAX_REQUEST_SIZE") {
            if let Ok(size) = val.parse() {
                config.max_request_size = size;
            }
        }

        // Validator configuration
        if let Ok(val) = std::env::var("CONSENSUS_VALIDATOR_DEFAULT_STAKE") {
            if let Ok(stake) = val.parse() {
                config.validator.default_stake = stake;
            }
        }
        if let Ok(val) = std::env::var("CONSENSUS_VALIDATOR_SHOW_INACTIVE") {
            config.validator.show_inactive = val.to_lowercase() == "true";
        }

        // Network configuration
        if let Ok(val) = std::env::var("CONSENSUS_NETWORK_DEFAULT_PORT") {
            if let Ok(port) = val.parse() {
                config.network.default_consensus_port = port;
            }
        }
        if let Ok(val) = std::env::var("CONSENSUS_NETWORK_WORKER_BASE_PORT") {
            if let Ok(port) = val.parse() {
                config.network.default_worker_base_port = port;
            }
        }

        // Performance configuration
        if let Ok(val) = std::env::var("CONSENSUS_PERF_HEADER_CACHE_SIZE") {
            if let Ok(size) = val.parse() {
                config.performance.header_cache_size = size;
            }
        }
        if let Ok(val) = std::env::var("CONSENSUS_PERF_WORKER_THREADS") {
            if let Ok(threads) = val.parse() {
                config.performance.worker_threads = threads;
            }
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConsensusRpcConfig::default();
        assert_eq!(config.validator.default_stake, 1000);
        assert_eq!(config.network.default_consensus_port, 8000);
        assert_eq!(config.performance.header_cache_size, 10000);
    }

    #[test]
    fn test_serialization() {
        let config = ConsensusRpcConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ConsensusRpcConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.validator.default_stake, deserialized.validator.default_stake);
    }
}