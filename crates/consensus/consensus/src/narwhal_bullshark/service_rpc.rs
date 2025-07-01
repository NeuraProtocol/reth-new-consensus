//! RPC implementation for the NarwhalBullsharkService
//! 
//! This module provides a standalone RPC server implementation that works
//! directly with the consensus service, rather than requiring the bridge.

use crate::rpc::{
    ConsensusApiServer, ConsensusAdminApiServer,
    ConsensusStatus, ConsensusState, CommitteeInfo, ValidatorInfo, ValidatorMetrics,
    CertificateInfo, FinalizedBatchInfo, ConsensusTransactionStatus, ConsensusMetrics,
    ConsensusConfig, DagInfo, StorageStats, InternalConsensusState,
    ValidatorSummary, StakeDistribution, ValidatorMetadata, ValidatorStatus,
    SignatureInfo, TransactionConsensusStatus, ThroughputMetrics, LatencyMetrics,
    ResourceMetrics, NetworkMetrics, AlgorithmConfig, NetworkConfig as RpcNetworkConfig,
    PerformanceConfig, DagStats, LogEntry,
};
use crate::ConsensusDbStats;
use super::{NarwhalBullsharkConfig};
use crate::consensus_storage::MdbxConsensusStorage;
use crate::narwhal_bullshark::validator_keys::ValidatorRegistry;
use alloy_primitives::{Address, B256, TxHash};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use fastcrypto::traits::EncodeDecodeBase64;
use narwhal::types::Committee;

/// Service-based implementation of the consensus RPC API
/// 
/// This implementation works directly with the NarwhalBullsharkService,
/// without requiring the NarwhalRethBridge.
#[derive(Clone)]
pub struct ServiceConsensusRpcImpl {
    /// Node configuration
    node_config: NarwhalBullsharkConfig,
    /// Current committee
    committee: Arc<RwLock<Committee>>,
    /// Validator registry (optional)
    validator_registry: Option<Arc<RwLock<ValidatorRegistry>>>,
    /// Consensus storage (optional)
    storage: Option<Arc<MdbxConsensusStorage>>,
    /// Whether the service is running
    is_running: Arc<RwLock<bool>>,
}

impl ServiceConsensusRpcImpl {
    /// Create a new service-based RPC implementation
    pub fn new(
        node_config: NarwhalBullsharkConfig,
        committee: Arc<RwLock<Committee>>,
        validator_registry: Option<Arc<RwLock<ValidatorRegistry>>>,
        storage: Option<Arc<MdbxConsensusStorage>>,
    ) -> Self {
        Self {
            node_config,
            committee,
            validator_registry,
            storage,
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Update the running state
    pub async fn set_running(&self, running: bool) {
        *self.is_running.write().await = running;
    }
}

#[jsonrpsee::core::async_trait]
impl ConsensusApiServer for ServiceConsensusRpcImpl {
    async fn get_status(&self) -> RpcResult<ConsensusStatus> {
        let committee = self.committee.read().await;
        let is_running = *self.is_running.read().await;
        
        // Get current round from storage if available
        let round = if let Some(ref storage) = self.storage {
            match storage.get_latest_round() {
                Ok(r) => r,
                Err(_) => 0,
            }
        } else {
            0
        };
        
        // Get last finalized batch info from storage if available
        let (last_finalized_batch, time_since_last_finalization) = 
            if let Some(ref storage) = self.storage {
                match storage.get_latest_finalized_batch_info() {
                    Ok(Some((batch_num, timestamp))) => {
                        let current_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        (Some(batch_num), Some(current_time - timestamp))
                    }
                    Ok(None) => (None, None),
                    Err(_) => (None, None),
                }
            } else {
                (None, None)
            };
        
        let state = if is_running {
            ConsensusState::Running
        } else {
            ConsensusState::Stalled { reason: "Consensus service not running".to_string() }
        };
        
        Ok(ConsensusStatus {
            healthy: is_running,
            epoch: committee.epoch,
            round,
            active_validators: committee.authorities.len(),
            state,
            last_finalized_batch,
            time_since_last_finalization,
            is_producing: is_running,
        })
    }

    async fn get_committee(&self) -> RpcResult<CommitteeInfo> {
        let committee = self.committee.read().await;
        
        let validators: Vec<ValidatorSummary> = if let Some(ref registry) = self.validator_registry {
            let reg = registry.read().await;
            reg.all_validators()
                .iter()
                .filter_map(|identity| {
                    // Check if this validator is in the current committee
                    if committee.authorities.contains_key(&identity.consensus_public_key) {
                        Some(ValidatorSummary {
                            evm_address: identity.evm_address,
                            consensus_key: identity.consensus_public_key.encode_base64(),
                            stake: committee.stake(&identity.consensus_public_key),
                            active: true,
                            name: identity.metadata.name.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            // No registry - use committee directly
            committee.authorities
                .iter()
                .map(|(pub_key, &stake)| ValidatorSummary {
                    evm_address: Address::ZERO, // Unknown without registry
                    consensus_key: pub_key.encode_base64(),
                    stake,
                    active: true,
                    name: None,
                })
                .collect()
        };

        let total_stake: u64 = validators.iter().map(|v| v.stake).sum();
        let stakes: Vec<u64> = validators.iter().map(|v| v.stake).collect();
        
        let stake_distribution = calculate_stake_distribution(&stakes, total_stake);

        Ok(CommitteeInfo {
            epoch: committee.epoch,
            total_validators: validators.len(),
            total_stake,
            validators,
            stake_distribution,
        })
    }

    async fn get_validator(&self, address: Address) -> RpcResult<Option<ValidatorInfo>> {
        if let Some(ref registry) = self.validator_registry {
            let reg = registry.read().await;
            let committee = self.committee.read().await;
            
            if let Some(identity) = reg.get_by_evm_address(&address) {
                let stake = committee.stake(&identity.consensus_public_key);
                let is_active = committee.authorities.contains_key(&identity.consensus_public_key);
                
                let validator_info = ValidatorInfo {
                    summary: ValidatorSummary {
                        evm_address: identity.evm_address,
                        consensus_key: identity.consensus_public_key.encode_base64(),
                        stake,
                        active: is_active,
                        name: identity.metadata.name.clone(),
                    },
                    metadata: ValidatorMetadata {
                        description: identity.metadata.description.clone(),
                        contact: identity.metadata.contact.clone(),
                        registered_at: None,
                        key_version: None,
                    },
                    status: if is_active { 
                        ValidatorStatus::Active 
                    } else { 
                        ValidatorStatus::Offline 
                    },
                    recent_metrics: None,
                };
                
                Ok(Some(validator_info))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn list_validators(
        &self,
        active_only: Option<bool>,
        limit: Option<usize>,
    ) -> RpcResult<Vec<ValidatorInfo>> {
        if let Some(ref registry) = self.validator_registry {
            let reg = registry.read().await;
            let committee = self.committee.read().await;
            
            let mut validators: Vec<ValidatorInfo> = reg
                .all_validators()
                .iter()
                .filter_map(|identity| {
                    let is_active = committee.authorities.contains_key(&identity.consensus_public_key);
                    
                    if active_only.unwrap_or(false) && !is_active {
                        return None;
                    }
                    
                    let stake = committee.stake(&identity.consensus_public_key);
                    
                    Some(ValidatorInfo {
                        summary: ValidatorSummary {
                            evm_address: identity.evm_address,
                            consensus_key: identity.consensus_public_key.encode_base64(),
                            stake,
                            active: is_active,
                            name: identity.metadata.name.clone(),
                        },
                        metadata: ValidatorMetadata {
                            description: identity.metadata.description.clone(),
                            contact: identity.metadata.contact.clone(),
                            registered_at: None,
                            key_version: None,
                        },
                        status: if is_active { 
                            ValidatorStatus::Active 
                        } else { 
                            ValidatorStatus::Offline 
                        },
                        recent_metrics: None,
                    })
                })
                .collect();

            if let Some(limit) = limit {
                validators.truncate(limit);
            }

            Ok(validators)
        } else {
            Ok(vec![])
        }
    }

    async fn get_validator_metrics(&self, address: Address) -> RpcResult<Option<ValidatorMetrics>> {
        // TODO: Implement actual metrics collection
        if let Some(ref registry) = self.validator_registry {
            let reg = registry.read().await;
            
            if reg.get_by_evm_address(&address).is_some() {
                Ok(Some(ValidatorMetrics {
                    certificates_produced: 0,
                    certificates_voted: 0,
                    participation_rate: 1.0,
                    avg_certificate_time: Some(100),
                    missed_rounds: 0,
                    reputation_score: 1.0,
                    time_window: 3600,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn get_certificate(&self, certificate_id: B256) -> RpcResult<Option<CertificateInfo>> {
        if let Some(ref storage) = self.storage {
            let cert_id = u64::from_le_bytes(certificate_id.as_slice()[0..8].try_into().unwrap_or([0u8; 8]));
            
            match storage.get_certificate(cert_id) {
                Ok(Some(_cert_data)) => {
                    // TODO: Properly deserialize certificate
                    Ok(Some(CertificateInfo {
                        id: certificate_id,
                        round: 0,
                        author: Address::ZERO,
                        parents: vec![],
                        transactions: vec![],
                        signature_info: SignatureInfo {
                            signature_count: 0,
                            signed_stake: 0,
                            has_quorum: true,
                            signers: vec![],
                        },
                        finalized: false,
                        timestamp: 0,
                    }))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(ErrorObject::owned(-32000, format!("Storage error: {}", e), None::<()>)),
            }
        } else {
            Ok(None)
        }
    }

    async fn get_finalized_batch(&self, batch_id: u64) -> RpcResult<Option<FinalizedBatchInfo>> {
        if let Some(ref storage) = self.storage {
            match storage.get_finalized_batch(batch_id) {
                Ok(Some(_block_hash)) => {
                    match storage.get_batch(batch_id) {
                        Ok(Some(batch_data)) => {
                            Ok(Some(FinalizedBatchInfo {
                                batch_id,
                                epoch: 0,
                                round_range: (0, 0),
                                certificates: vec![],
                                transaction_count: 0,
                                transactions: vec![],
                                finalized_at: 0,
                                size_bytes: batch_data.len(),
                            }))
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(ErrorObject::owned(-32000, format!("Storage error: {}", e), None::<()>)),
                    }
                }
                Ok(None) => Ok(None),
                Err(e) => Err(ErrorObject::owned(-32000, format!("Storage error: {}", e), None::<()>)),
            }
        } else {
            Ok(None)
        }
    }

    async fn get_recent_batches(&self, count: Option<usize>) -> RpcResult<Vec<FinalizedBatchInfo>> {
        if let Some(ref storage) = self.storage {
            let limit = count.unwrap_or(10).min(100);
            
            match storage.list_finalized_batches(Some(limit)) {
                Ok(batches) => {
                    let mut batch_infos = Vec::new();
                    for (batch_id, _block_hash) in batches {
                        batch_infos.push(FinalizedBatchInfo {
                            batch_id,
                            epoch: 0,
                            round_range: (0, 0),
                            certificates: vec![],
                            transaction_count: 0,
                            transactions: vec![],
                            finalized_at: 0,
                            size_bytes: 0,
                        });
                    }
                    Ok(batch_infos)
                }
                Err(e) => Err(ErrorObject::owned(-32000, format!("Storage error: {}", e), None::<()>)),
            }
        } else {
            Ok(vec![])
        }
    }

    async fn get_transaction_status(
        &self,
        _tx_hash: TxHash,
    ) -> RpcResult<Option<ConsensusTransactionStatus>> {
        // TODO: Implement transaction status lookup
        Ok(None)
    }

    async fn get_metrics(&self) -> RpcResult<ConsensusMetrics> {
        let is_running = *self.is_running.read().await;
        let committee = self.committee.read().await;
        let committee_size = committee.authorities.len();
        
        let stats = if let Some(ref storage) = self.storage {
            match storage.get_stats() {
                Ok(s) => s,
                Err(_) => Default::default(),
            }
        } else {
            Default::default()
        };
        
        Ok(ConsensusMetrics {
            throughput: ThroughputMetrics {
                tps_recent: 0.0,
                tps_total: 0.0,
                certificates_per_second: 0.0,
                batches_per_second: 0.0,
                peak_tps: 0.0,
            },
            latency: LatencyMetrics {
                avg_finalization_time: 1000,
                median_finalization_time: 900,
                p95_finalization_time: 2000,
                avg_certificate_time: 500,
                avg_round_time: 2000,
            },
            resources: ResourceMetrics {
                memory_usage: 0,
                database_size: stats.total_certificates + stats.total_batches + stats.total_dag_vertices,
                cpu_usage: 0.0,
                network_bandwidth: 0,
            },
            network: NetworkMetrics {
                connected_peers: if is_running { committee_size - 1 } else { 0 },
                messages_sent_per_sec: 0.0,
                messages_received_per_sec: 0.0,
                network_health: if is_running { 1.0 } else { 0.0 },
            },
        })
    }

    async fn get_config(&self) -> RpcResult<ConsensusConfig> {
        let committee = self.committee.read().await;
        let total_stake: u64 = committee.authorities.values().sum();
        let quorum_threshold = committee.quorum_threshold() as f32 / total_stake as f32;
        
        Ok(ConsensusConfig {
            algorithm: AlgorithmConfig {
                min_validators: 1,
                max_validators: Some(1000),
                quorum_threshold: quorum_threshold.into(),
                max_transactions_per_certificate: 1000,
                round_timeout_ms: 5000,
            },
            network: RpcNetworkConfig {
                listen_address: format!("{}", self.node_config.network.bind_address),
                max_message_size: 1024 * 1024,
                connection_timeout_ms: 30000,
                keepalive_interval_ms: 60000,
            },
            performance: PerformanceConfig {
                certificate_cache_size: 1000,
                transaction_batch_size: 100,
                worker_threads: 4,
                memory_pool_size: 1024 * 1024 * 100,
            },
        })
    }
}

/// Service-based administrative RPC implementation
#[derive(Clone)]
pub struct ServiceConsensusAdminRpcImpl {
    /// Reference to the consensus RPC implementation
    consensus_rpc: Arc<ServiceConsensusRpcImpl>,
}

impl ServiceConsensusAdminRpcImpl {
    /// Create a new admin RPC implementation
    pub fn new(consensus_rpc: Arc<ServiceConsensusRpcImpl>) -> Self {
        Self { consensus_rpc }
    }
}

#[jsonrpsee::core::async_trait]
impl ConsensusAdminApiServer for ServiceConsensusAdminRpcImpl {
    async fn update_validator_set(&self) -> RpcResult<bool> {
        // TODO: Implement validator set update
        Ok(false)
    }

    async fn get_dag_info(&self) -> RpcResult<DagInfo> {
        Ok(DagInfo {
            total_certificates: 0,
            current_height: 0,
            pending_certificates: 0,
            health_score: 1.0,
            recent_stats: DagStats {
                certificates_last_hour: 0,
                avg_certificates_per_round: 0.0,
                fork_rate: 0.0,
                avg_parents_per_certificate: 0.0,
            },
        })
    }

    async fn get_storage_stats(&self) -> RpcResult<StorageStats> {
        if let Some(ref storage) = self.consensus_rpc.storage {
            // TODO: Get actual storage statistics
            Ok(StorageStats {
                file_size: 0,
                used_size: 0,
                free_size: 0,
                page_count: 0,
                page_utilization: 0.0,
                certificate_count: 0,
                batch_count: 0,
                health_score: 1.0,
            })
        } else {
            Ok(Default::default())
        }
    }

    async fn compact_database(&self) -> RpcResult<bool> {
        if let Some(ref storage) = self.consensus_rpc.storage {
            match storage.compact() {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    async fn get_internal_state(&self) -> RpcResult<InternalConsensusState> {
        Ok(InternalConsensusState {
            mode: "running".to_string(),
            state_vars: HashMap::new(),
            recent_logs: vec![],
            counters: HashMap::new(),
        })
    }
}

// Helper function to calculate stake distribution
fn calculate_stake_distribution(stakes: &[u64], total_stake: u64) -> StakeDistribution {
    if stakes.is_empty() {
        return StakeDistribution {
            min_stake: 0,
            max_stake: 0,
            avg_stake: 0,
            median_stake: 0,
            concentration: 0.0,
        };
    }

    let mut sorted_stakes = stakes.to_vec();
    sorted_stakes.sort();
    
    StakeDistribution {
        min_stake: *sorted_stakes.first().unwrap_or(&0),
        max_stake: *sorted_stakes.last().unwrap_or(&0),
        avg_stake: total_stake / stakes.len() as u64,
        median_stake: sorted_stakes[stakes.len() / 2],
        concentration: calculate_gini_coefficient(&sorted_stakes),
    }
}

// Calculate Gini coefficient for stake distribution
fn calculate_gini_coefficient(stakes: &[u64]) -> f64 {
    if stakes.len() <= 1 {
        return 0.0;
    }

    let n = stakes.len() as f64;
    let total: u64 = stakes.iter().sum();
    
    if total == 0 {
        return 0.0;
    }

    let mut gini_sum = 0.0;
    for (i, &stake) in stakes.iter().enumerate() {
        gini_sum += (2.0 * (i as f64 + 1.0) - n - 1.0) * stake as f64;
    }

    gini_sum / (n * total as f64)
}

/// Start a standalone RPC server for the consensus service
pub async fn start_service_rpc_server(
    config: super::ConsensusRpcConfig,
    node_config: NarwhalBullsharkConfig,
    committee: Arc<RwLock<Committee>>,
    validator_registry: Option<Arc<RwLock<ValidatorRegistry>>>,
    storage: Option<Arc<MdbxConsensusStorage>>,
    is_running: Arc<RwLock<bool>>,
) -> Result<jsonrpsee::server::ServerHandle, anyhow::Error> {
    use jsonrpsee::server::{ServerBuilder, RpcModule};
    use tracing::info;
    
    info!("Starting standalone consensus RPC server on {}:{}", config.host, config.port);
    
    // Create RPC implementations
    let mut consensus_rpc = ServiceConsensusRpcImpl::new(
        node_config,
        committee,
        validator_registry,
        storage,
    );
    
    // Share the running state from the service
    consensus_rpc.is_running = is_running;
    
    // Create RPC module
    let mut module = RpcModule::new(());
    
    // Register consensus API - clone the implementation since into_rpc() takes ownership
    let consensus_rpc_for_api = consensus_rpc.clone();
    module.merge(consensus_rpc_for_api.into_rpc())?;
    
    // Register admin API if enabled
    if config.enable_admin {
        let consensus_rpc = Arc::new(consensus_rpc);
        let admin_rpc = ServiceConsensusAdminRpcImpl::new(consensus_rpc);
        module.merge(admin_rpc.into_rpc())?;
        info!("Admin RPC endpoints enabled");
    }
    
    // Start the server
    let server = ServerBuilder::default()
        .build(format!("{}:{}", config.host, config.port))
        .await?;
    
    let addr = server.local_addr()?;
    let handle = server.start(module);
    
    info!("Consensus RPC server listening on {}", addr);
    info!("Available namespaces: consensus{}", if config.enable_admin { ", consensus_admin" } else { "" });
    
    Ok(handle)
}