//! RPC API tests for Narwhal + Bullshark consensus
//! 
//! Tests for consensus-specific RPC endpoints including:
//! - Public consensus API (`consensus_*` namespace)
//! - Admin consensus API (`consensus_admin_*` namespace)
//! - Error handling and edge cases
//! - Performance and response times

use std::{sync::Arc, time::Duration, collections::HashMap, sync::Mutex};
use tokio::sync::RwLock;
use tempfile::TempDir;
use alloy_primitives::{Address, B256, TxHash};
use reth_consensus::{
    narwhal_bullshark::{
        integration::NarwhalRethBridge,
        validator_keys::{ValidatorKeyPair, ValidatorRegistry, ValidatorMetadata, ValidatorIdentity},
        types::NarwhalBullsharkConfig,
        integration::ServiceConfig,
    },
    consensus_storage::{MdbxConsensusStorage, DatabaseOps},
    rpc::{ConsensusRpcImpl, ConsensusAdminRpcImpl, ConsensusApiServer, ConsensusAdminApiServer},
};
use fastcrypto::traits::KeyPair;
use narwhal::types::Committee;

/// Mock database operations for testing
#[derive(Debug, Default)]
struct MockDatabaseOps {
    finalized_batches: Arc<Mutex<HashMap<u64, B256>>>,
    certificates: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    batches: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    dag_vertices: Arc<Mutex<HashMap<B256, Vec<u8>>>>,
    latest_finalized: Arc<Mutex<Option<u64>>>,
}

impl MockDatabaseOps {
    fn new() -> Self {
        Self::default()
    }
}

impl DatabaseOps for MockDatabaseOps {
    fn get_finalized_batch(&self, batch_id: u64) -> anyhow::Result<Option<B256>> {
        let batches = self.finalized_batches.lock().unwrap();
        Ok(batches.get(&batch_id).copied())
    }
    
    fn put_finalized_batch(&self, batch_id: u64, block_hash: B256) -> anyhow::Result<()> {
        let mut batches = self.finalized_batches.lock().unwrap();
        batches.insert(batch_id, block_hash);
        Ok(())
    }
    
    fn get_certificate(&self, cert_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let certs = self.certificates.lock().unwrap();
        Ok(certs.get(&cert_id).cloned())
    }
    
    fn put_certificate(&self, cert_id: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let mut certs = self.certificates.lock().unwrap();
        certs.insert(cert_id, data);
        Ok(())
    }
    
    fn get_batch(&self, batch_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let batches = self.batches.lock().unwrap();
        Ok(batches.get(&batch_id).cloned())
    }
    
    fn put_batch(&self, batch_id: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let mut batches = self.batches.lock().unwrap();
        batches.insert(batch_id, data);
        Ok(())
    }
    
    fn get_dag_vertex(&self, vertex_id: B256) -> anyhow::Result<Option<Vec<u8>>> {
        let vertices = self.dag_vertices.lock().unwrap();
        Ok(vertices.get(&vertex_id).cloned())
    }
    
    fn put_dag_vertex(&self, vertex_id: B256, data: Vec<u8>) -> anyhow::Result<()> {
        let mut vertices = self.dag_vertices.lock().unwrap();
        vertices.insert(vertex_id, data);
        Ok(())
    }
    
    fn get_latest_finalized(&self) -> anyhow::Result<Option<u64>> {
        let latest = self.latest_finalized.lock().unwrap();
        Ok(*latest)
    }
    
    fn put_latest_finalized(&self, cert_id: u64) -> anyhow::Result<()> {
        let mut latest = self.latest_finalized.lock().unwrap();
        *latest = Some(cert_id);
        Ok(())
    }
    
    fn list_finalized_batches(&self, _limit: Option<usize>) -> anyhow::Result<Vec<(u64, B256)>> {
        let batches = self.finalized_batches.lock().unwrap();
        Ok(batches.iter().map(|(k, v)| (*k, *v)).collect())
    }
    
    fn get_table_stats(&self) -> anyhow::Result<(u64, u64, u64)> {
        let certs = self.certificates.lock().unwrap().len() as u64;
        let batches = self.batches.lock().unwrap().len() as u64;
        let vertices = self.dag_vertices.lock().unwrap().len() as u64;
        Ok((certs, batches, vertices))
    }
}

/// RPC test harness for consensus API testing
struct ConsensusRpcTestHarness {
    /// Consensus RPC implementation
    consensus_rpc: Arc<ConsensusRpcImpl>,
    /// Admin RPC implementation
    admin_rpc: Arc<ConsensusAdminRpcImpl>,
    /// Temporary directory
    _temp_dir: TempDir,
}

impl ConsensusRpcTestHarness {
    async fn new() -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        
        // Create consensus storage
        let mut consensus_storage = MdbxConsensusStorage::new();
        // Set up mock database operations
        let mock_ops = Box::new(MockDatabaseOps::new());
        consensus_storage.set_db_ops(mock_ops);
        let consensus_storage = Arc::new(consensus_storage);

        // Create validator committee
        let (validator_registry, committee) = create_test_committee(4)?;
        let validator_registry = Arc::new(RwLock::new(validator_registry));

        // Create consensus bridge using proper ServiceConfig constructor (no networking for tests)
        let node_config = NarwhalBullsharkConfig::default();
        let service_config = ServiceConfig::new(node_config, committee);
        let consensus_bridge = Arc::new(RwLock::new(NarwhalRethBridge::new_for_testing(
            service_config,
            Some(consensus_storage.clone()),
        )?));

        // Create RPC implementations with new storage instance
        let mut rpc_storage = MdbxConsensusStorage::new();
        rpc_storage.set_db_ops(Box::new(MockDatabaseOps::new()));
        let consensus_rpc = Arc::new(ConsensusRpcImpl::new(
            consensus_bridge.clone(),
            validator_registry.clone(),
            Arc::new(RwLock::new(rpc_storage)),
        ));

        let admin_rpc = Arc::new(ConsensusAdminRpcImpl::new(consensus_rpc.clone()));

        Ok(Self {
            consensus_rpc,
            admin_rpc,
            _temp_dir: temp_dir,
        })
    }
}

/// Create a test validator committee
fn create_test_committee(count: usize) -> anyhow::Result<(ValidatorRegistry, Committee)> {
    let mut validator_registry = ValidatorRegistry::new();
    let mut stakes = HashMap::new();

    for i in 0..count {
        // Generate proper validator with both EVM and consensus keys
        let keypair = ValidatorKeyPair::generate()
            .map_err(|e| anyhow::anyhow!("Failed to generate validator keypair: {}", e))?;
        let evm_address = keypair.evm_address;
        
        let metadata = ValidatorMetadata {
            name: Some(format!("RPC Test Validator {}", i + 1)),
            description: Some(format!("Test validator {} for RPC testing", i + 1)),
            contact: Some(format!("rpc-test{}@example.com", i + 1)),
        };
        
        let identity = ValidatorIdentity {
            evm_address,
            consensus_public_key: keypair.consensus_keypair.public().clone(),
            metadata,
        };

        validator_registry.register_validator(identity)
            .map_err(|e| anyhow::anyhow!("Failed to register validator: {}", e))?;

        // Set stake amount
        stakes.insert(evm_address, 1000 + (i * 500) as u64);
    }

    // Create committee from stakes
    let committee = validator_registry.create_committee(0, &stakes)
        .map_err(|e| anyhow::anyhow!("Failed to create committee: {}", e))?;

    Ok((validator_registry, committee))
}

#[tokio::test]
async fn test_consensus_get_status() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call get_status RPC
    let status = harness.consensus_rpc.get_status().await?;
    
    // Verify status structure using actual field names
    assert!(status.healthy);
    assert_eq!(status.epoch, 0);
    assert_eq!(status.active_validators, 4);
    
    println!("✅ consensus_getStatus test passed");
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_committee() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call get_committee RPC
    let committee = harness.consensus_rpc.get_committee().await?;
    
    // Verify committee structure using actual field names
    assert_eq!(committee.epoch, 0);
    assert_eq!(committee.validators.len(), 4);
    assert_eq!(committee.total_validators, 4);
    
    println!("✅ consensus_getCommittee test passed");
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_validator() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Get committee to find a validator address
    let committee = harness.consensus_rpc.get_committee().await?;
    let test_validator = &committee.validators[0];
    
    // Call get_validator RPC
    let validator_info = harness.consensus_rpc.get_validator(test_validator.evm_address).await?;
    
    assert!(validator_info.is_some());
    let validator = validator_info.unwrap();
    
    // Verify validator details using actual field structure
    assert_eq!(validator.summary.evm_address, test_validator.evm_address);
    assert_eq!(validator.summary.consensus_key, test_validator.consensus_key);
    assert_eq!(validator.summary.stake, test_validator.stake);
    assert_eq!(validator.summary.active, test_validator.active);
    
    println!("✅ consensus_getValidator test passed - Validator: {:?}", validator.summary.name);
    Ok(())
}

#[tokio::test]
async fn test_consensus_list_validators() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Test listing all validators (uses 2 parameters: active_only, limit)
    let all_validators = harness.consensus_rpc.list_validators(None, None).await?;
    assert_eq!(all_validators.len(), 4);
    
    // Test listing active validators only
    let active_validators = harness.consensus_rpc.list_validators(Some(true), None).await?;
    assert_eq!(active_validators.len(), 4); // All should be active
    
    // Test listing with limit
    let limited_validators = harness.consensus_rpc.list_validators(None, Some(2)).await?;
    assert_eq!(limited_validators.len(), 2);
    
    println!("✅ consensus_listValidators test passed - Found {} validators", all_validators.len());
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_metrics() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call get_metrics RPC
    let metrics = harness.consensus_rpc.get_metrics().await?;
    
    // Verify metrics structure using actual field names
    assert!(metrics.throughput.tps_recent >= 0.0);
    assert!(metrics.throughput.tps_total >= 0.0);
    assert!(metrics.throughput.certificates_per_second >= 0.0);
    assert!(metrics.throughput.batches_per_second >= 0.0);
    
    assert!(metrics.latency.avg_finalization_time >= 0);
    assert!(metrics.latency.median_finalization_time >= 0);
    assert!(metrics.latency.p95_finalization_time >= 0);
    
    assert!(metrics.resources.memory_usage >= 0);
    assert!(metrics.resources.database_size >= 0);
    assert!(metrics.resources.cpu_usage >= 0.0);
    
    println!("✅ consensus_getMetrics test passed - Recent TPS: {}", metrics.throughput.tps_recent);
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_config() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call get_config RPC
    let config = harness.consensus_rpc.get_config().await?;
    
    // Verify config structure using actual field names
    assert!(config.algorithm.min_validators >= 1);
    assert!(config.algorithm.quorum_threshold > 0.0);
    assert!(config.network.listen_address.len() > 0);
    assert!(config.performance.worker_threads > 0);
    
    println!("✅ consensus_getConfig test passed - Min validators: {}", config.algorithm.min_validators);
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_recent_batches() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call get_recent_batches RPC (should be empty initially)
    let batches = harness.consensus_rpc.get_recent_batches(Some(10)).await?;
    
    // Should be empty for new consensus instance
    assert_eq!(batches.len(), 0);
    
    println!("✅ consensus_getRecentBatches test passed - Found {} recent batches", batches.len());
    Ok(())
}

#[tokio::test]
async fn test_consensus_get_transaction_status() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Test with a random transaction hash (should return None for non-existent)
    let random_tx_hash = TxHash::random();
    let status = harness.consensus_rpc.get_transaction_status(random_tx_hash).await?;
    
    // Should return None for non-existent transaction
    assert!(status.is_none());
    
    println!("✅ consensus_getTransactionStatus test passed - Returned None for non-existent transaction");
    Ok(())
}

#[tokio::test]
async fn test_admin_get_dag_info() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call admin get_dag_info RPC
    let dag_info = harness.admin_rpc.get_dag_info().await?;
    
    // Verify DAG info structure using actual field names
    assert!(dag_info.total_certificates >= 0);
    assert!(dag_info.current_height >= 0);
    assert!(dag_info.pending_certificates >= 0);
    assert!(dag_info.health_score >= 0.0);
    
    println!("✅ consensus_admin_getDagInfo test passed - Total certificates: {}, Height: {}", 
             dag_info.total_certificates, dag_info.current_height);
    Ok(())
}

#[tokio::test]
async fn test_admin_get_storage_stats() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call admin get_storage_stats RPC
    let stats = harness.admin_rpc.get_storage_stats().await?;
    
    // Verify storage stats structure using actual field names
    assert!(stats.file_size >= 0);
    assert!(stats.used_size >= 0);
    assert!(stats.free_size >= 0);
    assert!(stats.certificate_count >= 0);
    assert!(stats.batch_count >= 0);
    
    println!("✅ consensus_admin_getStorageStats test passed - File size: {} bytes", stats.file_size);
    Ok(())
}

#[tokio::test]
async fn test_admin_get_internal_state() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Call admin get_internal_state RPC
    let state = harness.admin_rpc.get_internal_state().await?;
    
    // Verify internal state structure using actual field names
    assert!(state.mode.len() > 0);
    assert!(state.state_vars.len() >= 0);
    assert!(state.recent_logs.len() >= 0);
    assert!(state.counters.len() >= 0);
    
    println!("✅ consensus_admin_getInternalState test passed - Mode: {}", state.mode);
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Test invalid validator address
    let invalid_address = Address::ZERO;
    let result = harness.consensus_rpc.get_validator(invalid_address).await?;
    assert!(result.is_none()); // Should return None for invalid address
    
    // Test invalid certificate ID
    let invalid_cert_id = B256::ZERO;
    let cert_result = harness.consensus_rpc.get_certificate(invalid_cert_id).await?;
    assert!(cert_result.is_none()); // Should return None for non-existent certificate
    
    // Test invalid batch ID
    let invalid_batch_id = u64::MAX;
    let batch_result = harness.consensus_rpc.get_finalized_batch(invalid_batch_id).await?;
    assert!(batch_result.is_none()); // Should return None for non-existent batch
    
    println!("✅ Error handling test passed - All invalid requests handled gracefully");
    Ok(())
}

#[tokio::test]
async fn test_rpc_performance() -> anyhow::Result<()> {
    let harness = ConsensusRpcTestHarness::new().await?;
    
    // Test response times for various endpoints
    let start = std::time::Instant::now();
    let _status = harness.consensus_rpc.get_status().await?;
    let status_time = start.elapsed();
    
    let start = std::time::Instant::now();
    let _committee = harness.consensus_rpc.get_committee().await?;
    let committee_time = start.elapsed();
    
    let start = std::time::Instant::now();
    let _metrics = harness.consensus_rpc.get_metrics().await?;
    let metrics_time = start.elapsed();
    
    let start = std::time::Instant::now();
    let _validators = harness.consensus_rpc.list_validators(None, None).await?;
    let validators_time = start.elapsed();
    
    // Verify reasonable response times (should be under 100ms for local calls)
    assert!(status_time < Duration::from_millis(100));
    assert!(committee_time < Duration::from_millis(100));
    assert!(metrics_time < Duration::from_millis(100));
    assert!(validators_time < Duration::from_millis(100));
    
    println!("✅ RPC performance test passed");
    println!("   - getStatus: {:?}", status_time);
    println!("   - getCommittee: {:?}", committee_time);
    println!("   - getMetrics: {:?}", metrics_time);
    println!("   - listValidators: {:?}", validators_time);
    
    Ok(())
} 