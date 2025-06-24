//! Consensus Service Integration Tests for Narwhal + Bullshark
//! 
//! Tests for core consensus service functionality including:
//! - Basic service components
//! - Configuration validation
//! - Error handling

use std::{sync::Arc, collections::HashMap, sync::Mutex};
use alloy_primitives::{B256, Address};
use reth_consensus::{
    consensus_storage::{MdbxConsensusStorage, DatabaseOps},
    narwhal_bullshark::mempool_bridge::{MempoolBridge, PoolStats},
};

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
    
    fn get_dag_vertex(&self, hash: B256) -> anyhow::Result<Option<Vec<u8>>> {
        let vertices = self.dag_vertices.lock().unwrap();
        Ok(vertices.get(&hash).cloned())
    }
    
    fn put_dag_vertex(&self, hash: B256, data: Vec<u8>) -> anyhow::Result<()> {
        let mut vertices = self.dag_vertices.lock().unwrap();
        vertices.insert(hash, data);
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
    
    fn list_finalized_batches(&self, limit: Option<usize>) -> anyhow::Result<Vec<(u64, B256)>> {
        let batches = self.finalized_batches.lock().unwrap();
        let mut results: Vec<(u64, B256)> = batches.iter().map(|(&k, &v)| (k, v)).collect();
        results.sort_by_key(|(k, _)| *k);
        
        if let Some(limit) = limit {
            results.truncate(limit);
        }
        
        Ok(results)
    }
    
    fn get_table_stats(&self) -> anyhow::Result<(u64, u64, u64)> {
        let certs_count = self.certificates.lock().unwrap().len() as u64;
        let batches_count = self.batches.lock().unwrap().len() as u64;
        let vertices_count = self.dag_vertices.lock().unwrap().len() as u64;
        Ok((certs_count, batches_count, vertices_count))
    }
}

/// Test helper to create a basic transaction hash
fn create_test_hash() -> B256 {
    B256::random()
}

/// Test helper to create consensus storage with mock operations
fn create_test_consensus_storage() -> MdbxConsensusStorage {
    let mut storage = MdbxConsensusStorage::new();
    
    // Inject mock database operations
    let mock_ops = Box::new(MockDatabaseOps::new());
    storage.set_db_ops(mock_ops);
    
    storage
}

#[test]
fn test_mempool_bridge_basic() -> anyhow::Result<()> {
    // Create test components
    let (tx_sender, _tx_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (_batch_sender, batch_receiver) = tokio::sync::mpsc::unbounded_channel();
    
    // Create the mempool bridge
    let mempool_bridge = MempoolBridge::new(tx_sender, batch_receiver);
    
    // Verify initialization
    let stats = mempool_bridge.get_pool_stats();
    assert_eq!(stats.pending_transactions, 0);
    assert_eq!(stats.queued_transactions, 0);
    
    println!("✅ Mempool bridge basic test passed");
    Ok(())
}

#[test]
fn test_pool_stats_structure() -> anyhow::Result<()> {
    // Test the PoolStats structure
    let stats = PoolStats {
        pending_transactions: 10,
        queued_transactions: 5,
        total_transactions: 15,
        current_block_number: 100,
        current_block_hash: B256::random(),
        processed_hashes_count: 50,
    };
    
    // Verify field access
    assert_eq!(stats.pending_transactions, 10);
    assert_eq!(stats.queued_transactions, 5);
    assert_eq!(stats.total_transactions, 15);
    assert_eq!(stats.current_block_number, 100);
    assert_eq!(stats.processed_hashes_count, 50);
    
    println!("✅ Pool stats structure test passed");
    Ok(())
}

#[test]
fn test_consensus_storage_basic() -> anyhow::Result<()> {
    let storage = create_test_consensus_storage();
    
    // Test storing a certificate
    let cert_id = 1u64;
    let cert_data = vec![1, 2, 3, 4, 5];
    
    storage.store_certificate(cert_id, cert_data.clone())?;
    
    // Retrieve and verify
    let retrieved = storage.get_certificate(cert_id)?;
    assert_eq!(retrieved, Some(cert_data));
    
    // Test finalized batch storage
    let batch_id = 1u64;
    let block_hash = B256::random();
    
    storage.record_finalized_batch(batch_id, block_hash)?;
    
    // Retrieve and verify
    let retrieved_batch = storage.get_finalized_batch(batch_id)?;
    assert_eq!(retrieved_batch, Some(block_hash));
    
    println!("✅ Consensus storage basic test passed");
    Ok(())
}

#[test]
fn test_hash_generation() -> anyhow::Result<()> {
    // Test that we can generate and use hashes
    let hash1 = create_test_hash();
    let hash2 = create_test_hash();
    
    // Hashes should be different (extremely high probability)
    assert_ne!(hash1, hash2);
    
    // Hashes should be the right size
    assert_eq!(hash1.as_slice().len(), 32);
    assert_eq!(hash2.as_slice().len(), 32);
    
    println!("✅ Hash generation test passed");
    Ok(())
}

#[test]
fn test_storage_error_handling() -> anyhow::Result<()> {
    let storage = create_test_consensus_storage();
    
    // Try to get non-existent certificate
    let non_existent_cert = storage.get_certificate(999u64)?;
    assert_eq!(non_existent_cert, None);
    
    // Try to get non-existent batch
    let non_existent_batch = storage.get_finalized_batch(999u64)?;
    assert_eq!(non_existent_batch, None);
    
    // Try to get non-existent DAG vertex
    let non_existent_vertex = storage.get_dag_vertex(B256::random())?;
    assert_eq!(non_existent_vertex, None);
    
    println!("✅ Storage error handling test passed");
    Ok(())
}

#[test]
fn test_latest_finalized_tracking() -> anyhow::Result<()> {
    let storage = create_test_consensus_storage();
    
    // Initially should be None
    let initial = storage.get_latest_finalized()?;
    assert_eq!(initial, None);
    
    // Set latest finalized
    let cert_id = 42u64;
    storage.set_latest_finalized(cert_id)?;
    
    // Retrieve latest finalized
    let retrieved = storage.get_latest_finalized()?;
    assert_eq!(retrieved, Some(cert_id));
    
    // Update latest finalized
    let new_cert_id = 84u64;
    storage.set_latest_finalized(new_cert_id)?;
    
    // Retrieve updated latest finalized
    let updated = storage.get_latest_finalized()?;
    assert_eq!(updated, Some(new_cert_id));
    
    println!("✅ Latest finalized tracking test passed");
    Ok(())
}

#[test]
fn test_dag_vertex_storage() -> anyhow::Result<()> {
    let storage = create_test_consensus_storage();
    
    // Test storing DAG vertices
    let hash_1 = B256::random();
    let vertex_data_1 = vec![21, 22, 23, 24, 25];
    let hash_2 = B256::random();
    let vertex_data_2 = vec![26, 27, 28, 29, 30];
    
    // Store vertices
    storage.store_dag_vertex(hash_1, vertex_data_1.clone())?;
    storage.store_dag_vertex(hash_2, vertex_data_2.clone())?;
    
    // Retrieve vertices
    let retrieved_1 = storage.get_dag_vertex(hash_1)?;
    let retrieved_2 = storage.get_dag_vertex(hash_2)?;
    
    assert_eq!(retrieved_1, Some(vertex_data_1));
    assert_eq!(retrieved_2, Some(vertex_data_2));
    
    println!("✅ DAG vertex storage test passed");
    Ok(())
}

#[test]
fn test_storage_statistics() -> anyhow::Result<()> {
    let storage = create_test_consensus_storage();
    
    // Get initial statistics
    let initial_stats = storage.get_stats()?;
    
    // Add some data
    storage.store_certificate(1, vec![1, 2, 3])?;
    storage.store_certificate(2, vec![4, 5, 6])?;
    storage.store_batch(1, vec![7, 8, 9])?;
    storage.store_dag_vertex(B256::random(), vec![10, 11, 12])?;
    
    // Get updated statistics
    let updated_stats = storage.get_stats()?;
    
    // Verify we can access the stats
    println!("✅ Storage statistics test passed");
    println!("   - Initial: {:?}", initial_stats);
    println!("   - Updated: {:?}", updated_stats);
    Ok(())
}

#[test]
fn test_integration_basic_workflow() -> anyhow::Result<()> {
    // Create consensus storage
    let storage = create_test_consensus_storage();
    
    // Create mempool bridge
    let (tx_sender, mut _tx_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (_batch_sender, batch_receiver) = tokio::sync::mpsc::unbounded_channel();
    let mempool_bridge = MempoolBridge::new(tx_sender, batch_receiver);
    
    // Simulate a basic workflow
    
    // 1. Store some consensus data
    let cert_id = 1u64;
    let cert_data = vec![1, 2, 3, 4, 5];
    storage.store_certificate(cert_id, cert_data.clone())?;
    
    // 2. Check mempool stats
    let stats = mempool_bridge.get_pool_stats();
    
    // 3. Store a finalized batch
    let batch_id = 1u64;
    let block_hash = B256::random();
    storage.record_finalized_batch(batch_id, block_hash)?;
    
    // 4. Verify data integrity
    let retrieved_cert = storage.get_certificate(cert_id)?;
    assert_eq!(retrieved_cert, Some(cert_data));
    
    let retrieved_batch = storage.get_finalized_batch(batch_id)?;
    assert_eq!(retrieved_batch, Some(block_hash));
    
    // 5. Check final stats
    let final_stats = storage.get_stats()?;
    
    println!("✅ Integration basic workflow test passed");
    println!("   - Certificate stored and retrieved: {}", cert_id);
    println!("   - Batch stored and retrieved: {}", batch_id);
    println!("   - Mempool stats: pending={}, queued={}", stats.pending_transactions, stats.queued_transactions);
    println!("   - Storage stats: {:?}", final_stats);
    Ok(())
} 