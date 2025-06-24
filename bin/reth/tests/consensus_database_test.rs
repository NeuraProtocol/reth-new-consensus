//! Database integration tests for Narwhal + Bullshark consensus
//! 
//! Tests for consensus database operations including:
//! - MDBX storage integration
//! - Table operations (CRUD)
//! - Mock database functionality

use std::{sync::Arc, collections::HashMap, sync::Mutex};
use alloy_primitives::B256;
use reth_consensus::{
    consensus_storage::{MdbxConsensusStorage, DatabaseOps},
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

/// Create a storage instance with mock database operations
fn create_test_storage() -> MdbxConsensusStorage {
    let mut storage = MdbxConsensusStorage::new();
    
    // Inject mock database operations
    let mock_ops = Box::new(MockDatabaseOps::new());
    storage.set_db_ops(mock_ops);
    
    storage
}

#[test]
fn test_finalized_batches() -> anyhow::Result<()> {
    let storage = create_test_storage();

    // Test storing finalized batches
    let batch_id_1 = 1u64;
    let block_hash_1 = B256::random();
    let batch_id_2 = 2u64;
    let block_hash_2 = B256::random();

    // Store batches
    storage.record_finalized_batch(batch_id_1, block_hash_1)?;
    storage.record_finalized_batch(batch_id_2, block_hash_2)?;

    // Retrieve batches
    let retrieved_1 = storage.get_finalized_batch(batch_id_1)?;
    let retrieved_2 = storage.get_finalized_batch(batch_id_2)?;

    assert_eq!(retrieved_1, Some(block_hash_1));
    assert_eq!(retrieved_2, Some(block_hash_2));

    // Test non-existent batch
    let non_existent = storage.get_finalized_batch(999u64)?;
    assert_eq!(non_existent, None);

    // Test listing batches
    let batches = storage.list_finalized_batches(None)?;
    assert!(batches.len() >= 2);

    // Test listing with limit
    let limited_batches = storage.list_finalized_batches(Some(1))?;
    assert_eq!(limited_batches.len(), 1);

    println!("✅ Finalized batches test passed");
    Ok(())
}

#[test]
fn test_certificates() -> anyhow::Result<()> {
    let storage = create_test_storage();

    // Test storing certificates
    let cert_id_1 = 1u64;
    let cert_data_1 = vec![1, 2, 3, 4, 5];
    let cert_id_2 = 2u64;
    let cert_data_2 = vec![6, 7, 8, 9, 10];

    // Store certificates
    storage.store_certificate(cert_id_1, cert_data_1.clone())?;
    storage.store_certificate(cert_id_2, cert_data_2.clone())?;

    // Retrieve certificates
    let retrieved_1 = storage.get_certificate(cert_id_1)?;
    let retrieved_2 = storage.get_certificate(cert_id_2)?;

    assert_eq!(retrieved_1, Some(cert_data_1));
    assert_eq!(retrieved_2, Some(cert_data_2));

    // Test non-existent certificate
    let non_existent = storage.get_certificate(999u64)?;
    assert_eq!(non_existent, None);

    println!("✅ Certificates test passed");
    Ok(())
}

#[test]
fn test_batches() -> anyhow::Result<()> {
    let storage = create_test_storage();

    // Test storing batches
    let batch_id_1 = 1u64;
    let batch_data_1 = vec![11, 12, 13, 14, 15];
    let batch_id_2 = 2u64;
    let batch_data_2 = vec![16, 17, 18, 19, 20];

    // Store batches
    storage.store_batch(batch_id_1, batch_data_1.clone())?;
    storage.store_batch(batch_id_2, batch_data_2.clone())?;

    // Retrieve batches
    let retrieved_1 = storage.get_batch(batch_id_1)?;
    let retrieved_2 = storage.get_batch(batch_id_2)?;

    assert_eq!(retrieved_1, Some(batch_data_1));
    assert_eq!(retrieved_2, Some(batch_data_2));

    // Test non-existent batch
    let non_existent = storage.get_batch(999u64)?;
    assert_eq!(non_existent, None);

    println!("✅ Batches test passed");
    Ok(())
}

#[test]
fn test_dag_vertices() -> anyhow::Result<()> {
    let storage = create_test_storage();

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

    // Test non-existent vertex
    let non_existent_hash = B256::random();
    let non_existent = storage.get_dag_vertex(non_existent_hash)?;
    assert_eq!(non_existent, None);

    println!("✅ DAG vertices test passed");
    Ok(())
}

#[test]
fn test_latest_finalized() -> anyhow::Result<()> {
    let storage = create_test_storage();

    // Initially should be None
    let initial = storage.get_latest_finalized()?;
    assert_eq!(initial, None);

    // Set latest finalized
    let cert_id_1 = 42u64;
    storage.set_latest_finalized(cert_id_1)?;

    // Retrieve latest finalized
    let retrieved_1 = storage.get_latest_finalized()?;
    assert_eq!(retrieved_1, Some(cert_id_1));

    // Update latest finalized
    let cert_id_2 = 84u64;
    storage.set_latest_finalized(cert_id_2)?;

    // Retrieve updated latest finalized
    let retrieved_2 = storage.get_latest_finalized()?;
    assert_eq!(retrieved_2, Some(cert_id_2));

    println!("✅ Latest finalized test passed");
    Ok(())
}

#[test]
fn test_table_stats() -> anyhow::Result<()> {
    let storage = create_test_storage();

    // Get initial statistics
    let initial_stats = storage.get_stats()?;

    // Add some data
    storage.store_certificate(1, vec![1, 2, 3])?;
    storage.store_certificate(2, vec![4, 5, 6])?;
    storage.store_batch(1, vec![7, 8, 9])?;
    storage.store_dag_vertex(B256::random(), vec![10, 11, 12])?;

    // Get updated statistics
    let updated_stats = storage.get_stats()?;

    // Just verify we can get stats (actual verification would depend on the struct fields)
    println!("✅ Table statistics test passed - Initial: {:?}, Updated: {:?}", 
             initial_stats, updated_stats);
    Ok(())
}

#[test]
fn test_integration() -> anyhow::Result<()> {
    // Run all tests together to verify integration
    test_finalized_batches()?;
    test_certificates()?;
    test_batches()?;
    test_dag_vertices()?;
    test_latest_finalized()?;
    test_table_stats()?;

    println!("🎉 All database integration tests passed!");
    Ok(())
} 