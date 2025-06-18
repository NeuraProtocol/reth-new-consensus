//! Integration tests for Narwhal DAG consensus

use narwhal::{
    DagService, types::{Vote, Committee, HeaderBuilder},
    NarwhalConfig, Transaction,
};
use tokio::sync::{mpsc, watch};
use std::collections::{HashMap, BTreeSet};
use std::time::Duration;
use fastcrypto::{traits::KeyPair, SignatureService};

/// Helper to create a test committee with multiple authorities
fn create_test_committee(size: usize) -> (Committee, Vec<fastcrypto::bls12381::BLS12381KeyPair>) {
    let mut authorities = HashMap::new();
    let mut keypairs = Vec::new();
    
    for _ in 0..size {
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        authorities.insert(keypair.public().clone(), 100);
        keypairs.push(keypair);
    }
    
    let committee = Committee::new(0, authorities);
    (committee, keypairs)
}

/// Helper to create test transactions
fn create_test_transactions(count: usize) -> Vec<Transaction> {
    (0..count)
        .map(|i| Transaction(format!("test_transaction_{}", i).into_bytes()))
        .collect()
}

fn create_signature_service() -> SignatureService<narwhal::types::Signature> {
    let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
    SignatureService::new(keypair)
}

#[tokio::test]
async fn test_dag_service_creation() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    let node_keypair = &keypairs[0];
    
    // Create channels
    let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
    let (cert_sender, _cert_receiver) = mpsc::unbounded_channel();
    let (committee_sender, committee_receiver) = watch::channel(committee.clone());
    
    // Create signature service
    let signature_service = create_signature_service();
    
    let dag_service = DagService::new(
        node_keypair.public().clone(),
        committee.clone(),
        config,
        signature_service,
        tx_receiver,
        cert_sender,
        committee_receiver,
    );

    // Test basic properties
    assert_eq!(dag_service.current_round(), 1);
    assert_eq!(dag_service.name, node_keypair.public().clone());
    assert_eq!(dag_service.committee.epoch, 0);
    
    // Send some test transactions
    let transactions = create_test_transactions(5);
    for tx in transactions {
        tx_sender.send(tx).expect("Failed to send transaction");
    }
    
    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    drop(committee_sender); // Clean up
}

#[tokio::test]
async fn test_dag_service_lifecycle() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    let node_keypair = &keypairs[0];
    
    // Create channels
    let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
    let (cert_sender, _cert_receiver) = mpsc::unbounded_channel();
    let (committee_sender, committee_receiver) = watch::channel(committee.clone());
    
    // Create signature service
    let signature_service = create_signature_service();
    
    let dag_service = DagService::new(
        node_keypair.public().clone(),
        committee.clone(),
        config,
        signature_service,
        tx_receiver,
        cert_sender,
        committee_receiver,
    );

    // Spawn the service
    let service_handle = dag_service.spawn();
    
    // Send some transactions
    let transactions = create_test_transactions(3);
    for tx in transactions {
        tx_sender.send(tx).expect("Failed to send transaction");
    }
    
    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Clean shutdown
    drop(tx_sender);
    drop(committee_sender);
    
    // Wait for service to complete
    let result = tokio::time::timeout(Duration::from_secs(1), service_handle).await;
    assert!(result.is_ok(), "Service should complete within timeout");
}

#[tokio::test]
async fn test_vote_creation_and_structure() {
    let (_committee, keypairs) = create_test_committee(4);
    let node_keypair = &keypairs[0];
    
    // Create a test header
    let header = HeaderBuilder::default()
        .author(node_keypair.public().clone())
        .round(1)
        .epoch(0)
        .payload(indexmap::IndexMap::new())
        .parents(BTreeSet::new())
        .build(node_keypair)
        .expect("Failed to build header");
    
    // Create a vote for the header
    let vote = Vote::new(&header, &keypairs[1].public());
    
    assert_eq!(vote.id, header.id);
    assert_eq!(vote.round, header.round);
    assert_eq!(vote.epoch, header.epoch);
    assert_eq!(vote.origin, header.author);
    assert_eq!(vote.author, keypairs[1].public().clone());
}

#[tokio::test]
async fn test_committee_operations() {
    let (committee, keypairs) = create_test_committee(4);
    
    // Test stake queries
    for keypair in &keypairs {
        let stake = committee.stake(&keypair.public());
        assert_eq!(stake, 100);
    }
    
    // Test thresholds
    let quorum_threshold = committee.quorum_threshold();
    let validity_threshold = committee.validity_threshold();
    
    assert_eq!(quorum_threshold, 267); // (400 * 2) / 3 + 1 = 267
    assert_eq!(validity_threshold, 134); // 400 / 3 + 1 = 134
    
    // Test leader selection
    let leader_round_0 = committee.leader(0);
    let leader_round_1 = committee.leader(1);
    
    // Leaders should be deterministic but may be different for different rounds
    assert!(committee.authorities.contains_key(leader_round_0));
    assert!(committee.authorities.contains_key(leader_round_1));
}

#[tokio::test]
async fn test_round_advancement() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    let node_keypair = &keypairs[0];
    
    // Create channels
    let (_tx_sender, tx_receiver) = mpsc::unbounded_channel();
    let (_cert_sender, _cert_receiver) = mpsc::unbounded_channel();
    let (_committee_sender, committee_receiver) = watch::channel(committee.clone());
    
    // Create signature service
    let signature_service = create_signature_service();
    
    let mut dag_service = DagService::new(
        node_keypair.public().clone(),
        committee.clone(),
        config,
        signature_service,
        tx_receiver,
        _cert_sender,
        committee_receiver,
    );

    // Test initial round
    assert_eq!(dag_service.current_round(), 1);
    
    // Advance round
    dag_service.advance_round();
    assert_eq!(dag_service.current_round(), 2);
    
    // Advance again
    dag_service.advance_round();
    assert_eq!(dag_service.current_round(), 3);
}

#[tokio::test]
async fn test_transaction_serialization() {
    // Test Transaction creation and serialization
    let tx_data = b"test transaction data";
    let tx = Transaction(tx_data.to_vec());
    
    assert_eq!(tx.as_bytes(), tx_data);
    
    // Test creating from different data
    let tx2 = Transaction("different data".as_bytes().to_vec());
    assert_ne!(tx.as_bytes(), tx2.as_bytes());
}

#[tokio::test]
async fn test_committee_reconfiguration() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    let node_keypair = &keypairs[0];
    
    // Create channels
    let (_tx_sender, tx_receiver) = mpsc::unbounded_channel();
    let (_cert_sender, _cert_receiver) = mpsc::unbounded_channel();
    let (committee_sender, committee_receiver) = watch::channel(committee.clone());
    
    // Create signature service
    let signature_service = create_signature_service();
    
    let dag_service = DagService::new(
        node_keypair.public().clone(),
        committee.clone(),
        config,
        signature_service,
        tx_receiver,
        _cert_sender,
        committee_receiver,
    );

    let service_handle = dag_service.spawn();
    
    // Create a new committee
    let (new_committee, _new_keypairs) = create_test_committee(6);
    let mut new_committee = new_committee;
    new_committee.epoch = 1; // Different epoch
    
    // Send committee update
    committee_sender.send(new_committee).expect("Failed to send committee update");
    
    // Let it process
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Clean shutdown
    drop(committee_sender);
    
    let result = tokio::time::timeout(Duration::from_secs(1), service_handle).await;
    assert!(result.is_ok(), "Service should handle committee updates");
}

#[tokio::test]
async fn test_header_creation_and_properties() {
    let (_committee, keypairs) = create_test_committee(4);
    let node_keypair = &keypairs[0];
    
    // Create a test header with some payload
    let mut payload = indexmap::IndexMap::new();
    payload.insert(narwhal::BatchDigest::default(), 1);
    
    let header = HeaderBuilder::default()
        .author(node_keypair.public().clone())
        .round(5)
        .epoch(0)
        .payload(payload.clone())
        .parents(BTreeSet::new())
        .build(node_keypair)
        .expect("Failed to build header");
    
    assert_eq!(header.author, node_keypair.public().clone());
    assert_eq!(header.round, 5);
    assert_eq!(header.epoch, 0);
    assert_eq!(header.payload, payload);
    assert!(header.parents.is_empty());
    
    // The digest should be computed
    assert_ne!(header.id, narwhal::types::HeaderDigest::default());
}

#[tokio::test]
async fn test_multiple_nodes_basic_setup() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    
    let mut service_handles = Vec::new();
    let mut committee_senders = Vec::new();
    
    // Create multiple DAG services
    for i in 0..4 {
        let (_tx_sender, tx_receiver) = mpsc::unbounded_channel();
        let (_cert_sender, _cert_receiver) = mpsc::unbounded_channel();
        let (committee_sender, committee_receiver) = watch::channel(committee.clone());
        
        let signature_service = create_signature_service();
        
        let dag_service = DagService::new(
            keypairs[i].public().clone(),
            committee.clone(),
            config.clone(),
            signature_service,
            tx_receiver,
            _cert_sender,
            committee_receiver,
        );
        
        let handle = dag_service.spawn();
        
        committee_senders.push(committee_sender);
        service_handles.push(handle);
    }
    
    // Let them initialize
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Clean shutdown
    for committee_sender in committee_senders {
        drop(committee_sender);
    }
    
    // Wait for all services to complete
    for handle in service_handles {
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "Service should complete within timeout");
    }
}

#[tokio::test]
async fn test_transaction_processing_flow() {
    let (committee, keypairs) = create_test_committee(4);
    let config = NarwhalConfig::default();
    let node_keypair = &keypairs[0];
    
    // Create channels
    let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
    let (_cert_sender, _cert_receiver) = mpsc::unbounded_channel();
    let (committee_sender, committee_receiver) = watch::channel(committee.clone());
    
    // Create signature service
    let signature_service = create_signature_service();
    
    let dag_service = DagService::new(
        node_keypair.public().clone(),
        committee.clone(),
        config,
        signature_service,
        tx_receiver,
        _cert_sender,
        committee_receiver,
    );

    let service_handle = dag_service.spawn();
    
    // Send a batch of transactions
    let transactions = create_test_transactions(10);
    for tx in transactions {
        tx_sender.send(tx).expect("Failed to send transaction");
    }
    
    // Let the service process the transactions
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Clean shutdown
    drop(tx_sender);
    drop(committee_sender);
    
    let result = tokio::time::timeout(Duration::from_secs(1), service_handle).await;
    assert!(result.is_ok(), "Service should process transactions and complete");
} 