//! Tests for Bullshark BFT consensus

use bullshark::{
    BftConfig, BullsharkConsensus, BftService,
    consensus::{ConsensusProtocol, InMemoryStorage},
    ConsensusStorage,
    dag::{BullsharkDag},
    finality::FinalityEngine,
};
use narwhal::{
    types::{Certificate, Committee, PublicKey},
    Round, Transaction as NarwhalTransaction,
};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::time::Duration;
use alloy_primitives::B256;
use tracing::info;
use fastcrypto::{traits::KeyPair, Hash};
use rand_08;

/// Helper to create test committee
fn create_test_committee(size: usize) -> (Committee, Vec<fastcrypto::bls12381::BLS12381KeyPair>) {
    use narwhal::types::{Authority, WorkerConfiguration};
    let mut authorities = HashMap::new();
    let mut keypairs = Vec::new();
    
    for i in 0..size {
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        let authority = Authority {
            stake: 100,
            primary_address: format!("127.0.0.1:{}", 8000 + i),
            network_key: keypair.public().clone(),
            workers: WorkerConfiguration {
                num_workers: 1,
                base_port: 10000 + (i * 100) as u16,
                base_address: "127.0.0.1".to_string(),
            },
        };
        authorities.insert(keypair.public().clone(), authority);
        keypairs.push(keypair);
    }
    
    (Committee::new(0, authorities), keypairs)
}

/// Helper to create test certificates
fn create_test_certificates(
    committee: &Committee,
    round: Round,
    keypairs: &[fastcrypto::bls12381::BLS12381KeyPair],
) -> Vec<Certificate> {
    let mut certificates = Vec::new();
    
    for keypair in keypairs {
        let certificate = create_mock_certificate(
            round,
            keypair.public().clone(),
            vec![],
        );
        certificates.push(certificate);
    }
    
    certificates
}

/// Create a mock certificate for testing
fn create_mock_certificate(
    round: Round,
    author: PublicKey,
    _payload: Vec<(narwhal::BatchDigest, narwhal::WorkerId)>,
) -> Certificate {
    use narwhal::types::{HeaderBuilder};
    use std::collections::BTreeSet;
    
    let header = HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(0)
        .payload(indexmap::IndexMap::new())
        .parents(BTreeSet::new())
        .build(&fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng()))
        .expect("Failed to build header");
    
    // Create dummy certificate (in real implementation, would have proper signatures)
    // For now, create a genesis certificate and replace the header
    use narwhal::types::{Authority, WorkerConfiguration};
    let authority = Authority {
        stake: 100,
        primary_address: "127.0.0.1:8000".to_string(),
        network_key: author.clone(),
        workers: WorkerConfiguration {
            num_workers: 1,
            base_port: 10000,
            base_address: "127.0.0.1".to_string(),
        },
    };
    let mut cert = Certificate::genesis(&Committee::new(0, [(author.clone(), authority)].into_iter().collect()))[0].clone();
    cert.header = header;
    cert
}

#[tokio::test]
async fn test_bullshark_dag_creation() {
    let (committee, _keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    
    let dag = BullsharkDag::new(genesis_certificates);
    let stats = dag.stats();
    
    assert_eq!(stats.total_rounds, 1); // Genesis round
    assert_eq!(stats.highest_round, 0);
    assert_eq!(stats.last_committed_round, 0);
    
    info!("Bullshark DAG creation test passed");
}

#[tokio::test]
async fn test_certificate_insertion() {
    let (committee, keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Create certificates for round 1
    let certificates = create_test_certificates(&committee, 1, &keypairs);
    
    for certificate in certificates {
        dag.insert_certificate(certificate).expect("Failed to insert certificate");
    }
    
    let stats = dag.stats();
    assert_eq!(stats.total_rounds, 2); // Genesis + round 1
    assert_eq!(stats.highest_round, 1);
    
    // Check we can retrieve certificates
    let round_1_certs = dag.get_certificates_at_round(1);
    assert_eq!(round_1_certs.len(), 4);
    
    info!("Certificate insertion test passed");
}

#[tokio::test]
async fn test_quorum_detection() {
    let (committee, keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Insert certificates one by one and check quorum
    let certificates = create_test_certificates(&committee, 2, &keypairs);
    
    // Insert first 2 certificates (not enough for quorum with 4 validators)
    for (i, certificate) in certificates.iter().take(2).enumerate() {
        dag.insert_certificate(certificate.clone()).expect("Failed to insert certificate");
        let has_quorum = dag.has_quorum_at_round(2, &committee);
        info!("After inserting {} certificates, has quorum: {}", i + 1, has_quorum);
    }
    
    // Insert third certificate (should reach quorum threshold)
    dag.insert_certificate(certificates[2].clone()).expect("Failed to insert certificate");
    let has_quorum = dag.has_quorum_at_round(2, &committee);
    assert!(has_quorum, "Should have quorum after 3 certificates");
    
    info!("Quorum detection test passed");
}

#[tokio::test]
async fn test_leader_selection() {
    let (committee, keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Insert certificates for even round (leader round)
    let certificates = create_test_certificates(&committee, 2, &keypairs);
    for certificate in certificates {
        dag.insert_certificate(certificate).expect("Failed to insert certificate");
    }
    
    // Test leader selection for even round 2
    let leader = dag.get_leader_certificate(2, &committee);
    assert!(leader.is_some(), "Should have leader for even round");
    
    let leader_cert = leader.unwrap();
    assert_eq!(leader_cert.round(), 2);
    
    // Test leader selection for odd round (should be None)
    let odd_leader = dag.get_leader_certificate(1, &committee);
    assert!(odd_leader.is_none(), "Should not have leader for odd round");
    
    info!("Leader selection test passed");
}

#[tokio::test]
async fn test_bullshark_consensus_algorithm() {
    let (committee, keypairs) = create_test_committee(4);
    let config = BftConfig::default();
    let mut consensus = BullsharkConsensus::new(committee.clone(), config);
    
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Create certificates for multiple rounds
    for round in 1..=4 {
        let certificates = create_test_certificates(&committee, round, &keypairs);
        for certificate in certificates {
            dag.insert_certificate(certificate).expect("Failed to insert certificate");
        }
    }
    
    // Process a certificate through consensus
    let test_certificate = create_test_certificates(&committee, 4, &keypairs)[0].clone();
    let outputs = consensus.process_certificate(&mut dag, 0, test_certificate)
        .expect("Failed to process certificate");
    
    info!("Bullshark consensus produced {} outputs", outputs.len());
    
    info!("Bullshark consensus algorithm test passed");
}

#[tokio::test]
async fn test_finality_engine() {
    let mut finality_engine = FinalityEngine::new(2, 10); // Need 2 confirmations
    
    // Test initial state
    assert_eq!(finality_engine.current_block_number(), 1);
    
    // Add certificates for finality consideration
    let (committee, keypairs) = create_test_committee(4);
    let certificates = create_test_certificates(&committee, 2, &keypairs); // Even round for finalization
    
    for certificate in certificates {
        finality_engine.add_certificate(certificate).await
            .expect("Failed to add certificate");
    }
    
    // Check finality (need a proper DAG for real finality check)
    let genesis_certificates = Certificate::genesis(&committee);
    let dag = BullsharkDag::new(genesis_certificates);
    
    let finalized_batches = finality_engine.check_finality(&dag, &committee).await
        .expect("Failed to check finality");
    
    let stats = finality_engine.stats();
    info!("Finality engine stats: {:?}", stats);
    
    info!("Finality engine test passed");
}

#[tokio::test]
async fn test_bft_service_lifecycle() {
    let (committee, keypairs) = create_test_committee(4);
    let config = BftConfig::default();
    let node_key = keypairs[0].public().clone();
    
    let (cert_sender, cert_receiver) = mpsc::unbounded_channel();
    let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel();
    
    let service = BftService::new(config, committee.clone(), cert_receiver, batch_sender);
    let service_handle = service.spawn();
    
    // Send a test certificate to verify the service can process it
    let test_certificate = create_test_certificates(
        &committee,
        2,
        &keypairs[0..1],
    )[0].clone();
    
    // Send the certificate
    cert_sender.send(test_certificate).expect("Failed to send certificate");
    
    // Give the service time to process the certificate
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Try to receive any finalized batches (there might not be any in this simple test)
    while let Ok(_batch) = batch_receiver.try_recv() {
        info!("Received finalized batch from service");
    }
    
    // Close the channel by dropping the sender
    drop(cert_sender);
    
    // The service should detect the closed channel and shutdown gracefully
    let result = tokio::time::timeout(Duration::from_secs(2), service_handle).await;
    assert!(result.is_ok(), "Service should complete within timeout");
    
    // Verify the result is Ok (no errors during shutdown)
    let service_result = result.unwrap();
    assert!(service_result.is_ok(), "Service should shut down without errors");
    
    info!("BFT service lifecycle test passed");
}

#[tokio::test]
async fn test_committee_update() {
    let (committee, _keypairs) = create_test_committee(4);
    let config = BftConfig::default();
    let mut consensus = BullsharkConsensus::new(committee.clone(), config);
    
    // Create new committee with different epoch
    let (new_committee, _) = create_test_committee(6);
    let mut new_committee = new_committee;
    new_committee.epoch = 1; // Different epoch
    
    // Update committee
    consensus.update_committee(new_committee.clone())
        .expect("Failed to update committee");
    
    info!("Committee update test passed");
}

#[tokio::test]
async fn test_garbage_collection() {
    let (committee, keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Insert certificates for many rounds
    for round in 1..=10 {
        let certificates = create_test_certificates(&committee, round, &keypairs);
        for certificate in certificates {
            dag.insert_certificate(certificate.clone()).expect("Failed to insert certificate");
            // Simulate committing this certificate
            dag.update_last_committed(&certificate, 3); // GC depth of 3
        }
    }
    
    let stats = dag.stats();
    info!("DAG stats after GC: {:?}", stats);
    
    // Should have garbage collected old rounds
    assert!(stats.total_rounds <= 5, "Should have garbage collected old rounds");
    
    info!("Garbage collection test passed");
}

#[tokio::test]
async fn test_leader_support_validation() {
    let (committee, keypairs) = create_test_committee(4);
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Create leader certificate for round 2
    let leader_cert = create_test_certificates(&committee, 2, &keypairs[0..1])[0].clone();
    dag.insert_certificate(leader_cert.clone()).expect("Failed to insert leader certificate");
    
    // Create child certificates in round 3 that reference the leader
    let child_certificates = create_test_certificates(&committee, 3, &keypairs);
    for certificate in child_certificates {
        dag.insert_certificate(certificate).expect("Failed to insert child certificate");
    }
    
    // Check if leader has support
    let leader_digest = leader_cert.digest();
    let has_support = dag.leader_has_support(&leader_digest, 2, &committee);
    
    info!("Leader has support: {}", has_support);
    
    info!("Leader support validation test passed");
}

#[tokio::test]
async fn test_consensus_metrics() {
    use bullshark::consensus::ConsensusMetrics;
    
    let mut metrics = ConsensusMetrics::default();
    
    // Record some finalization events
    metrics.record_finalization(5, 2); // 5 certificates, 2 rounds latency
    metrics.record_finalization(3, 3); // 3 certificates, 3 rounds latency
    
    assert_eq!(metrics.certificates_processed, 8);
    assert_eq!(metrics.sequences_finalized, 2);
    assert_eq!(metrics.avg_finalization_latency, 2.5);
    
    metrics.update_dag_size(100);
    assert_eq!(metrics.dag_size, 100);
    
    info!("Consensus metrics test passed");
}

#[tokio::test]
async fn test_storage_operations() {
    let storage = InMemoryStorage::new();
    
    // Test writing and reading consensus state
    storage.write_consensus_state(42, &Default::default())
        .expect("Failed to write state");
    
    let last_index = storage.read_last_consensus_index()
        .expect("Failed to read last index");
    assert_eq!(last_index, 42);
    
    // Test clearing storage
    storage.clear().expect("Failed to clear storage");
    let cleared_index = storage.read_last_consensus_index()
        .expect("Failed to read cleared index");
    assert_eq!(cleared_index, 0);
    
    info!("Storage operations test passed");
}

#[tokio::test]
async fn test_byzantine_fault_scenarios() {
    let (committee, keypairs) = create_test_committee(4); // f=1, so we can tolerate 1 faulty node
    let config = BftConfig::default();
    let mut _consensus = BullsharkConsensus::new(committee.clone(), config);
    
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    // Scenario 1: One node is down (only 3 out of 4 certificates)
    let partial_certificates = create_test_certificates(&committee, 2, &keypairs[0..3]);
    for certificate in partial_certificates {
        dag.insert_certificate(certificate).expect("Failed to insert certificate");
    }
    
    // Should still have quorum (3 out of 4 with stake threshold)
    let has_quorum = dag.has_quorum_at_round(2, &committee);
    assert!(has_quorum, "Should have quorum with 3 out of 4 validators");
    
    // Scenario 2: Test with insufficient certificates (only 2 out of 4)
    let mut dag2 = BullsharkDag::new(Certificate::genesis(&committee));
    let insufficient_certificates = create_test_certificates(&committee, 2, &keypairs[0..2]);
    for certificate in insufficient_certificates {
        dag2.insert_certificate(certificate).expect("Failed to insert certificate");
    }
    
    let has_quorum2 = dag2.has_quorum_at_round(2, &committee);
    assert!(!has_quorum2, "Should not have quorum with only 2 out of 4 validators");
    
    info!("Byzantine fault scenarios test passed");
}

#[tokio::test]
#[ignore] // Long-running performance test
async fn test_consensus_under_load() {
    let (committee, keypairs) = create_test_committee(6);
    let config = BftConfig::default();
    let mut consensus = BullsharkConsensus::new(committee.clone(), config);
    
    let genesis_certificates = Certificate::genesis(&committee);
    let mut dag = BullsharkDag::new(genesis_certificates);
    
    let start_time = std::time::Instant::now();
    let round_count = 100;
    
    // Process many rounds
    for round in 1..=round_count {
        let certificates = create_test_certificates(&committee, round, &keypairs);
        for certificate in certificates {
            dag.insert_certificate(certificate.clone()).expect("Failed to insert certificate");
            
            // Process through consensus occasionally
            if round % 10 == 0 {
                let _outputs = consensus.process_certificate(&mut dag, round * 10, certificate)
                    .expect("Failed to process certificate");
            }
        }
    }
    
    let duration = start_time.elapsed();
    info!("Processed {} rounds in {:?}", round_count, duration);
    
    let stats = dag.stats();
    info!("Final DAG stats: {:?}", stats);
    
    info!("Consensus under load test passed");
} 