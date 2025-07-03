//! Bullshark consensus algorithm implementation

use crate::{
    BullsharkResult, ConsensusOutput, SequenceNumber,
    dag::{BullsharkDag, Dag},
    utils::{order_leaders, order_dag},
    BftConfig,
    storage::{ConsensusStorage, Certificate as StorageCertificate},
};
use narwhal::{
    Round,
    types::{Certificate, CertificateDigest, Committee, PublicKey},
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};
use fastcrypto::Hash;

/// Trait for consensus protocols that process certificates
pub trait ConsensusProtocol {
    /// Process a certificate and return any finalized outputs
    fn process_certificate(
        &mut self,
        dag: &mut BullsharkDag,
        consensus_index: SequenceNumber,
        certificate: Certificate,
    ) -> BullsharkResult<Vec<ConsensusOutput>>;

    /// Update the committee configuration
    fn update_committee(&mut self, new_committee: Committee) -> BullsharkResult<()>;
}

/// Bullshark consensus algorithm implementation
pub struct BullsharkConsensus {
    /// Committee configuration
    committee: Committee,
    /// Configuration parameters
    config: BftConfig,
    /// Storage for persistent state
    storage: Option<Arc<dyn ConsensusStorage>>,
}

impl std::fmt::Debug for BullsharkConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BullsharkConsensus")
            .field("committee", &self.committee)
            .field("config", &self.config)
            .field("storage", &self.storage.is_some())
            .finish()
    }
}

impl BullsharkConsensus {
    /// Create a new Bullshark consensus instance
    pub fn new(committee: Committee, config: BftConfig) -> Self {
        Self {
            committee,
            config,
            storage: None,
        }
    }
    
    /// Create a new Bullshark consensus instance with storage
    pub fn with_storage(
        committee: Committee, 
        config: BftConfig,
        storage: Arc<dyn ConsensusStorage>
    ) -> Self {
        Self {
            committee,
            config,
            storage: Some(storage),
        }
    }

    /// Returns the certificate (and digest) of the leader for a specific round
    pub fn leader<'a>(
        committee: &Committee,
        round: Round,
        dag: &'a Dag,
    ) -> Option<&'a (CertificateDigest, Certificate)> {
        // Only even rounds have leaders in Bullshark
        if round % 2 != 0 {
            return None;
        }

        // Select the leader deterministically based on the round
        let leader_key = committee.leader(round);
        
        // Return the leader's certificate for this round
        dag.get(&round).and_then(|round_certs| round_certs.get(leader_key))
    }
}

impl ConsensusProtocol for BullsharkConsensus {
    fn process_certificate(
        &mut self,
        dag: &mut BullsharkDag,
        consensus_index: SequenceNumber,
        certificate: Certificate,
    ) -> BullsharkResult<Vec<ConsensusOutput>> {
        let round = certificate.round();
        let cert_author = certificate.origin();
        debug!("Processing certificate from {} at round {}", cert_author, round);
        let mut consensus_index = consensus_index;

        // Add the certificate to the DAG
        dag.insert_certificate(certificate)?;

        // Try to commit certificates starting from the highest even round
        // We need f+1 certificates to reveal the common coin
        let commit_round = round.saturating_sub(1);

        // Only process even rounds for leader election
        if commit_round % 2 != 0 || commit_round < self.config.min_leader_round {
            debug!("Skipping round {} - not an even round >= {}", commit_round, self.config.min_leader_round);
            return Ok(Vec::new());
        }
        
        info!("Checking for leader in round {} (certificate from round {})", commit_round, round);

        // Check if we already committed this round
        let dag_stats = dag.stats();
        if commit_round <= dag_stats.last_committed_round {
            return Ok(Vec::new());
        }

        // Get the leader certificate for this round
        let leader_result = dag.get_leader_certificate(commit_round, &self.committee);
        let (leader_digest, leader) = match leader_result {
            Some(leader_cert) => {
                let digest = leader_cert.digest();
                (digest, leader_cert.clone())
            }
            None => {
                debug!("No leader found for round {}", commit_round);
                return Ok(Vec::new());
            }
        };

        // Check if the leader has f+1 support from its children (round r+1)
        let support_round = commit_round + 1;
        info!("Checking support for leader {} in round {} from certificates in round {}", 
              leader.origin(), commit_round, support_round);
        if !dag.leader_has_support(&leader_digest, commit_round, &self.committee) {
            info!("Leader {} in round {} does not have sufficient support", leader.origin(), commit_round);
            return Ok(Vec::new());
        }

        info!("Leader {} in round {} has sufficient support, committing sequence", leader.origin(), commit_round);

        // Get the sequence of leaders to commit
        let leader_sequence = order_leaders(
            &self.committee,
            &leader,
            dag,
            Self::leader,
        );

        let mut outputs = Vec::new();

        // Process each leader in reverse order (oldest first)
        for leader_cert in leader_sequence.iter().rev() {
            debug!("Processing leader sequence for round {}", leader_cert.round());
            
            // Get all certificates in the sub-DAG rooted at this leader
            let ordered_certificates = order_dag(self.config.gc_depth, leader_cert, dag);

            // Create consensus outputs for each certificate
            for cert in ordered_certificates {
                let cert_digest = cert.digest();
                
                // Update DAG state
                dag.update_last_committed(&cert, self.config.gc_depth);

                // Create consensus output
                outputs.push(ConsensusOutput {
                    certificate: cert,
                    consensus_index,
                });

                consensus_index += 1;

                // Persist certificate and state to storage
                if let Some(storage) = &self.storage {
                    // Convert narwhal certificate to storage certificate
                    let storage_cert = StorageCertificate {
                        batch_id: consensus_index,
                        transactions: vec![], // TODO: extract transactions from certificate
                        block_hash: cert_digest.into(), // Convert digest to B256
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        signature: vec![], // TODO: extract signature from certificate
                    };
                    
                    // Store the certificate
                    storage.store_certificate(consensus_index, storage_cert)?;
                    
                    // Update latest finalized
                    storage.set_latest_finalized(consensus_index)?;
                }
            }
        }

        // Log statistics
        let final_stats = dag.stats();
        debug!(
            "Consensus committed {} certificates, DAG stats: {:?}",
            outputs.len(),
            final_stats
        );

        Ok(outputs)
    }

    fn update_committee(&mut self, new_committee: Committee) -> BullsharkResult<()> {
        info!("Updating committee from epoch {} to epoch {}", 
              self.committee.epoch, new_committee.epoch);
        
        // TODO: Add committee storage if needed for consensus state recovery
        
        self.committee = new_committee;
        Ok(())
    }
}



/// Legacy in-memory storage for backwards compatibility
#[derive(Debug)]
pub struct InMemoryStorage {
    last_consensus_index: std::sync::Mutex<SequenceNumber>,
    last_committed: std::sync::Mutex<HashMap<PublicKey, Round>>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage instance
    pub fn new() -> Self {
        Self {
            last_consensus_index: std::sync::Mutex::new(0),
            last_committed: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

// Legacy storage trait implementation for backwards compatibility
impl InMemoryStorage {
    pub fn write_consensus_state(
        &self,
        consensus_index: SequenceNumber,
        _certificate_digest: &CertificateDigest,
    ) -> BullsharkResult<()> {
        let mut index = self.last_consensus_index.lock().unwrap();
        *index = consensus_index;
        Ok(())
    }

    pub fn read_last_consensus_index(&self) -> BullsharkResult<SequenceNumber> {
        let index = self.last_consensus_index.lock().unwrap();
        Ok(*index)
    }

    pub fn read_last_committed(&self) -> BullsharkResult<HashMap<PublicKey, Round>> {
        let committed = self.last_committed.lock().unwrap();
        Ok(committed.clone())
    }

    pub fn clear(&self) -> BullsharkResult<()> {
        let mut index = self.last_consensus_index.lock().unwrap();
        let mut committed = self.last_committed.lock().unwrap();
        *index = 0;
        committed.clear();
        Ok(())
    }
}

/// Metrics for consensus performance
#[derive(Debug, Clone, Default)]
pub struct ConsensusMetrics {
    /// Total certificates processed
    pub certificates_processed: u64,
    /// Total sequences finalized
    pub sequences_finalized: u64,
    /// Average finalization latency (in rounds)
    pub avg_finalization_latency: f64,
    /// Current DAG size
    pub dag_size: usize,
}

impl ConsensusMetrics {
    /// Update metrics with a new finalization
    pub fn record_finalization(&mut self, certificates_count: usize, latency_rounds: u64) {
        self.certificates_processed += certificates_count as u64;
        self.sequences_finalized += 1;
        
        // Update running average
        let total_latency = self.avg_finalization_latency * (self.sequences_finalized - 1) as f64;
        self.avg_finalization_latency = (total_latency + latency_rounds as f64) / self.sequences_finalized as f64;
    }

    /// Update DAG size metric
    pub fn update_dag_size(&mut self, size: usize) {
        self.dag_size = size;
    }
}
