//! Bullshark consensus algorithm implementation

use crate::{
    BullsharkResult, ConsensusOutput, SequenceNumber,
    dag::{BullsharkDag, Dag},
    utils::{order_leaders, order_dag},
    BftConfig,
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
    /// Storage for persistent state (TODO: implement proper storage)
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
            storage: None, // TODO: Add proper storage
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
        debug!("Processing certificate: {:?}", certificate);
        let round = certificate.round();
        let mut consensus_index = consensus_index;

        // Add the certificate to the DAG
        dag.insert_certificate(certificate)?;

        // Try to commit certificates starting from the highest even round
        // We need f+1 certificates to reveal the common coin
        let commit_round = round.saturating_sub(1);

        // Only process even rounds for leader election
        if commit_round % 2 != 0 || commit_round < 2 {
            return Ok(Vec::new());
        }

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
        let _support_round = commit_round + 1;
        if !dag.leader_has_support(&leader_digest, commit_round, &self.committee) {
            debug!("Leader {:?} does not have sufficient support", leader);
            return Ok(Vec::new());
        }

        info!("Leader {:?} has sufficient support, committing sequence", leader);

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

                // TODO: Persist state to storage
                if let Some(storage) = &self.storage {
                    storage.write_consensus_state(consensus_index, &cert_digest)?;
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
        self.committee = new_committee;
        
        // TODO: Clear storage on epoch change
        if let Some(storage) = &self.storage {
            storage.clear()?;
        }
        
        Ok(())
    }
}

/// Trait for consensus storage operations
pub trait ConsensusStorage: Send + Sync {
    /// Write consensus state to persistent storage
    fn write_consensus_state(
        &self,
        consensus_index: SequenceNumber,
        certificate_digest: &CertificateDigest,
    ) -> BullsharkResult<()>;

    /// Read the last consensus index
    fn read_last_consensus_index(&self) -> BullsharkResult<SequenceNumber>;

    /// Read the last committed state
    fn read_last_committed(&self) -> BullsharkResult<HashMap<PublicKey, Round>>;

    /// Clear all storage (used on epoch change)
    fn clear(&self) -> BullsharkResult<()>;
}

/// In-memory storage implementation for testing
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

impl ConsensusStorage for InMemoryStorage {
    fn write_consensus_state(
        &self,
        consensus_index: SequenceNumber,
        _certificate_digest: &CertificateDigest,
    ) -> BullsharkResult<()> {
        let mut index = self.last_consensus_index.lock().unwrap();
        *index = consensus_index;
        Ok(())
    }

    fn read_last_consensus_index(&self) -> BullsharkResult<SequenceNumber> {
        let index = self.last_consensus_index.lock().unwrap();
        Ok(*index)
    }

    fn read_last_committed(&self) -> BullsharkResult<HashMap<PublicKey, Round>> {
        let committed = self.last_committed.lock().unwrap();
        Ok(committed.clone())
    }

    fn clear(&self) -> BullsharkResult<()> {
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
