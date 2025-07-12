//! DAG operations for Bullshark consensus

use crate::BullsharkResult;
use narwhal::{
    Round,
    types::{Certificate, CertificateDigest, PublicKey, Committee},
};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use fastcrypto::Hash;

/// Type alias for the DAG structure
/// Maps Round -> Authority -> (CertificateDigest, Certificate)
pub type Dag = HashMap<Round, HashMap<PublicKey, (CertificateDigest, Certificate)>>;

/// DAG management for Bullshark consensus
#[derive(Debug, Clone)]
pub struct BullsharkDag {
    /// The in-memory DAG
    dag: Dag,
    /// Last committed round for each authority
    last_committed: HashMap<PublicKey, Round>,
    /// Global last committed round
    last_committed_round: Round,
}

impl BullsharkDag {
    /// Create a new Bullshark DAG
    pub fn new(genesis_certificates: Vec<Certificate>) -> Self {
        let mut dag = HashMap::new();
        let mut last_committed = HashMap::new();

        // Insert genesis certificates at round 0
        let mut genesis_round = HashMap::new();
        info!("Creating DAG with {} genesis certificates", genesis_certificates.len());
        for cert in genesis_certificates {
            let digest = cert.digest();
            let author = cert.origin();
            info!("Inserting genesis certificate from {} at round 0", author);
            genesis_round.insert(author.clone(), (digest, cert.clone()));
            last_committed.insert(author, 0);
        }
        dag.insert(0, genesis_round);
        info!("DAG initialized with {} authorities at round 0", dag.get(&0).map(|r| r.len()).unwrap_or(0));

        Self {
            dag,
            last_committed,
            last_committed_round: 0,
        }
    }

    /// Add a certificate to the DAG
    pub fn insert_certificate(&mut self, certificate: Certificate) -> BullsharkResult<()> {
        let round = certificate.round();
        let author = certificate.origin();
        let digest = certificate.digest();

        debug!("Inserting certificate {} from {} at round {}", digest, author, round);

        // Add to DAG
        self.dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(author, (digest, certificate));

        Ok(())
    }

    /// Get certificates for a specific round
    pub fn get_certificates_at_round(&self, round: Round) -> Vec<&Certificate> {
        self.dag
            .get(&round)
            .map(|round_certs| round_certs.values().map(|(_, cert)| cert).collect())
            .unwrap_or_default()
    }

    /// Get a certificate by its digest
    pub fn get_certificate(&self, digest: &CertificateDigest) -> Option<&Certificate> {
        for round_certs in self.dag.values() {
            for (cert_digest, cert) in round_certs.values() {
                if cert_digest == digest {
                    return Some(cert);
                }
            }
        }
        None
    }

    /// Check if we have enough certificates at a round for consensus
    pub fn has_quorum_at_round(&self, round: Round, committee: &Committee) -> bool {
        if let Some(round_certs) = self.dag.get(&round) {
            let total_stake: u64 = round_certs
                .keys()
                .map(|authority| committee.stake(authority))
                .sum();
            total_stake >= committee.quorum_threshold()
        } else {
            false
        }
    }

    /// Find the leader certificate for a given round
    pub fn get_leader_certificate(&self, round: Round, committee: &Committee) -> Option<&Certificate> {
        // Only even rounds have leaders in Bullshark
        if round % 2 != 0 {
            debug!("Round {} is odd, no leader", round);
            return None;
        }

        let leader = committee.leader(round);
        info!("Looking for leader {} at round {}", leader, round);
        
        let round_certs = self.dag.get(&round);
        if let Some(certs) = round_certs {
            info!("Round {} has {} certificates", round, certs.len());
            for (author, _) in certs.iter() {
                debug!("  - Certificate from {}", author);
            }
        } else {
            warn!("Round {} has no certificates in DAG!", round);
        }
        
        self.dag
            .get(&round)
            .and_then(|round_certs| round_certs.get(leader))
            .map(|(_, cert)| cert)
    }

    /// Check if a leader has sufficient support from its children
    pub fn leader_has_support(
        &self,
        leader_digest: &CertificateDigest,
        round: Round,
        committee: &Committee,
    ) -> bool {
        let child_round = round + 1;
        
        if let Some(child_certs) = self.dag.get(&child_round) {
            // Debug: list all certificates in child round
            info!("Checking support for leader in round {} (digest: {:?})", round, leader_digest);
            info!("Found {} certificates in child round {}", child_certs.len(), child_round);
            
            let supporting_certs: Vec<_> = child_certs
                .values()
                .filter(|(_, cert)| cert.header.parents.contains(leader_digest))
                .collect();
                
            info!("Certificates with leader as parent: {}", supporting_certs.len());
            
            let support_stake: u64 = supporting_certs
                .iter()
                .map(|(_, cert)| committee.stake(&cert.origin()))
                .sum();

            // CRITICAL: We need quorum threshold (2f+1) not validity threshold (f+1)
            // This ensures we can't continue without proper Byzantine fault tolerance
            let threshold = committee.quorum_threshold();
            info!("Support stake: {} / {} (quorum threshold)", support_stake, threshold);
            
            support_stake >= threshold
        } else {
            info!("No certificates found in child round {} for leader in round {}", child_round, round);
            false
        }
    }

    /// Get all rounds in the DAG
    pub fn rounds(&self) -> Vec<Round> {
        let mut rounds: Vec<Round> = self.dag.keys().copied().collect();
        rounds.sort();
        rounds
    }

    /// Get the highest round in the DAG
    pub fn highest_round(&self) -> Round {
        self.dag.keys().max().copied().unwrap_or(0)
    }

    /// Update the last committed state
    pub fn update_last_committed(&mut self, certificate: &Certificate, gc_depth: Round) {
        let author = certificate.origin();
        let round = certificate.round();

        // Update last committed for this authority
        let current_committed = self.last_committed.get(&author).copied().unwrap_or(0);
        if round > current_committed {
            self.last_committed.insert(author, round);
        }

        // Update global last committed round
        let global_committed = self.last_committed.values().min().copied().unwrap_or(0);
        if global_committed > self.last_committed_round {
            self.last_committed_round = global_committed;
        }

        // Garbage collect old rounds
        self.garbage_collect(gc_depth);
    }

    /// Garbage collect old rounds
    fn garbage_collect(&mut self, gc_depth: Round) {
        let cutoff_round = self.last_committed_round.saturating_sub(gc_depth);
        
        // Remove old rounds
        let old_rounds: Vec<Round> = self.dag.keys()
            .filter(|&&round| round < cutoff_round)
            .copied()
            .collect();

        for round in old_rounds {
            self.dag.remove(&round);
            debug!("Garbage collected round {}", round);
        }
    }

    /// Get DAG statistics
    pub fn stats(&self) -> DagStats {
        let total_certificates = self.dag.values()
            .map(|round_certs| round_certs.len())
            .sum();

        DagStats {
            total_rounds: self.dag.len(),
            total_certificates,
            highest_round: self.highest_round(),
            last_committed_round: self.last_committed_round,
        }
    }
}

/// Statistics about the DAG
#[derive(Debug, Clone)]
pub struct DagStats {
    /// Total number of rounds
    pub total_rounds: usize,
    /// Total number of certificates
    pub total_certificates: usize,
    /// Highest round number
    pub highest_round: Round,
    /// Last committed round
    pub last_committed_round: Round,
} 