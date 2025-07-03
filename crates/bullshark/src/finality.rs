//! Finality engine for Bullshark consensus

use crate::{BullsharkResult, FinalizedBatchInternal, dag::BullsharkDag};
use narwhal::{
    types::{Certificate, CertificateDigest, Committee},
    Transaction as NarwhalTransaction,
    Round,
};
use alloy_primitives::B256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn, debug};
use fastcrypto::Hash;
use blake2::{digest::Update, VarBlake2b};
use rand_08;
use fastcrypto::traits::KeyPair;

/// Engine responsible for determining finality of certificates
#[derive(Debug)]
pub struct FinalityEngine {
    /// Certificates pending finalization ordered by round
    pending_certificates: HashMap<Round, Vec<Certificate>>,
    /// Certificates that have been finalized
    finalized_certificates: HashMap<CertificateDigest, Certificate>,
    /// Current block number being processed
    current_block_number: u64,
    /// Chain tip hash cache for block construction
    chain_tip_hash: B256,
    /// Last finalized round
    last_finalized_round: Round,
    /// Finality threshold (minimum confirmations needed)
    finality_threshold: usize,
    /// Maximum pending rounds to keep
    max_pending_rounds: usize,
}

impl FinalityEngine {
    /// Create a new finality engine
    pub fn new(finality_threshold: usize, max_pending_rounds: usize) -> Self {
        Self {
            pending_certificates: HashMap::new(),
            finalized_certificates: HashMap::new(),
            current_block_number: 1,
            chain_tip_hash: B256::ZERO, // Genesis parent
            last_finalized_round: 0,
            finality_threshold,
            max_pending_rounds,
        }
    }

    /// Add a certificate for finality consideration
    pub async fn add_certificate(&mut self, certificate: Certificate) -> BullsharkResult<()> {
        let round = certificate.round();
        let digest = certificate.digest();

        debug!("Adding certificate {} from round {} for finality consideration", digest, round);

        // Add to pending certificates
        self.pending_certificates
            .entry(round)
            .or_insert_with(Vec::new)
            .push(certificate);

        // Clean up old pending certificates
        self.cleanup_old_pending_certificates();

        Ok(())
    }

    /// Check if a certificate can be finalized based on Bullshark rules
    pub async fn check_finality(
        &mut self,
        dag: &BullsharkDag,
        committee: &Committee,
    ) -> BullsharkResult<Vec<FinalizedBatchInternal>> {
        let mut finalized_batches = Vec::new();

        // Get all rounds that might be ready for finalization
        let candidate_rounds = self.get_finalization_candidates();

        for round in candidate_rounds {
            if let Some(certificates) = self.pending_certificates.get(&round).cloned() {
                // Check if this round can be finalized
                if self.can_finalize_round(round, &certificates, dag, committee).await? {
                    let batches = self.finalize_round(round, certificates).await?;
                    finalized_batches.extend(batches);
                }
            }
        }

        Ok(finalized_batches)
    }

    /// Get rounds that are candidates for finalization
    fn get_finalization_candidates(&self) -> Vec<Round> {
        let mut candidates: Vec<Round> = self.pending_certificates.keys()
            .filter(|&&round| round > self.last_finalized_round)
            .copied()
            .collect();
        
        candidates.sort();
        candidates
    }

    /// Check if a round can be finalized
    async fn can_finalize_round(
        &self,
        round: Round,
        certificates: &[Certificate],
        dag: &BullsharkDag,
        committee: &Committee,
    ) -> BullsharkResult<bool> {
        // Rule 1: Must have enough certificates to meet finality threshold
        if certificates.len() < self.finality_threshold {
            debug!("Round {} has only {} certificates, need {}", 
                   round, certificates.len(), self.finality_threshold);
            return Ok(false);
        }

        // Rule 2: For Bullshark, only even rounds can be finalized (leader rounds)
        if round % 2 != 0 {
            return Ok(false);
        }

        // Rule 3: Check if we have a quorum of certificates for this round
        if !dag.has_quorum_at_round(round, committee) {
            debug!("Round {} does not have quorum", round);
            return Ok(false);
        }

        // Rule 4: Check if there's a valid leader with sufficient support
        if let Some(leader_cert) = dag.get_leader_certificate(round, committee) {
            let leader_digest = leader_cert.digest();
            if dag.leader_has_support(&leader_digest, round, committee) {
                debug!("Round {} has valid leader with support, can finalize", round);
                return Ok(true);
            }
        }

        debug!("Round {} does not meet finality conditions", round);
        Ok(false)
    }

    /// Finalize a round and create batches
    async fn finalize_round(
        &mut self,
        round: Round,
        certificates: Vec<Certificate>,
    ) -> BullsharkResult<Vec<FinalizedBatchInternal>> {
        info!("Finalizing round {} with {} certificates", round, certificates.len());

        let mut finalized_batches = Vec::new();

        // Extract all transactions from the certificates
        let all_transactions = self.extract_transactions_from_certificates(&certificates).await?;

        if !all_transactions.is_empty() {
            let batch = self.create_finalized_batch(
                all_transactions,
                round,
                certificates.clone(),
            ).await?;

            finalized_batches.push(batch);
            self.current_block_number += 1;
        }

        // Mark certificates as finalized
        for certificate in certificates {
            let digest = certificate.digest();
            self.finalized_certificates.insert(digest, certificate);
        }

        // Remove from pending
        self.pending_certificates.remove(&round);
        self.last_finalized_round = round;

        // Update chain tip hash (would be set by actual block execution)
        // For now, use a deterministic hash based on the round
        self.chain_tip_hash = B256::from_slice(&fastcrypto::blake2b_256(|hasher: &mut blake2::VarBlake2b| {
            hasher.update(round.to_le_bytes());
            hasher.update(self.current_block_number.to_le_bytes());
        }));

        Ok(finalized_batches)
    }

    /// Extract transactions from a set of certificates
    async fn extract_transactions_from_certificates(
        &self,
        certificates: &[Certificate],
    ) -> BullsharkResult<Vec<NarwhalTransaction>> {
        let mut all_transactions = Vec::new();

        for certificate in certificates {
            // In a real implementation, this would fetch batches from workers
            // For now, create dummy transactions
            for (batch_digest, _worker_id) in &certificate.header.payload {
                            let dummy_tx = format!("tx_from_batch_{}_round_{}", 
                                     batch_digest, certificate.round()).into_bytes();
            all_transactions.push(NarwhalTransaction(dummy_tx));
            }
        }

        // Remove duplicates and maintain deterministic ordering
        all_transactions.sort();
        all_transactions.dedup();

        Ok(all_transactions)
    }

    /// Create a finalized batch from transactions and certificates
    async fn create_finalized_batch(
        &self,
        transactions: Vec<NarwhalTransaction>,
        round: Round,
        certificates: Vec<Certificate>,
    ) -> BullsharkResult<FinalizedBatchInternal> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let batch = FinalizedBatchInternal {
            block_number: self.current_block_number,
            parent_hash: self.chain_tip_hash,
            transactions,
            timestamp,
            round,
            certificates,
        };

        debug!(
            "Created finalized batch for block {} with {} transactions from round {}",
            batch.block_number,
            batch.transactions.len(),
            batch.round
        );

        Ok(batch)
    }

    /// Clean up old pending certificates that won't be finalized
    fn cleanup_old_pending_certificates(&mut self) {
        let cutoff_round = self.last_finalized_round.saturating_sub(self.max_pending_rounds as u64);
        
        let old_rounds: Vec<Round> = self.pending_certificates.keys()
            .filter(|&&round| round < cutoff_round)
            .copied()
            .collect();

        for round in old_rounds {
            if let Some(certs) = self.pending_certificates.remove(&round) {
                warn!("Cleaned up {} pending certificates from round {} (too old)", 
                      certs.len(), round);
            }
        }
    }

    /// Check if a certificate has been finalized
    pub fn is_finalized(&self, digest: &CertificateDigest) -> bool {
        self.finalized_certificates.contains_key(digest)
    }

    /// Get the current block number
    pub fn current_block_number(&self) -> u64 {
        self.current_block_number
    }

    /// Get the current chain tip hash
    pub fn chain_tip_hash(&self) -> B256 {
        self.chain_tip_hash
    }

    /// Update the chain tip hash (called after block execution)
    pub fn update_chain_tip(&mut self, new_tip: B256) {
        self.chain_tip_hash = new_tip;
    }

    /// Get finality statistics
    pub fn stats(&self) -> FinalityStats {
        FinalityStats {
            pending_certificates: self.pending_certificates.values()
                .map(|certs| certs.len())
                .sum(),
            finalized_certificates: self.finalized_certificates.len(),
            last_finalized_round: self.last_finalized_round,
            current_block_number: self.current_block_number,
            pending_rounds: self.pending_certificates.len(),
        }
    }

    /// Reset state for a new epoch
    pub fn reset_for_epoch(&mut self, new_block_number: u64) {
        self.pending_certificates.clear();
        self.finalized_certificates.clear();
        self.current_block_number = new_block_number;
        self.last_finalized_round = 0;
        self.chain_tip_hash = B256::ZERO;
        
        info!("Reset finality engine for new epoch, starting at block {}", new_block_number);
    }
}

impl Default for FinalityEngine {
    fn default() -> Self {
        Self::new(3, 10) // Default: need 3 confirmations, keep 10 rounds
    }
}

/// Statistics about the finality engine
#[derive(Debug, Clone)]
pub struct FinalityStats {
    /// Number of pending certificates
    pub pending_certificates: usize,
    /// Number of finalized certificates
    pub finalized_certificates: usize,
    /// Last finalized round
    pub last_finalized_round: Round,
    /// Current block number
    pub current_block_number: u64,
    /// Number of rounds with pending certificates
    pub pending_rounds: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use narwhal::types::{Committee, PublicKey};
    use std::collections::HashMap;

    fn create_test_committee() -> Committee {
        use narwhal::types::{Authority, WorkerConfiguration};
        let mut authorities = HashMap::new();
        for i in 0..4 {
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
        }
        Committee::new(0, authorities)
    }

    #[tokio::test]
    async fn test_finality_engine_creation() {
        let engine = FinalityEngine::new(3, 10);
        assert_eq!(engine.current_block_number(), 1);
        assert_eq!(engine.last_finalized_round, 0);
    }

    #[tokio::test]
    async fn test_add_certificate() {
        let mut engine = FinalityEngine::new(3, 10);
        let committee = create_test_committee();
        let genesis_certs = Certificate::genesis(&committee);
        
        if let Some(cert) = genesis_certs.into_iter().next() {
            engine.add_certificate(cert).await.unwrap();
            let stats = engine.stats();
            assert_eq!(stats.pending_certificates, 1);
        }
    }
}
