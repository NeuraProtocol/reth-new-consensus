//! Round-based certificate batching for proper consensus finalization
//! 
//! This module ensures we wait for complete rounds before creating blocks,
//! especially ensuring we have the leader's certificate with canonical metadata.

use narwhal::{types::{Certificate, Committee, PublicKey}, Round};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Tracks certificate collection per round to ensure complete rounds before finalization
pub struct RoundCompletionTracker {
    /// Track certificates received per round
    round_certificates: HashMap<Round, HashMap<PublicKey, Certificate>>,
    /// Track if we've seen the leader's certificate for even rounds
    leader_certificates: HashMap<Round, bool>,
    /// Minimum wait time after first certificate of a round
    round_completion_timeout: Duration,
    /// First certificate arrival time per round
    round_first_seen: HashMap<Round, Instant>,
    /// Maximum rounds to track (for memory management)
    max_tracked_rounds: usize,
}

impl RoundCompletionTracker {
    /// Create a new round completion tracker
    pub fn new(round_completion_timeout: Duration) -> Self {
        Self {
            round_certificates: HashMap::new(),
            leader_certificates: HashMap::new(),
            round_completion_timeout,
            round_first_seen: HashMap::new(),
            max_tracked_rounds: 100, // Track up to 100 rounds
        }
    }
    
    /// Add a certificate to the tracker
    pub fn add_certificate(&mut self, certificate: Certificate, committee: &Committee) {
        let round = certificate.round();
        let author = certificate.origin();
        
        // Track first seen time for this round
        self.round_first_seen.entry(round).or_insert_with(Instant::now);
        
        // Add certificate to round tracking
        let round_certs = self.round_certificates.entry(round).or_insert_with(HashMap::new);
        round_certs.insert(author.clone(), certificate.clone());
        
        // Check if this is the leader's certificate for even rounds
        if round % 2 == 0 {
            let leader = committee.leader(round);
            if author == *leader {
                info!("Received leader certificate for round {} from {}", round, author);
                self.leader_certificates.insert(round, true);
                
                // Log if certificate has canonical metadata
                if !certificate.header.canonical_metadata.is_empty() {
                    info!("Leader certificate for round {} contains canonical metadata ({} bytes)", 
                          round, certificate.header.canonical_metadata.len());
                }
            }
        }
        
        // Clean up old rounds if we're tracking too many
        if self.round_certificates.len() > self.max_tracked_rounds {
            self.cleanup_old_rounds();
        }
    }
    
    /// Check if a round should be finalized
    pub fn should_finalize_round(&self, round: Round, committee: &Committee) -> bool {
        // Only finalize even rounds (Bullshark leaders)
        if round % 2 != 0 {
            return false;
        }
        
        let certs = match self.round_certificates.get(&round) {
            Some(c) => c,
            None => return false,
        };
        
        // Check if we have quorum
        let total_stake: u64 = certs.keys()
            .map(|pk| committee.stake(pk))
            .sum();
        
        let quorum_threshold = committee.quorum_threshold();
        if total_stake < quorum_threshold {
            debug!("Round {} has stake {} < quorum {}", round, total_stake, quorum_threshold);
            return false;
        }
        
        // Check if we have the leader's certificate
        let has_leader = self.leader_certificates.get(&round).copied().unwrap_or(false);
        
        // If we have the leader certificate, finalize immediately for fast block times
        if has_leader {
            info!("Round {} ready to finalize: has leader certificate", round);
            return true;
        }
        
        // Otherwise, check if enough time has passed since first certificate
        let elapsed = self.round_first_seen.get(&round)
            .map(|t| t.elapsed() >= self.round_completion_timeout)
            .unwrap_or(false);
        
        if elapsed {
            warn!("Round {} ready to finalize: timeout exceeded (no leader certificate)", round);
            true
        } else {
            let remaining = self.round_first_seen.get(&round)
                .map(|t| self.round_completion_timeout.saturating_sub(t.elapsed()))
                .unwrap_or(Duration::ZERO);
            debug!("Round {} not ready: waiting for leader or timeout ({:.1}s remaining)", 
                   round, remaining.as_secs_f64());
            false
        }
    }
    
    /// Get all certificates for a round
    pub fn get_round_certificates(&self, round: Round) -> Vec<Certificate> {
        self.round_certificates
            .get(&round)
            .map(|certs| certs.values().cloned().collect())
            .unwrap_or_default()
    }
    
    /// Mark a round as complete and remove it from tracking
    pub fn mark_round_complete(&mut self, round: Round) {
        self.round_certificates.remove(&round);
        self.leader_certificates.remove(&round);
        self.round_first_seen.remove(&round);
        info!("Marked round {} as complete and removed from tracker", round);
    }
    
    /// Get rounds that might be ready for finalization
    pub fn get_pending_rounds(&self) -> Vec<Round> {
        let mut rounds: Vec<Round> = self.round_certificates.keys()
            .filter(|&&r| r % 2 == 0) // Only even rounds
            .copied()
            .collect();
        rounds.sort();
        rounds
    }
    
    /// Clean up old rounds to prevent memory growth
    fn cleanup_old_rounds(&mut self) {
        let mut rounds: Vec<Round> = self.round_certificates.keys().copied().collect();
        rounds.sort();
        
        // Keep only the most recent rounds
        let to_remove = rounds.len().saturating_sub(self.max_tracked_rounds);
        for round in rounds.into_iter().take(to_remove) {
            self.mark_round_complete(round);
            debug!("Cleaned up old round {} from tracker", round);
        }
    }
    
    /// Get statistics about tracked rounds
    pub fn stats(&self) -> RoundTrackerStats {
        let total_rounds = self.round_certificates.len();
        let rounds_with_leader = self.leader_certificates.len();
        let pending_even_rounds = self.round_certificates.keys()
            .filter(|&&r| r % 2 == 0)
            .count();
            
        RoundTrackerStats {
            total_rounds,
            rounds_with_leader,
            pending_even_rounds,
        }
    }
}

/// Statistics about the round tracker
#[derive(Debug, Clone)]
pub struct RoundTrackerStats {
    /// Total number of rounds being tracked
    pub total_rounds: usize,
    /// Number of rounds where we have the leader certificate
    pub rounds_with_leader: usize,
    /// Number of even rounds pending finalization
    pub pending_even_rounds: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastcrypto::traits::KeyPair;
    
    #[test]
    fn test_round_completion() {
        // TODO: Add tests for round completion logic
    }
}