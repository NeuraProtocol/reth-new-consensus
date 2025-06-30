//! Vote and Certificate aggregation for Narwhal consensus
//! 
//! This module implements the logic for collecting votes and forming certificates
//! with proper BLS signature aggregation.

use crate::{
    types::{Vote, Certificate, Header, PublicKey, Signature, HeaderDigest},
    DagError, DagResult, Round,
};
use fastcrypto::{
    traits::{AggregateAuthenticator, KeyPair},
    bls12381::{BLS12381AggregateSignature, BLS12381PublicKey, BLS12381Signature},
};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

/// Aggregates votes for a specific header to form a certificate
#[derive(Debug)]
pub struct VotesAggregator {
    /// The header we're collecting votes for
    header: Option<Header>,
    /// Collected votes as (PublicKey, Signature) pairs
    votes: Vec<(PublicKey, Signature)>,
    /// Set of authorities that have voted (to prevent duplicates)
    voters: HashSet<PublicKey>,
    /// Total stake accumulated from votes
    stake: u64,
}

impl VotesAggregator {
    /// Create a new vote aggregator
    pub fn new() -> Self {
        Self {
            header: None,
            votes: Vec::new(),
            voters: HashSet::new(),
            stake: 0,
        }
    }

    /// Create a new aggregator for a specific header
    pub fn with_header(header: Header) -> Self {
        Self {
            header: Some(header),
            votes: Vec::new(),
            voters: HashSet::new(),
            stake: 0,
        }
    }

    /// Add a vote to the aggregator
    pub fn add_vote(&mut self, vote: Vote, committee: &crate::types::Committee) -> DagResult<()> {
        // Check if we already have a vote from this authority
        if self.voters.contains(&vote.author) {
            debug!("Duplicate vote from {}", vote.author);
            return Ok(());
        }

        // Verify the vote matches our header (if set)
        if let Some(ref header) = self.header {
            if vote.id != header.id {
                return Err(DagError::Consensus(format!(
                    "Vote for wrong header: expected {}, got {}",
                    header.id, vote.id
                )));
            }
        }

        // Add the vote
        self.votes.push((vote.author.clone(), vote.signature.clone()));
        self.voters.insert(vote.author.clone());
        
        // Update stake
        let voter_stake = committee.stake(&vote.author);
        self.stake = self.stake.saturating_add(voter_stake);

        debug!(
            "Added vote from {} (stake: {}), total stake: {}",
            vote.author, voter_stake, self.stake
        );

        Ok(())
    }

    /// Check if we have reached quorum
    pub fn has_quorum(&self, committee: &crate::types::Committee) -> bool {
        self.stake >= committee.quorum_threshold()
    }

    /// Try to form a certificate if we have quorum
    pub fn try_form_certificate(
        &self,
        committee: &crate::types::Committee,
    ) -> DagResult<Option<Certificate>> {
        if !self.has_quorum(committee) {
            return Ok(None);
        }

        let header = self.header.as_ref()
            .ok_or_else(|| DagError::Consensus("No header set for aggregator".to_string()))?;

        // Create certificate with the collected votes
        let certificate = Certificate::new(committee, header.clone(), self.votes.clone())?;
        
        info!(
            "Formed certificate for round {} with {} votes (stake: {})",
            header.round,
            self.votes.len(),
            self.stake
        );

        Ok(Some(certificate))
    }

    /// Get the current stake total
    pub fn stake(&self) -> u64 {
        self.stake
    }

    /// Get the number of votes collected
    pub fn vote_count(&self) -> usize {
        self.votes.len()
    }

    /// Clear all votes (for reuse)
    pub fn clear(&mut self) {
        self.header = None;
        self.votes.clear();
        self.voters.clear();
        self.stake = 0;
    }
}

/// Aggregates certificates to track when we have enough for the next round
#[derive(Debug)]
pub struct CertificatesAggregator {
    /// Round this aggregator is for
    round: Round,
    /// Certificates collected for this round
    certificates: HashMap<PublicKey, Certificate>,
    /// Total stake of collected certificates
    stake: u64,
}

impl CertificatesAggregator {
    /// Create a new certificate aggregator for a round
    pub fn new(round: Round) -> Self {
        Self {
            round,
            certificates: HashMap::new(),
            stake: 0,
        }
    }

    /// Add a certificate to the aggregator
    pub fn add_certificate(
        &mut self,
        certificate: Certificate,
        committee: &crate::types::Committee,
    ) -> DagResult<()> {
        // Verify the certificate is for the correct round
        if certificate.round() != self.round {
            return Err(DagError::Consensus(format!(
                "Certificate for wrong round: expected {}, got {}",
                self.round,
                certificate.round()
            )));
        }

        let author = certificate.origin();
        
        // Check if we already have a certificate from this authority
        if self.certificates.contains_key(&author) {
            debug!("Duplicate certificate from {} for round {}", author, self.round);
            return Ok(());
        }

        // Add the certificate
        let author_stake = committee.stake(&author);
        self.certificates.insert(author.clone(), certificate);
        self.stake = self.stake.saturating_add(author_stake);

        debug!(
            "Added certificate from {} for round {} (stake: {}), total stake: {}",
            author, self.round, author_stake, self.stake
        );

        Ok(())
    }

    /// Check if we have a quorum of certificates
    pub fn has_quorum(&self, committee: &crate::types::Committee) -> bool {
        self.stake >= committee.quorum_threshold()
    }

    /// Get all certificates
    pub fn certificates(&self) -> Vec<Certificate> {
        self.certificates.values().cloned().collect()
    }

    /// Get the round
    pub fn round(&self) -> Round {
        self.round
    }

    /// Get the current stake total
    pub fn stake(&self) -> u64 {
        self.stake
    }

    /// Get the number of certificates
    pub fn certificate_count(&self) -> usize {
        self.certificates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{HeaderBuilder, Committee};
    use std::collections::HashMap;

    fn create_test_committee() -> (Committee, Vec<fastcrypto::bls12381::BLS12381KeyPair>) {
        let mut authorities = HashMap::new();
        let mut keypairs = Vec::new();
        
        for _ in 0..4 {
            let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
            authorities.insert(keypair.public().clone(), 100);
            keypairs.push(keypair);
        }
        
        (Committee::new(0, authorities), keypairs)
    }

    #[test]
    fn test_vote_aggregation() {
        let (committee, keypairs) = create_test_committee();
        let header = HeaderBuilder::default()
            .author(keypairs[0].public().clone())
            .round(1)
            .epoch(0)
            .build(&keypairs[0])
            .unwrap();

        let mut aggregator = VotesAggregator::with_header(header.clone());

        // Add votes from 3 out of 4 validators (75% stake)
        for i in 0..3 {
            let vote = Vote::new(&header, keypairs[i].public());
            aggregator.add_vote(vote, &committee).unwrap();
        }

        // Should have quorum (need 67% for 2f+1)
        assert!(aggregator.has_quorum(&committee));
        assert_eq!(aggregator.vote_count(), 3);
        assert_eq!(aggregator.stake(), 300);

        // Should be able to form certificate
        let certificate = aggregator.try_form_certificate(&committee).unwrap();
        assert!(certificate.is_some());
    }

    #[test]
    fn test_duplicate_vote_rejected() {
        let (committee, keypairs) = create_test_committee();
        let header = HeaderBuilder::default()
            .author(keypairs[0].public().clone())
            .round(1)
            .epoch(0)
            .build(&keypairs[0])
            .unwrap();

        let mut aggregator = VotesAggregator::with_header(header.clone());

        // Add same vote twice
        let vote = Vote::new(&header, keypairs[0].public());
        aggregator.add_vote(vote.clone(), &committee).unwrap();
        aggregator.add_vote(vote, &committee).unwrap();

        // Should only count once
        assert_eq!(aggregator.vote_count(), 1);
        assert_eq!(aggregator.stake(), 100);
    }
}