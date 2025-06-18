//! Storage for Narwhal DAG

use crate::{DagError, types::*, Round};
use fastcrypto::Hash;
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Storage for DAG headers, votes, and certificates
#[derive(Debug)]
pub struct DagStorage {
    /// Stored headers by digest
    headers: RwLock<HashMap<HeaderDigest, Header>>,
    /// Stored votes by header digest
    votes: RwLock<HashMap<HeaderDigest, Vec<Vote>>>,
    /// Stored certificates by digest
    certificates: RwLock<HashMap<CertificateDigest, Certificate>>,
    /// Headers by round
    headers_by_round: RwLock<HashMap<Round, Vec<HeaderDigest>>>,
    /// Certificates by round
    certificates_by_round: RwLock<HashMap<Round, Vec<CertificateDigest>>>,
}

impl DagStorage {
    /// Create a new DAG storage instance
    pub fn new() -> Self {
        Self {
            headers: RwLock::new(HashMap::new()),
            votes: RwLock::new(HashMap::new()),
            certificates: RwLock::new(HashMap::new()),
            headers_by_round: RwLock::new(HashMap::new()),
            certificates_by_round: RwLock::new(HashMap::new()),
        }
    }

    /// Store a header
    pub async fn store_header(&self, header: Header) -> Result<(), DagError> {
        let digest = header.digest();
        let round = header.round;

        let mut headers = self.headers.write().await;
        headers.insert(digest, header);

        let mut headers_by_round = self.headers_by_round.write().await;
        headers_by_round.entry(round).or_insert_with(Vec::new).push(digest);

        Ok(())
    }

    /// Get a header by digest
    pub async fn get_header(&self, digest: &HeaderDigest) -> Option<Header> {
        let headers = self.headers.read().await;
        headers.get(digest).cloned()
    }

    /// Get all headers for a given round
    pub async fn get_headers_by_round(&self, round: Round) -> Vec<Header> {
        let headers_by_round = self.headers_by_round.read().await;
        if let Some(digests) = headers_by_round.get(&round) {
            let headers = self.headers.read().await;
            digests.iter().filter_map(|d| headers.get(d).cloned()).collect()
        } else {
            Vec::new()
        }
    }

    /// Store a vote
    pub async fn store_vote(&self, vote: Vote) -> Result<(), DagError> {
        let header_digest = vote.id;
        
        let mut votes = self.votes.write().await;
        votes.entry(header_digest).or_insert_with(Vec::new).push(vote);

        Ok(())
    }

    /// Get all votes for a header
    pub async fn get_votes(&self, header_digest: &HeaderDigest) -> Vec<Vote> {
        let votes = self.votes.read().await;
        votes.get(header_digest).cloned().unwrap_or_default()
    }

    /// Store a certificate
    pub async fn store_certificate(&self, certificate: Certificate) -> Result<(), DagError> {
        let digest = certificate.digest();
        let round = certificate.round();

        let mut certificates = self.certificates.write().await;
        certificates.insert(digest, certificate);

        let mut certificates_by_round = self.certificates_by_round.write().await;
        certificates_by_round.entry(round).or_insert_with(Vec::new).push(digest);

        Ok(())
    }

    /// Get a certificate by digest
    pub async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
        let certificates = self.certificates.read().await;
        certificates.get(digest).cloned()
    }

    /// Get all certificates for a given round
    pub async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
        let certificates_by_round = self.certificates_by_round.read().await;
        if let Some(digests) = certificates_by_round.get(&round) {
            let certificates = self.certificates.read().await;
            digests.iter().filter_map(|d| certificates.get(d).cloned()).collect()
        } else {
            Vec::new()
        }
    }

    /// Get the highest round with certificates
    pub async fn get_last_committed_round(&self) -> Round {
        let certificates_by_round = self.certificates_by_round.read().await;
        certificates_by_round.keys().max().copied().unwrap_or(0)
    }

    /// Clean up storage older than gc_depth rounds
    pub async fn garbage_collect(&self, last_committed_round: Round, gc_depth: Round) {
        let cutoff_round = last_committed_round.saturating_sub(gc_depth);
        
        // Remove old headers
        let mut headers_by_round = self.headers_by_round.write().await;
        let mut headers = self.headers.write().await;
        
        let old_rounds: Vec<Round> = headers_by_round.keys()
            .filter(|&&round| round < cutoff_round)
            .copied()
            .collect();
            
        for round in old_rounds {
            if let Some(digests) = headers_by_round.remove(&round) {
                for digest in digests {
                    headers.remove(&digest);
                }
            }
        }

        // Remove old certificates
        let mut certificates_by_round = self.certificates_by_round.write().await;
        let mut certificates = self.certificates.write().await;
        
        let old_rounds: Vec<Round> = certificates_by_round.keys()
            .filter(|&&round| round < cutoff_round)
            .copied()
            .collect();
            
        for round in old_rounds {
            if let Some(digests) = certificates_by_round.remove(&round) {
                for digest in digests {
                    certificates.remove(&digest);
                }
            }
        }

        // Remove old votes
        let mut votes = self.votes.write().await;
        let header_digests_to_remove: Vec<HeaderDigest> = votes.keys()
            .filter(|digest| {
                // Check if the header for this vote has been garbage collected
                !headers.contains_key(digest)
            })
            .copied()
            .collect();
            
        for digest in header_digests_to_remove {
            votes.remove(&digest);
        }
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> StorageStats {
        let headers = self.headers.read().await;
        let votes = self.votes.read().await;
        let certificates = self.certificates.read().await;

        StorageStats {
            headers_count: headers.len(),
            votes_count: votes.values().map(|v| v.len()).sum(),
            certificates_count: certificates.len(),
        }
    }
}

impl Default for DagStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Number of stored headers
    pub headers_count: usize,
    /// Number of stored votes
    pub votes_count: usize,
    /// Number of stored certificates
    pub certificates_count: usize,
}
