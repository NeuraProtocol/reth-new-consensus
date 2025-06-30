// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core consensus logic for Narwhal Primary
//! 
//! The Core is responsible for:
//! - Processing headers received from other primaries
//! - Creating and sending votes on valid headers
//! - Collecting votes to form certificates
//! - Broadcasting certificates to other primaries
//! - Maintaining the DAG structure

use crate::{
    synchronizer::Synchronizer,
    metrics::PrimaryMetrics,
    types::{
        Certificate, Header, HeaderDigest, Vote, VoteDigest, Round,
        PrimaryMessage, Committee, PublicKey, ReconfigureNotification,
        RoundVoteDigestPair,
    },
    network::{P2pNetwork, ReliableNetwork},
    storage::CertificateStore,
    aggregators::{VotesAggregator, CertificatesAggregator},
    error::CoreError,
};

use crypto::{Signature};
use fastcrypto::{traits::EncodeDecodeBase64, SignatureService, Hash as _};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use store::Store;
use tokio::{
    sync::{watch, mpsc::{UnboundedReceiver, UnboundedSender}},
    task::JoinHandle,
};
use tracing::{debug, info, warn, error};

/// The Core handles the main consensus logic
pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Persistent storage for headers.
    header_store: Store<HeaderDigest, Header>,
    /// Persistent storage for certificates.
    certificate_store: CertificateStore,
    /// Synchronizer for handling missing dependencies.
    synchronizer: Synchronizer,
    /// Signature service for creating votes.
    signature_service: SignatureService<Signature>,
    /// Watch channel for consensus round updates.
    rx_consensus_round_updates: watch::Receiver<u64>,
    /// Watch channel for committee updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// Receiver for messages from other primaries.
    rx_primary_messages: UnboundedReceiver<PrimaryMessage>,
    /// Receiver for headers from our proposer.
    rx_headers: UnboundedReceiver<Header>,
    /// Receiver for votes from network.
    rx_votes: UnboundedReceiver<Vote>,
    /// Receiver for certificates from certificate waiter.
    rx_certificates: UnboundedReceiver<Certificate>,
    /// Sender for certificates to consensus layer.
    tx_consensus: UnboundedSender<Certificate>,
    /// Sender for parent certificates to proposer.
    tx_parents: UnboundedSender<(Vec<Certificate>, Round)>,
    /// Network for communication.
    network: P2pNetwork,
    /// Metrics handler.
    metrics: Option<Arc<PrimaryMetrics>>,
    
    // Internal state
    /// The last garbage collected round.
    gc_round: Round,
    /// Headers currently being processed.
    processing: HashMap<Round, HashSet<HeaderDigest>>,
    /// Current header we proposed (waiting for votes).
    current_header: Option<Header>,
    /// Vote aggregator for collecting votes.
    votes_aggregator: VotesAggregator,
    /// Certificate aggregators per round.
    certificates_aggregators: HashMap<Round, CertificatesAggregator>,
    /// Store for tracking votes to prevent equivocation.
    vote_digest_store: HashMap<PublicKey, RoundVoteDigestPair>,
}

impl Core {
    /// Create and spawn a new Core instance
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        header_store: Store<HeaderDigest, Header>,
        certificate_store: CertificateStore,
        synchronizer: Synchronizer,
        signature_service: SignatureService<Signature>,
        rx_consensus_round_updates: watch::Receiver<u64>,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_primary_messages: UnboundedReceiver<PrimaryMessage>,
        rx_headers: UnboundedReceiver<Header>,
        rx_votes: UnboundedReceiver<Vote>,
        rx_certificates: UnboundedReceiver<Certificate>,
        tx_consensus: UnboundedSender<Certificate>,
        tx_parents: UnboundedSender<(Vec<Certificate>, Round)>,
        network: P2pNetwork,
        metrics: Option<Arc<PrimaryMetrics>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut core = Self {
                name,
                committee,
                header_store,
                certificate_store,
                synchronizer,
                signature_service,
                rx_consensus_round_updates,
                rx_reconfigure,
                rx_primary_messages,
                rx_headers,
                rx_votes,
                rx_certificates,
                tx_consensus,
                tx_parents,
                network,
                metrics,
                gc_round: 0,
                processing: HashMap::new(),
                current_header: None,
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::new(),
                vote_digest_store: HashMap::new(),
            };

            if let Err(e) = core.run().await {
                error!("Core failed: {}", e);
            }
        })
    }

    /// Main event loop for the Core
    async fn run(&mut self) -> Result<(), CoreError> {
        info!("Starting Core for primary {}", self.name.encode_base64());

        loop {
            tokio::select! {
                // Handle primary messages (headers, votes, certificates)
                Some(message) = self.rx_primary_messages.recv() => {
                    self.handle_primary_message(message).await?;
                }

                // Handle headers from our own proposer
                Some(header) = self.rx_headers.recv() => {
                    self.process_own_header(header).await?;
                }

                // Handle votes
                Some(vote) = self.rx_votes.recv() => {
                    self.process_vote(vote).await?;
                }

                // Handle certificates from certificate waiter
                Some(certificate) = self.rx_certificates.recv() => {
                    self.process_certificate(certificate).await?;
                }

                // Handle committee changes
                Ok(()) = self.rx_reconfigure.changed() => {
                    let message = self.rx_reconfigure.borrow().clone();
                    match message {
                        ReconfigureNotification::UpdateCommittee(new_committee) => {
                            self.change_committee(new_committee).await?;
                        }
                        ReconfigureNotification::Shutdown => {
                            info!("Core shutting down");
                            break;
                        }
                        _ => {}
                    }
                }

                else => break,
            }
        }

        info!("Core stopped for primary {}", self.name.encode_base64());
        Ok(())
    }

    /// Handle messages from other primaries
    async fn handle_primary_message(&mut self, message: PrimaryMessage) -> Result<(), CoreError> {
        match message {
            PrimaryMessage::Header(header) => {
                debug!("Processing header from {}: round {}", header.author.encode_base64(), header.round);
                self.process_header(header).await
            }
            PrimaryMessage::Vote(vote) => {
                debug!("Processing vote from {}: round {}", vote.origin.encode_base64(), vote.round);
                self.process_vote(vote).await
            }
            PrimaryMessage::Certificate(certificate) => {
                debug!("Processing certificate: round {}", certificate.round());
                self.process_certificate(certificate).await
            }
            _ => {
                debug!("Received other message type");
                Ok(())
            }
        }
    }

    /// Process a header from our own proposer
    async fn process_own_header(&mut self, header: Header) -> Result<(), CoreError> {
        debug!("Processing our own header: round {}", header.round);

        // Store the header as our current proposal
        self.current_header = Some(header.clone());
        
        // Reset votes aggregator for new header
        self.votes_aggregator = VotesAggregator::new();

        // Broadcast the header to other primaries
        let peers: Vec<_> = self.committee.authorities.keys()
            .filter(|&pk| pk != &self.name)
            .cloned()
            .collect();

        let message = PrimaryMessage::Header(header.clone());
        self.network.broadcast(peers, &message).await
            .map_err(|e| CoreError::NetworkError(format!("Failed to broadcast header: {}", e)))?;

        // Process the header through normal validation
        self.process_header(header).await
    }

    /// Process a header from any primary (including ourselves)
    async fn process_header(&mut self, header: Header) -> Result<(), CoreError> {
        // Check if we've already processed this header
        let already_processing = self.processing
            .get(&header.round)
            .map(|set| set.contains(&header.digest()))
            .unwrap_or(false);

        if already_processing {
            debug!("Already processing header {}", header.digest());
            return Ok(());
        }

        // Mark as processing
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.digest());

        // Validate the header
        if let Err(e) = self.validate_header(&header).await {
            warn!("Invalid header {}: {}", header.digest(), e);
            return Ok(());
        }

        // Store the header
        self.header_store.write(header.digest(), header.clone()).await
            .map_err(|e| CoreError::StorageError(format!("Failed to store header: {}", e)))?;

        // Create and send vote if we haven't voted for this round yet
        self.send_vote(&header).await?;

        Ok(())
    }

    /// Validate a header
    async fn validate_header(&self, header: &Header) -> Result<(), CoreError> {
        // Basic validation
        if header.round == 0 {
            return Err(CoreError::InvalidHeader("Genesis round".to_string()));
        }

        // Check if author is in committee
        if !self.committee.authorities.contains_key(&header.author) {
            return Err(CoreError::InvalidHeader("Unknown author".to_string()));
        }

        // Validate parent certificates exist (for non-genesis headers)
        if header.round > 1 {
            let missing_parents = self.synchronizer.missing_parents(header).await
                .map_err(|e| CoreError::SynchronizerError(format!("Failed to check parents: {}", e)))?;
            
            if !missing_parents.is_empty() {
                return Err(CoreError::InvalidHeader("Missing parent certificates".to_string()));
            }
        }

        Ok(())
    }

    /// Create and send a vote for a header
    async fn send_vote(&mut self, header: &Header) -> Result<(), CoreError> {
        // Check if we already voted for this author in this round
        if let Some(prev_vote) = self.vote_digest_store.get(&header.author) {
            if prev_vote.round == header.round {
                debug!("Already voted for {} in round {}", header.author.encode_base64(), header.round);
                return Ok(());
            }
        }

        // Create vote
        let vote = Vote::new(header, &self.name, &mut self.signature_service).await
            .map_err(|e| CoreError::SignatureError(format!("Failed to create vote: {}", e)))?;

        debug!("Created vote for header {} in round {}", header.digest(), header.round);

        // Record the vote to prevent equivocation
        let vote_digest = vote.digest();
        self.vote_digest_store.insert(
            header.author.clone(),
            RoundVoteDigestPair {
                round: header.round,
                vote_digest,
            },
        );

        // Send vote to header author
        if vote.origin == self.name {
            // If it's our own header, process the vote locally
            self.process_vote(vote).await?;
        } else {
            // Send to the header author
            let message = PrimaryMessage::Vote(vote);
            self.network.send(header.author.clone(), &message).await
                .map_err(|e| CoreError::NetworkError(format!("Failed to send vote: {}", e)))?;
        }

        Ok(())
    }

    /// Process a vote on one of our headers
    async fn process_vote(&mut self, vote: Vote) -> Result<(), CoreError> {
        debug!("Processing vote from {} for round {}", vote.origin.encode_base64(), vote.round);

        // Only process votes for our current header
        let current_header = match &self.current_header {
            Some(header) if header.round == vote.round => header.clone(),
            _ => {
                debug!("Vote not for our current header, ignoring");
                return Ok(());
            }
        };

        // Add vote to aggregator
        if let Some(certificate) = self.votes_aggregator.append(vote, &self.committee, &current_header)
            .map_err(|e| CoreError::AggregatorError(format!("Failed to add vote: {}", e)))? {
            
            info!("Formed certificate for round {}", certificate.round());

            // Broadcast the certificate
            let peers: Vec<_> = self.committee.authorities.keys()
                .filter(|&pk| pk != &self.name)
                .cloned()
                .collect();

            let message = PrimaryMessage::Certificate(certificate.clone());
            self.network.broadcast(peers, &message).await
                .map_err(|e| CoreError::NetworkError(format!("Failed to broadcast certificate: {}", e)))?;

            // Process the certificate
            self.process_certificate(certificate).await?;
        }

        Ok(())
    }

    /// Process a certificate (from ourselves or others)
    async fn process_certificate(&mut self, certificate: Certificate) -> Result<(), CoreError> {
        debug!("Processing certificate for round {}", certificate.round());

        // Store the certificate
        self.certificate_store.write(certificate.clone())
            .map_err(|e| CoreError::StorageError(format!("Failed to store certificate: {}", e)))?;

        // Send to consensus layer
        if let Err(_) = self.tx_consensus.send(certificate.clone()) {
            warn!("Failed to send certificate to consensus layer");
        }

        // Check if we have enough certificates to advance to next round
        let mut certificates_aggregator = self.certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(CertificatesAggregator::new);

        if let Some(parents) = certificates_aggregator.append(certificate, &self.committee)
            .map_err(|e| CoreError::AggregatorError(format!("Failed to add certificate: {}", e)))? {
            
            info!("Collected enough certificates for round {}, can advance", certificate.round());
            
            // Send parent certificates to proposer for next round
            if let Err(_) = self.tx_parents.send((parents, certificate.round())) {
                warn!("Failed to send parents to proposer");
            }
        }

        Ok(())
    }

    /// Update committee configuration
    async fn change_committee(&mut self, new_committee: Committee) -> Result<(), CoreError> {
        info!("Updating committee from epoch {} to epoch {}", 
              self.committee.epoch, new_committee.epoch);

        self.committee = new_committee;

        // Reset state for new epoch
        self.processing.clear();
        self.votes_aggregator = VotesAggregator::new();
        self.certificates_aggregators.clear();
        self.vote_digest_store.clear();
        self.current_header = None;
        self.gc_round = 0;

        Ok(())
    }
}

// Placeholder vote aggregator for collecting votes into certificates
pub struct VotesAggregator {
    votes: HashMap<PublicKey, Vote>,
}

impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            votes: HashMap::new(),
        }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> Result<Option<Certificate>, String> {
        // Add vote to collection
        self.votes.insert(vote.origin.clone(), vote);

        // Check if we have enough votes for a certificate
        let total_stake: u64 = self.votes.keys()
            .filter_map(|pk| committee.authorities.get(pk))
            .map(|info| info.stake)
            .sum();

        let threshold = committee.quorum_threshold();

        if total_stake >= threshold {
            // Form certificate
            let certificate = Certificate::new(
                header.clone(),
                self.votes.values().cloned().collect(),
            );
            
            // Reset aggregator
            self.votes.clear();
            
            Ok(Some(certificate))
        } else {
            Ok(None)
        }
    }
}

// Placeholder certificate aggregator for collecting certificates
pub struct CertificatesAggregator {
    certificates: HashMap<PublicKey, Certificate>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self {
            certificates: HashMap::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Result<Option<Vec<Certificate>>, String> {
        // Add certificate to collection
        self.certificates.insert(certificate.origin(), certificate);

        // Check if we have enough certificates (quorum)
        let total_stake: u64 = self.certificates.keys()
            .filter_map(|pk| committee.authorities.get(pk))
            .map(|info| info.stake)
            .sum();

        let threshold = committee.quorum_threshold();

        if total_stake >= threshold {
            // Return all certificates as parents for next round
            let parents = self.certificates.values().cloned().collect();
            Ok(Some(parents))
        } else {
            Ok(None)
        }
    }
} 