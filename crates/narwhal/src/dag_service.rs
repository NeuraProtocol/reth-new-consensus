//! Narwhal DAG service implementation adapted from reference implementation

use crate::{
    types::{Certificate, Committee, Header, Vote, PublicKey, Signature, HeaderDigest}, 
    NarwhalConfig, DagError, DagResult, Transaction, Round, Batch
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, debug};
use fastcrypto::{SignatureService, Hash, Digest, traits::Signer, Verifier};
use std::sync::Arc;
use std::collections::{BTreeSet, HashMap, HashSet};
use indexmap::IndexMap;
use crate::CertificateDigest;

/// Messages that can be received from the network
#[derive(Debug, Clone)]
pub enum DagMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
}

/// Main service that runs the Narwhal DAG protocol
/// Adapted from reference implementation's Core but simplified for Reth integration
pub struct DagService {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Configuration parameters
    config: NarwhalConfig,
    /// Service to sign headers.
    signature_service: Arc<tokio::sync::Mutex<SignatureService<Signature>>>,
    /// Receiver for new transactions to include in proposals
    transaction_receiver: mpsc::UnboundedReceiver<Transaction>,
    /// Receiver for DAG messages (headers, votes, certificates) from network
    rx_network_messages: mpsc::UnboundedReceiver<DagMessage>,
    /// Sender for notifying Bullshark of new certificates
    certificate_output_sender: mpsc::UnboundedSender<Certificate>,
    /// Watch channel for reconfiguration
    reconfigure_receiver: watch::Receiver<Committee>,
    
    // State tracking (from reference implementation)
    /// Current round for transaction batching
    current_round: Round,
    /// The set of headers we are currently processing
    processing: HashMap<Round, HashSet<HeaderDigest>>,
    /// The last header we proposed (for which we are waiting votes)
    current_header: Option<Header>,
    /// Aggregates votes into a certificate (simplified - we'll use a basic version)
    pending_votes: HashMap<HeaderDigest, Vec<Vote>>,
    /// Certificate storage for parent tracking
    certificates: HashMap<CertificateDigest, Certificate>,
    /// Latest certificates by author for parent tracking (for DAG parents)
    latest_certificates: HashMap<PublicKey, Certificate>,
    /// Current transaction batch being assembled
    current_batch: Vec<Transaction>,
}

impl std::fmt::Debug for DagService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagService")
            .field("name", &self.name)
            .field("committee", &self.committee)
            .field("current_round", &self.current_round)
            .finish_non_exhaustive()
    }
}

impl DagService {
    /// Create a new DAG service
    pub fn new(
        name: PublicKey,
        committee: Committee,
        config: NarwhalConfig,
        signature_service: SignatureService<Signature>,
        transaction_receiver: mpsc::UnboundedReceiver<Transaction>,
        rx_network_messages: mpsc::UnboundedReceiver<DagMessage>,
        certificate_output_sender: mpsc::UnboundedSender<Certificate>,
        reconfigure_receiver: watch::Receiver<Committee>,
    ) -> Self {
        Self {
            name,
            committee,
            config,
            signature_service: Arc::new(tokio::sync::Mutex::new(signature_service)),
            transaction_receiver,
            rx_network_messages,
            certificate_output_sender,
            reconfigure_receiver,
            current_round: 1,
            processing: HashMap::new(),
            current_header: None,
            pending_votes: HashMap::new(),
            certificates: HashMap::new(),
            latest_certificates: HashMap::new(),
            current_batch: Vec::new(),
        }
    }

    /// Spawn the DAG service
    pub fn spawn(self) -> JoinHandle<DagResult<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }

    /// Main run loop for the DAG service (adapted from reference implementation)
    pub async fn run(mut self) -> DagResult<()> {
        info!("DAG service started for node {}", self.name);
        
        // Timer for active block production
        let max_header_delay = self.config.max_batch_delay;
        let timer = tokio::time::sleep(max_header_delay);
        tokio::pin!(timer);
        
        loop {
            let result = tokio::select! {
                // Process network messages (headers, votes, certificates) - from reference implementation
                Some(message) = self.rx_network_messages.recv() => {
                    match message {
                        DagMessage::Header(header) => {
                            // Sanitize and process header similar to reference implementation
                            match self.sanitize_header(&header).await {
                                Ok(()) => self.process_header(&header).await,
                                Err(e) => {
                                    warn!("Header sanitization failed: {}", e);
                                    Ok(())
                                }
                            }
                        },
                        DagMessage::Vote(vote) => {
                            // Sanitize and process vote similar to reference implementation
                            match self.sanitize_vote(&vote).await {
                                Ok(()) => self.process_vote(vote).await,
                                Err(e) => {
                                    warn!("Vote sanitization failed: {}", e);
                                    Ok(())
                                }
                            }
                        },
                        DagMessage::Certificate(certificate) => {
                            // Sanitize and process certificate similar to reference implementation
                            match self.sanitize_certificate(&certificate).await {
                                Ok(()) => self.process_certificate(certificate).await,
                                Err(e) => {
                                    warn!("Certificate sanitization failed: {}", e);
                                    Ok(())
                                }
                            }
                        }
                    }
                },

                // Process new transactions for batching
                Some(transaction) = self.transaction_receiver.recv() => {
                    self.current_batch.push(transaction);
                    
                    // Check if we should create a header
                    let enough_transactions = self.current_batch.len() >= self.config.max_batch_size;
                    let timer_expired = timer.is_elapsed();
                    
                    if (enough_transactions || timer_expired) && !self.current_batch.is_empty() {
                        self.create_and_propose_header().await?;
                        
                        // Reset timer
                        let deadline = tokio::time::Instant::now() + max_header_delay;
                        timer.as_mut().reset(deadline);
                    }
                    
                    Ok(())
                },

                // Timer for active block production
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.create_and_propose_header().await?;
                    }
                    
                    // Reset timer for next round
                    let deadline = tokio::time::Instant::now() + max_header_delay;
                    timer.as_mut().reset(deadline);
                    
                    Ok(())
                },

                // Committee updates
                Ok(()) = self.reconfigure_receiver.changed() => {
                    let new_committee = self.reconfigure_receiver.borrow().clone();
                    info!("Committee updated: epoch {}", new_committee.epoch);
                    self.change_epoch(new_committee).await;
                    Ok(())
                }
            };

            // Handle errors similar to reference implementation
            match result {
                Ok(()) => (),
                Err(DagError::ShuttingDown) => {
                    debug!("DAG service shutting down");
                    break;
                }
                Err(e) => {
                    warn!("DAG service error: {}", e);
                }
            }
        }
        
                 Ok(())
     }

     /// Create and propose a new header (adapted from reference implementation)
     async fn create_and_propose_header(&mut self) -> DagResult<()> {
         info!("Creating header with {} transactions", self.current_batch.len());
         
         // Get parent certificates from previous round
         let parents: BTreeSet<CertificateDigest> = self.latest_certificates
             .values()
             .filter(|cert| cert.round() == self.current_round.saturating_sub(1))
             .map(|cert| cert.digest())
             .collect();

         // Create batch and payload
         let batch = Batch(self.current_batch.clone());
         let mut payload = IndexMap::new();
         payload.insert(batch.digest(), 0u32); // worker_id = 0

         // Create header
         let mut header = Header {
             author: self.name.clone(),
             round: self.current_round,
             epoch: self.committee.epoch,
             payload,
             parents,
             id: HeaderDigest::default(),
             signature: Signature::default(),
         };

         // Sign header
         header.id = header.digest();
         let signature = {
             let mut sig_service = self.signature_service.lock().await;
             let header_digest: Digest = header.id.into();
             sig_service.request_signature(header_digest).await
         };
         header.signature = signature;

         // Store as current header for vote aggregation
         self.current_header = Some(header.clone());
         
         // Mark as processing
         self.processing
             .entry(header.round)
             .or_insert_with(HashSet::new)
             .insert(header.id);

         // TODO: Broadcast header to network (network integration needed)
         info!("Created header {} for round {}", header.id, header.round);

         // Clear batch and advance round
         self.current_batch.clear();
         self.current_round += 1;

         Ok(())
     }

     /// Sanitize header (from reference implementation)
     async fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
         // Check epoch
         if header.epoch != self.committee.epoch {
             return Err(DagError::InvalidEpoch {
                 expected: self.committee.epoch,
                 received: header.epoch,
             });
         }

         // Check author is in committee
         if self.committee.stake(&header.author) == 0 {
             return Err(DagError::UnknownAuthority(header.author.to_string()));
         }

         // Verify header signature
         let header_digest: Digest = header.id.into();
         header.author.verify(header_digest.as_ref(), &header.signature)
             .map_err(DagError::InvalidSignature)?;

         Ok(())
     }

     /// Sanitize vote (from reference implementation)
     async fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
         // Check epoch
         if vote.epoch != self.committee.epoch {
             return Err(DagError::InvalidEpoch {
                 expected: self.committee.epoch,
                 received: vote.epoch,
             });
         }

         // Verify we have a current header to vote on
         let current_header = self.current_header.as_ref()
             .ok_or_else(|| DagError::Consensus("No current header to vote on".to_string()))?;

         // Ensure vote is for expected header
         if vote.id != current_header.id || vote.round != current_header.round {
             return Err(DagError::Consensus("Vote not for current header".to_string()));
         }

         // Verify vote signature
         vote.verify(&self.committee)?;
         Ok(())
     }

     /// Sanitize certificate (from reference implementation)
     async fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
         // Check epoch
         if certificate.epoch() != self.committee.epoch {
             return Err(DagError::InvalidEpoch {
                 expected: self.committee.epoch,
                 received: certificate.epoch(),
             });
         }

         // Verify certificate signature and quorum
         certificate.verify(&self.committee)
     }

     /// Process header (adapted from reference implementation)
     async fn process_header(&mut self, header: &Header) -> DagResult<()> {
         debug!("Processing header from {}", header.author);

         // Don't process our own headers
         if header.author == self.name {
             return Ok(());
         }

         // Mark as processing
         self.processing
             .entry(header.round)
             .or_insert_with(HashSet::new)
             .insert(header.id);

         // Create and send vote for this header
         self.send_vote(header).await
     }

     /// Send vote for header (from reference implementation)
     async fn send_vote(&mut self, header: &Header) -> DagResult<()> {
         let vote = Vote::new(header, &self.name);
         debug!("Created vote for header {}", header.id);

         // Process our own vote
         self.process_vote(vote).await
     }

     /// Process vote (adapted from reference implementation)  
     async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
         debug!("Processing vote from {}", vote.author);
         
         let header_id = vote.id;
         
         // Store vote
         self.pending_votes.entry(header_id).or_insert_with(Vec::new).push(vote);
         
         // Check if we have enough votes for certificate
         let votes = &self.pending_votes[&header_id];
         let total_stake: u64 = votes.iter()
             .map(|v| self.committee.stake(&v.author))
             .sum();
         
         let quorum_threshold = self.committee.quorum_threshold();
         
         if total_stake >= quorum_threshold {
             // Create certificate
             if let Some(header) = self.current_header.as_ref().filter(|h| h.id == header_id) {
                 // Convert votes to (PublicKey, Signature) format
                 let vote_sigs: Vec<(PublicKey, Signature)> = votes.iter()
                     .map(|v| (v.author.clone(), v.signature.clone()))
                     .collect();
                 
                 let certificate = Certificate::new(&self.committee, header.clone(), vote_sigs)?;
                 
                 info!("Created certificate from {} votes", votes.len());
                 
                 // Process the certificate
                 self.process_certificate(certificate).await?;
                 
                 // Clean up votes
                 self.pending_votes.remove(&header_id);
             }
         }

         Ok(())
     }

     /// Process certificate (adapted from reference implementation)
     async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
         debug!("Processing certificate for round {}", certificate.round());

         let cert_digest = certificate.digest();
         let author = certificate.origin();

         // Store certificate
         self.certificates.insert(cert_digest, certificate.clone());
         self.latest_certificates.insert(author, certificate.clone());

         // Send to Bullshark consensus
         if let Err(e) = self.certificate_output_sender.send(certificate) {
             warn!("Failed to send certificate to Bullshark: {}", e);
         }

         Ok(())
     }

     /// Update committee and cleanup state (from reference implementation)
     async fn change_epoch(&mut self, committee: Committee) {
         info!("Changing epoch from {} to {}", self.committee.epoch, committee.epoch);
         
         // Update committee
         self.committee = committee;
         
         // Reset state for new epoch
         self.current_round = 1;
         self.current_batch.clear();
         self.processing.clear();
         self.pending_votes.clear();
         self.current_header = None;
         // Keep certificates for parent tracking
           }

    /// Get the current round
    pub fn current_round(&self) -> Round {
        self.current_round
    }

    /// Advance to the next round
    pub fn advance_round(&mut self) {
        self.current_round += 1;
        info!("Advanced to round {}", self.current_round);
    }
}

/// Configuration for DAG service channels (adapted from reference implementation)
#[derive(Debug)]
pub struct DagServiceChannels {
    /// Send new certificates to consensus (output from DAG)
    pub certificate_sender: mpsc::UnboundedSender<Certificate>,
    /// Receive new certificates from consensus (input to DAG)
    pub certificate_receiver: mpsc::UnboundedReceiver<Certificate>,
    /// Send transactions to DAG
    pub transaction_sender: mpsc::UnboundedSender<Transaction>,
    /// Send network messages (headers, votes, certificates) to DAG
    pub network_message_sender: mpsc::UnboundedSender<DagMessage>,
    /// Committee updates
    pub committee_sender: watch::Sender<Committee>,
}

impl DagServiceChannels {
    /// Create new channels for DAG service
    pub fn new() -> Self {
        let (certificate_sender, certificate_receiver) = mpsc::unbounded_channel();
        let (transaction_sender, _) = mpsc::unbounded_channel();
        let (network_message_sender, _) = mpsc::unbounded_channel();
        let (committee_sender, _) = watch::channel(Committee::new(0, Default::default()));

        Self {
            certificate_sender,
            certificate_receiver,
            transaction_sender,
            network_message_sender,
            committee_sender,
        }
    }
}
