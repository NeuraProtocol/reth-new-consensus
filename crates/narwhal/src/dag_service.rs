//! Narwhal DAG service implementation adapted from reference implementation

use crate::{
    types::{Certificate, Committee, Header, Vote, PublicKey, Signature, HeaderDigest}, 
    NarwhalConfig, DagError, DagResult, Transaction, Round, Batch, BatchDigest, WorkerId,
    aggregators::VotesAggregator,
    storage_trait::{DagStorageInterface, DagStorageRef},
    metrics_collector::{metrics, MetricTimer},
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, debug};
use fastcrypto::traits::EncodeDecodeBase64;
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
    certificate_output_sender: mpsc::Sender<Certificate>,
    /// Watch channel for reconfiguration
    reconfigure_receiver: watch::Receiver<Committee>,
    /// Storage backend for persistence
    storage: DagStorageRef,
    /// Sender for outbound network messages (headers, votes, certificates)
    network_sender: Option<mpsc::UnboundedSender<DagMessage>>,
    /// Receiver for batch digests from workers
    rx_batch_digests: Option<mpsc::UnboundedReceiver<(BatchDigest, WorkerId)>>,
    
    // State tracking (from reference implementation)
    /// Current round for transaction batching
    current_round: Round,
    /// The set of headers we are currently processing
    processing: HashMap<Round, HashSet<HeaderDigest>>,
    /// The last header we proposed
    current_header: Option<Header>,
    /// Vote aggregators for ALL headers we've seen (not just our own)
    /// Maps HeaderDigest -> VotesAggregator
    vote_aggregators: HashMap<HeaderDigest, VotesAggregator>,
    /// Certificate storage for parent tracking
    certificates: HashMap<CertificateDigest, Certificate>,
    /// Latest certificates by author for parent tracking (for DAG parents)
    latest_certificates: HashMap<PublicKey, Certificate>,
    /// Current transaction batch being assembled
    current_batch: Vec<Transaction>,
    /// Worker batch digests ready to be included in the next header
    worker_batches: IndexMap<BatchDigest, WorkerId>,
    /// Garbage collection round - we can clean up data older than this
    gc_round: Round,
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
        certificate_output_sender: mpsc::Sender<Certificate>,
        reconfigure_receiver: watch::Receiver<Committee>,
        storage: DagStorageRef,
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
            storage,
            network_sender: None,
            rx_batch_digests: None,
            current_round: 1,
            processing: HashMap::new(),
            current_header: None,
            vote_aggregators: HashMap::new(),
            certificates: HashMap::new(),
            latest_certificates: HashMap::new(),
            current_batch: Vec::new(),
            worker_batches: IndexMap::new(),
            gc_round: 0,
        }
    }
    
    /// Create a new DAG service with network sender
    pub fn with_network_sender(
        name: PublicKey,
        committee: Committee,
        config: NarwhalConfig,
        signature_service: SignatureService<Signature>,
        transaction_receiver: mpsc::UnboundedReceiver<Transaction>,
        rx_network_messages: mpsc::UnboundedReceiver<DagMessage>,
        certificate_output_sender: mpsc::Sender<Certificate>,
        reconfigure_receiver: watch::Receiver<Committee>,
        storage: DagStorageRef,
        network_sender: mpsc::UnboundedSender<DagMessage>,
    ) -> Self {
        let mut service = Self::new(
            name,
            committee,
            config,
            signature_service,
            transaction_receiver,
            rx_network_messages,
            certificate_output_sender,
            reconfigure_receiver,
            storage,
        );
        service.network_sender = Some(network_sender);
        service
    }
    
    /// Set the batch digest receiver for worker integration
    pub fn with_batch_digest_receiver(
        mut self,
        rx_batch_digests: mpsc::UnboundedReceiver<(BatchDigest, WorkerId)>,
    ) -> Self {
        self.rx_batch_digests = Some(rx_batch_digests);
        self
    }

    /// Spawn the DAG service
    pub fn spawn(self) -> JoinHandle<DagResult<()>> {
        tokio::spawn(async move {
            info!("DAG service task started");
            let result = self.run().await;
            warn!("DAG service task completed with result: {:?}", result);
            result
        })
    }

    /// Main run loop for the DAG service (adapted from reference implementation)
    pub async fn run(mut self) -> DagResult<()> {
        info!("DAG service started for node {}", self.name);
        info!("Certificate output channel is ready: {}", !self.certificate_output_sender.is_closed());
        
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
                    
                    // Record metrics
                    if let Some(m) = metrics() {
                        m.record_transaction_received("mempool");
                        m.set_transactions_in_flight("batching", self.current_batch.len() as i64);
                    }
                    
                    // Check if we should create a header
                    let enough_transactions = self.current_batch.len() >= self.config.max_batch_size;
                    let timer_expired = timer.is_elapsed();
                    
                    if (enough_transactions || timer_expired) && !self.current_batch.is_empty() {
                        // Also check quorum before creating header
                        let prev_round = self.current_round.saturating_sub(1);
                        let mut total_stake = 0u64;
                        
                        // Special case for round 1 - we can always proceed from genesis
                        if self.current_round == 1 {
                            total_stake = self.committee.total_stake();
                        } else {
                            for (author, cert) in &self.latest_certificates {
                                if cert.round() == prev_round {
                                    total_stake += self.committee.stake(author);
                                }
                            }
                        }
                        
                        if total_stake >= self.committee.quorum_threshold() {
                            self.create_and_propose_header().await?;
                        } else {
                            debug!("Have {} transactions but only {} stake, waiting for quorum", 
                                   self.current_batch.len(), total_stake);
                        }
                        
                        // Reset timer
                        let deadline = tokio::time::Instant::now() + max_header_delay;
                        timer.as_mut().reset(deadline);
                    }
                    
                    Ok(())
                },

                // Process batch digests from workers
                Some((batch_digest, worker_id)) = async {
                    if let Some(ref mut rx) = self.rx_batch_digests {
                        rx.recv().await
                    } else {
                        None
                    }
                } => {
                    info!("Primary received batch digest from worker {}", worker_id);
                    self.worker_batches.insert(batch_digest, worker_id);
                    
                    // Check if we should create a header now that we have worker batches
                    let enough_batches = self.worker_batches.len() >= self.config.max_batch_size;
                    let timer_expired = timer.is_elapsed();
                    
                    if (enough_batches || timer_expired) && !self.worker_batches.is_empty() {
                        // Check quorum before creating header
                        let prev_round = self.current_round.saturating_sub(1);
                        let mut total_stake = 0u64;
                        
                        if self.current_round == 1 {
                            total_stake = self.committee.total_stake();
                        } else {
                            for (author, cert) in &self.latest_certificates {
                                if cert.round() == prev_round {
                                    total_stake += self.committee.stake(author);
                                }
                            }
                        }
                        
                        if total_stake >= self.committee.quorum_threshold() {
                            info!("Have {} worker batches and quorum, creating header", self.worker_batches.len());
                            self.create_and_propose_header().await?;
                        } else {
                            debug!("Have {} worker batches but only {} stake, waiting for quorum", 
                                   self.worker_batches.len(), total_stake);
                        }
                        
                        // Reset timer
                        let deadline = tokio::time::Instant::now() + max_header_delay;
                        timer.as_mut().reset(deadline);
                    }
                    
                    Ok(())
                },

                // Timer for active block production (heartbeat)
                () = &mut timer => {
                    // Check if we have quorum of certificates from previous round before advancing
                    let prev_round = self.current_round.saturating_sub(1);
                    let mut total_stake = 0u64;
                    let mut cert_count = 0;
                    
                    if prev_round == 0 {
                        // Genesis case - we can always proceed from round 1
                        total_stake = self.committee.total_stake();
                        cert_count = self.committee.authorities.len();
                    } else {
                        for (author, cert) in &self.latest_certificates {
                            if cert.round() == prev_round {
                                total_stake += self.committee.stake(author);
                                cert_count += 1;
                            }
                        }
                    }
                    
                    if total_stake >= self.committee.quorum_threshold() {
                        // Only create header if we have transactions or worker batches
                        // This prevents creating thousands of empty headers
                        if !self.current_batch.is_empty() || !self.worker_batches.is_empty() {
                            info!("Timer expired for round {}, have quorum ({} certs, {} stake), {} transactions and {} worker batches, creating header", 
                                  self.current_round, cert_count, total_stake, self.current_batch.len(), self.worker_batches.len());
                            self.create_and_propose_header().await?;
                        } else {
                            debug!("Timer expired for round {} with quorum but no transactions or worker batches, skipping header creation", 
                                   self.current_round);
                        }
                    } else {
                        debug!("Timer expired for round {} but only have {} stake (need {}), waiting for more certificates", 
                               self.current_round, total_stake, self.committee.quorum_threshold());
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
         
         // Get ALL certificates from previous round as parents
         // This is critical for Bullshark consensus - we need to include ALL certificates
         // from round r-1, not just the latest per validator
         let prev_round = self.current_round.saturating_sub(1);
         let parents: BTreeSet<CertificateDigest> = if prev_round == 0 {
             // Genesis case - use genesis certificates from committee
             self.committee.authorities.keys()
                 .map(|_| CertificateDigest::default()) // Genesis certificates
                 .collect()
         } else {
             self.certificates
                 .values()
                 .filter(|cert| cert.round() == prev_round)
                 .map(|cert| cert.digest())
                 .collect()
         };
             
         info!("Including {} certificates from round {} as parents", parents.len(), prev_round);

         // Create payload - include both primary batch and worker batches
         let mut payload = IndexMap::new();
         
         // Include primary's own batch if we have transactions
         if !self.current_batch.is_empty() {
            let batch = Batch(self.current_batch.clone());
            payload.insert(batch.digest(), 0u32); // worker_id = 0 for primary
         }
         
         // Include all worker batches
         for (batch_digest, worker_id) in &self.worker_batches {
             payload.insert(*batch_digest, *worker_id);
         }
         
         info!("Creating header with {} batches ({} from primary, {} from workers)", 
               payload.len(), 
               if self.current_batch.is_empty() { 0 } else { 1 },
               self.worker_batches.len());

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
        
        // Create vote aggregator for this header
        self.vote_aggregators.insert(header.id, VotesAggregator::with_header(header.clone()));
         
         // Mark as processing
         self.processing
             .entry(header.round)
             .or_insert_with(HashSet::new)
             .insert(header.id);

         // Send header for network broadcast
         if let Some(ref sender) = self.network_sender {
             if sender.send(DagMessage::Header(header.clone())).is_err() {
                 warn!("Failed to send header for broadcast - channel closed");
             } else {
                 info!("✅ Sent header {} to network sender for broadcast", header.id);
             }
         } else {
             warn!("❌ No network sender configured, skipping header broadcast");
         }
         
         info!("Created header {} for round {}", header.id, header.round);

         // Record metrics
         if let Some(m) = metrics() {
             m.record_header_created(&self.name.to_string());
             m.record_transactions_batched("0", self.current_batch.len() as u64);
             m.record_batch_size("0", self.current_batch.len() as f64);
             m.set_dag_depth(&self.name.to_string(), self.current_round as i64);
         }

         // Clear batch and worker batches, then advance round
         self.current_batch.clear();
         self.worker_batches.clear();
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
             warn!(
                 "Unknown authority: {} (base64: {}). Committee has {} members: {:?}", 
                 header.author.to_string(),
                 header.author.encode_base64(),
                 self.committee.authorities.len(),
                 self.committee.authorities.keys().map(|k| k.encode_base64()).collect::<Vec<_>>()
             );
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

         // In Narwhal, votes are for ANY header, not just our own
         // The vote's validity is checked by its signature, not by matching our current header
         // We'll validate that we have seen the header the vote is for in process_vote

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
             
         // Create vote aggregator for this header if we don't have one
         if !self.vote_aggregators.contains_key(&header.id) {
             self.vote_aggregators.insert(header.id, VotesAggregator::with_header(header.clone()));
         }

         // Create and send vote for this header
         self.send_vote(header).await
     }

     /// Send vote for header (from reference implementation)
     async fn send_vote(&mut self, header: &Header) -> DagResult<()> {
         // Create vote with proper signature using fastcrypto API
         let vote = {
             let mut sig_service = self.signature_service.lock().await;
             Vote::new_with_signature_service(header, &self.name, &mut *sig_service).await
         };
         debug!("Created signed vote for header {}", header.id);

         // Send vote for network broadcast
         if let Some(ref sender) = self.network_sender {
             if sender.send(DagMessage::Vote(vote.clone())).is_err() {
                 warn!("Failed to send vote for broadcast - channel closed");
             } else {
                 debug!("Sent vote for header {} for network broadcast", header.id);
             }
         } else {
             debug!("No network sender configured, skipping vote broadcast");
         }

         // Process our own vote
         self.process_vote(vote).await
     }

    /// Process vote (adapted from reference implementation)  
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        info!("Processing vote from {} for header {}", vote.author, vote.id);
        
        // Record metrics
        if let Some(m) = metrics() {
            m.record_vote_cast(&vote.author.to_string(), vote.round);
        }
        
        // Find the vote aggregator for this header
        let aggregator = self.vote_aggregators.get_mut(&vote.id)
            .ok_or_else(|| DagError::Consensus(format!("No header found for vote: {}", vote.id)))?;
        
        // Store vote in persistent storage
        let _timer = metrics().map(|m| MetricTimer::new(m.storage_operation_duration.clone(), vec!["write", "votes"]));
        self.storage.store_vote(vote.id, vote.clone()).await?;
        
        // Add vote to aggregator
        aggregator.add_vote(vote.clone(), &self.committee)?;
        
        // Log current aggregator state
        info!("Vote aggregator for header {} now has {} votes with stake {} (quorum threshold: {})",
              vote.id, aggregator.vote_count(), aggregator.stake(), self.committee.quorum_threshold());
        
        // Check if we can form a certificate
        if let Some(certificate) = aggregator.try_form_certificate(&self.committee)? {
            let header_id = certificate.header.id;
            info!("Created certificate for header {} with {} votes", 
                  header_id, aggregator.vote_count());
            
            // Record metrics
            if let Some(m) = metrics() {
                m.record_certificate_formed(&certificate.origin().to_string(), certificate.round());
            }
            
            // Send certificate for network broadcast
            if let Some(ref sender) = self.network_sender {
                if sender.send(DagMessage::Certificate(certificate.clone())).is_err() {
                    warn!("Failed to send certificate for broadcast - channel closed");
                } else {
                    debug!("Sent certificate for network broadcast");
                }
            }
            
            // Process the certificate
            self.process_certificate(certificate).await?;
            
            // Remove the aggregator for this header since we have a certificate
            self.vote_aggregators.remove(&vote.id);
        }

        Ok(())
    }

     /// Process certificate (adapted from reference implementation)
     async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
         let cert_digest = certificate.digest();
         let author = certificate.origin();
         let cert_round = certificate.round();
         
         // Check if we've already processed this certificate
         if self.certificates.contains_key(&cert_digest) {
             debug!("Certificate {} from {} for round {} already processed, skipping", 
                   cert_digest, author, cert_round);
             return Ok(());
         }
         
         info!("Processing certificate from {} for round {}", author, cert_round);

         // Store certificate in persistent storage
         let _timer = metrics().map(|m| MetricTimer::new(m.storage_operation_duration.clone(), vec!["write", "certificates"]));
         self.storage.store_certificate(certificate.clone()).await?;
         self.storage.store_latest_certificate(author.clone(), certificate.clone()).await?;
         
         // Record metrics
         if let Some(m) = metrics() {
             m.record_certificate_stored(cert_round);
             m.record_storage_operation("write", "certificates");
         }

         // Also keep in memory for fast access
         self.certificates.insert(cert_digest, certificate.clone());
         self.latest_certificates.insert(author.clone(), certificate.clone());
         
         // Log certificate tracking
         let certs_in_round = self.certificates.values()
             .filter(|c| c.round() == cert_round)
             .count();
         info!("Now tracking {} certificates for round {} (total: {})", 
               certs_in_round, cert_round, self.certificates.len());

         // Send to Bullshark consensus
         match self.certificate_output_sender.try_send(certificate.clone()) {
             Ok(()) => {
                 debug!("Sent certificate for round {} to Bullshark consensus", cert_round);
             }
             Err(mpsc::error::TrySendError::Full(_)) => {
                 warn!("Certificate channel full, Bullshark is falling behind. Dropping certificate for round {}", cert_round);
                 // Note: In production, we might want to handle this differently,
                 // but for now we drop to prevent unbounded memory growth
             }
             Err(mpsc::error::TrySendError::Closed(_)) => {
                 warn!("Failed to send certificate to Bullshark: channel closed");
             }
         }

         // Check if we should advance rounds based on certificates collected
         self.try_advance_round(cert_round).await?;
         
         // Perform garbage collection if needed
         self.garbage_collect(cert_round);

         Ok(())
     }
     
     /// Try to advance to the next round if we have enough certificates
     async fn try_advance_round(&mut self, cert_round: Round) -> DagResult<()> {
         // Only consider advancing if this certificate is from our current round - 1
         if cert_round != self.current_round.saturating_sub(1) {
             return Ok(());
         }
         
         // Count certificates from the previous round
         let mut total_stake = 0u64;
         let mut certificates_for_round = Vec::new();
         
         for (author, cert) in &self.latest_certificates {
             if cert.round() == cert_round {
                 total_stake += self.committee.stake(author);
                 certificates_for_round.push(cert.clone());
             }
         }
         
         // Check if we have quorum of certificates from previous round
         if total_stake >= self.committee.quorum_threshold() {
             info!("Collected quorum of certificates for round {}, can advance to round {}", 
                   cert_round, self.current_round + 1);
             
             // Check if we should advance the round
             // We advance if:
             // 1. We have pending transactions OR
             // 2. We're falling behind (the certificates are from the previous round)
             if !self.current_batch.is_empty() || cert_round == self.current_round - 1 {
                 self.create_and_propose_header().await?;
             }
         }
         
         Ok(())
     }
     
     /// Garbage collect old state
     fn garbage_collect(&mut self, cert_round: Round) {
         // Update GC round if we've advanced
         let new_gc_round = cert_round.saturating_sub(self.config.gc_depth);
         if new_gc_round > self.gc_round {
             self.gc_round = new_gc_round;
             
             // Clean up old processing state
             self.processing.retain(|&round, _| round > self.gc_round);
             
             // Clean up old certificates (but keep latest certificates for parent tracking)
             self.certificates.retain(|_, cert| cert.round() > self.gc_round);
             
             // Clean up vote aggregators for old headers
             self.vote_aggregators.retain(|header_id, _| {
                 // Keep aggregators for headers we're still processing
                 self.processing.values()
                     .any(|headers| headers.contains(header_id))
             });
             
             debug!("Garbage collected state up to round {}", self.gc_round);
         }
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
         self.vote_aggregators.clear();
         self.current_header = None;
         self.gc_round = 0;
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
    pub certificate_sender: mpsc::Sender<Certificate>,
    /// Receive new certificates from consensus (input to DAG)
    pub certificate_receiver: mpsc::Receiver<Certificate>,
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
        let (certificate_sender, certificate_receiver) = mpsc::channel(1000);
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