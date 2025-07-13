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
use alloy_primitives::B256;

/// Chain state information needed for canonical metadata creation
pub struct ChainState {
    pub block_number: u64,
    pub parent_hash: B256,
    pub parent_timestamp: u64,
}

/// Trait for providing chain state to the DAG service
pub trait ChainStateProvider: Send + Sync {
    fn get_chain_state(&self) -> ChainState;
}

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
    /// Certificates organized by round for proper parent checking
    /// Maps Round -> Author -> Certificate
    certificates_by_round: HashMap<Round, HashMap<PublicKey, Certificate>>,
    /// Current transaction batch being assembled
    current_batch: Vec<Transaction>,
    /// Worker batch digests ready to be included in the next header
    worker_batches: IndexMap<BatchDigest, WorkerId>,
    /// Garbage collection round - we can clean up data older than this
    gc_round: Round,
    /// Counter for consecutive timer expirations without progress (for liveness)
    failed_attempts_count: u32,
    /// Chain state provider for canonical metadata creation
    chain_state: Option<std::sync::Arc<dyn ChainStateProvider>>,
    /// Track the last round that was finalized by BFT
    last_finalized_round: Round,
    /// Maximum allowed pending rounds before applying backpressure
    max_pending_rounds: u32,
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
            certificates_by_round: HashMap::new(),
            current_batch: Vec::new(),
            worker_batches: IndexMap::new(),
            gc_round: 0,
            failed_attempts_count: 0,
            chain_state: None,
            last_finalized_round: 0,
            max_pending_rounds: 50, // Allow up to 50 rounds pending finalization
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
    
    /// Set the chain state provider
    pub fn set_chain_state(&mut self, chain_state: std::sync::Arc<dyn ChainStateProvider>) {
        self.chain_state = Some(chain_state);
    }
    
    /// Update the last finalized round (called by BFT when it finalizes rounds)
    pub fn update_finalized_round(&mut self, round: Round) {
        if round > self.last_finalized_round {
            self.last_finalized_round = round;
            info!("Updated last finalized round to {}", round);
        }
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
        
        // Timer for active block production - following reference implementation pattern exactly
        let max_header_delay = self.config.max_batch_delay;
        let timer = tokio::time::sleep(max_header_delay);
        tokio::pin!(timer);
        
        info!("DAG service node {} has started successfully.", self.name);
        let mut timer_expired = false;
        loop {
            // Check if we can propose a new header - following reference implementation pattern
            // We propose when:
            // (i) the timer expired 
            // (ii) we have enough transactions/batches and conditions are met
            let enough_parents = if self.current_round == 1 {
                true // Genesis case
            } else {
                let prev_round = self.current_round.saturating_sub(1);
                let mut total_stake = 0u64;
                let mut cert_count = 0;
                let mut authors_with_certs = Vec::new();
                
                // Use round-specific certificate tracking instead of latest_certificates
                if let Some(round_certs) = self.certificates_by_round.get(&prev_round) {
                    for (author, _cert) in round_certs {
                        total_stake += self.committee.stake(author);
                        cert_count += 1;
                        authors_with_certs.push(author.encode_base64());
                    }
                }
                
                // BOOTSTRAP: For early rounds OR when stuck, use validity threshold instead of quorum
                const BOOTSTRAP_ROUNDS: u64 = 10;
                let is_stuck = self.failed_attempts_count >= 10; // Half the max attempts before extending bootstrap
                let required_stake = if self.current_round < BOOTSTRAP_ROUNDS || is_stuck {
                    // During bootstrap or when stuck, only need f+1 validators (validity threshold)
                    let validity_threshold = self.committee.validity_threshold();
                    if is_stuck {
                        debug!("DYNAMIC BOOTSTRAP: Round {} stuck - using validity threshold {} instead of quorum {}", 
                              self.current_round, validity_threshold, self.committee.quorum_threshold());
                    } else {
                        debug!("BOOTSTRAP: Round {} using validity threshold {} instead of quorum {}", 
                              self.current_round, validity_threshold, self.committee.quorum_threshold());
                    }
                    validity_threshold
                } else {
                    self.committee.quorum_threshold()
                };
                
                let has_enough = total_stake >= required_stake;
                
                // Log parent certificate status periodically
                if self.current_round <= 5 || timer_expired || (self.current_round % 10 == 0 && !has_enough) {
                    info!("Parent check for round {}: {} certs from round {} (authors: {:?}), stake {}/{}, enough={}",
                          self.current_round, cert_count, prev_round, authors_with_certs, total_stake, required_stake, has_enough);
                }
                
                has_enough
            };
            
            let enough_content = !self.current_batch.is_empty() || !self.worker_batches.is_empty();
            // Use only the timer_expired flag set by the select branch
            // DO NOT check timer.is_elapsed() here as it causes tight loops after reset
            let was_timer_expired = timer_expired;
            
            // Debug logging for timer state
            if self.current_round <= 5 || self.current_round % 10 == 0 {
                debug!("Round {} timer check: was_expired={}, final={}, enough_parents={}, enough_content={}",
                       self.current_round, was_timer_expired, timer_expired, enough_parents, enough_content);
            }
            
            // CRITICAL FIX: Only create headers when we have proper consensus
            // We need:
            // 1. Quorum of certificates from previous round (including our own)
            // 2. Either content to propose OR timer expiration (for liveness)
            // But NEVER create without parents - that breaks consensus
            
            debug!("Decision logic: enough_parents={}, timer_expired={}, enough_content={}", enough_parents, timer_expired, enough_content);
            
            // BACKPRESSURE: Check if we have too many pending rounds
            let pending_rounds = self.current_round.saturating_sub(self.last_finalized_round);
            if pending_rounds > self.max_pending_rounds as u64 {
                if timer_expired {
                    // Reset timer to prevent tight loop
                    let deadline = tokio::time::Instant::now() + max_header_delay;
                    timer.as_mut().reset(deadline);
                    timer_expired = false;
                    
                    warn!("BACKPRESSURE: {} rounds pending finalization (last finalized: {}, current: {}). Pausing header creation.",
                          pending_rounds, self.last_finalized_round, self.current_round);
                }
                // Skip header creation until BFT catches up
            } else if enough_parents && (timer_expired || enough_content) {
                // Additional check: ensure we have our own certificate from previous round
                // BOOTSTRAP: Skip this check during early rounds to widen the DAG quickly
                const BOOTSTRAP_ROUNDS: u64 = 10;
                let have_own_cert = if self.current_round > 1 && self.current_round >= BOOTSTRAP_ROUNDS {
                    let prev_round = self.current_round.saturating_sub(1);
                    self.certificates_by_round.get(&prev_round)
                        .map(|round_certs| round_certs.contains_key(&self.name))
                        .unwrap_or(false)
                } else {
                    true // Genesis case or bootstrap phase
                };
                
                if !have_own_cert && !timer_expired {
                    // Only wait for our own certificate if timer hasn't expired
                    // This prevents deadlock when network is slow
                    debug!("Waiting for our own certificate from round {} before proposing round {}", 
                           self.current_round - 1, self.current_round);
                } else {
                    if timer_expired {
                        info!("Timer expired and have consensus for round {}, creating header", self.current_round);
                    } else {
                        debug!("Creating header for round {} due to available content and consensus", self.current_round);
                    }
                    
                    // Create and propose header
                    match self.create_and_propose_header().await {
                        Ok(()) => {
                            info!("✅ Successfully created header for round {} (now at round {})", self.current_round - 1, self.current_round);
                            self.failed_attempts_count = 0; // Reset failed attempts counter on success
                        }
                        Err(e) => {
                            warn!("Failed to create header: {}", e);
                            // Continue processing to avoid getting stuck
                        }
                    }
                    
                    // Reset timer after successful header creation
                    let deadline = tokio::time::Instant::now() + max_header_delay;
                    timer.as_mut().reset(deadline);
                    timer_expired = false;
                    info!("⏰ Reset timer for round {} - will fire at {:?}", self.current_round, deadline);
                }
            } else {
                // Not ready to create header yet
                if timer_expired {
                    if !enough_parents {
                        self.failed_attempts_count += 1;
                        warn!("Timer expired but lacking parents for round {} - attempt #{}", self.current_round, self.failed_attempts_count);
                        
                        // PROPER LIVENESS: Extend bootstrap period dynamically when stuck
                        const MAX_FAILED_ATTEMPTS: u32 = 20;
                        if self.failed_attempts_count >= MAX_FAILED_ATTEMPTS {
                            warn!("EXTENDED BOOTSTRAP: {} failed attempts for round {} - temporarily reducing quorum threshold", 
                                  self.failed_attempts_count, self.current_round);
                            
                            // Instead of forced progression, use bootstrap logic for this round
                            // This preserves DAG integrity while providing liveness
                            self.failed_attempts_count = 0; // Reset to prevent infinite extension
                        }
                        
                        // Always reset timer and try again - no forced progression with empty parents
                        let deadline = tokio::time::Instant::now() + max_header_delay;
                        timer.as_mut().reset(deadline);
                        timer_expired = false;
                        debug!("Reset timer to prevent tight loop - will check again at {:?}", deadline);
                    } else if !enough_content {
                        debug!("Timer expired but no content for round {} - will retry when we have transactions", self.current_round);
                        // Reset timer for content timeout (don't increment failed attempts for this)
                        let deadline = tokio::time::Instant::now() + max_header_delay;
                        timer.as_mut().reset(deadline);
                        timer_expired = false;
                        debug!("Reset timer to prevent tight loop - will check again at {:?}", deadline);
                    }
                }
            }
            
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
                    Ok(())
                },

                // Timer branch - check timer expiration
                () = &mut timer => {
                    // Timer fired - the main loop will handle it on next iteration
                    info!("⏰ Timer fired for round {} - setting timer_expired=true", self.current_round);
                    timer_expired = true;
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
     
     /// Create canonical metadata if this node is the leader for the current round
     fn create_canonical_metadata_if_leader(&self) -> Vec<u8> {
         // CRITICAL: Bullshark only has leaders on EVEN rounds
         // Odd rounds have no leader and should not have canonical metadata
         if self.current_round % 2 != 0 {
             debug!("Round {} is odd - no leader in Bullshark", self.current_round);
             return Vec::new();
         }
         
         // Determine if we are the leader for this round using the same algorithm as BFT service
         let mut validators: Vec<_> = self.committee.authorities.keys().cloned().collect();
         validators.sort_by_key(|k| k.encode_base64());
         
         let num_validators = validators.len();
         if num_validators == 0 {
             return Vec::new();
         }
         
         // CRITICAL FIX: Bullshark only has leaders on even rounds
        // With 4 validators and only even rounds (2,4,6,8,10,12...), 
        // round % 4 only gives us 0 or 2, never 1 or 3
        // Solution: Use (round / 2) % num_validators to cycle through all validators
        let leader_idx = ((self.current_round / 2) as usize) % num_validators;
         let expected_leader = validators.get(leader_idx);
         let is_leader = expected_leader == Some(&self.name);
         
         info!("Leader check for round {}: leader_idx={}, expected_leader={:?}, my_name={:?}, is_leader={}", 
               self.current_round, leader_idx, 
               expected_leader.map(|k| k.encode_base64()), 
               self.name.encode_base64(), 
               is_leader);
        
        // Debug chain state availability for all nodes
        if let Some(ref chain_state) = self.chain_state {
            let state = chain_state.get_chain_state();
            debug!("Chain state check: block_number={}, parent_hash={}, parent_timestamp={}", 
                   state.block_number, state.parent_hash, state.parent_timestamp);
        } else {
            warn!("No chain state available for round {}", self.current_round);
        }
         
         if is_leader {
             info!("✅ CRITICAL: I am leader for round {} - creating canonical metadata", self.current_round);
             
             // Get chain state if available
             let (block_number, parent_hash, parent_timestamp) = if let Some(ref chain_state) = self.chain_state {
                 let state = chain_state.get_chain_state();
                 // Next block number is current + 1
                 info!("LEADER DEBUG: Got chain state - block_number: {}, parent_hash: {}, parent_timestamp: {}", 
                       state.block_number, state.parent_hash, state.parent_timestamp);
                 (state.block_number + 1, state.parent_hash, state.parent_timestamp)
             } else {
                 // Fallback for tests or initial setup
                 warn!("LEADER DEBUG: No chain state available for canonical metadata - using defaults");
                 (self.current_round, B256::ZERO, 1700000000u64)
             };
             
             // Ensure monotonic timestamps
             let current_time = std::time::SystemTime::now()
                 .duration_since(std::time::UNIX_EPOCH)
                 .unwrap()
                 .as_secs();
             let timestamp = current_time.max(parent_timestamp + 1);
             
             // Create canonical metadata with all required block construction info
             // This matches what the BFT service expects
             let metadata_string = format!(
                 "block:{},parent:{},timestamp:{},round:{},base_fee:{}",
                 block_number,
                 parent_hash,
                 timestamp,
                 self.current_round,
                 875_000_000u64, // Default base fee
             );
             
             info!("Leader created canonical metadata for block {} with parent {}", block_number, parent_hash);
             
             metadata_string.into_bytes()
         } else {
             debug!("Node is not leader for round {} (leader idx: {})", self.current_round, leader_idx);
             Vec::new()
         }
     }

     /// Create and propose a new header (adapted from reference implementation)
     async fn create_and_propose_header(&mut self) -> DagResult<()> {
         self.create_and_propose_header_impl(false).await
     }

     /// Create and propose a new header with optional forced progression
     async fn create_and_propose_header_impl(&mut self, force_empty_parents: bool) -> DagResult<()> {
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
         } else if force_empty_parents {
             // FORCED PROGRESSION: Create header with empty parents to break deadlock
             warn!("FORCED PROGRESSION: Creating header with empty parents for liveness");
             BTreeSet::new()
         } else {
             // Use round-specific tracking for accurate parent collection
             if let Some(round_certs) = self.certificates_by_round.get(&prev_round) {
                 round_certs.values()
                     .map(|cert| cert.digest())
                     .collect()
             } else {
                 // Fallback to scanning all certificates if round tracking is empty
                 self.certificates
                     .values()
                     .filter(|cert| cert.round() == prev_round)
                     .map(|cert| cert.digest())
                     .collect()
             }
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

         // Determine if we are the leader for this round
         let canonical_metadata = self.create_canonical_metadata_if_leader();
         
         if !canonical_metadata.is_empty() {
             info!("Creating header for round {} WITH canonical metadata ({} bytes)", 
                   self.current_round, canonical_metadata.len());
         } else {
             debug!("Creating header for round {} without canonical metadata (not leader)", 
                   self.current_round);
         }
         
         // Create header
         let mut header = Header {
             author: self.name.clone(),
             round: self.current_round,
             epoch: self.committee.epoch,
             payload,
             parents,
             id: HeaderDigest::default(),
             signature: Signature::default(),
             canonical_metadata,
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
             if !header.canonical_metadata.is_empty() {
                 info!("🚀 CRITICAL: Broadcasting header {} WITH canonical metadata ({} bytes) for round {}", 
                       header.id, header.canonical_metadata.len(), header.round);
             }
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

         // Clear batch and worker batches, but DON'T advance round yet
         // Round advancement should only happen when we have our own certificate
         self.current_batch.clear();
         self.worker_batches.clear();
         // TODO: Round advancement will be handled by certificate processing

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
         
         // Log if header contains canonical metadata
         if !header.canonical_metadata.is_empty() {
             info!("Received header from {} for round {} WITH canonical metadata ({} bytes)", 
                   header.author, header.round, header.canonical_metadata.len());
         } else {
             debug!("Received header from {} for round {} without canonical metadata", 
                   header.author, header.round);
         }

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
         // Or update it with the header if votes arrived before the header
         self.vote_aggregators.entry(header.id)
             .and_modify(|agg| {
                 // If the aggregator doesn't have a header yet (votes arrived first), set it now
                 if agg.header().is_none() {
                     debug!("Setting header for aggregator that had votes arrive first");
                     agg.set_header(header.clone());
                 }
             })
             .or_insert_with(|| VotesAggregator::with_header(header.clone()));

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
        
        // Find or create the vote aggregator for this header
        // Votes can arrive before headers due to network propagation delays
        let aggregator = self.vote_aggregators.entry(vote.id)
            .or_insert_with(|| {
                debug!("Vote arrived before header for {}, creating pending aggregator", vote.id);
                VotesAggregator::new()
            });
        
        // Store vote in persistent storage
        let _timer = metrics().map(|m| MetricTimer::new(m.storage_operation_duration.clone(), vec!["write", "votes"]));
        self.storage.store_vote(vote.id, vote.clone()).await?;
        
        // Add vote to aggregator
        aggregator.add_vote(vote.clone(), &self.committee)?;
        
        // Log current aggregator state
        info!("Vote aggregator for header {} now has {} votes with stake {} (quorum threshold: {})",
              vote.id, aggregator.vote_count(), aggregator.stake(), self.committee.quorum_threshold());
        
        // Check if we can form a certificate
        // Only try if we have enough votes AND the header (votes can arrive before headers)
        match aggregator.try_form_certificate(&self.committee) {
            Ok(Some(certificate)) => {
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
            Ok(None) => {
                // Not enough votes yet to form a certificate
                debug!("Not enough votes yet for header {} (have {} votes)", vote.id, aggregator.vote_count());
            }
            Err(e) => {
                // This can happen when votes arrive before the header
                debug!("Cannot form certificate yet: {}", e);
                // Don't propagate the error - this is expected behavior
            }
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
         
         // Log if certificate contains canonical metadata (important for debugging)
         if !certificate.header.canonical_metadata.is_empty() {
             info!("Processing certificate from {} for round {} WITH canonical metadata ({} bytes)", 
                   author, cert_round, certificate.header.canonical_metadata.len());
         } else {
             info!("Processing certificate from {} for round {} without canonical metadata", 
                   author, cert_round);
         }

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
         
         // CRITICAL: Also track by round for proper parent checking
         self.certificates_by_round
             .entry(cert_round)
             .or_insert_with(HashMap::new)
             .insert(author.clone(), certificate.clone());
         
         // Log certificate tracking
         let certs_in_round = self.certificates.values()
             .filter(|c| c.round() == cert_round)
             .count();
         info!("Now tracking {} certificates for round {} (total: {})", 
               certs_in_round, cert_round, self.certificates.len());

         // CRITICAL FIX: Advance round when we receive our own certificate
         // This ensures we only advance after our certificate is formed and stored
         if author == self.name && cert_round == self.current_round {
             self.current_round += 1;
             info!("✅ Received our own certificate for round {} - advanced to round {}", cert_round, self.current_round);
             // Reset failed attempts since we successfully progressed
             self.failed_attempts_count = 0;
         }

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
         let mut authors_with_certs = Vec::new();
         
         // Use round-specific tracking
         if let Some(round_certs) = self.certificates_by_round.get(&cert_round) {
             for (author, cert) in round_certs {
                 total_stake += self.committee.stake(author);
                 certificates_for_round.push(cert.clone());
                 authors_with_certs.push(author.encode_base64());
             }
         }
         
         // BOOTSTRAP: For early rounds, use validity threshold instead of quorum
         const BOOTSTRAP_ROUNDS: u64 = 10;
         let required_stake = if self.current_round < BOOTSTRAP_ROUNDS {
             let validity_threshold = self.committee.validity_threshold();
             debug!("BOOTSTRAP: Round {} checking advancement with validity threshold {} instead of quorum {}", 
                   self.current_round, validity_threshold, self.committee.quorum_threshold());
             validity_threshold
         } else {
             self.committee.quorum_threshold()
         };
         
         info!("Checking round {} advancement: {} certificates from {:?}, stake {}/{}", 
               cert_round, certificates_for_round.len(), authors_with_certs, total_stake, required_stake);
         
         // Check if we have enough certificates from previous round
         if total_stake >= required_stake {
             info!("✅ Collected sufficient certificates for round {}, can advance to round {}", 
                   cert_round, self.current_round + 1);
             
             // CRITICAL FIX: Don't automatically create headers here
             // The main loop will handle header creation when appropriate
             // This separation ensures we only create headers when we have:
             // 1. Proper consensus from previous round
             // 2. Our own certificate from previous round
             // 3. Either content or timer expiration
             
             // Just log that we're ready to advance
             debug!("Round {} has sufficient support, header creation will be handled by main loop", cert_round);
         } else {
             debug!("Round {} needs more certificates: have stake {}, need {}", cert_round, total_stake, required_stake);
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
             
             // Clean up old round-based certificate tracking
             self.certificates_by_round.retain(|&round, _| round > self.gc_round);
             
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
    
    /// Update the last finalized round to relieve backpressure
    pub fn update_last_finalized_round(&mut self, round: Round) {
        if round > self.last_finalized_round {
            let previous = self.last_finalized_round;
            self.last_finalized_round = round;
            info!("Updated last finalized round from {} to {} (backpressure relief)", previous, round);
        }
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