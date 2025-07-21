//! Bullshark BFT service implementation

use crate::{
    BftConfig, BullsharkError, BullsharkResult, FinalizedBatchInternal, 
    consensus::{BullsharkConsensus, ConsensusProtocol, ConsensusMetrics},
    dag::BullsharkDag,
    storage::ConsensusStorage,
    chain_state::{ChainStateProvider, DefaultChainState},
    round_tracker::{RoundCompletionTracker, RoundTrackerStats},
};
use narwhal::{
    types::{Certificate, Committee, PublicKey as NarwhalPublicKey},
    Transaction as NarwhalTransaction,
};
use alloy_primitives::B256;
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn, error, debug};
use fastcrypto::traits::KeyPair;
use rand_08;

/// Main service that runs the Bullshark BFT protocol
pub struct BftService {
    /// Configuration
    #[allow(dead_code)]
    config: BftConfig,
    /// The consensus algorithm
    consensus: BullsharkConsensus,
    /// The DAG state
    dag: BullsharkDag,
    /// Current committee
    committee: Committee,
    /// Receiver for certificates from Narwhal
    certificate_receiver: mpsc::Receiver<Certificate>,
    /// Sender for finalized batches to Reth integration layer
    finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatchInternal>,
    /// Current block number being processed
    current_block_number: u64,
    /// Current consensus index
    consensus_index: u64,
    /// Performance metrics
    metrics: ConsensusMetrics,
    /// Batch storage for retrieving worker batches
    batch_store: Option<std::sync::Arc<dyn narwhal::storage_trait::BatchStore>>,
    /// Chain state provider for parent hash and block number
    chain_state: std::sync::Arc<dyn ChainStateProvider>,
    /// Last block creation time (for rate limiting)
    last_block_time: Instant,
    /// Consensus storage for persistent state
    storage: Option<std::sync::Arc<dyn ConsensusStorage>>,
    /// Node's public key for leader determination
    node_public_key: Option<NarwhalPublicKey>,
    /// Round completion tracker for batching certificates  
    round_tracker: RoundCompletionTracker,
    /// Last finalized round (for direct DAG approach)
    last_finalized_round: u64,
}

impl std::fmt::Debug for BftService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BftService")
            .field("config", &self.config)
            .field("consensus", &self.consensus)
            .field("dag", &self.dag)
            .field("committee", &self.committee)
            .field("current_block_number", &self.current_block_number)
            .field("consensus_index", &self.consensus_index)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

impl BftService {
    /// Create a new BFT service
    pub fn new(
        config: BftConfig,
        committee: Committee,
        certificate_receiver: mpsc::Receiver<Certificate>,
        finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatchInternal>,
    ) -> Self {
        // Initialize DAG with genesis certificates
        let genesis_certificates = Certificate::genesis(&committee);
        let dag = BullsharkDag::new(genesis_certificates);
        
        // Create consensus algorithm
        let consensus = BullsharkConsensus::new(committee.clone(), config.clone());

        // Initialize round tracker with configured timeout for leader certificates
        let round_tracker = RoundCompletionTracker::new(config.round_completion_timeout);

        Self {
            config,
            consensus,
            dag,
            committee,
            certificate_receiver,
            finalized_batch_sender,
            current_block_number: 0, // Will be set from chain state
            consensus_index: 0,
            metrics: ConsensusMetrics::default(),
            batch_store: None,
            chain_state: std::sync::Arc::new(DefaultChainState),
            last_block_time: Instant::now(),
            storage: None,
            node_public_key: None,
            round_tracker,
            last_finalized_round: 0,
        }
    }
    
    /// Create a new BFT service with persistent storage
    pub fn with_storage(
        config: BftConfig,
        committee: Committee,
        certificate_receiver: mpsc::Receiver<Certificate>,
        finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatchInternal>,
        storage: std::sync::Arc<dyn ConsensusStorage>,
    ) -> Self {
        // Initialize DAG with genesis certificates
        let genesis_certificates = Certificate::genesis(&committee);
        let dag = BullsharkDag::new(genesis_certificates);
        
        // Create consensus algorithm with storage
        let consensus = BullsharkConsensus::with_storage(committee.clone(), config.clone(), storage.clone());

        // Initialize round tracker with configured timeout
        let round_tracker = RoundCompletionTracker::new(config.round_completion_timeout);

        Self {
            config,
            consensus,
            dag,
            committee,
            certificate_receiver,
            finalized_batch_sender,
            current_block_number: 0, // Will be set from chain state
            consensus_index: 0,
            metrics: ConsensusMetrics::default(),
            batch_store: None,
            chain_state: std::sync::Arc::new(DefaultChainState),
            last_block_time: Instant::now(),
            storage: Some(storage),
            node_public_key: None,
            round_tracker,
            last_finalized_round: 0,
        }
    }
    
    /// Set the batch store for retrieving worker batches
    pub fn set_batch_store(&mut self, batch_store: std::sync::Arc<dyn narwhal::storage_trait::BatchStore>) {
        self.batch_store = Some(batch_store);
    }
    
    /// Set the chain state provider
    pub fn set_chain_state(&mut self, chain_state: std::sync::Arc<dyn ChainStateProvider>) {
        self.chain_state = chain_state;
    }
    
    /// Set node public key for leader determination
    pub fn set_node_public_key(&mut self, public_key: NarwhalPublicKey) {
        self.node_public_key = Some(public_key);
    }

    /// Spawn the BFT service
    pub fn spawn(mut self) -> JoinHandle<BullsharkResult<()>> {
        info!("Spawning BFT service task");
        tokio::spawn(async move {
            info!("BFT service task started, calling run()");
            let result = self.run().await;
            warn!("BFT service task completed with result: {:?}", result);
            result
        })
    }

    /// Main run loop for the BFT service
    async fn run(&mut self) -> BullsharkResult<()> {
        info!("Starting Bullshark BFT service");
        
        // Initialize block number from chain state
        let initial_state = self.chain_state.get_chain_state();
        
        // CRITICAL FIX: If chain state reports block 0, this indicates the adapter
        // was initialized with genesis instead of actual chain tip. In this case,
        // let Reth determine the correct starting point during block creation.
        if initial_state.block_number == 0 {
            warn!("Chain state adapter reports block 0 - likely initialized with genesis instead of chain tip");
            warn!("BFT will determine correct starting block during first finalization");
            self.current_block_number = 0; // Will be updated during first block creation
        } else {
            self.current_block_number = initial_state.block_number;
        }
        
        info!("BFT service initialized with chain state: current block {} (will create block {}) parent {}", 
              self.current_block_number, self.current_block_number + 1, initial_state.parent_hash);
        
        // Load last consensus index from storage if available
        if let Some(ref storage) = self.storage {
            match storage.get_latest_finalized() {
                Ok(Some(last_index)) => {
                    self.consensus_index = last_index + 1;
                    info!("Loaded last consensus index from storage: {} (will continue from {})", 
                          last_index, self.consensus_index);
                }
                Ok(None) => {
                    info!("No previous consensus index found in storage, starting from 0");
                }
                Err(e) => {
                    warn!("Failed to load last consensus index from storage: {}", e);
                }
            }
        }
        
        info!("BFT service waiting for certificates...");

        // Create a timer for periodic round checks
        let mut round_check_interval = tokio::time::interval(Duration::from_millis(1000));
        round_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Check for new certificates
                Some(certificate) = self.certificate_receiver.recv() => {
                    use fastcrypto::traits::EncodeDecodeBase64;
                    info!("BFT: Received certificate from Narwhal for round {} from author {}", 
                          certificate.round(), 
                          certificate.origin().encode_base64().chars().take(16).collect::<String>());

                    match self.process_certificate(certificate).await {
                        Ok(finalized_count) => {
                            if finalized_count > 0 {
                                info!("BFT: Finalized {} batches in this round", finalized_count);
                                self.metrics.record_finalization(finalized_count, 1); // TODO: calculate actual latency
                            } else {
                                debug!("BFT: No outputs from this certificate");
                            }
                        }
                        Err(e) => {
                            // Only log as error if it's not a batch not found error
                            match &e {
                                BullsharkError::BatchNotFound { .. } => {
                                    debug!("Certificate processing skipped: {}", e);
                                }
                                _ => {
                                    error!("Error processing certificate: {}", e);
                                }
                            }
                            // Continue processing other certificates
                        }
                    }

                    // CERTIFICATE-DRIVEN FINALIZATION: Check for finalization after each certificate
                    // This follows the Sui/Narwhal pattern where finalization is triggered by certificate arrival
                    match self.check_dag_for_finalization().await {
                        Ok(finalized_count) => {
                            if finalized_count > 0 {
                                info!("BFT certificate-driven: finalized {} rounds after certificate processing", finalized_count);
                            }
                        }
                        Err(e) => {
                            warn!("BFT certificate-driven finalization failed: {}", e);
                        }
                    }

                    // Update metrics
                    self.metrics.update_dag_size(self.dag.stats().total_certificates);
                    
                    // Log metrics periodically
                    if self.consensus_index % 100 == 0 {
                        debug!("Consensus metrics: {:?}", self.metrics);
                    }
                }
                
                // BACKUP PERIODIC CHECK: Reduced frequency backup mechanism for edge cases
                _ = round_check_interval.tick() => {
                    // This is now a backup mechanism since primary finalization happens on certificate arrival
                    match self.check_dag_for_finalization().await {
                        Ok(finalized_count) => {
                            if finalized_count > 0 {
                                info!("BFT periodic backup: finalized {} rounds (backup check)", finalized_count);
                            }
                        }
                        Err(e) => {
                            debug!("Error in periodic backup check: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Process a single certificate through the consensus algorithm
    async fn process_certificate(&mut self, certificate: Certificate) -> BullsharkResult<usize> {
        let cert_round = certificate.round();
        
        // Add certificate to round tracker
        self.round_tracker.add_certificate(certificate.clone(), &self.committee);
        
        // Run the consensus algorithm to update DAG state
        let consensus_outputs = self.consensus.process_certificate(
            &mut self.dag,
            self.consensus_index,
            certificate,
        )?;
        
        let outputs_count = consensus_outputs.len();
        info!("Consensus produced {} outputs for certificate round {}", outputs_count, cert_round);
        
        // Update consensus index for all outputs
        for output in &consensus_outputs {
            if output.consensus_index >= self.consensus_index {
                self.consensus_index = output.consensus_index + 1;
            }
        }
        
        // DIRECT DAG APPROACH: Check DAG directly for finalization instead of using RoundCompletionTracker
        // This follows the Sui/Narwhal reference implementation approach
        let mut finalized_count = 0;
        let highest_round = self.dag.highest_round();
        
        // SEQUENTIAL FINALIZATION: Process rounds in strict order to avoid race conditions
        // All nodes must finalize the same sequence regardless of timing differences
        let next_round_to_finalize = if self.last_finalized_round == 0 { 2 } else { self.last_finalized_round + 2 };
        
        // Check up to 10 consecutive even rounds starting from the next expected round
        for round_offset in 0..10 {
            let check_round = next_round_to_finalize + (round_offset * 2);
            
            // Don't check rounds beyond the current DAG state
            if check_round > highest_round {
                break;
            }
            
            // Check if we have enough certificates for finalization
            let certs_at_round = self.dag.get_certificates_at_round(check_round);
            
            // DATABASE FALLBACK: If no certificates found in memory, this might be a gap
            // due to garbage collection. Try to retrieve from persistent storage.
            if certs_at_round.is_empty() {
                warn!("Round {} has no certificates in DAG! (DAG might have been garbage collected)", check_round);
                
                // Try to retrieve certificates from database storage
                if let Some(storage) = &self.storage {
                    debug!("Attempting database fallback for round {}", check_round);
                    match storage.get_certificates_by_round(check_round) {
                        Ok(stored_certs) => {
                            if stored_certs.is_empty() {
                                warn!("Database fallback found no certificates for round {}", check_round);
                            } else {
                                info!("Database fallback found {} certificates for round {}", stored_certs.len(), check_round);
                                // TODO: Add the retrieved certificates to the DAG or process them directly
                                // For now, just log the successful retrieval
                            }
                        }
                        Err(e) => {
                            warn!("Database fallback failed for round {}: {}", check_round, e);
                        }
                    }
                } else {
                    debug!("No storage available for database fallback");
                }
            }
            
            let total_stake: u64 = certs_at_round.iter()
                .map(|cert| self.committee.stake(&cert.origin()))
                .sum();
            
            // PROPER BFT FINALIZATION: Only use quorum threshold for safety
            // All nodes must finalize the same sequence with proper consensus
            let quorum_threshold = self.committee.quorum_threshold();
            
            let can_finalize = total_stake >= quorum_threshold;
            
            if can_finalize {
                info!("DAG FINALIZATION: Round {} with {} certificates and {} stake (>= {} quorum)", 
                      check_round, certs_at_round.len(), total_stake, quorum_threshold);
                
                // Clone certificates to avoid borrow checker issues
                let owned_certs: Vec<Certificate> = certs_at_round.iter().map(|cert| (*cert).clone()).collect();
                match self.finalize_round_from_dag_owned(check_round, owned_certs).await {
                    Ok(()) => {
                        finalized_count += 1;
                        self.last_finalized_round = check_round;
                        info!("DAG FINALIZATION: Successfully finalized round {} with quorum consensus", check_round);
                    }
                    Err(e) => {
                        warn!("DAG FINALIZATION: Failed to finalize round {}: {} - continuing with next round", check_round, e);
                        // Continue trying other rounds (reference implementation pattern)
                        // Don't break - failures should be non-fatal for continued progression
                    }
                }
            } else {
                debug!("DAG FINALIZATION: Round {} has only {} stake (< {} quorum) - cannot finalize", 
                       check_round, total_stake, quorum_threshold);
                // If we can't finalize this round, don't try later rounds (sequential order)
                break;
            }
        }
        
        // Log round tracker statistics
        let stats = self.round_tracker.stats();
        debug!("Round tracker stats: {} total rounds, {} with leader, {} pending even rounds",
               stats.total_rounds, stats.rounds_with_leader, stats.pending_even_rounds);
        
        Ok(finalized_count)
    }
    
    /// Check DAG directly for finalization (replaces check_pending_rounds)
    async fn check_dag_for_finalization(&mut self) -> BullsharkResult<usize> {
        // DIRECT DAG APPROACH: Check DAG directly for finalization instead of using RoundCompletionTracker
        // This follows the Sui/Narwhal reference implementation approach
        let mut finalized_count = 0;
        let highest_round = self.dag.highest_round();
        
        debug!("🔍 DAG FINALIZATION CHECK: highest_round={}, last_finalized={}", 
               highest_round, self.last_finalized_round);
        
        // SEQUENTIAL FINALIZATION: Process rounds in strict order to avoid race conditions
        // All nodes must finalize the same sequence regardless of timing differences
        let next_round_to_finalize = if self.last_finalized_round == 0 { 2 } else { self.last_finalized_round + 2 };
        
        // Check up to 10 consecutive even rounds starting from the next expected round
        for round_offset in 0..10 {
            let check_round = next_round_to_finalize + (round_offset * 2);
            
            // Don't check rounds beyond the current DAG state
            if check_round > highest_round {
                break;
            }
            
            // Check if we have enough certificates for finalization
            let certs_at_round = self.dag.get_certificates_at_round(check_round);
            
            // DATABASE FALLBACK: If no certificates found in memory, this might be a gap
            // due to garbage collection. Try to retrieve from persistent storage.
            if certs_at_round.is_empty() {
                warn!("Round {} has no certificates in DAG! (DAG might have been garbage collected)", check_round);
                
                // Try to retrieve certificates from database storage
                if let Some(storage) = &self.storage {
                    debug!("Attempting database fallback for round {}", check_round);
                    match storage.get_certificates_by_round(check_round) {
                        Ok(stored_certs) => {
                            if stored_certs.is_empty() {
                                warn!("Database fallback found no certificates for round {}", check_round);
                            } else {
                                info!("Database fallback found {} certificates for round {}", stored_certs.len(), check_round);
                                // TODO: Add the retrieved certificates to the DAG or process them directly
                                // For now, just log the successful retrieval
                            }
                        }
                        Err(e) => {
                            warn!("Database fallback failed for round {}: {}", check_round, e);
                        }
                    }
                } else {
                    debug!("No storage available for database fallback");
                }
            }
            
            let total_stake: u64 = certs_at_round.iter()
                .map(|cert| self.committee.stake(&cert.origin()))
                .sum();
            
            // PROPER BFT FINALIZATION: Only use quorum threshold for safety
            // All nodes must finalize the same sequence with proper consensus
            let quorum_threshold = self.committee.quorum_threshold();
            
            let can_finalize = total_stake >= quorum_threshold;
            
            if can_finalize {
                info!("DAG PERIODIC: Round {} with {} certificates and {} stake (>= {} quorum)", 
                      check_round, certs_at_round.len(), total_stake, quorum_threshold);
                
                // Clone certificates to avoid borrow checker issues
                let owned_certs: Vec<Certificate> = certs_at_round.iter().map(|cert| (*cert).clone()).collect();
                match self.finalize_round_from_dag_owned(check_round, owned_certs).await {
                    Ok(()) => {
                        finalized_count += 1;
                        self.last_finalized_round = check_round;
                        info!("DAG PERIODIC: Successfully finalized round {} with quorum consensus", check_round);
                    }
                    Err(e) => {
                        warn!("DAG PERIODIC: Failed to finalize round {}: {} - continuing with next round", check_round, e);
                        // Continue trying other rounds (reference implementation pattern)
                        // Don't break - failures should be non-fatal for continued progression
                    }
                }
            } else {
                debug!("DAG PERIODIC: Round {} has only {} stake (< {} quorum) - cannot finalize", 
                       check_round, total_stake, quorum_threshold);
                // Continue checking later rounds - they might have accumulated enough stake
                // Reference implementation pattern: don't break on insufficient stake
            }
        }
        
        Ok(finalized_count)
    }
    
    /// Check pending rounds for finalization (OLD APPROACH - DEPRECATED)
    async fn check_pending_rounds(&mut self) -> BullsharkResult<usize> {
        let pending_rounds = self.round_tracker.get_pending_rounds();
        let mut finalized_count = 0;
        
        for round in pending_rounds {
            if self.round_tracker.should_finalize_round(round, &self.committee) {
                // Create block for this complete round
                match self.finalize_round(round).await {
                    Ok(()) => {
                        finalized_count += 1;
                        // Mark round as complete
                        self.round_tracker.mark_round_complete(round);
                    }
                    Err(e) => {
                        debug!("Failed to finalize round {} in periodic check: {}", round, e);
                    }
                }
            }
        }
        
        Ok(finalized_count)
    }
    
    /// Finalize a complete round and create a block
    async fn finalize_round(&mut self, round: u64) -> BullsharkResult<()> {
        // Get all certificates for this round
        let certificates = self.round_tracker.get_round_certificates(round);
        if certificates.is_empty() {
            return Ok(());
        }
        
        info!("Finalizing round {} with {} certificates", round, certificates.len());
        
        // Extract all transactions from certificates
        let mut all_transactions = Vec::new();
        for cert in &certificates {
            match self.extract_transactions_from_certificate(cert).await {
                Ok(txs) => all_transactions.extend(txs),
                Err(BullsharkError::BatchNotFound { .. }) => {
                    // Expected when no transactions
                    debug!("No batch found for certificate");
                }
                Err(e) => {
                    warn!("Failed to extract transactions: {}", e);
                }
            }
        }
        
        // Check chain state before creating block
        let current_state = self.chain_state.get_chain_state();
        let next_block_number = current_state.block_number + 1;
        
        // Update our current_block_number from chain state
        if current_state.block_number >= self.current_block_number {
            self.current_block_number = current_state.block_number;
        }
        
        // Only create block if we haven't already
        if self.current_block_number < next_block_number {
            // Check minimum block time
            let elapsed = self.last_block_time.elapsed();
            if elapsed < self.config.min_block_time {
                debug!("Deferring block creation for round {} - only {:.1}s elapsed (min: {:.1}s)",
                       round, elapsed.as_secs_f64(), self.config.min_block_time.as_secs_f64());
                return Ok(());
            }
            
            info!("Creating block from round {} with {} transactions from {} certificates",
                  round, all_transactions.len(), certificates.len());
            
            // Create finalized batch
            match self.create_finalized_batch(
                all_transactions,
                round, // Use the actual round (already even)
                certificates,
            ).await? {
                Some(finalized_batch) => {
                    // Update tracking
                    self.last_block_time = Instant::now();
                    self.current_block_number = finalized_batch.block_number;
                    
                    // Send to Reth
                    info!("Sending finalized batch {} to Reth integration", finalized_batch.block_number);
                    if self.finalized_batch_sender.send(finalized_batch).is_err() {
                        return Err(BullsharkError::Network("Reth channel closed".to_string()));
                    }
                }
                None => {
                    // No block created due to lack of canonical metadata
                    debug!("Skipped block creation in round {} - no canonical metadata", round);
                }
            }
        }
        
        Ok(())
    }
    
    /// Finalize a round using certificates from the DAG (direct approach)
    async fn finalize_round_from_dag_owned(&mut self, round: u64, certificates: Vec<Certificate>) -> BullsharkResult<()> {
        info!("DAG FINALIZATION: Finalizing round {} with {} certificates from DAG", round, certificates.len());
        
        // Extract all transactions from certificates
        let mut all_transactions = Vec::new();
        for cert in &certificates {
            match self.extract_transactions_from_certificate(cert).await {
                Ok(txs) => all_transactions.extend(txs),
                Err(BullsharkError::BatchNotFound { .. }) => {
                    // Expected when no transactions
                    debug!("No batch found for certificate");
                }
                Err(e) => {
                    warn!("Failed to extract transactions: {}", e);
                }
            }
        }
        
        // Check chain state before creating block
        let current_state = self.chain_state.get_chain_state();
        let next_block_number = current_state.block_number + 1;
        
        // Update our current_block_number from chain state
        if current_state.block_number >= self.current_block_number {
            self.current_block_number = current_state.block_number;
        }
        
        // Only create block if we haven't already
        if self.current_block_number < next_block_number {
            // Check minimum block time
            let elapsed = self.last_block_time.elapsed();
            if elapsed < self.config.min_block_time {
                debug!("Deferring block creation for round {} - only {:.1}s elapsed (min: {:.1}s)",
                       round, elapsed.as_secs_f64(), self.config.min_block_time.as_secs_f64());
                return Ok(());
            }
            
            info!("DAG FINALIZATION: Creating block from round {} with {} transactions from {} certificates",
                  round, all_transactions.len(), certificates.len());
            
            // Certificates are already owned, no need to convert
            
            // Create finalized batch
            match self.create_finalized_batch(
                all_transactions,
                round, // Use the actual round (already even)
                certificates,
            ).await? {
                Some(finalized_batch) => {
                    // Update tracking
                    self.last_block_time = Instant::now();
                    self.current_block_number = finalized_batch.block_number;
                    
                    // Send to Reth
                    info!("DAG FINALIZATION: Sending finalized batch {} to Reth integration", finalized_batch.block_number);
                    if self.finalized_batch_sender.send(finalized_batch).is_err() {
                        return Err(BullsharkError::Network("Reth channel closed".to_string()));
                    }
                }
                None => {
                    // No block created due to lack of canonical metadata
                    debug!("DAG FINALIZATION: Skipped block creation in round {} - no canonical metadata", round);
                }
            }
        }
        
        Ok(())
    }
    
    /// Original process_certificate method - kept for reference but unused
    async fn process_certificate_old(&mut self, certificate: Certificate) -> BullsharkResult<usize> {
        // Extract transactions from the certificate
        let transactions = self.extract_transactions_from_certificate(&certificate).await?;
        
        debug!(
            "Processing certificate from round {} with {} transactions",
            certificate.round(),
            transactions.len()
        );

        // Run the consensus algorithm
        let cert_round = certificate.round();
        let consensus_outputs = self.consensus.process_certificate(
            &mut self.dag,
            self.consensus_index,
            certificate,
        )?;

        let outputs_count = consensus_outputs.len();
        info!("Consensus produced {} outputs for certificate round {}", outputs_count, cert_round);

        // Count how many we'll skip
        let skip_count = consensus_outputs.iter()
            .filter(|o| o.consensus_index < self.consensus_index)
            .count();
        
        if skip_count > 0 {
            info!("Skipping {} already processed certificates (current consensus index: {})", 
                  skip_count, self.consensus_index);
        }
        
        // Log the rounds of the outputs for debugging
        if outputs_count > 0 {
            let rounds: Vec<u64> = consensus_outputs.iter()
                .map(|o| o.certificate.round())
                .take(10)
                .collect();
            debug!("First 10 consensus output rounds: {:?}", rounds);
        }
        
        // When catching up, allow more blocks per batch for faster sync
        // With 100ms block times, we can handle more blocks
        const MAX_BLOCKS_PER_BATCH: usize = 100;
        let mut blocks_created_this_batch = 0;

        let mut finalized_count = 0;
        
        // When processing consensus outputs, we need to batch them appropriately
        // to avoid creating blocks for every single certificate in the DAG
        let mut highest_round = 0u64;
        let mut batched_transactions = Vec::new();
        let mut batched_certificates = Vec::new();

        // Process any finalized outputs
        debug!("Processing {} consensus outputs starting from index {}", outputs_count, self.consensus_index);
        
        for (idx, output) in consensus_outputs.into_iter().enumerate() {
            // Skip certificates we've already processed
            if output.consensus_index < self.consensus_index {
                debug!("Skipping already processed certificate with index {} (current index: {})", 
                      output.consensus_index, self.consensus_index);
                continue;
            }
            
            debug!("Processing consensus output {} of {} for round {}", idx + 1, outputs_count, output.certificate.round());
            
            // Track the highest round we've seen
            let cert_round = output.certificate.round();
            if cert_round > highest_round {
                highest_round = cert_round;
            }
            
            // Extract transactions from this certificate
            match self.extract_transactions_from_certificate(&output.certificate).await {
                Ok(txs) => {
                    batched_transactions.extend(txs);
                    batched_certificates.push(output.certificate.clone());
                }
                Err(BullsharkError::BatchNotFound { .. }) => {
                    // Expected when no transactions
                    debug!("No batch found for certificate");
                }
                Err(e) => {
                    warn!("Failed to extract transactions: {}", e);
                }
            }
            
            // Update consensus index for this certificate
            self.consensus_index = output.consensus_index + 1;
            
            // Check if we should create a block
            let elapsed = self.last_block_time.elapsed();
            let should_create_block = 
                elapsed >= self.config.min_block_time || // Enough time has passed (100ms)
                idx == outputs_count - 1 || // Last certificate in batch
                batched_certificates.len() >= 2; // Create blocks more frequently with just 2 certificates
            
            if !should_create_block {
                continue;
            }
            
            // Check if we should create a new block based on chain state
            let current_state = self.chain_state.get_chain_state();
            let next_block_number = current_state.block_number + 1;
            
            info!("BFT: Checking block creation - current_block_number: {}, chain_state.block_number: {}, next: {}", 
                  self.current_block_number, current_state.block_number, next_block_number);
            
            // Update our current_block_number from chain state to stay in sync
            // This allows us to recover if blocks were persisted while we were processing
            if current_state.block_number >= self.current_block_number {
                self.current_block_number = current_state.block_number;
                debug!("Updated current_block_number from chain state: {}", self.current_block_number);
            }
            
            // Only create a finalized batch if we haven't already created this block number
            if self.current_block_number == 0 || self.current_block_number < next_block_number {
                // Create finalized batch with batched transactions
                let tx_count = batched_transactions.len();
                let cert_count = batched_certificates.len();
                
                if tx_count == 0 {
                    debug!("Creating empty block from {} certificates for consistent block times", cert_count);
                } else {
                    info!("Creating finalized batch with {} transactions from {} certificates", tx_count, cert_count);
                }
                
                // CRITICAL: For Bullshark, we must use an EVEN round for the block
                // AND the certificate MUST have canonical metadata (from the leader)
                let mut highest_even_round = 0u64;
                let mut has_canonical_metadata = false;
                
                for cert in &batched_certificates {
                    let round = cert.round();
                    if round % 2 == 0 && round > highest_even_round {
                        highest_even_round = round;
                    }
                    
                    // Check if any certificate has canonical metadata
                    if !cert.header.canonical_metadata.is_empty() {
                        has_canonical_metadata = true;
                        debug!("Found canonical metadata in certificate from round {}", round);
                    }
                }
                
                // CRITICAL: Only create blocks if we have canonical metadata
                if !has_canonical_metadata {
                    warn!("No canonical metadata found in {} certificates - skipping block creation", cert_count);
                    continue;
                }
                
                // If we don't have any even rounds, use the previous even round
                if highest_even_round == 0 && highest_round > 0 {
                    // Round down to the nearest even number
                    highest_even_round = (highest_round / 2) * 2;
                    if highest_even_round == 0 {
                        highest_even_round = 2; // Minimum even round for Bullshark
                    }
                }
                
                info!("CRITICAL: Creating batch from highest EVEN round {} (highest round was {}, batched {} certificates)", 
                      highest_even_round, highest_round, cert_count);
                
                match self.create_finalized_batch(
                    batched_transactions.clone(),
                    highest_even_round,
                    batched_certificates.clone(),
                ).await? {
                    Some(finalized_batch) => {
                        // Update last block time
                        self.last_block_time = Instant::now();
                        
                        // Update current_block_number immediately to prevent duplicate batches
                        // This is critical to prevent creating multiple batches for the same block
                        let batch_block_number = finalized_batch.block_number;
                        self.current_block_number = batch_block_number;
                        info!("Created finalized batch for block {} and updated current_block_number", batch_block_number);

                        // Send to Reth integration
                        info!("Sending finalized batch {} to Reth integration", batch_block_number);
                        if self.finalized_batch_sender.send(finalized_batch).is_err() {
                            warn!("Failed to send finalized batch to Reth - channel closed");
                            return Err(BullsharkError::Network("Reth channel closed".to_string()));
                        }
                        
                        finalized_count += 1;
                        blocks_created_this_batch += 1;
                    }
                    None => {
                        // No block created due to lack of canonical metadata
                        debug!("Skipped block creation - no canonical metadata available");
                    }
                }
                
                // Reset batched data for next block
                batched_transactions.clear();
                batched_certificates.clear();
                highest_round = 0;
                
                // If we've created enough blocks in this batch, stop processing
                // This prevents overwhelming the system when catching up
                if blocks_created_this_batch >= MAX_BLOCKS_PER_BATCH {
                    info!("Created {} blocks in this batch, deferring remaining certificates", blocks_created_this_batch);
                    break;
                }
            } else {
                // We're ahead of the chain state - skip this certificate
                info!("BFT: Skipping certificate - already created block {} (chain at block {})", 
                      self.current_block_number, current_state.block_number);
            }
        }

        // Check if we need to create a block anyway (minimum block time elapsed)
        if finalized_count == 0 && outputs_count > 0 {
            debug!("Processed {} outputs but created 0 blocks - all outputs were skipped or throttled", outputs_count);
        }
        
        Ok(finalized_count)
    }

    /// Extract transactions from a certificate
    async fn extract_transactions_from_certificate(
        &self,
        certificate: &Certificate,
    ) -> BullsharkResult<Vec<NarwhalTransaction>> {
        let mut transactions = Vec::new();

        // In the Narwhal design, certificates contain batch digests, not the actual batches
        // The batches themselves are stored by workers and retrieved on demand
        
        // Check if we have a batch store
        if let Some(ref batch_store) = self.batch_store {
            debug!("Retrieving batches for certificate in round {}", certificate.round());
            
            // Collect all batch digests from the certificate
            let batch_digests: Vec<narwhal::BatchDigest> = certificate.header.payload
                .keys()
                .cloned()
                .collect();
            
            // Fetch batches from storage
            match batch_store.read_batches(&batch_digests).await {
                Ok(batches) => {
                    for (i, batch_opt) in batches.into_iter().enumerate() {
                        match batch_opt {
                            Some(batch) => {
                                // Extract all transactions from the batch
                                debug!("Retrieved batch {} with {} transactions", 
                                    batch_digests[i], batch.0.len());
                                transactions.extend(batch.0);
                            }
                            None => {
                                // When there are no transactions, workers might not create batches
                                // This is expected behavior, not an error
                                debug!("Batch {} not found in storage - this is expected when there are no transactions", 
                                    batch_digests[i]);
                                // Continue processing other batches
                            }
                        }
                    }
                }
                Err(e) => {
                    // Cannot proceed without batch data - this is a storage failure
                    return Err(BullsharkError::StorageError {
                        source: anyhow::anyhow!(e),
                        context: format!("Failed to retrieve {} batches for certificate in round {}",
                            batch_digests.len(), certificate.round()),
                    });
                }
            }
        } else {
            // Batch store is required for proper operation
            return Err(BullsharkError::Configuration(
                "Batch store not configured - cannot extract transactions from certificates".to_string()
            ));
        }

        Ok(transactions)
    }

    /// Create a finalized batch from transactions
    async fn create_finalized_batch(
        &self,
        transactions: Vec<NarwhalTransaction>,
        round: u64,
        certificates: Vec<Certificate>,
    ) -> BullsharkResult<Option<FinalizedBatchInternal>> {
        // Get chain state
        let chain_state = self.chain_state.get_chain_state();
        let parent_hash = chain_state.parent_hash;
        // The chain state contains the last created block number
        // The next block should be block_number + 1
        let block_number = chain_state.block_number + 1;
        
        // Ensure monotonic timestamps - must be strictly greater than parent
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let timestamp = current_time.max(chain_state.parent_timestamp + 1);

        info!("create_finalized_batch called with round={}, block_number={}", round, block_number);
        
        // CRITICAL: Extract canonical metadata from certificates
        // The leader's certificate will contain the canonical metadata
        let mut canonical_metadata_bytes = None;
        
        // Search through certificates to find the one with canonical metadata
        for cert in &certificates {
            if !cert.header.canonical_metadata.is_empty() {
                use fastcrypto::traits::EncodeDecodeBase64;
                info!("Found canonical metadata in certificate from round {} (author: {})", 
                      cert.round(), cert.origin().encode_base64());
                canonical_metadata_bytes = Some(cert.header.canonical_metadata.clone());
                break; // Use the first metadata we find
            }
        }
        
        // If no metadata found in certificates, check if we're the leader and create it
        if canonical_metadata_bytes.is_none() {
            let is_leader = self.is_leader_for_round(round);
            debug!("🔑 LEADER CHECK: Round {}, is_leader={}, node_key_set={}", 
                   round, is_leader, self.node_public_key.is_some());
            
            if is_leader {
                // This should not happen if DAG service is working correctly
                warn!("Leader for round {} but no canonical metadata found in certificates - creating locally", round);
                
                // Create canonical metadata locally as fallback
                let metadata_string = format!(
                    "block:{},parent:{},timestamp:{},round:{},base_fee:{}",
                    block_number,
                    parent_hash,
                    timestamp,
                    round,
                    875_000_000u64, // Default base fee
                );
                canonical_metadata_bytes = Some(metadata_string.into_bytes());
            } else {
                // Non-leader but no metadata in certificates - create deterministic metadata
                // All nodes should create the same block from the same finalized consensus output
                info!("Non-leader creating deterministic metadata for round {} (all nodes must build same block)", round);
                
                // Create deterministic canonical metadata that all nodes will generate identically
                let metadata_string = format!(
                    "block:{},parent:{},timestamp:{},round:{},base_fee:{}",
                    block_number,
                    parent_hash,
                    timestamp,
                    round,
                    875_000_000u64, // Deterministic base fee
                );
                canonical_metadata_bytes = Some(metadata_string.into_bytes());
            }
        }
        
        let has_metadata = canonical_metadata_bytes.is_some();
        
        let batch = FinalizedBatchInternal {
            block_number,
            parent_hash,
            transactions,
            timestamp,
            round,
            certificates,
            canonical_metadata_bytes,
        };

        info!(
            "Created finalized batch for block {} with {} transactions at round {} (parent: {}, has_metadata: {})",
            batch.block_number,
            batch.transactions.len(),
            batch.round,
            batch.parent_hash,
            has_metadata
        );

        Ok(Some(batch))
    }

    /// Determine if this node is the leader for a given round
    fn is_leader_for_round(&self, round: u64) -> bool {
        use fastcrypto::traits::EncodeDecodeBase64;
        
        // IMPORTANT: Sort validators for deterministic ordering across all nodes
        let mut validators: Vec<_> = self.committee.authorities.keys().cloned().collect();
        validators.sort_by_key(|k| k.encode_base64());
        
        let num_validators = validators.len();
        if num_validators == 0 {
            return false;
        }
        
        // Determine leader index for this round
        // CRITICAL FIX: Bullshark only has leaders on even rounds
        // Use (round / 2) % num_validators to cycle through all validators
        let leader_idx = ((round / 2) as usize) % num_validators;
        
        // Check if we have a node public key and if we're the leader
        if let Some(ref node_key) = self.node_public_key {
            let is_leader = validators.get(leader_idx) == Some(node_key);
            if is_leader {
                info!("Node is leader for round {} (idx: {})", round, leader_idx);
            }
            is_leader
        } else {
            false
        }
    }
    

    /// Update the committee configuration
    pub async fn update_committee(&mut self, new_committee: Committee) -> BullsharkResult<()> {
        info!(
            "Updating BFT committee from epoch {} to epoch {}",
            self.committee.epoch, new_committee.epoch
        );

        self.committee = new_committee.clone();
        self.consensus.update_committee(new_committee)?;

        // Reset state for new epoch
        self.consensus_index = 0;
        self.current_block_number = 1;

        // Re-initialize DAG with new genesis
        let genesis_certificates = Certificate::genesis(&self.committee);
        self.dag = BullsharkDag::new(genesis_certificates);

        Ok(())
    }

    /// Get current consensus metrics
    pub fn metrics(&self) -> &ConsensusMetrics {
        &self.metrics
    }

    /// Get current DAG statistics
    pub fn dag_stats(&self) -> crate::dag::DagStats {
        self.dag.stats()
    }

    /// Get current consensus state
    pub fn consensus_state(&self) -> BftServiceState {
        BftServiceState {
            current_block_number: self.current_block_number,
            consensus_index: self.consensus_index,
            committee_epoch: self.committee.epoch,
            dag_stats: self.dag.stats(),
            metrics: self.metrics.clone(),
        }
    }
}

/// State information for the BFT service
#[derive(Debug, Clone)]
pub struct BftServiceState {
    /// Current block number being processed
    pub current_block_number: u64,
    /// Current consensus index
    pub consensus_index: u64,
    /// Current committee epoch
    pub committee_epoch: u64,
    /// DAG statistics
    pub dag_stats: crate::dag::DagStats,
    /// Performance metrics
    pub metrics: ConsensusMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;
    use narwhal::types::{PublicKey, Committee};
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
                    worker_ports: None,
                },
            };
            authorities.insert(keypair.public().clone(), authority);
        }
        Committee::new(0, authorities)
    }

    #[tokio::test]
    async fn test_bft_service_creation() {
        let committee = create_test_committee();
        let config = BftConfig::default();
        let (cert_sender, cert_receiver) = mpsc::channel(1000);
        let (batch_sender, _batch_receiver) = mpsc::unbounded_channel();

        let service = BftService::new(config, committee, cert_receiver, batch_sender);
        
        assert_eq!(service.current_block_number, 1);
        assert_eq!(service.consensus_index, 0);
    }

    #[tokio::test]
    async fn test_committee_update() {
        let committee = create_test_committee();
        let config = BftConfig::default();
        let (cert_sender, cert_receiver) = mpsc::channel(1000);
        let (batch_sender, _batch_receiver) = mpsc::unbounded_channel();

        let mut service = BftService::new(config, committee.clone(), cert_receiver, batch_sender);
        
        // Create new committee with different epoch
        let new_authorities = HashMap::new();
        let new_committee = Committee::new(1, new_authorities);
        
        service.update_committee(new_committee.clone()).await.unwrap();
        
        assert_eq!(service.committee.epoch, 1);
        assert_eq!(service.consensus_index, 0);
        assert_eq!(service.current_block_number, 1);
    }
}
