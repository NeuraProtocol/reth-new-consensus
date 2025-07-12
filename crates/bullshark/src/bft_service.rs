//! Bullshark BFT service implementation

use crate::{
    BftConfig, BullsharkError, BullsharkResult, FinalizedBatchInternal, 
    consensus::{BullsharkConsensus, ConsensusProtocol, ConsensusMetrics},
    dag::BullsharkDag,
    storage::ConsensusStorage,
    chain_state::{ChainStateProvider, DefaultChainState},
};
use narwhal::{
    types::{Certificate, Committee, PublicKey as NarwhalPublicKey},
    Transaction as NarwhalTransaction,
};
use alloy_primitives::B256;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
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
        self.current_block_number = initial_state.block_number;
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

        loop {
            match self.certificate_receiver.recv().await {
                Some(certificate) => {
                    info!("BFT: Received certificate from Narwhal for round {}", certificate.round());
                    debug!("Received certificate from Narwhal: {:?}", certificate);

            match self.process_certificate(certificate).await {
                Ok(finalized_count) => {
                    if finalized_count > 0 {
                        info!("Finalized {} batches in this round", finalized_count);
                        self.metrics.record_finalization(finalized_count, 1); // TODO: calculate actual latency
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

            // Update metrics
            self.metrics.update_dag_size(self.dag.stats().total_certificates);
            
                    // Log metrics periodically
                    if self.consensus_index % 100 == 0 {
                        debug!("Consensus metrics: {:?}", self.metrics);
                    }
                }
                None => {
                    warn!("Certificate receiver channel closed, shutting down BFT service");
                    break;
                }
            }
        }

        warn!("Certificate receiver channel closed, shutting down BFT service");
        Ok(())
    }

    /// Process a single certificate through the consensus algorithm
    async fn process_certificate(&mut self, certificate: Certificate) -> BullsharkResult<usize> {
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
        
        // When catching up, limit how many blocks we create in one batch
        // This prevents the system from getting overwhelmed
        const MAX_BLOCKS_PER_BATCH: usize = 10;
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
                elapsed >= self.config.min_block_time || // Enough time has passed
                idx == outputs_count - 1 || // Last certificate in batch
                batched_certificates.len() >= 10; // Accumulated enough certificates
            
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
                
                // Use the highest round from all batched certificates
                info!("CRITICAL: Creating batch from highest round {} (batched {} certificates)", 
                      highest_round, cert_count);
                
                let finalized_batch = self.create_finalized_batch(
                    batched_transactions.clone(),
                    highest_round,
                    batched_certificates.clone(),
                ).await?;
                
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
    ) -> BullsharkResult<FinalizedBatchInternal> {
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
                // Non-leader and no metadata in certificates
                warn!("Not leader and no canonical metadata provided - blocks will diverge!");
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

        Ok(batch)
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
        let leader_idx = (round as usize) % num_validators;
        
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
