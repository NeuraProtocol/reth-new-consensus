//! Bullshark BFT service implementation

use crate::{
    BftConfig, BullsharkError, BullsharkResult, FinalizedBatchInternal, 
    consensus::{BullsharkConsensus, ConsensusProtocol, ConsensusMetrics},
    dag::BullsharkDag,
    storage::ConsensusStorage,
    chain_state::{ChainStateProvider, DefaultChainState},
};
use narwhal::{
    types::{Certificate, Committee},
    Transaction as NarwhalTransaction,
};
use alloy_primitives::B256;
use std::time::{SystemTime, UNIX_EPOCH};
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
            current_block_number: 1,
            consensus_index: 0,
            metrics: ConsensusMetrics::default(),
            batch_store: None,
            chain_state: std::sync::Arc::new(DefaultChainState),
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
        let consensus = BullsharkConsensus::with_storage(committee.clone(), config.clone(), storage);

        Self {
            config,
            consensus,
            dag,
            committee,
            certificate_receiver,
            finalized_batch_sender,
            current_block_number: 1,
            consensus_index: 0,
            metrics: ConsensusMetrics::default(),
            batch_store: None,
            chain_state: std::sync::Arc::new(DefaultChainState),
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

        info!("Consensus produced {} outputs for certificate round {}", consensus_outputs.len(), cert_round);

        let mut finalized_count = 0;

        // Process any finalized outputs
        for output in consensus_outputs {
            self.consensus_index = output.consensus_index + 1;
            
            // Extract all transactions from the finalized certificate
            let cert_transactions = self.extract_transactions_from_certificate(&output.certificate).await?;
            
            if !cert_transactions.is_empty() {
                info!("Creating finalized batch with {} transactions from certificate", cert_transactions.len());
                let finalized_batch = self.create_finalized_batch(
                    cert_transactions,
                    output.certificate.round(),
                    vec![output.certificate],
                ).await?;

                // Send to Reth integration
                info!("Sending finalized batch {} to Reth integration", finalized_batch.block_number);
                if self.finalized_batch_sender.send(finalized_batch).is_err() {
                    warn!("Failed to send finalized batch to Reth - channel closed");
                    return Err(BullsharkError::Network("Reth channel closed".to_string()));
                }

                finalized_count += 1;
                self.current_block_number += 1;
            } else {
                debug!("No transactions in certificate, skipping batch creation");
            }
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
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Get chain state
        let chain_state = self.chain_state.get_chain_state();
        let parent_hash = chain_state.parent_hash;
        let block_number = chain_state.block_number;

        let batch = FinalizedBatchInternal {
            block_number,
            parent_hash,
            transactions,
            timestamp,
            round,
            certificates,
        };

        info!(
            "Created finalized batch for block {} with {} transactions at round {} (parent: {})",
            batch.block_number,
            batch.transactions.len(),
            batch.round,
            batch.parent_hash
        );

        Ok(batch)
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
