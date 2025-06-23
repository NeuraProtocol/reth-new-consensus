//! Bullshark BFT service implementation

use crate::{
    BftConfig, BullsharkError, BullsharkResult, FinalizedBatchInternal, 
    consensus::{BullsharkConsensus, ConsensusProtocol, ConsensusMetrics},
    dag::BullsharkDag,
    storage::ConsensusStorage,
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
    certificate_receiver: mpsc::UnboundedReceiver<Certificate>,
    /// Sender for finalized batches to Reth integration layer
    finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatchInternal>,
    /// Current block number being processed
    current_block_number: u64,
    /// Current consensus index
    consensus_index: u64,
    /// Performance metrics
    metrics: ConsensusMetrics,
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
        certificate_receiver: mpsc::UnboundedReceiver<Certificate>,
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
        }
    }
    
    /// Create a new BFT service with persistent storage
    pub fn with_storage(
        config: BftConfig,
        committee: Committee,
        certificate_receiver: mpsc::UnboundedReceiver<Certificate>,
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
        }
    }

    /// Spawn the BFT service
    pub fn spawn(mut self) -> JoinHandle<BullsharkResult<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }

    /// Main run loop for the BFT service
    async fn run(&mut self) -> BullsharkResult<()> {
        info!("Starting Bullshark BFT service");

        while let Some(certificate) = self.certificate_receiver.recv().await {
            debug!("Received certificate from Narwhal: {:?}", certificate);

            match self.process_certificate(certificate).await {
                Ok(finalized_count) => {
                    if finalized_count > 0 {
                        info!("Finalized {} batches in this round", finalized_count);
                        self.metrics.record_finalization(finalized_count, 1); // TODO: calculate actual latency
                    }
                }
                Err(e) => {
                    error!("Error processing certificate: {}", e);
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
        let consensus_outputs = self.consensus.process_certificate(
            &mut self.dag,
            self.consensus_index,
            certificate,
        )?;

        let mut finalized_count = 0;

        // Process any finalized outputs
        for output in consensus_outputs {
            self.consensus_index = output.consensus_index + 1;
            
            // Extract all transactions from the finalized certificate
            let cert_transactions = self.extract_transactions_from_certificate(&output.certificate).await?;
            
            if !cert_transactions.is_empty() {
                let finalized_batch = self.create_finalized_batch(
                    cert_transactions,
                    output.certificate.round(),
                    vec![output.certificate],
                ).await?;

                // Send to Reth integration
                if self.finalized_batch_sender.send(finalized_batch).is_err() {
                    warn!("Failed to send finalized batch to Reth - channel closed");
                    return Err(BullsharkError::Network("Reth channel closed".to_string()));
                }

                finalized_count += 1;
                self.current_block_number += 1;
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
        // For this simplified implementation, we'll simulate having the transactions
        
        // TODO: In a real implementation, this would:
        // 1. Look up batch digests from the certificate
        // 2. Fetch actual batches from workers
        // 3. Extract transactions from batches
        
        // For now, create dummy transactions based on the certificate
        for (batch_digest, _worker_id) in &certificate.header.payload {
            // Simulate transactions (in reality, would fetch from worker)
            let dummy_tx = format!("tx_from_batch_{}", batch_digest).into_bytes();
            transactions.push(narwhal::Transaction(dummy_tx));
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

        // For parent hash, we'd normally get this from Reth's chain state
        // For now, use a placeholder
        let parent_hash = B256::ZERO; // TODO: Get actual parent hash from Reth

        let batch = FinalizedBatchInternal {
            block_number: self.current_block_number,
            parent_hash,
            transactions,
            timestamp,
            round,
            certificates,
        };

        info!(
            "Created finalized batch for block {} with {} transactions at round {}",
            batch.block_number,
            batch.transactions.len(),
            batch.round
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
        let mut authorities = HashMap::new();
        for i in 0..4 {
            let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
            authorities.insert(keypair.public().clone(), 100);
        }
        Committee::new(0, authorities)
    }

    #[tokio::test]
    async fn test_bft_service_creation() {
        let committee = create_test_committee();
        let config = BftConfig::default();
        let (cert_sender, cert_receiver) = mpsc::unbounded_channel();
        let (batch_sender, _batch_receiver) = mpsc::unbounded_channel();

        let service = BftService::new(config, committee, cert_receiver, batch_sender);
        
        assert_eq!(service.current_block_number, 1);
        assert_eq!(service.consensus_index, 0);
    }

    #[tokio::test]
    async fn test_committee_update() {
        let committee = create_test_committee();
        let config = BftConfig::default();
        let (cert_sender, cert_receiver) = mpsc::unbounded_channel();
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
