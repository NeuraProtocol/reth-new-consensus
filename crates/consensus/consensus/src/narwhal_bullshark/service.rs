//! Narwhal + Bullshark consensus service

use crate::narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkConfig};
use anyhow::Result;
use narwhal::{
    DagService, Transaction as NarwhalTransaction,
    types::{Committee, Certificate},
};
use bullshark::{BftService, FinalizedBatchInternal};
use reth_primitives::{TransactionSigned as RethTransaction};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use fastcrypto::traits::KeyPair;
use rand_08;

/// Main service that orchestrates Narwhal DAG + Bullshark BFT consensus
pub struct NarwhalBullsharkService {
    /// Configuration
    config: NarwhalBullsharkConfig,
    /// Current committee
    committee: Committee,
    /// Channel for receiving transactions from Reth mempool
    transaction_receiver: mpsc::UnboundedReceiver<RethTransaction>,
    /// Channel for sending finalized batches to Reth
    finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    /// Committee updates
    committee_receiver: watch::Receiver<Committee>,
}

impl std::fmt::Debug for NarwhalBullsharkService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalBullsharkService")
            .field("config", &self.config)
            .field("committee", &self.committee)
            .finish_non_exhaustive()
    }
}

impl NarwhalBullsharkService {
    /// Create a new Narwhal + Bullshark service
    pub fn new(
        config: NarwhalBullsharkConfig,
        committee: Committee,
        transaction_receiver: mpsc::UnboundedReceiver<RethTransaction>,
        finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
        committee_receiver: watch::Receiver<Committee>,
    ) -> Self {
        Self {
            config,
            committee,
            transaction_receiver,
            finalized_batch_sender,
            committee_receiver,
        }
    }

    /// Spawn the consensus service
    pub fn spawn(self) -> Result<Vec<JoinHandle<()>>> {
        info!("Starting Narwhal + Bullshark consensus service");

        // Set up channels for communication between Narwhal and Bullshark
        let (dag_to_bft_sender, dag_to_bft_receiver) = mpsc::unbounded_channel::<Certificate>();
        let (bft_to_reth_sender, mut bft_to_reth_receiver) = mpsc::unbounded_channel::<FinalizedBatchInternal>();

        // Convert Reth transactions to Narwhal transactions
        let (narwhal_tx_sender, narwhal_tx_receiver) = mpsc::unbounded_channel::<NarwhalTransaction>();
        
        // Create signature service for our node
        let signature_service = Self::create_signature_service()?;
        
        // Spawn transaction converter task
        let transaction_converter_handle = {
            let mut transaction_receiver = self.transaction_receiver;
            tokio::spawn(async move {
                while let Some(reth_tx) = transaction_receiver.recv().await {
                    let narwhal_tx = Self::reth_tx_to_narwhal_tx(reth_tx);
                    if narwhal_tx_sender.send(narwhal_tx).is_err() {
                        warn!("Failed to send transaction to Narwhal - channel closed");
                        break;
                    }
                }
                info!("Transaction converter task terminated");
            })
        };

        // Spawn Narwhal DAG service
        let dag_service = DagService::new(
            self.config.node_public_key.clone(),
            self.committee.clone(),
            self.config.narwhal.clone(),
            signature_service,
            narwhal_tx_receiver,
            dag_to_bft_sender,
            self.committee_receiver.clone(),
        );
        let dag_service_handle = dag_service.spawn();
        let dag_handle = tokio::spawn(async move {
            if let Err(e) = dag_service_handle.await.unwrap_or_else(|e| {
                warn!("DAG service task panicked: {}", e);
                Err(narwhal::DagError::Configuration("Task panicked".to_string()))
            }) {
                warn!("DAG service error: {}", e);
            }
        });

        // Spawn Bullshark BFT service
        let bft_service = BftService::new(
            self.config.bullshark.clone(),
            self.committee.clone(),
            dag_to_bft_receiver,
            bft_to_reth_sender,
        );
        let bft_service_handle = bft_service.spawn();
        let bft_handle = tokio::spawn(async move {
            if let Err(e) = bft_service_handle.await.unwrap_or_else(|e| {
                warn!("BFT service task panicked: {}", e);
                Err(bullshark::BullsharkError::Configuration("Task panicked".to_string()))
            }) {
                warn!("BFT service error: {}", e);
            }
        });

        // Spawn finalized batch converter task
        let batch_converter_handle = {
            let finalized_batch_sender = self.finalized_batch_sender;
            tokio::spawn(async move {
                while let Some(internal_batch) = bft_to_reth_receiver.recv().await {
                    let reth_batch = FinalizedBatch {
                        block_number: internal_batch.block_number,
                        transactions: internal_batch.transactions.into_iter()
                            .filter_map(|tx| Self::narwhal_tx_to_reth_tx(tx).ok())
                            .collect(),
                        parent_hash: internal_batch.parent_hash,
                        timestamp: internal_batch.timestamp,
                    };
                    
                    if finalized_batch_sender.send(reth_batch).is_err() {
                        warn!("Failed to send finalized batch to Reth - channel closed");
                        break;
                    }
                }
                info!("Batch converter task terminated");
            })
        };

        Ok(vec![
            transaction_converter_handle,
            dag_handle,
            bft_handle,
            batch_converter_handle,
        ])
    }

    /// Create a signature service for this node
    fn create_signature_service() -> Result<fastcrypto::SignatureService<narwhal::types::Signature>> {
        // In a real implementation, this would load the node's private key
        // For now, create a dummy signature service 
        use fastcrypto::traits::KeyPair;
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        Ok(fastcrypto::SignatureService::new(keypair))
    }
    
    /// Convert a Reth transaction to a Narwhal transaction
    fn reth_tx_to_narwhal_tx(reth_tx: RethTransaction) -> NarwhalTransaction {
        // For now, just serialize the transaction hash as bytes
        // In a real implementation, this would use proper encoding
        let tx_hash = reth_tx.tx_hash();
        NarwhalTransaction(tx_hash.0.to_vec())
    }
    
    /// Convert a Narwhal transaction to a Reth transaction
    fn narwhal_tx_to_reth_tx(_narwhal_tx: NarwhalTransaction) -> Result<RethTransaction> {
        // For now, create a dummy transaction since we only stored the hash
        // In a real implementation, this would properly decode the transaction
        // For testing purposes, return an error since we can't reconstruct from hash alone
        Err(anyhow::anyhow!("Cannot reconstruct transaction from hash - need proper transaction storage"))
    }
}

/// Configuration for creating a Narwhal + Bullshark service
#[derive(Debug)]
pub struct ServiceConfig {
    /// Node configuration
    pub node_config: NarwhalBullsharkConfig,
    /// Initial committee
    pub committee: Committee,
}

impl ServiceConfig {
    /// Create a new service configuration
    pub fn new(node_config: NarwhalBullsharkConfig, committee: Committee) -> Self {
        Self {
            node_config,
            committee,
        }
    }
} 