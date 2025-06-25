//! Narwhal + Bullshark consensus service

use crate::{
    narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkConfig},
    consensus_storage::MdbxConsensusStorage,
    bullshark_storage_adapter::BullsharkMdbxAdapter,
};
use anyhow::Result;
use narwhal::{
    DagService, DagMessage, Transaction as NarwhalTransaction,
    types::{Committee, Certificate},
};
use bullshark::{BftService, FinalizedBatchInternal};
use reth_primitives::{TransactionSigned as RethTransaction};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, error};
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
    /// MDBX storage for consensus persistence
    storage: Option<std::sync::Arc<MdbxConsensusStorage>>,
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
        storage: Option<std::sync::Arc<MdbxConsensusStorage>>,
    ) -> Self {
        Self {
            config,
            committee,
            transaction_receiver,
            finalized_batch_sender,
            committee_receiver,
            storage,
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
        
        // Set up network message channel for DAG consensus (headers, votes, certificates from other nodes)
        let (network_message_sender, network_message_receiver) = mpsc::unbounded_channel::<DagMessage>();
        // TODO: Connect network_message_sender to actual P2P network layer for distributed consensus
        // For now, only transactions from local mempool are processed
        
        // Create signature service for our node
        let signature_service = Self::create_signature_service()?;
        
        // Spawn transaction converter task
        let transaction_converter_handle = {
            let mut transaction_receiver = self.transaction_receiver;
            tokio::spawn(async move {
                let mut transaction_count = 0u64;
                info!("Transaction converter task started");
                
                while let Some(reth_tx) = transaction_receiver.recv().await {
                    transaction_count += 1;
                    let tx_hash = reth_tx.tx_hash();
                    
                    let narwhal_tx = Self::reth_tx_to_narwhal_tx(reth_tx.clone());
                    
                    // Log the size of the converted transaction for monitoring
                    let tx_size = narwhal_tx.0.len();
                    if transaction_count % 100 == 0 || tx_size > 10000 { // Log every 100 txs or large txs
                        info!("Converted transaction {} (hash: {}, size: {} bytes)", 
                            transaction_count, tx_hash, tx_size);
                    }
                    
                    if narwhal_tx_sender.send(narwhal_tx).is_err() {
                        warn!("Failed to send transaction to Narwhal - channel closed");
                        break;
                    }
                }
                info!("Transaction converter task terminated after processing {} transactions", 
                    transaction_count);
            })
        };

        // Spawn Narwhal DAG service
        let dag_service = DagService::new(
            self.config.node_public_key.clone(),
            self.committee.clone(),
            self.config.narwhal.clone(),
            signature_service,
            narwhal_tx_receiver,
            network_message_receiver,
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
        let bft_service = if let Some(storage) = &self.storage {
            info!("Creating BFT service with MDBX storage");
            let storage_adapter = std::sync::Arc::new(BullsharkMdbxAdapter::new(storage.clone()));
            BftService::with_storage(
                self.config.bullshark.clone(),
                self.committee.clone(),
                dag_to_bft_receiver,
                bft_to_reth_sender,
                storage_adapter,
            )
        } else {
            info!("Creating BFT service without storage (in-memory mode)");
            BftService::new(
                self.config.bullshark.clone(),
                self.committee.clone(),
                dag_to_bft_receiver,
                bft_to_reth_sender,
            )
        };
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
                    info!("Converting finalized batch {} with {} transactions", 
                        internal_batch.block_number, internal_batch.transactions.len());
                    
                    let mut successful_conversions = 0;
                    let mut failed_conversions = 0;
                    
                    let transactions: Vec<_> = internal_batch.transactions.into_iter()
                        .filter_map(|tx| {
                            match Self::narwhal_tx_to_reth_tx(tx) {
                                Ok(reth_tx) => {
                                    successful_conversions += 1;
                                    Some(reth_tx)
                                }
                                Err(e) => {
                                    failed_conversions += 1;
                                    warn!("Failed to convert Narwhal transaction to Reth: {}", e);
                                    None
                                }
                            }
                        })
                        .collect();

                    if failed_conversions > 0 {
                        warn!("Block {}: {} transaction conversions failed, {} succeeded", 
                            internal_batch.block_number, failed_conversions, successful_conversions);
                    } else {
                        info!("Block {}: All {} transactions converted successfully", 
                            internal_batch.block_number, successful_conversions);
                    }
                    
                    let reth_batch = FinalizedBatch {
                        block_number: internal_batch.block_number,
                        transactions,
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
        // Generate a keypair for this node - in production this would be loaded from secure storage
        use fastcrypto::traits::KeyPair;
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        Ok(fastcrypto::SignatureService::new(keypair))
    }
    
    /// Convert a Reth transaction to a Narwhal transaction
    fn reth_tx_to_narwhal_tx(reth_tx: RethTransaction) -> NarwhalTransaction {
        // Try to serialize the complete transaction using bincode
        match bincode::serialize(&reth_tx) {
            Ok(serialized_tx) => {
                // Prefix with a version byte for future compatibility
                let mut data = vec![0x01u8]; // Version 1
                data.extend(serialized_tx);
                NarwhalTransaction(data)
            }
            Err(e) => {
                // Log the specific serialization error for debugging
                error!("Failed to serialize Reth transaction with bincode: {}. Transaction type: {:?}", 
                    e, std::any::type_name::<RethTransaction>());
                
                // Try alternative serialization using serde_json as a more compatible fallback
                match serde_json::to_vec(&reth_tx) {
                    Ok(json_serialized) => {
                        let mut data = vec![0x02u8]; // Version 2 for JSON serialization
                        data.extend(json_serialized);
                        NarwhalTransaction(data)
                    }
                    Err(json_err) => {
                        error!("JSON serialization also failed: {}", json_err);
                        // Last resort: store just the hash
                        let tx_hash = reth_tx.tx_hash();
                        let mut data = vec![0x00u8]; // Version 0 (hash-only fallback)
                        data.extend(tx_hash.0);
                        NarwhalTransaction(data)
                    }
                }
            }
        }
    }
    
    /// Convert a Narwhal transaction to a Reth transaction
    fn narwhal_tx_to_reth_tx(narwhal_tx: NarwhalTransaction) -> Result<RethTransaction> {
        let data = &narwhal_tx.0;
        
        // Check if we have any data
        if data.is_empty() {
            return Err(anyhow::anyhow!("Empty Narwhal transaction data"));
        }
        
        // Check version byte
        match data[0] {
            0x01 => {
                // Version 1: Bincode serialization
                if data.len() < 2 {
                    return Err(anyhow::anyhow!("Invalid transaction data length"));
                }
                
                let serialized_tx = &data[1..];
                bincode::deserialize::<RethTransaction>(serialized_tx)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize transaction with bincode: {}", e))
            }
            0x02 => {
                // Version 2: JSON serialization
                if data.len() < 2 {
                    return Err(anyhow::anyhow!("Invalid JSON transaction data length"));
                }
                
                let json_data = &data[1..];
                serde_json::from_slice::<RethTransaction>(json_data)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize transaction with JSON: {}", e))
            }
            0x00 => {
                // Version 0: Hash-only (cannot reconstruct)
                Err(anyhow::anyhow!(
                    "Cannot reconstruct transaction from hash-only format (version 0). \
                     This is a fallback format used when serialization fails."
                ))
            }
            version => {
                Err(anyhow::anyhow!("Unknown transaction format version: {}", version))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use reth_primitives::{TransactionSigned, Transaction};
    use alloy_consensus::{TxEip1559, Transaction as AlloyCTransactionTrait};
    use alloy_primitives::{Address, U256, TxKind, ChainId, Signature};

    /// Create a test transaction for testing serialization
    fn create_test_transaction() -> TransactionSigned {
        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: ChainId::from(1u64),
            nonce: 42,
            gas_limit: 21000,
            max_fee_per_gas: 20_000_000_000, // 20 gwei
            max_priority_fee_per_gas: 1_000_000_000, // 1 gwei
            to: TxKind::Call(Address::from([0x01; 20])), // Fixed address for testing
            value: U256::from(1000000000000000000u64), // 1 ETH
            access_list: Default::default(),
            input: vec![0x60, 0x60, 0x60, 0x40].into(), // Simple bytecode
        });

        // Create a dummy signature for testing (r, s, y_parity)
        let signature = Signature::new(
            U256::from(0x123456789abcdefu64),
            U256::from(0xfedcba9876543210u64),
            false, // y_parity
        );

        TransactionSigned::new_unchecked(tx, signature, Default::default())
    }

    #[test]
    fn test_transaction_serialization_roundtrip() {
        // Create a test transaction
        let original_tx = create_test_transaction();
        let original_hash = original_tx.tx_hash();

        // Convert to Narwhal format
        let narwhal_tx = NarwhalBullsharkService::reth_tx_to_narwhal_tx(original_tx.clone());

        // Convert back to Reth format
        let recovered_tx = NarwhalBullsharkService::narwhal_tx_to_reth_tx(narwhal_tx)
            .expect("Should be able to recover transaction");

        // Verify the transactions are identical
        assert_eq!(original_hash, recovered_tx.tx_hash(), "Transaction hashes should match");
        
        // Use trait methods via conversion to compare transaction fields
        let original_tx_inner = original_tx.into_typed_transaction();
        let recovered_tx_inner = recovered_tx.into_typed_transaction();
        assert_eq!(original_tx_inner.nonce(), recovered_tx_inner.nonce(), "Nonces should match");
        assert_eq!(original_tx_inner.gas_limit(), recovered_tx_inner.gas_limit(), "Gas limits should match");
        assert_eq!(original_tx_inner.value(), recovered_tx_inner.value(), "Values should match");
    }

    #[test]
    fn test_transaction_version_handling() {
        // Test that we can handle different versions correctly
        let test_tx = create_test_transaction();
        let narwhal_tx = NarwhalBullsharkService::reth_tx_to_narwhal_tx(test_tx);

        // Verify version byte is correct
        assert!(!narwhal_tx.0.is_empty(), "Transaction data should not be empty");
        // TransactionSigned uses OnceLock which doesn't work with bincode, so expect JSON serialization
        assert_eq!(narwhal_tx.0[0], 0x02, "Should use version 2 (JSON) since TransactionSigned contains OnceLock");

        // Test that we reject unknown versions
        let mut bad_data = narwhal_tx.0.clone();
        bad_data[0] = 0xFF; // Unknown version
        let bad_narwhal_tx = NarwhalTransaction(bad_data);

        let result = NarwhalBullsharkService::narwhal_tx_to_reth_tx(bad_narwhal_tx);
        assert!(result.is_err(), "Should reject unknown version");
        assert!(result.unwrap_err().to_string().contains("Unknown transaction format version"));
    }

    #[test]
    fn test_empty_transaction_handling() {
        // Test empty transaction data
        let empty_tx = NarwhalTransaction(vec![]);
        let result = NarwhalBullsharkService::narwhal_tx_to_reth_tx(empty_tx);
        assert!(result.is_err(), "Should reject empty transaction data");
        assert!(result.unwrap_err().to_string().contains("Empty Narwhal transaction data"));
    }

    #[test]
    fn test_json_serialization_specifically() {
        // Test that JSON serialization works specifically for TransactionSigned
        let test_tx = create_test_transaction();
        
        // Test JSON serialization directly
        let json_serialized = serde_json::to_vec(&test_tx).expect("JSON serialization should work");
        let json_deserialized: TransactionSigned = serde_json::from_slice(&json_serialized)
            .expect("JSON deserialization should work");
        
        // Verify they're equivalent
        assert_eq!(test_tx.tx_hash(), json_deserialized.tx_hash(), "Transaction hashes should match after JSON roundtrip");
        
        // Verify bincode fails (confirming our analysis)
        let bincode_result = bincode::serialize(&test_tx);
        assert!(bincode_result.is_err(), "Bincode serialization should fail due to OnceLock");
    }
}