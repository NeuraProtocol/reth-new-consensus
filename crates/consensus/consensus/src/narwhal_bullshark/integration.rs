//! Integration bridge between Narwhal/Bullshark consensus and Reth execution pipeline

use crate::{
    narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkService, ServiceConfig, MempoolBridge},
    consensus_storage::MdbxConsensusStorage,
};
use narwhal::{types::{Committee, PublicKey}, NarwhalNetwork};
use reth_primitives::{
    TransactionSigned as RethTransaction, Header as RethHeader, SealedBlock,
};
use reth_ethereum_primitives::{Block};
use alloy_primitives::{B256, U256, Address, Bloom};
use reth_execution_types::{ExecutionOutcome, BlockExecutionOutput};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::info;
use anyhow::Result;

/// Bridge between Narwhal/Bullshark consensus and Reth blockchain execution
pub struct NarwhalRethBridge {
    /// The consensus service
    service: Option<NarwhalBullsharkService>,
    /// Networking for Narwhal
    #[allow(dead_code)]
    network: Option<NarwhalNetwork>,
    /// Channel for receiving transactions from Reth mempool (fallback mode)
    transaction_sender: mpsc::UnboundedSender<RethTransaction>,
    /// Channel for receiving finalized batches
    finalized_batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    /// Committee updates
    committee_sender: watch::Sender<Committee>,
    /// Current block number
    current_block_number: u64,
    /// Current parent hash
    current_parent_hash: B256,
    /// Execution outcome accumulator
    #[allow(dead_code)]
    execution_outcomes: Vec<ExecutionOutcome>,
    /// Block execution callback
    #[allow(dead_code)]
    block_executor: Option<Arc<dyn BlockExecutor + Send + Sync>>,
    /// MDBX storage for consensus persistence
    storage: Option<Arc<MdbxConsensusStorage>>,
    /// Optional mempool bridge for real pool integration (set externally)
    mempool_bridge: Option<MempoolBridge>,
}

impl std::fmt::Debug for NarwhalRethBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalRethBridge")
            .field("current_block_number", &self.current_block_number)
            .field("current_parent_hash", &self.current_parent_hash)
            .field("has_service", &self.service.is_some())
            .field("has_network", &self.network.is_some())
            .field("has_block_executor", &self.block_executor.is_some())
            .field("has_storage", &self.storage.is_some())
            .field("has_mempool_bridge", &self.mempool_bridge.is_some())
            .finish_non_exhaustive()
    }
}

/// Trait for executing blocks in Reth
pub trait BlockExecutor {
    /// Execute a block and return the execution outcome
    fn execute_block(&self, block: &SealedBlock) -> Result<BlockExecutionOutput<ExecutionOutcome>>;
    
    /// Get the current chain tip
    fn chain_tip(&self) -> Result<(u64, B256)>;
    
    /// Validate a block before execution
    fn validate_block(&self, block: &SealedBlock) -> Result<()>;
}

impl NarwhalRethBridge {
    /// Create a new Narwhal-Reth bridge
    pub fn new(config: ServiceConfig, storage: Option<Arc<MdbxConsensusStorage>>) -> Result<Self> {
        Self::new_with_network_config(config, storage, None)
    }

    /// Create a new Narwhal-Reth bridge with custom network configuration
    pub fn new_with_network_config(
        config: ServiceConfig, 
        storage: Option<Arc<MdbxConsensusStorage>>,
        network_config: Option<RethIntegrationConfig>
    ) -> Result<Self> {
        let (transaction_sender, transaction_receiver) = mpsc::unbounded_channel();
        let (finalized_batch_sender, finalized_batch_receiver) = mpsc::unbounded_channel();
        let (committee_sender, committee_receiver) = watch::channel(config.committee.clone());

        // Create networking with configurable address
        let network = if let Some(net_config) = network_config {
            if net_config.enable_networking {
                let bind_address = net_config.network_address;
                info!("Starting Narwhal network on {}", bind_address);
                let network_private_key = [0u8; 32]; // TODO: Load from config or generate securely
                let (network, _network_events) = NarwhalNetwork::new(
                    config.node_config.node_public_key.clone(),
                    config.committee.clone(),
                    bind_address,
                    network_private_key,
                )?;
                Some(network)
            } else {
                info!("Networking disabled for testing");
                None
            }
        } else {
            // Default: use random port for testing
            let bind_address = "127.0.0.1:0".parse().unwrap(); // Port 0 = random port
            info!("Starting Narwhal network on {} (random port)", bind_address);
            let network_private_key = [0u8; 32]; // TODO: Load from config or generate securely
            let (network, _network_events) = NarwhalNetwork::new(
                config.node_config.node_public_key.clone(),
                config.committee.clone(),
                bind_address,
                network_private_key,
            )?;
            Some(network)
        };

        let service = NarwhalBullsharkService::new(
            config.node_config,
            config.committee,
            transaction_receiver,
            finalized_batch_sender,
            committee_receiver,
            storage.clone(), // Properly inject the MDBX storage
        );

        Ok(Self {
            service: Some(service),
            network,
            transaction_sender,
            finalized_batch_receiver,
            committee_sender,
            current_block_number: 1,
            current_parent_hash: B256::ZERO, // Genesis parent
            execution_outcomes: Vec::new(),
            block_executor: None,
            storage,
            mempool_bridge: None, // TODO: Implement with_pool constructor
        })
    }

    /// Create a new bridge for testing without networking
    pub fn new_for_testing(config: ServiceConfig, storage: Option<Arc<MdbxConsensusStorage>>) -> Result<Self> {
        let test_config = RethIntegrationConfig {
            network_address: "127.0.0.1:0".parse().unwrap(),
            enable_networking: false, // Disable networking for tests
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: false,
        };
        Self::new_with_network_config(config, storage, Some(test_config))
    }

    /// Start the consensus service
    pub fn start(&mut self) -> Result<()> {
        if let Some(service) = self.service.take() {
            let _handles = service.spawn()?;
            info!("Narwhal + Bullshark consensus service started");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service already started"))
        }
    }

    /// Submit a transaction to the mempool
    pub fn submit_transaction(&self, transaction: RethTransaction) -> Result<()> {
        self.transaction_sender
            .send(transaction)
            .map_err(|_| anyhow::anyhow!("Failed to submit transaction - consensus is shutting down"))
    }

    /// Get the next finalized batch and convert it to a Reth block
    pub async fn get_next_block(&mut self) -> Result<Option<SealedBlock>> {
        if let Some(batch) = self.finalized_batch_receiver.recv().await {
            let block = self.batch_to_block(batch)?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    /// Update the committee configuration
    pub fn update_committee(&self, committee: Committee) -> Result<()> {
        self.committee_sender
            .send(committee)
            .map_err(|_| anyhow::anyhow!("Failed to update committee"))
    }

    /// Convert a finalized batch to a Reth block
    fn batch_to_block(&mut self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // Create block header
        let header = RethHeader {
            parent_hash: batch.parent_hash,
            ommers_hash: B256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000, // 30M gas limit
            gas_used: 0, // Will be calculated during execution
            timestamp: batch.timestamp,
            difficulty: U256::ZERO, // No PoW in Narwhal/Bullshark
            nonce: Default::default(),
            mix_hash: B256::ZERO,
            beneficiary: Address::ZERO, // No coinbase in this consensus
            state_root: B256::ZERO, // Will be calculated during execution
            transactions_root: B256::ZERO, // Will be calculated
            receipts_root: B256::ZERO, // Will be calculated
            logs_bloom: Bloom::ZERO,
            extra_data: Default::default(),
            base_fee_per_gas: Some(1_000_000_000), // 1 gwei
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            withdrawals_root: None,
            requests_hash: None,
        };

        // Create block body
        let body = reth_primitives::BlockBody {
            transactions: batch.transactions,
            ommers: vec![], // No uncles in Narwhal/Bullshark
            withdrawals: None,
        };

        // Create the block
        let block = Block::new(header, body);
        
        // For now, we'll use a dummy seal. In a real implementation,
        // this would contain the Narwhal/Bullshark consensus proof
        let sealed_block = SealedBlock::seal_slow(block);

        // Update our state
        self.current_block_number = batch.block_number + 1;
        self.current_parent_hash = sealed_block.hash();

        Ok(sealed_block)
    }

    /// Get the current block number
    pub fn current_block_number(&self) -> u64 {
        self.current_block_number
    }

    /// Get the current parent hash
    pub fn current_parent_hash(&self) -> B256 {
        self.current_parent_hash
    }

    /// Set the mempool operations provider (dependency injection)
    pub fn set_mempool_operations(&mut self, mempool_ops: Box<dyn super::mempool_bridge::MempoolOperations>) -> Result<()> {
        // Create mempool bridge if it doesn't exist
        if self.mempool_bridge.is_none() {
            let (consensus_tx_sender, consensus_tx_receiver) = mpsc::unbounded_channel();
            let (_finalized_batch_sender, finalized_batch_receiver) = mpsc::unbounded_channel();
            
            // Store the sender for transaction submission
            self.transaction_sender = consensus_tx_sender;
            
            // Create the bridge
            let mut bridge = super::mempool_bridge::MempoolBridge::new(
                self.transaction_sender.clone(),
                finalized_batch_receiver,
            );
            
            // Set the mempool operations
            bridge.set_mempool_operations(mempool_ops);
            
            self.mempool_bridge = Some(bridge);
        } else {
            // Bridge already exists, just set the operations
            if let Some(ref mut bridge) = self.mempool_bridge {
                bridge.set_mempool_operations(mempool_ops);
            }
        }
        
        info!("Mempool operations configured for consensus bridge");
        Ok(())
    }
}

/// Helper function to create a test committee
pub fn create_test_committee(authorities: Vec<PublicKey>) -> Committee {
    let mut authority_map = std::collections::HashMap::new();
    for (_i, authority) in authorities.into_iter().enumerate() {
        authority_map.insert(authority, 100); // Equal stake of 100 for all authorities
    }
    Committee::new(0, authority_map)
}

/// Configuration for Reth integration with Narwhal + Bullshark
#[derive(Debug, Clone)]
pub struct RethIntegrationConfig {
    /// Network address for Narwhal networking
    pub network_address: std::net::SocketAddr,
    /// Maximum pending transactions to buffer
    pub max_pending_transactions: usize,
    /// Timeout for block execution
    pub execution_timeout: std::time::Duration,
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Enable networking
    pub enable_networking: bool,
}

impl Default for RethIntegrationConfig {
    fn default() -> Self {
        Self {
            network_address: "127.0.0.1:0".parse().unwrap(), // Use random port by default
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: true,
            enable_networking: true,
        }
    }
}

/// Helper function to create test configuration
pub fn create_test_config(node_key: PublicKey, committee: Committee) -> ServiceConfig {
    let node_config = crate::narwhal_bullshark::types::NarwhalBullsharkConfig {
        node_public_key: node_key,
        narwhal: narwhal::NarwhalConfig::default(),
        bullshark: bullshark::BftConfig::default(),
    };
    
    ServiceConfig::new(node_config, committee)
} 