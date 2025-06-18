//! Integration bridge between Narwhal/Bullshark consensus and Reth execution pipeline

use crate::narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkService, ServiceConfig};
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
    /// Channel for receiving transactions from Reth mempool
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
}

impl std::fmt::Debug for NarwhalRethBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalRethBridge")
            .field("current_block_number", &self.current_block_number)
            .field("current_parent_hash", &self.current_parent_hash)
            .field("has_service", &self.service.is_some())
            .field("has_network", &self.network.is_some())
            .field("has_block_executor", &self.block_executor.is_some())
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
    pub fn new(config: ServiceConfig) -> Result<Self> {
        let (transaction_sender, transaction_receiver) = mpsc::unbounded_channel();
        let (finalized_batch_sender, finalized_batch_receiver) = mpsc::unbounded_channel();
        let (committee_sender, committee_receiver) = watch::channel(config.committee.clone());

        // Create networking
        let bind_address = "127.0.0.1:9000".parse().unwrap(); // TODO: Make configurable
        let (network, _network_events) = NarwhalNetwork::new(
            config.node_config.node_public_key.clone(),
            config.committee.clone(),
            bind_address,
        )?;

        let service = NarwhalBullsharkService::new(
            config.node_config,
            config.committee,
            transaction_receiver,
            finalized_batch_sender,
            committee_receiver,
        );

        Ok(Self {
            service: Some(service),
            network: Some(network),
            transaction_sender,
            finalized_batch_receiver,
            committee_sender,
            current_block_number: 1,
            current_parent_hash: B256::ZERO, // Genesis parent
            execution_outcomes: Vec::new(),
            block_executor: None,
        })
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
}

impl Default for RethIntegrationConfig {
    fn default() -> Self {
        Self {
            network_address: "127.0.0.1:9000".parse().unwrap(),
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: true,
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