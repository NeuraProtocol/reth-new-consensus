//! Integration bridge between Narwhal/Bullshark consensus and Reth execution pipeline

use crate::{
    narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkService, MempoolBridge, NarwhalBullsharkConfig, ConsensusSeal},
    consensus_storage::MdbxConsensusStorage,
};
use narwhal::{types::{Committee, PublicKey}, NarwhalNetwork};
use reth_primitives::{
    TransactionSigned as RethTransaction, Header as RethHeader, SealedBlock,
};
use reth_ethereum_primitives::{Block};
use alloy_primitives::{B256, U256, Address, Bloom, Bytes};
use reth_execution_types::{ExecutionOutcome, BlockExecutionOutput};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn, debug, error};
use anyhow::Result;
use fastcrypto::traits::{ToFromBytes, AggregateAuthenticator};
use serde::{Serialize, Deserialize};

/// Service configuration for compatibility with integration layer
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub node_config: NarwhalBullsharkConfig,
    pub committee: Committee,
}

impl ServiceConfig {
    pub fn new(node_config: NarwhalBullsharkConfig, committee: Committee) -> Self {
        Self { node_config, committee }
    }
}

/// Bridge between Narwhal/Bullshark consensus and Reth blockchain execution
pub struct NarwhalRethBridge {
    /// The consensus service
    service: Option<NarwhalBullsharkService>,
    /// Networking for Narwhal
    network: Option<NarwhalNetwork>,
    /// Network event receiver for forwarding to consensus
    network_event_receiver: Option<tokio::sync::broadcast::Receiver<narwhal::NetworkEvent>>,
    /// Channel for receiving transactions from Reth mempool (fallback mode)
    transaction_sender: mpsc::UnboundedSender<RethTransaction>,
    /// Channel for receiving finalized batches
    finalized_batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
    /// Broadcast sender for finalized batches (to send to multiple receivers like mempool bridge)
    finalized_batch_broadcast: tokio::sync::broadcast::Sender<FinalizedBatch>,
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
    /// Peer addresses for network connections
    peer_addresses: Vec<std::net::SocketAddr>,
    /// Committee configuration for peer mapping
    committee: Committee,
    /// Our node's public key
    node_public_key: PublicKey,
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
        
        // Create broadcast channel for finalized batches (so mempool bridge can also listen)
        let (finalized_batch_broadcast, _) = tokio::sync::broadcast::channel(100);

        // Clone values before moving them
        let committee_clone = config.committee.clone();
        let node_public_key_clone = config.node_config.node_public_key.clone();
        let peer_addresses_clone = network_config.as_ref().map_or(Vec::new(), |nc| nc.peer_addresses.clone());

        // Create networking with configurable address
        let (network, network_event_receiver) = if let Some(ref net_config) = network_config {
            if net_config.enable_networking {
                let bind_address = net_config.network_address;
                info!("Starting Narwhal network on {}", bind_address);
                
                // Generate unique network private key from validator consensus key
                let network_private_key = {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    
                    let consensus_key_bytes = config.node_config.node_public_key.as_bytes();
                    let mut hasher = DefaultHasher::new();
                    b"narwhal_network_key:".hash(&mut hasher);
                    consensus_key_bytes.hash(&mut hasher);
                    bind_address.to_string().hash(&mut hasher);
                    
                    let hash_value = hasher.finish();
                    let mut key = [0u8; 32];
                    
                    // Fill the key with hash-derived bytes
                    for (i, chunk) in hash_value.to_le_bytes().iter().cycle().take(32).enumerate() {
                        key[i] = *chunk;
                    }
                    
                    key
                };
                
                let (network, network_events) = NarwhalNetwork::new(
                    config.node_config.node_public_key.clone(),
                    config.committee.clone(),
                    bind_address,
                    network_private_key,
                    narwhal::config::NarwhalConfig::default(),
                )?;
                
                // Connect to peer addresses if provided
                if !net_config.peer_addresses.is_empty() {
                    info!("Will connect to {} peer addresses after startup", net_config.peer_addresses.len());
                    
                    let committee_keys: Vec<_> = config.committee.authorities.keys().cloned().collect();
                    info!("Committee has {} members, {} peer addresses provided", 
                          committee_keys.len(), net_config.peer_addresses.len());
                }
                
                (Some(network), Some(network_events))
            } else {
                info!("Networking disabled for testing");
                (None, None)
            }
        } else {
            // Default: use random port for testing
            let bind_address: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap(); // Port 0 = random port
            info!("Starting Narwhal network on {} (random port)", bind_address);
            
            // Generate unique network private key from validator consensus key
            let network_private_key = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                
                let consensus_key_bytes = config.node_config.node_public_key.as_bytes();
                let mut hasher = DefaultHasher::new();
                b"narwhal_network_key:".hash(&mut hasher);
                consensus_key_bytes.hash(&mut hasher);
                bind_address.to_string().hash(&mut hasher);
                
                let hash_value = hasher.finish();
                let mut key = [0u8; 32];
                
                // Fill the key with hash-derived bytes
                for (i, chunk) in hash_value.to_le_bytes().iter().cycle().take(32).enumerate() {
                    key[i] = *chunk;
                }
                
                key
            };
            
            let (network, network_events) = NarwhalNetwork::new(
                config.node_config.node_public_key.clone(),
                config.committee.clone(),
                bind_address,
                network_private_key,
                narwhal::config::NarwhalConfig::default(),
            )?;
            (Some(network), Some(network_events))
        };

        // Create peer address mapping for committee
        let mut peer_addresses_map = std::collections::HashMap::new();
        if let Some(ref net_config) = network_config {
            // Map peer addresses to committee public keys
            let committee_keys: Vec<_> = config.committee.authorities.keys()
                .filter(|&key| key != &config.node_config.node_public_key)
                .cloned()
                .collect();
            
            for (i, &addr) in net_config.peer_addresses.iter().enumerate() {
                if i < committee_keys.len() {
                    peer_addresses_map.insert(committee_keys[i].clone(), addr);
                }
            }
        }

        let listen_address = network_config.as_ref()
            .map(|nc| nc.network_address)
            .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());

        let mut service = NarwhalBullsharkService::new(
            config.node_config.clone(),
            config.committee,
            transaction_receiver,
            finalized_batch_sender,
            committee_receiver,
            storage.clone(), // Properly inject the MDBX storage
            network_event_receiver, // Connect network events for REAL distributed consensus
            network.clone().map(|n| Arc::new(n)), // ✅ FIX: Pass cloned network handle for outbound broadcasting
        )?;
        
        // Configure RPC if port is set in network config
        if let Some(ref net_config) = network_config {
            if net_config.enable_networking && net_config.network_address.port() > 0 {
                // Check if we should enable RPC based on a convention
                // (e.g., RPC port = network port + 1000)
                let rpc_port = net_config.network_address.port() + 1000;
                let rpc_config = super::types::ConsensusRpcConfig {
                    port: rpc_port,
                    host: "127.0.0.1".to_string(),
                    enable_admin: false,
                };
                service = service.with_rpc(rpc_config);
                info!("Consensus RPC will be available on port {}", rpc_port);
            }
        }

        Ok(Self {
            service: Some(service),
            network,
            network_event_receiver: None, // Network events are passed to the service
            transaction_sender,
            finalized_batch_receiver,
            finalized_batch_broadcast,
            committee_sender,
            current_block_number: 1,
            // Use actual Neura genesis hash as parent for block 1
            current_parent_hash: "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
                .parse::<B256>()
                .unwrap_or(B256::ZERO),
            execution_outcomes: Vec::new(),
            block_executor: None,
            storage,
            mempool_bridge: None, // Will be set via set_mempool_operations
            peer_addresses: peer_addresses_clone,
            committee: committee_clone,
            node_public_key: node_public_key_clone,
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
            peer_addresses: Vec::new(),
        };
        Self::new_with_network_config(config, storage, Some(test_config))
    }

    /// Start the consensus service
    pub async fn start(&mut self) -> Result<()> {
        if let Some(mut service) = self.service.take() {
            // Initialize chain state in the consensus service before starting
            // This ensures BFT service creates blocks with correct block numbers
            if self.block_executor.is_some() {
                // If we have a block executor, use its chain tip
                if let Some(ref executor) = self.block_executor {
                    if let Ok((block_number, parent_hash)) = executor.chain_tip() {
                        self.current_block_number = block_number + 1;
                        self.current_parent_hash = parent_hash;
                        // Update consensus service with initial chain state
                        service.update_chain_state(block_number, parent_hash).await;
                        info!("Initialized consensus chain state from block executor: next block {} parent {}", 
                              self.current_block_number, self.current_parent_hash);
                    }
                }
            } else {
                // No block executor, use our current state (genesis or configured)
                // For genesis, we want to create block 1 (current_block_number = 1)
                // So the chain state should show block_number = 0, parent_hash = genesis hash
                let chain_block_number = if self.current_block_number > 0 { 
                    self.current_block_number - 1 
                } else { 
                    0 
                };
                
                // Use the actual Neura genesis block hash instead of 0x0
                let genesis_hash = "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
                    .parse::<B256>()
                    .unwrap_or(B256::ZERO);
                
                // If we're at genesis (block 0), use the genesis hash as parent
                let parent_hash = if chain_block_number == 0 {
                    genesis_hash
                } else {
                    self.current_parent_hash
                };
                
                service.update_chain_state(chain_block_number, parent_hash).await;
                info!("Initialized consensus chain state: current block {} (next: {}) parent {}", 
                      chain_block_number, self.current_block_number, parent_hash);
            }
            
            // IMPORTANT: Connect to peers BEFORE starting the consensus service
            // This avoids the race condition where DAG service starts broadcasting before connections exist
            if let Some(ref mut network) = self.network {
                if !self.peer_addresses.is_empty() {
                    info!("🔗 Establishing peer connections BEFORE starting consensus service...");
                    
                    // Create address mapping from committee members to network addresses
                    let mut peer_address_map = std::collections::HashMap::new();
                    
                    // Get sorted list of all committee members for consistent ordering
                    let mut all_validators: Vec<_> = self.committee.authorities.keys().cloned().collect();
                    all_validators.sort_by_key(|k| k.as_ref().to_vec()); // Sort by public key bytes for deterministic order
                    
                    // Find our index in the sorted list
                    let our_index = all_validators.iter().position(|k| k == &self.node_public_key)
                        .expect("Our key should be in committee");
                    
                    // Map peer addresses based on validator order
                    // The peer addresses are expected to be provided in order of validator indices
                    // excluding ourselves
                    let mut peer_addr_index = 0;
                    for (validator_index, validator_key) in all_validators.iter().enumerate() {
                        if validator_index != our_index && peer_addr_index < self.peer_addresses.len() {
                            let addr = self.peer_addresses[peer_addr_index];
                            peer_address_map.insert(validator_key.clone(), addr);
                            info!("Validator {} (index {}) -> {} at {}", 
                                  validator_key, validator_index, 
                                  if validator_index < our_index { "peer before us" } else { "peer after us" },
                                  addr);
                            peer_addr_index += 1;
                        }
                    }
                    
                    // Wait for initial connections to be established
                    info!("🔍 DEBUG: About to call wait_for_initial_connections...");
                    let connection_timeout = std::time::Duration::from_secs(5);
                    let wait_result = network.wait_for_initial_connections(&peer_address_map, connection_timeout).await;
                    info!("🔍 DEBUG: wait_for_initial_connections returned: {:?}", wait_result);
                    
                    match wait_result {
                        Ok(()) => info!("✅ Initial peer connections established"),
                        Err(e) => {
                            warn!("⚠️ Failed to establish all peer connections: {}", e);
                            // Continue anyway - partial connectivity is better than none
                        }
                    }
                    
                    // Spawn a connection maintenance task with the proper peer mapping
                    let network_clone = network.clone();
                    let peer_map_for_task = peer_address_map.clone();
                    let _connection_task = tokio::spawn(async move {
                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                        loop {
                            interval.tick().await;
                            
                            // Check and reconnect to any disconnected peers
                            for (pubkey, addr) in &peer_map_for_task {
                                if let Err(e) = network_clone.add_peer(pubkey.clone(), *addr).await {
                                    debug!("Failed to reconnect to {}: {}", pubkey, e);
                                }
                            }
                        }
                    });
                    
                    info!("🔄 Connection maintenance task started");
                }
            }
            
            info!("✅ Peer connection phase completed, proceeding to start consensus service");
            
            // Initialize consensus service chain state
            info!("🔍 DEBUG: About to update consensus service chain state...");
            service.update_chain_state(self.current_block_number, self.current_parent_hash).await;
            info!("Initialized consensus chain state: block {} parent {}", 
                  self.current_block_number, self.current_parent_hash);
            
            // NOW start the consensus service after connections are ready
            info!("🔍 DEBUG: About to spawn consensus service...");
            let spawn_result = service.spawn().await;
            info!("🔍 DEBUG: service.spawn() returned: {:?}", spawn_result);
            spawn_result?;
            info!("✅ Narwhal + Bullshark consensus service started");
            
            // Put the service back so it doesn't get dropped!
            self.service = Some(service);
            
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
            // Broadcast the batch to other listeners (e.g., mempool bridge)
            // We ignore the error if there are no receivers
            let _ = self.finalized_batch_broadcast.send(batch.clone());
            
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
        // Create consensus seal from batch data
        let consensus_seal = self.create_consensus_seal(&batch);
        
        // Encode consensus seal into extra_data field
        let extra_data = consensus_seal.encode();

        // Create block header with consensus proof
        let header = RethHeader {
            parent_hash: batch.parent_hash,
            ommers_hash: B256::ZERO,
            number: batch.block_number,
            gas_limit: 30_000_000, // 30M gas limit
            gas_used: 0, // Will be calculated during execution
            timestamp: batch.timestamp,
            difficulty: U256::ZERO, // No PoW in Narwhal/Bullshark
            nonce: Default::default(),
            // Use certificate digest as mix_hash for consensus proof reference
            mix_hash: batch.certificate_digest,
            beneficiary: Address::ZERO, // No coinbase in this consensus
            state_root: B256::ZERO, // Will be calculated during execution
            transactions_root: B256::ZERO, // Will be calculated
            receipts_root: B256::ZERO, // Will be calculated
            logs_bloom: Bloom::ZERO,
            extra_data,
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
        
        // Seal the block with proper hash calculation
        let sealed_block = SealedBlock::seal_slow(block);

        // Update our state
        // batch.block_number is the block we just created
        // So the next block number is batch.block_number + 1
        self.current_block_number = batch.block_number + 1;
        self.current_parent_hash = sealed_block.hash();
        
        // Update consensus service chain state
        // The chain state should reflect the block we just created (batch.block_number)
        // with its hash as the parent for the next block
        if let Some(ref service) = self.service {
            service.update_chain_state_sync(batch.block_number, sealed_block.hash());
            debug!("Updated consensus service chain state to block {} (next: {}) parent {}", 
                   batch.block_number, self.current_block_number, sealed_block.hash());
        }

        Ok(sealed_block)
    }

    /// Create consensus seal from finalized batch
    fn create_consensus_seal(&self, batch: &FinalizedBatch) -> ConsensusSeal {
        use fastcrypto::{
            traits::{ToFromBytes, AggregateAuthenticator},
            bls12381::{BLS12381AggregateSignature, BLS12381Signature},
        };
        
        // Aggregate signatures from validators
        let mut signatures = Vec::new();
        let mut signers_bitmap = vec![0u8; (batch.validator_signatures.len() + 7) / 8];
        
        // Collect BLS signatures and build bitmap
        for (i, (_pubkey, sig_bytes)) in batch.validator_signatures.iter().enumerate() {
            // Set bit in bitmap
            signers_bitmap[i / 8] |= 1 << (i % 8);
            
            // Try to parse BLS signature
            if let Ok(sig) = BLS12381Signature::from_bytes(sig_bytes) {
                signatures.push(sig);
            }
        }
        
        // Aggregate the signatures
        let aggregated_signature = if signatures.is_empty() {
            // No signatures, create empty aggregate
            Bytes::from(vec![0u8; 96]) // BLS12-381 signature size
        } else {
            // Aggregate all signatures
            match BLS12381AggregateSignature::aggregate(signatures) {
                Ok(agg_sig) => Bytes::from(agg_sig.as_bytes().to_vec()),
                Err(_) => Bytes::from(vec![0u8; 96]), // Fallback on error
            }
        };
        
        ConsensusSeal {
            round: batch.consensus_round,
            certificate_digest: batch.certificate_digest,
            aggregated_signature,
            signers_bitmap: Bytes::from(signers_bitmap),
        }
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
        // Create a new receiver for the mempool bridge to listen to finalized batches
        let (mempool_finalized_sender, mempool_finalized_receiver) = mpsc::unbounded_channel();
        
        // Create mempool bridge with the existing transaction_sender that's connected to the DAG service
        // and its own finalized batch receiver
        let mut bridge = super::mempool_bridge::MempoolBridge::new(
            self.transaction_sender.clone(),
            mempool_finalized_receiver,
        );
        
        // Set the mempool operations
        bridge.set_mempool_operations(mempool_ops);
        
        // Spawn the mempool bridge tasks
        let handles = bridge.spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn mempool bridge: {}", e))?;
        
        info!("Mempool bridge spawned with {} tasks", handles.len());
        
        // Spawn a task to forward finalized batches to the mempool bridge
        let mut finalized_broadcast_receiver = self.finalized_batch_broadcast.subscribe();
        tokio::spawn(async move {
            // Subscribe to finalized batches and forward them to mempool bridge
            while let Ok(batch) = finalized_broadcast_receiver.recv().await {
                if mempool_finalized_sender.send(batch).is_err() {
                    warn!("Mempool bridge stopped receiving finalized batches");
                    break;
                }
            }
        });
        
        // Store the spawned tasks (we'll need to track these for shutdown)
        // For now, just detach them
        for handle in handles {
            tokio::spawn(async move {
                if let Err(e) = handle.await {
                    error!("Mempool bridge task error: {:?}", e);
                }
            });
        }
        
        info!("✅ Mempool operations configured and bridge running");
        Ok(())
    }

    /// Get the current committee
    pub fn get_current_committee(&self) -> Committee {
        self.committee.clone()
    }

    /// Check if consensus is running
    pub fn is_running(&self) -> bool {
        self.service.is_some() && self.service.as_ref().unwrap().is_running()
    }
    
    /// Configure the consensus service with RPC (must be called before start)
    pub fn with_rpc(&mut self, config: super::types::ConsensusRpcConfig) -> Result<()> {
        if let Some(service) = self.service.take() {
            self.service = Some(service.with_rpc(config));
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service already started"))
        }
    }
    
    /// Set block executor (dependency injection)
    pub fn set_block_executor(&mut self, executor: Arc<dyn BlockExecutor + Send + Sync>) {
        // Get chain tip from executor and update our state
        if let Ok((block_number, parent_hash)) = executor.chain_tip() {
            self.current_block_number = block_number + 1; // Next block number
            self.current_parent_hash = parent_hash;
            info!("Updated chain state from block executor: block {} parent {}", 
                  self.current_block_number, self.current_parent_hash);
        }
        
        self.block_executor = Some(executor);
    }
}

/// Helper function to create a test committee
pub fn create_test_committee(authorities: Vec<PublicKey>) -> Committee {
    let mut authority_map = std::collections::HashMap::new();
    for (_i, authority) in authorities.into_iter().enumerate() {
        authority_map.insert(authority, 100); // Equal stake of 100 for all authorities
    }
    Committee::new_simple(0, authority_map)
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
    /// Peer addresses for other validators
    pub peer_addresses: Vec<std::net::SocketAddr>,
}

impl Default for RethIntegrationConfig {
    fn default() -> Self {
        Self {
            network_address: "127.0.0.1:0".parse().unwrap(), // Use random port by default
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: true,
            enable_networking: true,
            peer_addresses: Vec::new(),
        }
    }
}

/// Helper function to create test configuration
pub fn create_test_config(node_key: PublicKey, committee: Committee) -> ServiceConfig {
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    
    // Generate test keypair for this node
    let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
    let consensus_private_key_bytes = keypair.private().as_bytes().to_vec();
    
    let node_config = crate::narwhal_bullshark::types::NarwhalBullsharkConfig {
        node_public_key: node_key,
        consensus_private_key_bytes,
        narwhal: narwhal::NarwhalConfig::default(),
        bullshark: bullshark::BftConfig::default(),
        network: crate::narwhal_bullshark::types::NetworkConfig::default(),
    };
    
    ServiceConfig::new(node_config, committee)
}

/// Helper function to create configuration with explicit network settings
pub fn create_test_config_with_network(
    node_key: PublicKey, 
    committee: Committee,
    bind_port: u16,
    peer_addresses: std::collections::HashMap<String, std::net::SocketAddr>
) -> ServiceConfig {
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    
    let bind_address = format!("127.0.0.1:{}", bind_port).parse().expect("Valid address");
    
    // Generate test keypair for this node
    let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
    let consensus_private_key_bytes = keypair.private().as_bytes().to_vec();
    
    let node_config = crate::narwhal_bullshark::types::NarwhalBullsharkConfig {
        node_public_key: node_key,
        consensus_private_key_bytes,
        narwhal: narwhal::NarwhalConfig::default(),
        bullshark: bullshark::BftConfig::default(),
        network: crate::narwhal_bullshark::types::NetworkConfig {
            bind_address,
            peer_addresses,
        },
    };
    
    ServiceConfig::new(node_config, committee)
} 