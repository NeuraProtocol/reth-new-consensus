//! Complete integration bridge between Narwhal/Bullshark consensus and Reth execution pipeline
//! This is the restored working implementation from the pre-move version (749 lines)

use crate::{
    types::{FinalizedBatch, ConsensusConfig, ConsensusRpcConfig},
    narwhal_bullshark_service::NarwhalBullsharkService,
    mempool_bridge::{MempoolBridge, MempoolOperations},
    consensus_storage::MdbxConsensusStorage,
    canonical_hash_tracker::CanonicalHashTracker,
};
use narwhal::{types::{Committee, PublicKey}, NarwhalNetwork};
use reth_primitives::{
    TransactionSigned as RethTransaction, Header as RethHeader, SealedBlock, Receipt,
};
use reth_ethereum_primitives::{Block};
use alloy_primitives::{B256, U256, Address, Bloom, Bytes};
use alloy_consensus::proofs;
use reth_execution_types::{ExecutionOutcome, BlockExecutionOutput};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn, debug, error};
use anyhow::Result;
use fastcrypto::traits::{ToFromBytes, AggregateAuthenticator};
use serde::{Serialize, Deserialize};
use reth_chainspec::{EthereumHardforks, ChainSpec, EthChainSpec};
use alloy_eips::eip1559::calc_next_block_base_fee;

/// Service configuration for compatibility with integration layer
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub node_config: ConsensusConfig,
    pub committee: Committee,
}

impl ServiceConfig {
    pub fn new(node_config: ConsensusConfig, committee: Committee) -> Self {
        Self { node_config, committee }
    }
}

/// Consensus seal data structure for block headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusSeal {
    /// Consensus round number
    pub round: u64,
    /// Certificate digest that finalized this block
    pub certificate_digest: B256,
    /// Aggregated BLS signature from validators
    pub aggregated_signature: Bytes,
    /// Bitmap of validators who signed
    pub signers_bitmap: Bytes,
}

impl ConsensusSeal {
    /// Encode consensus seal for storage in block header extra_data
    pub fn encode(&self) -> Bytes {
        // Simple encoding: round (8 bytes) + cert_digest (32 bytes) + sig_len (4 bytes) + sig + bitmap_len (4 bytes) + bitmap
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&self.round.to_be_bytes());
        encoded.extend_from_slice(self.certificate_digest.as_slice());
        encoded.extend_from_slice(&(self.aggregated_signature.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.aggregated_signature);
        encoded.extend_from_slice(&(self.signers_bitmap.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.signers_bitmap);
        Bytes::from(encoded)
    }
}

/// Bridge between Narwhal/Bullshark consensus and Reth blockchain execution
/// This is the working implementation from the pre-move version
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
    /// Parent block information for base fee calculation
    parent_block_info: Arc<std::sync::Mutex<Option<(u64, u64, u64)>>>, // (gas_limit, gas_used, base_fee)
    /// Last block timestamp in seconds (for compatibility) 
    last_block_timestamp: u64,
    /// Last block timestamp in milliseconds for precise ordering
    last_block_timestamp_ms: u64,
    /// Execution outcome accumulator
    #[allow(dead_code)]
    execution_outcomes: Vec<ExecutionOutcome>,
    /// Block execution callback
    #[allow(dead_code)]
    block_executor: Option<Arc<dyn BlockExecutor + Send + Sync>>,
    /// MDBX storage for consensus persistence
    storage: Option<Arc<MdbxConsensusStorage>>,
    /// Optional mempool bridge for real pool integration (set externally)
    mempool_bridge: Option<Box<dyn MempoolOperations>>,
    /// Peer addresses for network connections
    peer_addresses: Vec<std::net::SocketAddr>,
    /// Committee configuration for peer mapping
    committee: Committee,
    /// Our node's public key
    node_public_key: PublicKey,
    /// Chain specification for base fee calculations
    chain_spec: Option<Arc<ChainSpec>>,
    /// Canonical hash tracker
    canonical_hash_tracker: Arc<CanonicalHashTracker>,
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

impl NarwhalRethBridge {
    /// Create a new Narwhal-Reth bridge
    pub fn new(config: ServiceConfig, storage: Option<Arc<MdbxConsensusStorage>>) -> Result<Self> {
        Self::new_with_network_config(config, storage, None)
    }
    
    /// Set the chain spec for proper base fee calculations
    pub fn set_chain_spec(&mut self, chain_spec: Arc<ChainSpec>) {
        self.chain_spec = Some(chain_spec);
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
                    use blake2::{Blake2b, Digest, digest::consts::U64};
                    use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PrivateKey};
                    use fastcrypto::traits::{KeyPair as _, ToFromBytes};
                    
                    let consensus_key_bytes = config.node_config.node_public_key.as_bytes();
                    let mut hasher = Blake2b::<U64>::new();
                    hasher.update(b"narwhal_network_key:");
                    hasher.update(consensus_key_bytes);
                    hasher.update(bind_address.to_string().as_bytes());
                    
                    let hash = hasher.finalize();
                    let mut seed = [0u8; 32];
                    seed.copy_from_slice(&hash[..32]);
                    
                    // Create Ed25519 keypair from seed
                    let private_key = Ed25519PrivateKey::from_bytes(&seed)
                        .expect("Failed to create Ed25519 private key");
                    let keypair = Ed25519KeyPair::from(private_key);
                    
                    // Get public key for debugging
                    let public_key_hex = hex::encode(keypair.public().as_bytes());
                    debug!("Generated network Ed25519 key for {} at {} - public key: {}", 
                           config.node_config.node_public_key, bind_address, 
                           public_key_hex);
                    
                    // anemo expects the private key seed bytes directly
                    seed
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
                use blake2::{Blake2b, Digest, digest::consts::U64};
                use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PrivateKey};
                use fastcrypto::traits::{KeyPair as _, ToFromBytes};
                
                let consensus_key_bytes = config.node_config.node_public_key.as_bytes();
                let mut hasher = Blake2b::<U64>::new();
                hasher.update(b"narwhal_network_key:");
                hasher.update(consensus_key_bytes);
                hasher.update(bind_address.to_string().as_bytes());
                
                let hash = hasher.finalize();
                let mut seed = [0u8; 32];
                seed.copy_from_slice(&hash[..32]);
                
                // Create Ed25519 keypair from seed
                let private_key = Ed25519PrivateKey::from_bytes(&seed)
                    .expect("Failed to create Ed25519 private key");
                let keypair = Ed25519KeyPair::from(private_key);
                
                // Get public key for debugging
                let public_key_hex = hex::encode(keypair.public().as_bytes());
                debug!("Generated network Ed25519 key for {} at {} - public key: {}", 
                       config.node_config.node_public_key, bind_address, 
                       public_key_hex);
                
                // anemo expects the private key seed bytes directly
                seed
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
                let rpc_config = ConsensusRpcConfig {
                    addr: format!("127.0.0.1:{}", rpc_port).parse().unwrap(),
                    enable_admin: false,
                    enable_metrics: true,
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
            // Genesis block info for base fee calculation
            parent_block_info: Arc::new(Mutex::new(Some((134_217_728u64, 0u64, 875_000_000u64)))), // Genesis: (gas_limit, gas_used, base_fee)
            // Initialize with genesis timestamp (or a reasonable default)
            last_block_timestamp: 1752102000, // A timestamp before our first block  
            last_block_timestamp_ms: 1752102000 * 1000, // Same in milliseconds
            execution_outcomes: Vec::new(),
            block_executor: None,
            storage,
            mempool_bridge: None, // Will be set via set_mempool_operations
            peer_addresses: peer_addresses_clone,
            committee: committee_clone,
            node_public_key: node_public_key_clone,
            chain_spec: None,
            canonical_hash_tracker: Arc::new(CanonicalHashTracker::new()),
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
    
    /// Get the next finalized batch without converting to block
    pub async fn get_next_finalized_batch(&mut self) -> Result<Option<FinalizedBatch>> {
        if let Some(batch) = self.finalized_batch_receiver.recv().await {
            // Broadcast the batch to other listeners (e.g., mempool bridge)
            // We ignore the error if there are no receivers
            let _ = self.finalized_batch_broadcast.send(batch.clone());
            
            Ok(Some(batch))
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

    /// Calculate next base fee according to EIP-1559 using standard algorithm
    fn calculate_next_base_fee(&self, parent_gas_used: u64, parent_gas_limit: u64, parent_base_fee: u64, timestamp: u64) -> u64 {
        // Use the standard EIP-1559 calculation from alloy_eips
        if let Some(ref chain_spec) = self.chain_spec {
            // Get the base fee params for the given timestamp
            let base_fee_params = chain_spec.base_fee_params_at_timestamp(timestamp);
            
            debug!("Using chain spec base fee params for timestamp {}", timestamp);
            
            // Use the standard calculation that the engine also uses
            calc_next_block_base_fee(
                parent_gas_used,
                parent_gas_limit,
                parent_base_fee,
                base_fee_params,
            )
        } else {
            // Fallback to simple calculation if no chain spec available
            // This should not happen in production
            warn!("❌ No chain spec available for base fee calculation, using fallback");
            warn!("❌ This will likely cause base fee mismatches with the engine!");
            
            // Simple EIP-1559 calculation
            let target_gas_used = parent_gas_limit / 2;
            
            if parent_gas_used == target_gas_used {
                return parent_base_fee;
            }
            
            let gas_used_delta = if parent_gas_used > target_gas_used {
                parent_gas_used - target_gas_used
            } else {
                target_gas_used - parent_gas_used
            };
            
            let base_fee_delta = (parent_base_fee * gas_used_delta) / (target_gas_used * 8);
            
            if parent_gas_used > target_gas_used {
                parent_base_fee + base_fee_delta
            } else {
                parent_base_fee.saturating_sub(base_fee_delta)
            }
        }
    }
    
    /// Get parent block information for proper base fee calculation
    fn get_parent_block_info(&self, parent_hash: B256) -> Result<(u64, u64, u64)> {
        // For genesis block, return default values
        if parent_hash == B256::ZERO || parent_hash.to_string() == "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd" {
            return Ok((134_217_728u64, 0u64, 875_000_000u64)); // (gas_limit, gas_used, base_fee)
        }
        
        // TODO: Query parent block from provider when available
        // This would give us the actual parent data from the chain
        // For now, we rely on the cache
        
        // Fall back to cached parent block info if provider query failed
        if let Ok(guard) = self.parent_block_info.lock() {
            if let Some((gas_limit, gas_used, base_fee)) = *guard {
                debug!("Using cached parent block info: gas_limit={}, gas_used={}, base_fee={}", gas_limit, gas_used, base_fee);
                return Ok((gas_limit, gas_used, base_fee));
            }
        }
        
        // Last resort: use genesis defaults
        warn!("No parent block info available for {}, using genesis defaults", parent_hash);
        Ok((134_217_728u64, 0u64, 875_000_000u64))
    }

    /// Convert a finalized batch to a Reth block
    fn batch_to_block(&mut self, batch: FinalizedBatch) -> Result<SealedBlock> {
        // Check if the batch's parent matches our expected parent
        if batch.parent_hash != self.current_parent_hash && batch.block_number > 1 {
            warn!("⚠️ Parent hash mismatch! Batch #{} expects parent {} but consensus has {}", 
                  batch.block_number, batch.parent_hash, self.current_parent_hash);
        }
        
        // Use certificate digest as extra_data (exactly 32 bytes as required by Ethereum)
        // This provides consensus proof while meeting engine API validation requirements
        let extra_data = Bytes::from(batch.certificate_digest.to_vec());

        // Create empty receipts for transactions (simplified)
        let receipts: Vec<Receipt> = batch.transactions.iter().map(|_| Receipt {
            tx_type: reth_primitives::TxType::Legacy,
            success: true,
            cumulative_gas_used: 21000, // Basic gas cost
            logs: vec![],
        }).collect();

        // Get parent block information for proper base fee calculation
        let (parent_gas_limit, parent_gas_used, parent_base_fee) = self.get_parent_block_info(batch.parent_hash)?;
        
        // Calculate timestamp first (needed for base fee calculation)
        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        // Check if batch timestamp is already in milliseconds or seconds
        let batch_timestamp_ms = if batch.timestamp > 1_000_000_000_000 {
            // Already in milliseconds (timestamp > year 2001 in milliseconds)
            batch.timestamp
        } else {
            // In seconds, convert to milliseconds
            batch.timestamp * 1000
        };
        
        let block_timestamp_ms = if batch_timestamp_ms > self.last_block_timestamp_ms {
            // Batch timestamp is valid (in milliseconds)
            batch_timestamp_ms
        } else {
            // Batch timestamp is not greater than parent, increment by 1ms
            std::cmp::max(self.last_block_timestamp_ms + 1, current_time_ms)
        };
        
        // Use millisecond timestamp directly in the block header
        let block_timestamp = block_timestamp_ms;
        
        debug!("Block #{} timestamp: {}ms (batch: {}ms, parent: {}ms)", 
               batch.block_number, block_timestamp_ms, batch_timestamp_ms, self.last_block_timestamp_ms);
        
        // Calculate proper base fee based on parent block usage according to EIP-1559
        let base_fee = if batch.block_number == 1 {
            875_000_000u64 // Initial base fee for block #1 (genesis)
        } else {
            // Use the block timestamp in seconds for base fee params lookup
            let timestamp_seconds = block_timestamp / 1000;
            self.calculate_next_base_fee(parent_gas_used, parent_gas_limit, parent_base_fee, timestamp_seconds)
        };
        
        info!("📊 BASE FEE CALCULATION for block #{}: calculated={}, parent_hash={}", 
              batch.block_number, base_fee, batch.parent_hash);
        info!("📊 Parent block info: gas_used={}, gas_limit={}, base_fee={}", 
              parent_gas_used, parent_gas_limit, parent_base_fee);
        info!("📊 Cached parent info: {:?}", self.parent_block_info.lock().ok().and_then(|g| *g));
        info!("📊 Current consensus state: block_number={}, parent_hash={}", 
              self.current_block_number, self.current_parent_hash);
        
        if batch.block_number > 1 {
            let target_gas_used = parent_gas_limit / 2;
            let gas_used_delta = if parent_gas_used > target_gas_used {
                parent_gas_used - target_gas_used
            } else {
                target_gas_used - parent_gas_used
            };
            let base_fee_delta = (parent_base_fee * gas_used_delta) / (target_gas_used * 8);
            info!("📊 EIP-1559 calc details: target_gas={}, delta={}, base_fee_delta={}", 
                  target_gas_used, gas_used_delta, base_fee_delta);
        }

        // Create block header with properly calculated values
        let header = RethHeader {
            parent_hash: batch.parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            number: batch.block_number,
            gas_limit: parent_gas_limit,
            gas_used: receipts.len() as u64 * 21000, // Basic gas calculation
            timestamp: block_timestamp,
            difficulty: U256::ZERO, // No PoW in Narwhal/Bullshark
            nonce: Default::default(),
            // Mix hash should be deterministic - zero for BFT consensus
            mix_hash: B256::ZERO,
            beneficiary: Address::ZERO, // No coinbase in this consensus
            state_root: B256::ZERO, // Will be calculated during execution
            transactions_root: alloy_consensus::proofs::calculate_transaction_root(&batch.transactions),
            receipts_root: alloy_consensus::proofs::calculate_receipt_root(&receipts),
            logs_bloom: Bloom::ZERO, // No logs in simplified receipts
            extra_data,
            base_fee_per_gas: Some(base_fee),
            blob_gas_used: Some(0),  // No blob transactions in Narwhal/Bullshark
            excess_blob_gas: Some(0), // Required for EIP-4844, set to 0 for no blob transactions
            parent_beacon_block_root: Some(B256::ZERO), // Required for EIP-4788, using zero for consensus blocks
            withdrawals_root: Some(alloy_consensus::proofs::calculate_withdrawals_root(&[])), // Required for Shanghai, empty withdrawals
            requests_hash: Some(alloy_eips::eip7685::EMPTY_REQUESTS_HASH),
        };

        // Create block body
        let body = reth_primitives::BlockBody {
            transactions: batch.transactions,
            ommers: vec![], // No uncles in Narwhal/Bullshark
            withdrawals: Some(vec![].into()), // Required for Shanghai, empty withdrawals
        };

        // Create the block
        let block = Block::new(header, body);
        
        // Seal the block with proper hash calculation
        let sealed_block = SealedBlock::seal_slow(block);

        // Track the block hash for this validator
        // Determine if we are the leader for this round
        let validators: Vec<_> = self.committee.authorities.keys().cloned().collect();
        let leader_idx = (batch.round as usize) % validators.len();
        let is_leader = validators.get(leader_idx).map_or(false, |leader| leader == &self.node_public_key);
        
        // Convert our consensus public key to EVM address
        // For now, use a deterministic mapping
        let our_address = Address::from_slice(&self.node_public_key.as_bytes()[0..20]);
        
        // Record the hash
        if let Err(e) = self.canonical_hash_tracker.record_validator_hash(
            batch.block_number,
            batch.round,
            our_address,
            sealed_block.hash(),
            is_leader,
        ) {
            warn!("Failed to track block hash: {}", e);
        }
        
        info!(
            "Block #{} hash {} recorded for validator {} (leader: {})",
            batch.block_number,
            sealed_block.hash(),
            our_address,
            is_leader
        );

        // Update our state
        // batch.block_number is the block we just created
        // So the next block number is batch.block_number + 1
        self.current_block_number = batch.block_number + 1;
        self.current_parent_hash = sealed_block.hash();
        
        // DO NOT update parent block info here - it should only be updated after engine accepts the block!
        // The update happens in update_chain_state_with_block_info() after successful validation
        
        // Update last block timestamp (both seconds and milliseconds)
        self.last_block_timestamp = sealed_block.timestamp / 1000; // Convert ms to seconds for compatibility
        self.last_block_timestamp_ms = sealed_block.timestamp; // Store full millisecond precision
        
        debug!("Updated parent block info: gas_limit={}, gas_used={}, base_fee={}, timestamp={}ms ({}s)", 
               sealed_block.gas_limit, sealed_block.gas_used, sealed_block.base_fee_per_gas.unwrap_or(base_fee), 
               sealed_block.timestamp, sealed_block.timestamp / 1000);
        
        // NOTE: Do NOT update consensus service chain state here!
        // Chain state should only be updated AFTER successful engine API submission
        // This prevents the BFT service from getting ahead of the actual persisted state
        debug!("Block {} created locally (hash: {}), chain state will be updated after engine API submission", 
               batch.block_number, sealed_block.hash());

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
    pub fn set_mempool_operations(&mut self, mempool_ops: Box<dyn MempoolOperations>) -> Result<()> {
        // Store the mempool operations for later use
        self.mempool_bridge = Some(mempool_ops);
        
        info!("✅ Mempool operations configured successfully");
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
    pub fn with_rpc(&mut self, config: ConsensusRpcConfig) -> Result<()> {
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
    
    /// Update chain state after block persistence
    /// This ensures the BFT service knows about persisted blocks
    pub async fn update_chain_state(&self, block_number: u64, block_hash: B256) {
        if let Some(ref service) = self.service {
            service.update_chain_state(block_number, block_hash).await;
            info!("Updated consensus service chain state: block {} hash {}", block_number, block_hash);
        }
    }
    
    /// Update chain state with full block information for proper parent caching
    pub async fn update_chain_state_with_block_info(&self, block_number: u64, block_hash: B256, gas_limit: u64, gas_used: u64, base_fee: u64, timestamp: u64) {
        // Cache this block's info as the parent for the next block
        if let Ok(mut guard) = self.parent_block_info.lock() {
            *guard = Some((gas_limit, gas_used, base_fee));
        }
        
        info!("Updated parent block cache: gas_limit={}, gas_used={}, base_fee={}", gas_limit, gas_used, base_fee);
        
        // Update the consensus service chain state with timestamp
        if let Some(ref service) = self.service {
            service.update_chain_state_with_timestamp(block_number, block_hash, timestamp).await;
            info!("Updated consensus service chain state: block {} hash {} timestamp {}", block_number, block_hash, timestamp);
        }
    }
    
    /// Clear parent block cache when a reorganization occurs or blocks are rejected
    pub fn clear_parent_cache(&self) {
        if let Ok(mut guard) = self.parent_block_info.lock() {
            *guard = None;
        }
        warn!("Cleared parent block cache due to reorganization or rejected blocks");
    }
    
    /// Get canonical hash for a block number
    pub fn get_canonical_hash(&self, block_number: u64) -> Option<B256> {
        self.canonical_hash_tracker.get_canonical_hash(block_number)
    }
    
    /// Get consensus summary for recent blocks
    pub fn get_consensus_summary(&self, last_n_blocks: u64) -> Vec<(u64, bool, usize, usize)> {
        self.canonical_hash_tracker.get_consensus_summary(last_n_blocks)
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

/// Helper function to create test configuration
pub fn create_test_config(node_key: PublicKey, committee: Committee) -> ServiceConfig {
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    
    // Generate test keypair for this node
    let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
    let consensus_private_key_bytes = keypair.private().as_ref().to_vec();
    
    let node_config = ConsensusConfig {
        node_public_key: node_key,
        consensus_private_key_bytes,
        ..Default::default()
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
    
    let bind_address: std::net::SocketAddr = format!("127.0.0.1:{}", bind_port).parse().expect("Valid address");
    
    // Generate test keypair for this node
    let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
    let consensus_private_key_bytes = keypair.private().as_ref().to_vec();
    
    let node_config = ConsensusConfig {
        node_public_key: node_key,
        consensus_private_key_bytes,
        ..Default::default()
    };
    
    ServiceConfig::new(node_config, committee)
}