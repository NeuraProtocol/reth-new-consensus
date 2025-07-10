//! Integration bridge between Narwhal/Bullshark consensus and Reth execution pipeline
//! This is the working implementation copied from the pre-move version

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    validator_keys::ValidatorKeyPair,
    chain_state::ChainStateTracker,
};
use narwhal::{types::{Committee, PublicKey as NarwhalPublicKey}, NarwhalNetwork};
use reth_primitives::{
    TransactionSigned as RethTransaction, SealedBlock,
};
use alloy_primitives::{B256, Address};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn, debug, error};
use anyhow::Result;
use fastcrypto::traits::{ToFromBytes, EncodeDecodeBase64};

/// Configuration for Reth integration
#[derive(Debug, Clone)]
pub struct RethIntegrationConfig {
    /// Network address to bind to
    pub network_address: std::net::SocketAddr,
    /// Enable networking (false for testing)
    pub enable_networking: bool,
    /// Maximum pending transactions
    pub max_pending_transactions: usize,
    /// Execution timeout
    pub execution_timeout: std::time::Duration,
    /// Enable metrics
    pub enable_metrics: bool,
    /// Peer addresses for connections
    pub peer_addresses: Vec<std::net::SocketAddr>,
}

/// Bridge between Narwhal/Bullshark consensus and Reth blockchain execution
/// This is the working implementation that handles multi-validator networking
pub struct NarwhalRethBridge {
    /// Current block number
    current_block_number: u64,
    /// Current parent hash
    current_parent_hash: B256,
    /// Peer addresses for network connections
    peer_addresses: Vec<std::net::SocketAddr>,
    /// Committee configuration for peer mapping
    committee: Committee,
    /// Our node's public key
    node_public_key: NarwhalPublicKey,
    /// Chain state tracker
    chain_state: Arc<ChainStateTracker>,
    /// Network instance for peer connections
    network: Option<NarwhalNetwork>,
    /// Network event receiver
    network_event_receiver: Option<tokio::sync::broadcast::Receiver<narwhal::NetworkEvent>>,
}

impl std::fmt::Debug for NarwhalRethBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalRethBridge")
            .field("current_block_number", &self.current_block_number)
            .field("current_parent_hash", &self.current_parent_hash)
            .field("peer_addresses", &self.peer_addresses)
            .field("committee_size", &self.committee.authorities.len())
            .field("has_network", &self.network.is_some())
            .finish_non_exhaustive()
    }
}

impl NarwhalRethBridge {
    /// Create a new Narwhal-Reth bridge with proper networking
    pub fn new(
        config: ConsensusConfig,
        committee: Committee,
        node_public_key: NarwhalPublicKey,
        peer_addresses: Vec<std::net::SocketAddr>,
    ) -> Result<Self> {
        let chain_state = Arc::new(ChainStateTracker::new());
        
        // Initialize with genesis state
        let genesis_hash = "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
            .parse::<B256>()
            .unwrap_or(B256::ZERO);
        chain_state.update(0, genesis_hash);
        
        Ok(Self {
            current_block_number: 1,
            current_parent_hash: genesis_hash,
            peer_addresses,
            committee,
            node_public_key,
            chain_state,
            network: None,
            network_event_receiver: None,
        })
    }

    /// Create a new bridge with network configuration
    pub fn new_with_network_config(
        config: ConsensusConfig,
        committee: Committee,
        node_public_key: NarwhalPublicKey,
        network_config: RethIntegrationConfig,
    ) -> Result<Self> {
        let chain_state = Arc::new(ChainStateTracker::new());
        
        // Initialize with genesis state
        let genesis_hash = "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
            .parse::<B256>()
            .unwrap_or(B256::ZERO);
        chain_state.update(0, genesis_hash);
        
        // Create networking if enabled
        let (network, network_event_receiver) = if network_config.enable_networking {
            let bind_address = network_config.network_address;
            info!("Starting Narwhal network on {}", bind_address);
            
            // Generate unique network private key from validator consensus key
            let network_private_key = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                
                let consensus_key_bytes = node_public_key.as_bytes();
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
                node_public_key.clone(),
                committee.clone(),
                bind_address,
                network_private_key,
                narwhal::config::NarwhalConfig::default(),
            )?;
            
            (Some(network), Some(network_events))
        } else {
            info!("Networking disabled for testing");
            (None, None)
        };
        
        Ok(Self {
            current_block_number: 1,
            current_parent_hash: genesis_hash,
            peer_addresses: network_config.peer_addresses,
            committee,
            node_public_key,
            chain_state,
            network,
            network_event_receiver,
        })
    }

    /// Start the consensus service with proper peer connections
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting Narwhal+Bullshark consensus with {} peers", self.peer_addresses.len());
        
        // If we have a network, connect to peers
        if let Some(ref mut network) = self.network {
            // Map peer addresses to committee members
            let mut peer_address_map = std::collections::HashMap::new();
            
            // Get all validators sorted by their public key for consistent ordering
            let mut all_validators: Vec<_> = self.committee.authorities.keys().cloned().collect();
            all_validators.sort_by(|a, b| a.encode_base64().cmp(&b.encode_base64()));
            
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
        
        info!("✅ Peer connection phase completed");
        
        Ok(())
    }
    
    /// Update chain state
    pub fn update_chain_state(&self, block_number: u64, parent_hash: B256) {
        self.chain_state.update(block_number, parent_hash);
    }
    
    /// Get network statistics
    pub fn network_stats(&self) -> Option<(usize, usize)> {
        self.network.as_ref().map(|n| {
            let stats = n.stats();
            (stats.connected_peers, stats.committee_size)
        })
    }
    
    /// Get the next block from consensus (placeholder implementation)
    /// TODO: This should actually interface with the consensus service to get finalized blocks
    pub async fn get_next_block(&mut self) -> Result<Option<reth_primitives::SealedBlock>> {
        // This is a placeholder - the real implementation would receive blocks
        // from the Narwhal+Bullshark consensus service
        warn!("get_next_block called on NarwhalRethBridge - this is a placeholder");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(None)
    }
}