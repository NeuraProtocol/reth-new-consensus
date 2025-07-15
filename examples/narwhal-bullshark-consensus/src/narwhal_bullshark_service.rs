//! Narwhal + Bullshark consensus service integration layer
//! 
//! This module provides a thin coordination layer between:
//! - narwhal::DagService (DAG consensus)
//! - bullshark::BftService (BFT consensus)
//! - Reth's execution layer
//!
//! It does NOT reimplement consensus - it uses the existing crates.
//! This is the working implementation copied from the pre-move version.

use crate::{
    types::{FinalizedBatch, ConsensusConfig, ConsensusRpcConfig},
    validator_keys::ValidatorRegistry,
    chain_state::ChainStateTracker,
    consensus_storage::MdbxConsensusStorage,
    dag_storage_adapter::DagStorageAdapter,
    batch_storage_adapter::BatchStorageAdapter,
};
use anyhow::Result;
use reth_primitives::TransactionSigned as RethTransaction;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn, debug, error};
use std::sync::Arc;
use std::net::SocketAddr;
use alloy_rlp::Decodable;
use alloy_primitives::{B256, Address};

// Use the REAL implementations from the proper crates
use narwhal::{
    DagService, DagMessage, NetworkEvent,
    types::{Committee, PublicKey as NarwhalPublicKey},
    Transaction as NarwhalTransaction,
    config::NarwhalConfig,
    Worker, crypto::KeyPair,
    storage_trait::BatchStore,
    InMemoryBatchStore,
};
use bullshark::{
    BftService, BftConfig, FinalizedBatchInternal,
    storage::InMemoryConsensusStorage,
};
use fastcrypto::{
    traits::{KeyPair as _, EncodeDecodeBase64, ToFromBytes},
    bls12381::BLS12381KeyPair,
    SignatureService,
    Hash,
};

/// Thin coordination service that connects Narwhal DAG + Bullshark BFT
/// This is the working implementation that had functional consensus/DAG networking
pub struct NarwhalBullsharkService {
    /// Real Narwhal DAG service
    dag_service: Option<DagService>,
    /// Real Bullshark BFT service  
    bft_service: Option<JoinHandle<Result<(), bullshark::BullsharkError>>>,
    /// Service handles for cleanup
    task_handles: Vec<JoinHandle<()>>,
    /// Channels for communication
    transaction_receiver: Option<mpsc::UnboundedReceiver<RethTransaction>>,
    finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    committee_receiver: watch::Receiver<Committee>,
    /// Network events from NarwhalNetwork for real distributed consensus
    network_event_receiver: Option<tokio::sync::broadcast::Receiver<narwhal::NetworkEvent>>,
    /// Network reference for outbound broadcasting
    network_handle: Option<Arc<narwhal::NarwhalNetwork>>,
    /// Storage backend for consensus data
    storage: Option<Arc<MdbxConsensusStorage>>,
    /// Running state (shared with RPC)
    is_running: Arc<RwLock<bool>>,
    /// RPC server configuration
    rpc_config: Option<ConsensusRpcConfig>,
    /// RPC server handle (if running)
    rpc_server_handle: Option<jsonrpsee::server::ServerHandle>,
    /// Node configuration for RPC
    node_config: ConsensusConfig,
    /// Current committee for RPC
    current_committee: Arc<RwLock<Committee>>,
    /// Channel to send batch digests from workers to primary
    tx_primary: Option<mpsc::UnboundedSender<(narwhal::BatchDigest, narwhal::WorkerId)>>,
    /// Chain state tracker for parent hash and block number
    chain_state: ChainStateTracker,
    /// Chain state adapter for BFT service
    chain_state_adapter: Option<Arc<crate::chain_state_adapter::ChainStateAdapter>>,
}

impl std::fmt::Debug for NarwhalBullsharkService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use try_read to avoid blocking in async context
        let is_running = self.is_running.try_read()
            .map(|guard| *guard)
            .unwrap_or_else(|_| false);
            
        f.debug_struct("NarwhalBullsharkService")
            .field("is_running", &is_running)
            .field("has_dag_service", &self.dag_service.is_some())
            .field("has_bft_service", &self.bft_service.is_some())
            .finish()
    }
}

impl NarwhalBullsharkService {
    /// Create new service using REAL narwhal and bullshark implementations
    pub fn new(
        config: ConsensusConfig,
        committee: Committee,
        transaction_receiver: mpsc::UnboundedReceiver<RethTransaction>,
        finalized_batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
        committee_receiver: watch::Receiver<Committee>,
        storage: Option<Arc<MdbxConsensusStorage>>,
        network_event_receiver: Option<tokio::sync::broadcast::Receiver<narwhal::NetworkEvent>>,
        network_handle: Option<Arc<narwhal::NarwhalNetwork>>,
    ) -> Result<Self> {
        info!("Creating Narwhal + Bullshark consensus service");
        info!("Committee size: {}", committee.authorities.len());
        info!("🔍 Network handle provided: {}", network_handle.is_some());
        info!("🔍 Network event receiver provided: {}", network_event_receiver.is_some());

        let current_committee = Arc::new(RwLock::new(committee.clone()));
        
        Ok(Self {
            dag_service: None,
            bft_service: None,
            task_handles: Vec::new(),
            transaction_receiver: Some(transaction_receiver),
            finalized_batch_sender,
            committee_receiver,
            network_event_receiver,
            network_handle,
            storage,
            is_running: Arc::new(RwLock::new(false)),
            rpc_config: None,
            rpc_server_handle: None,
            node_config: config,
            current_committee,
            tx_primary: None,
            chain_state: ChainStateTracker::new(),
            chain_state_adapter: None,
        })
    }

    /// Start the consensus service using REAL implementations
    pub async fn spawn(&mut self) -> Result<()> {
        if *self.is_running.read().await {
            return Err(anyhow::anyhow!("Service already running"));
        }

        info!("🚀 Starting Narwhal + Bullshark consensus coordination");
        
        // Initialize metrics collection
        let metrics = narwhal::init_metrics(prometheus::default_registry());
        info!("📊 Initialized metrics collection with Prometheus");

        // Create channels for DAG ↔ BFT communication
        // Use bounded channel for certificates to prevent unbounded backlog
        // Very large buffer size to handle burst traffic when catching up
        let (certificate_sender, certificate_receiver) = mpsc::channel(50000); // Very large buffer for catching up
        let (dag_tx_sender, dag_tx_receiver) = mpsc::unbounded_channel();
        let (dag_network_sender, dag_network_receiver) = mpsc::unbounded_channel();
        let (dag_outbound_sender, dag_outbound_receiver) = mpsc::unbounded_channel();
        let (committee_sender, dag_committee_receiver) = watch::channel(self.committee_receiver.borrow().clone());
        
        // Create channel for worker batch digests to primary
        let (tx_primary_sender, mut rx_primary) = mpsc::unbounded_channel::<(narwhal::BatchDigest, narwhal::WorkerId)>();
        self.tx_primary = Some(tx_primary_sender);

        // Use the actual consensus keypair from node configuration
        let node_key = self.node_config.node_public_key.clone();
        
        // Reconstruct keypair from private key bytes
        use fastcrypto::traits::{KeyPair, ToFromBytes};
        use fastcrypto::bls12381::{BLS12381PrivateKey, BLS12381KeyPair};
        
        let private_key = BLS12381PrivateKey::from_bytes(&self.node_config.consensus_private_key_bytes)
            .expect("Valid private key bytes from config");
        let keypair = BLS12381KeyPair::from(private_key);
        
        // Verify the public key matches
        assert_eq!(keypair.public(), &node_key, "Consensus keypair public key mismatch");
        
        let signature_service = SignatureService::new(keypair);
        
        info!("Using node consensus key: {}", node_key.encode_base64());

        // Create storage adapter for DAG service
        // Using the real MDBX storage implementation
        let dag_storage: Arc<dyn narwhal::storage_trait::DagStorageInterface> = if let Some(storage) = &self.storage {
            info!("✅ MDBX storage provided for DAG service - using real database operations");
            Arc::new(DagStorageAdapter::new(storage.clone()))
        } else {
            info!("⚠️ No MDBX storage provided, using in-memory storage for DAG");
            Arc::new(narwhal::storage_inmemory::InMemoryDagStorage::new())
        };

        // Create REAL Narwhal DAG service with storage and network sender
        let committee_for_dag = self.committee_receiver.borrow().clone();
        info!("📋 Creating DAG service with committee: epoch {}, {} members", 
              committee_for_dag.epoch, committee_for_dag.authorities.len());
        
        // Log committee members for debugging (in consistent order)
        let sorted_authorities: Vec<_> = committee_for_dag.authorities.iter()
            .map(|(k, v)| (k.encode_base64(), k, v))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(encoded, k, v)| (encoded, k, v))
            .collect();
        let mut sorted_authorities = sorted_authorities;
        sorted_authorities.sort_by(|a, b| a.0.cmp(&b.0));
        
        for (i, (encoded, _pubkey, authority)) in sorted_authorities.iter().enumerate() {
            info!("  Committee member {}: {} (stake: {})", i, encoded, authority.stake);
        }
        
        let dag_service = DagService::with_network_sender(
            node_key.clone(),
            committee_for_dag,
            NarwhalConfig::default(),
            signature_service,
            dag_tx_receiver,
            dag_network_receiver,
            certificate_sender,
            dag_committee_receiver,
            dag_storage,
            dag_outbound_sender,
        );

        info!("✅ Created real Narwhal DAG service for node {}", node_key.encode_base64());
        
        // Store DAG service for later configuration with worker batch digests
        self.dag_service = Some(dag_service);

        // Create REAL Bullshark BFT service  
        let bft_config = BftConfig::default();
        let storage = Arc::new(InMemoryConsensusStorage::new());
        let (bft_output_sender, bft_output_receiver) = mpsc::unbounded_channel();

        info!("Creating BFT service with certificate receiver channel");
        let mut bft_service = BftService::with_storage(
            bft_config,
            self.committee_receiver.borrow().clone(),
            certificate_receiver,
            bft_output_sender,
            storage,
        );
        
        // Set node public key for leader determination
        bft_service.set_node_public_key(self.node_config.node_public_key.clone());
        info!("✅ BFT service configured with node public key for leader determination");
        
        // Create batch store for BFT service if we have storage
        if let Some(ref mdbx_storage) = self.storage {
            let batch_store = Arc::new(BatchStorageAdapter::new(mdbx_storage.clone()));
            bft_service.set_batch_store(batch_store);
            info!("✅ BFT service configured with MDBX batch store");
        } else {
            info!("⚠️ BFT service using dummy transactions (no batch store)");
        }
        
        // Set chain state provider with proper genesis hash
        let chain_state_adapter = Arc::new(crate::chain_state_adapter::ChainStateAdapter::new());
        
        // Initialize with genesis state (block 0, genesis hash)
        // This ensures the first block created will be block 1 with correct parent
        let genesis_hash = "0x514191893c03d851abdf3534c946dd3e8d0f71685629bbf46957f2a0b0067cbd"
            .parse::<B256>()
            .unwrap_or(B256::ZERO);
        chain_state_adapter.update(0, genesis_hash);
        
        bft_service.set_chain_state(chain_state_adapter.clone());
        info!("✅ BFT service configured with chain state provider (genesis: {})", genesis_hash);
        
        // Store the adapter so we can update it later
        self.chain_state_adapter = Some(chain_state_adapter);

        info!("✅ Created real Bullshark BFT service");

        // Spawn BFT service
        info!("Spawning BFT service task...");
        let bft_handle = bft_service.spawn();
        self.bft_service = Some(bft_handle);
        info!("✅ BFT service task spawned");

        // Spawn workers and transaction adapter
        let (worker_channels, worker_handles) = self.spawn_workers(&committee_sender).await?;
        for handle in worker_handles {
            self.task_handles.push(handle);
        }
        
        // Spawn transaction bridge that sends to workers via adapter
        let tx_bridge_handle = self.spawn_transaction_bridge_with_workers(worker_channels).await?;
        self.task_handles.push(tx_bridge_handle);
        
        // Connect the rx_primary channel to the DAG service for worker batch digest integration
        if let Some(mut dag_service) = self.dag_service.take() {
            dag_service = dag_service.with_batch_digest_receiver(rx_primary);
            
            // Set the chain state provider for canonical metadata creation
            if let Some(ref chain_state_adapter) = self.chain_state_adapter {
                dag_service.set_chain_state(chain_state_adapter.clone());
                info!("✅ DAG service configured with chain state provider");
            }
            
            // Now spawn the DAG service with the batch digest receiver connected
            let dag_handle = dag_service.spawn();
            // Convert Result<(), DagError> to () for consistency
            let dag_handle_wrapped = tokio::spawn(async move {
                if let Err(e) = dag_handle.await {
                    warn!("DAG service error: {:?}", e);
                }
            });
            self.task_handles.push(dag_handle_wrapped);
            
            info!("✅ DAG service spawned with worker batch digest integration");
        } else {
            warn!("❌ Failed to connect worker batch digest channel - DAG service not initialized");
            drop(rx_primary);
        }

        // Spawn finalized batch processor  
        let batch_processor_handle = self.spawn_batch_processor(bft_output_receiver).await?;
        self.task_handles.push(batch_processor_handle);

        // Spawn committee update processor
        let committee_handle = self.spawn_committee_updater(committee_sender).await?;
        self.task_handles.push(committee_handle);

        // Spawn network event bridge for REAL distributed consensus (INBOUND)
        if let Some(network_events) = self.network_event_receiver.take() {
            let network_bridge_handle = self.spawn_network_event_bridge(network_events, dag_network_sender).await?;
            self.task_handles.push(network_bridge_handle);
        }

        // ✅ FIX: Add OUTBOUND network bridge for broadcasting headers/votes
        let outbound_bridge_handle = self.spawn_outbound_network_bridge(dag_outbound_receiver).await?;
        self.task_handles.push(outbound_bridge_handle);

        *self.is_running.write().await = true;

        // Start RPC server if configured
        if let Err(e) = self.start_rpc_server().await {
            warn!("Failed to start RPC server: {}", e);
        }

        info!("🎉 Narwhal + Bullshark consensus active using REAL implementations");
        info!("🔄 DAG service processing transactions → BFT service providing finality");
        info!("🌐 OUTBOUND network bridge active for broadcasting headers/votes");

        Ok(())
    }

    /// Process finalized batches from Bullshark to Reth
    async fn spawn_batch_processor(
        &self,
        mut batch_receiver: mpsc::UnboundedReceiver<FinalizedBatchInternal>,
    ) -> Result<JoinHandle<()>> {
        let finalized_sender = self.finalized_batch_sender.clone();
        let mut committee_receiver = self.committee_receiver.clone();
        let node_public_key = self.node_config.node_public_key.clone();

        let handle = tokio::spawn(async move {
            info!("🏭 Batch processor active: Bullshark → Reth");

            while let Some(internal_batch) = batch_receiver.recv().await {
                info!("🎯 Batch processor received finalized batch: block #{} with {} transactions from round {}", 
                      internal_batch.block_number, internal_batch.transactions.len(), internal_batch.round);
                
                // Convert Bullshark batch to Reth format
                // Decode Narwhal transactions back to Reth transactions
                let mut reth_transactions = Vec::new();
                let mut decode_errors = 0;
                
                for narwhal_tx in &internal_batch.transactions {
                    // Decode RLP encoded transaction
                    let mut tx_bytes = narwhal_tx.as_bytes();
                    match RethTransaction::decode(&mut tx_bytes) {
                        Ok(tx) => {
                            reth_transactions.push(tx);
                        },
                        Err(e) => {
                            warn!("Failed to decode transaction: {:?}", e);
                            decode_errors += 1;
                        }
                    }
                }
                
                // Extract consensus information from the internal batch
                let consensus_round = internal_batch.round;
                
                // Get the first certificate's digest (most important for finalization)
                let certificate_digest = if let Some(cert) = internal_batch.certificates.first() {
                    // Convert Narwhal certificate digest to B256
                    use fastcrypto::Hash;
                    let digest = cert.digest();
                    let digest_bytes: [u8; 32] = digest.to_bytes();
                    B256::from(digest_bytes)
                } else {
                    // Fallback if no certificates
                    B256::ZERO
                };
                
                // Get current committee
                let committee = committee_receiver.borrow().clone();
                
                // Extract validator signatures from the certificates
                let mut validator_signatures = Vec::new();
                // For now, use placeholder signatures since we have aggregated signatures
                // In a real implementation, we would decompose the aggregate or store individual sigs
                for cert in &internal_batch.certificates {
                    // Get the aggregated signature bytes
                    use fastcrypto::traits::ToFromBytes;
                    let agg_sig_bytes = cert.aggregated_signature().as_bytes();
                    // For each signer in the certificate, add to validator signatures
                    // Note: This is a simplification - in reality we'd need the individual signatures
                    for (validator, _) in cert.signers(&committee) {
                        validator_signatures.push((validator, agg_sig_bytes.to_vec()));
                    }
                }
                
                // Determine if we are the leader for this round
                // IMPORTANT: Sort validators for deterministic ordering across all nodes
                let mut validators: Vec<_> = committee.authorities.keys().cloned().collect();
                validators.sort_by_key(|k| k.encode_base64());
                let leader_idx = (consensus_round as usize) % validators.len();
                let leader_pubkey = validators.get(leader_idx).cloned();
                let is_leader = leader_pubkey.as_ref() == Some(&node_public_key);
                
                debug!("Leader determination: round={}, leader_idx={}, leader={:?}, us={}, is_leader={}", 
                      consensus_round, leader_idx, 
                      leader_pubkey.as_ref().map(|k| k.encode_base64()), 
                      node_public_key.encode_base64(), is_leader);
                
                // Handle canonical metadata
                let canonical_metadata = if let Some(metadata_bytes) = &internal_batch.canonical_metadata_bytes {
                    // Try to parse the simple string format first
                    if let Ok(metadata_str) = std::str::from_utf8(metadata_bytes) {
                        // Parse simple format: "block:1,parent:0x...,timestamp:123,round:1,base_fee:875000000"
                        let mut block_number = 0u64;
                        let mut parent_hash = B256::ZERO;
                        let mut timestamp = 0u64;
                        let mut base_fee = 875_000_000u64;
                        
                        for part in metadata_str.split(',') {
                            if let Some((key, value)) = part.split_once(':') {
                                match key {
                                    "block" => block_number = value.parse().unwrap_or(0),
                                    "parent" => parent_hash = value.parse().unwrap_or(B256::ZERO),
                                    "timestamp" => timestamp = value.parse().unwrap_or(0),
                                    "base_fee" => base_fee = value.parse().unwrap_or(875_000_000),
                                    _ => {}
                                }
                            }
                        }
                        
                        info!("📋 Parsed simple canonical metadata for block {} from consensus", block_number);
                        
                        use crate::canonical_block_metadata::CanonicalBlockMetadata;
                        Some(CanonicalBlockMetadata::new(
                            block_number,
                            parent_hash,
                            timestamp,
                            alloy_primitives::U256::from(base_fee),
                            134_217_728, // Default gas limit
                            reth_transactions.iter().map(|tx| *tx.hash()).collect(),
                        ))
                    } else {
                        // Try binary decoding as fallback
                        use crate::canonical_block_metadata::CanonicalBlockMetadata;
                        match CanonicalBlockMetadata::decode(metadata_bytes) {
                            Ok(metadata) => {
                                info!("📋 Decoded binary canonical metadata for block {} from consensus", internal_batch.block_number);
                                Some(metadata)
                            }
                            Err(e) => {
                                error!("Failed to decode canonical metadata: {}", e);
                                None
                            }
                        }
                    }
                } else if is_leader {
                    // We're the leader but didn't receive metadata - this shouldn't happen in production
                    // as the leader should have injected it during consensus
                    warn!("Leader for round {} but no canonical metadata in batch - creating now", consensus_round);
                    use crate::canonical_block_metadata::{CanonicalBlockMetadata, CanonicalMetadataBuilder};
                    
                    let metadata = CanonicalMetadataBuilder::new(
                        internal_batch.block_number,
                        internal_batch.parent_hash,
                    )
                    .build(consensus_round, reth_transactions.iter().map(|tx| *tx.hash()).collect::<Vec<_>>());
                    
                    Some(metadata)
                } else {
                    warn!("Not leader and no canonical metadata provided - blocks will diverge!");
                    None
                };
                
                let finalized_batch = FinalizedBatch {
                    round: consensus_round,
                    block_number: internal_batch.block_number,
                    parent_hash: internal_batch.parent_hash,
                    transactions: reth_transactions,
                    timestamp: internal_batch.timestamp,
                    consensus_round,
                    certificate_digest,
                    proposer: Address::ZERO,  // TODO: Convert from Narwhal PublicKey to Address
                    validator_signatures,
                    canonical_metadata,
                };

                info!("✅ Finalized batch {} with {}/{} transactions (decode errors: {})",
                     finalized_batch.block_number,
                     finalized_batch.transactions.len(),
                     internal_batch.transactions.len(),
                     decode_errors);

                if finalized_sender.send(finalized_batch.clone()).is_err() {
                    warn!("Failed to send finalized batch to Reth - channel closed");
                    break;
                } else {
                    info!("✅ Successfully sent finalized batch #{} to Reth integration", 
                          finalized_batch.block_number);
                }
            }

            info!("🔚 Batch processor stopped");
        });

        Ok(handle)
    }

    /// Handle committee updates
    async fn spawn_committee_updater(
        &self,
        committee_sender: watch::Sender<Committee>,
    ) -> Result<JoinHandle<()>> {
        let mut committee_receiver = self.committee_receiver.clone();

        let handle = tokio::spawn(async move {
            info!("👥 Committee updater active");

            while committee_receiver.changed().await.is_ok() {
                let new_committee = committee_receiver.borrow().clone();
                info!("📢 Committee update: epoch {}, {} validators",
                     new_committee.epoch,
                     new_committee.authorities.len());

                if committee_sender.send(new_committee).is_err() {
                    warn!("Failed to send committee update - receiver dropped");
                    break;
                }
            }

            info!("🔚 Committee updater stopped");
        });

        Ok(handle)
    }

    /// Bridge network events to DAG service for REAL distributed consensus
    async fn spawn_network_event_bridge(
        &self,
        mut network_event_receiver: tokio::sync::broadcast::Receiver<NetworkEvent>,
        dag_network_sender: mpsc::UnboundedSender<DagMessage>,
    ) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            info!("🌐 Network event bridge active: converting NetworkEvent → DagMessage");

            while let Ok(network_event) = network_event_receiver.recv().await {
                let dag_message = match network_event {
                    NetworkEvent::HeaderReceived(header) => DagMessage::Header(header),
                    NetworkEvent::VoteReceived(vote) => DagMessage::Vote(vote),
                    NetworkEvent::CertificateReceived(certificate) => DagMessage::Certificate(certificate),
                    NetworkEvent::PeerConnected(peer) => {
                        debug!("Peer connected: {}", peer);
                        continue; // Don't forward peer events to DAG
                    }
                    NetworkEvent::PeerDisconnected(peer) => {
                        debug!("Peer disconnected: {}", peer);
                        continue; // Don't forward peer events to DAG
                    }
                };

                if dag_network_sender.send(dag_message).is_err() {
                    warn!("Failed to send network message to DAG - channel closed");
                    break;
                }
            }

            info!("🔚 Network event bridge stopped");
        });

        Ok(handle)
    }

    /// Bridge outbound messages from DAG service to network for broadcasting
    async fn spawn_outbound_network_bridge(
        &self,
        mut outbound_receiver: mpsc::UnboundedReceiver<DagMessage>,
    ) -> Result<JoinHandle<()>> {
        let network = self.network_handle.clone();
        let has_network = network.is_some();
        
        let handle = tokio::spawn(async move {
            info!("📡 OUTBOUND network bridge active: broadcasting DAG messages to peers");
            info!("🔍 Network handle available: {}", has_network);
            
            let mut message_count = 0;
            let mut header_count = 0;
            let mut vote_count = 0;
            let mut certificate_count = 0;
            let mut error_count = 0;
            let mut last_log_time = std::time::Instant::now();
            let mut last_message_time = std::time::Instant::now();
            let mut consecutive_timeouts = 0;
            
            loop {
                // Log stats every 30 seconds
                if last_log_time.elapsed() > std::time::Duration::from_secs(30) {
                    let time_since_last_message = last_message_time.elapsed();
                    info!("📊 Outbound bridge stats: {} total messages ({} headers, {} votes, {} certificates), {} errors",
                          message_count, header_count, vote_count, certificate_count, error_count);
                    info!("⏰ Time since last message: {:?}", time_since_last_message);
                    
                    // Warn if no messages for a while
                    if time_since_last_message > std::time::Duration::from_secs(120) {
                        warn!("⚠️ WARNING: No messages sent for {:?} - DAG may be stalled!", time_since_last_message);
                    }
                    
                    last_log_time = std::time::Instant::now();
                }
                
                // Use timeout to detect if channel is stuck
                match tokio::time::timeout(
                    std::time::Duration::from_secs(60),
                    outbound_receiver.recv()
                ).await {
                    Ok(Some(dag_message)) => {
                        message_count += 1;
                        last_message_time = std::time::Instant::now();
                        consecutive_timeouts = 0; // Reset timeout counter
                        
                        // Log every 100th message and all certificates
                        let should_log = message_count % 100 == 0 || matches!(dag_message, DagMessage::Certificate(_));
                        
                        if should_log {
                            info!("📨 Outbound bridge processing message #{}", message_count);
                        }
                        
                        if let Some(network) = &network {
                            let start_time = std::time::Instant::now();
                            
                            match dag_message {
                                DagMessage::Header(header) => {
                                    header_count += 1;
                                    let header_id = header.id;
                                    let round = header.round;
                                    
                                    info!("📤 Broadcasting header {} for round {}", header_id, round);
                                    
                                    match network.broadcast_header(header).await {
                                        Ok(()) => {
                                            let latency = start_time.elapsed();
                                            info!("✅ Header {} broadcast successful (latency: {:?})", header_id, latency);
                                        }
                                        Err(e) => {
                                            error_count += 1;
                                            error!("❌ Failed to broadcast header {} for round {}: {:?}", header_id, round, e);
                                            error!("🔍 Error details: {}", e);
                                            // Check if this is a network connectivity issue
                                            if format!("{:?}", e).contains("connection") || format!("{:?}", e).contains("timeout") {
                                                error!("🌐 Network connectivity issue detected - peers may be unreachable");
                                            }
                                            // Continue processing - don't let one failure stop the bridge
                                        }
                                    }
                                }
                                DagMessage::Vote(vote) => {
                                    vote_count += 1;
                                    let round = vote.round;
                                    let header_id = vote.id;
                                    
                                    if vote_count % 100 == 0 {
                                        info!("📤 Broadcasting vote #{} for round {}", vote_count, round);
                                    }
                                    
                                    match network.broadcast_vote(vote).await {
                                        Ok(()) => {
                                            if vote_count % 100 == 0 {
                                                let latency = start_time.elapsed();
                                                info!("✅ Vote broadcast successful (latency: {:?})", latency);
                                            }
                                        }
                                        Err(e) => {
                                            error_count += 1;
                                            error!("❌ Failed to broadcast vote for header {} round {}: {:?}", header_id, round, e);
                                        }
                                    }
                                }
                                DagMessage::Certificate(certificate) => {
                                    certificate_count += 1;
                                    let round = certificate.header.round;
                                    let origin = certificate.origin();
                                    let digest = certificate.digest();
                                    
                                    info!("📤 Broadcasting certificate {} from {} for round {}", digest, origin, round);
                                    
                                    match network.broadcast_certificate(certificate).await {
                                        Ok(()) => {
                                            let latency = start_time.elapsed();
                                            info!("✅ Certificate {} broadcast successful (latency: {:?})", digest, latency);
                                        }
                                        Err(e) => {
                                            error_count += 1;
                                            error!("❌ Failed to broadcast certificate {} for round {}: {:?}", digest, round, e);
                                        }
                                    }
                                }
                            }
                        } else {
                            warn!("⚠️ No network handle available for broadcasting - message #{} dropped", message_count);
                            error_count += 1;
                        }
                    }
                    Ok(None) => {
                        error!("❌ Outbound channel closed after {} messages - DAG service has stopped!", message_count);
                        break;
                    }
                    Err(_) => {
                        // Timeout - channel might be empty or stuck
                        consecutive_timeouts += 1;
                        let time_since_last = last_message_time.elapsed();
                        
                        warn!("⏱️ Outbound bridge timeout #{} - no messages for 60s (processed {} so far, last message {:?} ago)", 
                              consecutive_timeouts, message_count, time_since_last);
                        
                        // If we've had multiple consecutive timeouts, something might be wrong
                        if consecutive_timeouts >= 3 {
                            error!("🚨 CRITICAL: {} consecutive timeouts - channel may be permanently stuck!", consecutive_timeouts);
                            error!("🔍 Last successful message was {:?} ago", time_since_last);
                            
                            // Try to check if channel is still functional
                            // Since we can't directly check if closed, we'll just warn
                            warn!("📡 Channel appears stuck - DAG service may not be producing messages");
                            warn!("🔍 Consider checking if DAG service is blocked or has crashed");
                        }
                        
                        // Continue waiting - don't exit unless channel is closed
                    }
                }
            }
            
            error!("🔚 CRITICAL: Outbound network bridge STOPPED after {} messages ({} errors)!", message_count, error_count);
            error!("📊 Final stats: {} headers, {} votes, {} certificates", header_count, vote_count, certificate_count);
            error!("⚠️ This means NO MORE messages will be broadcast to peers - consensus will STALL!");
        });

        Ok(handle)
    }
    
    /// Spawn workers for transaction batching
    async fn spawn_workers(
        &self,
        committee_sender: &watch::Sender<Committee>,
    ) -> Result<(Vec<mpsc::UnboundedSender<NarwhalTransaction>>, Vec<JoinHandle<()>>)> {
        let committee = committee_sender.borrow().clone();
        
        // Find our authority info to get worker configuration
        let our_authority = committee.authority(&self.node_config.node_public_key)
            .ok_or_else(|| anyhow::anyhow!("Our node not in committee"))?;
        
        let num_workers = our_authority.workers.num_workers;
        
        info!("Spawning {} workers for transaction batching (base port: {})", 
            num_workers, our_authority.workers.base_port);
        
        let mut worker_tx_channels = Vec::new();
        let mut worker_handles = Vec::new();
        let mut worker_networks = Vec::new();
        
        for worker_id in 0..num_workers {
            // Get worker address from authority configuration
            let worker_address_str = our_authority.workers.get_worker_address(worker_id)
                .ok_or_else(|| anyhow::anyhow!("Invalid worker ID {}", worker_id))?;
            
            let worker_address = worker_address_str
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid worker address {}: {}", worker_address_str, e))?;
            
            // Derive worker keypair from primary key
            let worker_keypair = KeyPair::derive_worker_keypair(
                &self.node_config.node_public_key,
                worker_id
            );
            
            // Create batch store for worker
            let batch_store: Arc<dyn BatchStore> = if let Some(ref storage) = self.storage {
                // Use MDBX batch store (wrapped as Arc)
                Arc::new(BatchStorageAdapter::new(storage.clone()))
            } else {
                // Use in-memory batch store
                Arc::new(InMemoryBatchStore::new())
            };
            
            // Create worker with batch store
            let worker = Worker::with_store(
                self.node_config.node_public_key.clone(),
                worker_id,
                committee.clone(),
                NarwhalConfig::default(),
                worker_keypair,
                worker_address,
                batch_store,
            );
            
            // Spawn worker and get channels
            let (channels, handles, network) = worker.create_and_spawn();
            
            // Store worker network for later registration
            worker_networks.push(network.clone());
            
            // Store transaction sender channel
            worker_tx_channels.push(channels.tx_transaction);
            
            // Connect worker batch digests to the primary
            if let Some(tx_primary) = &self.tx_primary {
                let tx_primary_clone = tx_primary.clone();
                let mut rx_batch_digest = channels.rx_batch_digest;
                
                // Spawn task to forward batch digests from worker to primary
                let forward_handle = tokio::spawn(async move {
                    while let Some((digest, worker_id)) = rx_batch_digest.recv().await {
                        info!("Worker {} forwarding batch digest to primary: {:?}", worker_id, digest);
                        if let Err(e) = tx_primary_clone.send((digest, worker_id)) {
                            warn!("Failed to forward batch digest to primary: {}", e);
                            break;
                        }
                    }
                    debug!("Batch digest forwarder for worker {} stopped", worker_id);
                });
                worker_handles.push(forward_handle);
            }
            
            // Convert DagResult handles to regular handles
            for handle in handles {
                let wrapped_handle = tokio::spawn(async move {
                    if let Err(e) = handle.await {
                        warn!("Worker task error: {:?}", e);
                    }
                });
                worker_handles.push(wrapped_handle);
            }
            
            info!("✅ Spawned worker {} on {} with network active", worker_id, worker_address);
        }
        
        // Now register all workers with each other
        info!("Registering workers with each other for batch replication...");
        for (authority_name, authority_info) in &committee.authorities {
            for worker_id in 0..authority_info.workers.num_workers {
                if let Some(worker_addr_str) = authority_info.workers.get_worker_address(worker_id) {
                    if let Ok(worker_addr) = worker_addr_str.parse::<SocketAddr>() {
                        let worker_info = narwhal::worker_network::WorkerInfo {
                            primary: authority_name.clone(),
                            worker_id,
                            worker_address: worker_addr,
                        };
                        
                        // Register this worker with all our workers
                        for network in &worker_networks {
                            if let Err(e) = network.register_worker(worker_info.clone()).await {
                                warn!("Failed to register worker {} of {} with our workers: {:?}", 
                                    worker_id, authority_name, e);
                            }
                        }
                    }
                }
            }
        }
        info!("✅ Worker registration complete");
        
        Ok((worker_tx_channels, worker_handles))
    }
    
    /// Spawn transaction bridge with worker adapter
    async fn spawn_transaction_bridge_with_workers(
        &mut self,
        worker_channels: Vec<mpsc::UnboundedSender<NarwhalTransaction>>,
    ) -> Result<JoinHandle<()>> {
        let mut transaction_receiver = self.transaction_receiver
            .take()
            .ok_or_else(|| anyhow::anyhow!("Transaction receiver already taken"))?;
        
        // Create transaction adapter
        let (adapter, tx_to_adapter) = crate::transaction_adapter::TransactionAdapterBuilder::new()
            .add_workers(worker_channels)
            .build();
        
        // Adapter is ready to use (no spawn needed)
        
        // Spawn bridge that converts and forwards transactions
        let handle = tokio::spawn(async move {
            info!("🌉 Transaction bridge active: Reth → Workers");
            
            while let Some(reth_tx) = transaction_receiver.recv().await {
                // Convert to RLP bytes
                let tx_bytes = alloy_rlp::encode(&reth_tx);
                
                info!("🔄 Bridging transaction: {} -> {} bytes", 
                    reth_tx.hash(), tx_bytes.len());
                
                if let Err(e) = adapter.process_transaction(reth_tx) {
                    warn!("Failed to process transaction through adapter: {}", e);
                    break;
                }
            }
            
            info!("🔚 Transaction bridge stopped");
        });
        
        Ok(handle)
    }

    /// Stop the service
    pub async fn stop(&mut self) -> Result<()> {
        if !*self.is_running.read().await {
            return Ok(());
        }

        info!("🛑 Stopping Narwhal + Bullshark consensus service");

        // Stop BFT service
        if let Some(bft_handle) = self.bft_service.take() {
            bft_handle.abort();
        }

        // Stop all other tasks
        for handle in self.task_handles.drain(..) {
            handle.abort();
        }

        // Stop RPC server
        self.stop_rpc_server();

        *self.is_running.write().await = false;
        info!("✅ Consensus service stopped");

        Ok(())
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        // Use try_read to avoid blocking in async context
        self.is_running.try_read()
            .map(|guard| *guard)
            .unwrap_or(false)
    }

    /// Configure RPC server (must be called before spawn)
    pub fn with_rpc(mut self, config: ConsensusRpcConfig) -> Self {
        self.rpc_config = Some(config);
        self
    }

    /// Start the RPC server if configured
    async fn start_rpc_server(&mut self) -> Result<()> {
        if let Some(rpc_config) = &self.rpc_config {
            info!("Starting consensus RPC server on {}", rpc_config.addr);
            
            // Create validator registry if we have storage
            let validator_registry = if let Some(ref _storage) = self.storage {
                // Create a new empty validator registry
                let registry = ValidatorRegistry::new();
                Some(Arc::new(RwLock::new(registry)))
            } else {
                None
            };
            
            // For now, just log that RPC would be started
            info!("📡 RPC endpoints would be available at http://{}/", rpc_config.addr);
        }

        Ok(())
    }

    /// Stop the RPC server if running
    fn stop_rpc_server(&mut self) {
        if let Some(handle) = self.rpc_server_handle.take() {
            handle.stop().ok();
            info!("Consensus RPC server stopped");
        }
    }
    
    /// Get the chain state tracker
    pub fn chain_state(&self) -> ChainStateTracker {
        self.chain_state.clone()
    }
    
    /// Update chain state from external source
    pub async fn update_chain_state(&self, block_number: u64, parent_hash: B256) {
        self.chain_state.update_tip(block_number, parent_hash).await;
        
        // Also update the chain state adapter if it exists
        if let Some(ref adapter) = self.chain_state_adapter {
            adapter.update(block_number, parent_hash);
        }
    }
    
    /// Update chain state with timestamp from external source
    pub async fn update_chain_state_with_timestamp(&self, block_number: u64, parent_hash: B256, timestamp: u64) {
        // Update the full chain state including timestamp
        let mut state = self.chain_state.get_state().await;
        state.parent_hash = state.block_hash;
        state.block_number = block_number;
        state.block_hash = parent_hash;
        state.timestamp = timestamp;
        self.chain_state.update_state(state).await;
        
        // Also update the chain state adapter with timestamp
        if let Some(ref adapter) = self.chain_state_adapter {
            adapter.update_with_timestamp(block_number, parent_hash, timestamp);
        }
    }
    
    /// Update chain state synchronously (for non-async contexts)
    pub fn update_chain_state_sync(&self, block_number: u64, parent_hash: B256) {
        let chain_state = self.chain_state.clone();
        tokio::spawn(async move {
            // Use update_tip instead of update to ensure blocking write
            // This prevents silent failures when state is locked
            chain_state.update_tip(block_number, parent_hash).await;
        });
    }
}

impl Drop for NarwhalBullsharkService {
    fn drop(&mut self) {
        // Use try_read to avoid blocking in async context
        let is_running = self.is_running.try_read()
            .map(|guard| *guard)
            .unwrap_or(false);
            
        if is_running {
            // Best effort cleanup
            if let Some(bft_handle) = self.bft_service.take() {
                bft_handle.abort();
            }
            for handle in self.task_handles.drain(..) {
                handle.abort();
            }
            self.stop_rpc_server();
        }
    }
}