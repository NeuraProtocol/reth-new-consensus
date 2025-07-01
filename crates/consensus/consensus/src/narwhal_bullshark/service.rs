//! Narwhal + Bullshark consensus service integration layer
//! 
//! This module provides a thin coordination layer between:
//! - narwhal::DagService (DAG consensus)
//! - bullshark::BftService (BFT consensus)
//! - Reth's execution layer
//!
//! It does NOT reimplement consensus - it uses the existing crates.

use crate::{
    narwhal_bullshark::{FinalizedBatch, NarwhalBullsharkConfig, ConsensusRpcConfig},
    consensus_storage::MdbxConsensusStorage,
    rpc::{ConsensusRpcImpl, ConsensusAdminRpcImpl, ConsensusApiServer, ConsensusAdminApiServer},
};
#[allow(unused_imports)]
use crate::narwhal_bullshark::dag_storage_adapter::MdbxDagStorageAdapter;
use crate::narwhal_bullshark::consensus_db_ops_impl::create_mdbx_dag_storage;
use anyhow::Result;
use reth_primitives::TransactionSigned as RethTransaction;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, debug};
use std::sync::Arc;
use std::net::SocketAddr;
use alloy_rlp::Decodable;
use tokio::sync::RwLock;

// Use the REAL implementations from the proper crates
use narwhal::{
    DagService, DagMessage, NetworkEvent,
    types::{Committee, PublicKey},
    Transaction as NarwhalTransaction,
    NarwhalConfig,
};
use bullshark::{
    BftService, BftConfig, FinalizedBatchInternal,
    storage::InMemoryConsensusStorage,
};
use fastcrypto::{
    traits::{KeyPair as _, EncodeDecodeBase64, ToFromBytes},
    bls12381::BLS12381KeyPair,
    SignatureService,
};

/// Thin coordination service that connects Narwhal DAG + Bullshark BFT
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
    node_config: NarwhalBullsharkConfig,
    /// Current committee for RPC
    current_committee: Arc<RwLock<Committee>>,
}

impl std::fmt::Debug for NarwhalBullsharkService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalBullsharkService")
            .field("is_running", &*self.is_running.blocking_read())
            .field("has_dag_service", &self.dag_service.is_some())
            .field("has_bft_service", &self.bft_service.is_some())
            .finish()
    }
}

impl NarwhalBullsharkService {
    /// Create new service using REAL narwhal and bullshark implementations
    pub fn new(
        config: NarwhalBullsharkConfig,
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
        })
    }

    /// Start the consensus service using REAL implementations
    pub async fn spawn(&mut self) -> Result<()> {
        if *self.is_running.read().await {
            return Err(anyhow::anyhow!("Service already running"));
        }

        info!("🚀 Starting Narwhal + Bullshark consensus coordination");

        // Create channels for DAG ↔ BFT communication
        let (certificate_sender, certificate_receiver) = mpsc::unbounded_channel();
        let (dag_tx_sender, dag_tx_receiver) = mpsc::unbounded_channel();
        let (dag_network_sender, dag_network_receiver) = mpsc::unbounded_channel();
        let (dag_outbound_sender, dag_outbound_receiver) = mpsc::unbounded_channel();
        let (committee_sender, dag_committee_receiver) = watch::channel(self.committee_receiver.borrow().clone());

        // Generate keypair for consensus (in production: load from secure storage)
        let keypair = BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        let node_key = keypair.public().clone();
        let signature_service = SignatureService::new(keypair);

        // Create storage adapter for DAG service
        // Using the real MDBX storage implementation
        let dag_storage = if let Some(storage) = &self.storage {
            info!("✅ MDBX storage provided for DAG service - using real database operations");
            create_mdbx_dag_storage(storage.clone())
        } else {
            info!("⚠️ No MDBX storage provided, using in-memory storage for DAG");
            narwhal::storage_inmemory::InMemoryDagStorage::new_ref()
        };

        // Create REAL Narwhal DAG service with storage and network sender
        let dag_service = DagService::with_network_sender(
            node_key.clone(),
            self.committee_receiver.borrow().clone(),
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

        // Spawn DAG service
        let dag_handle = dag_service.spawn();
        // Convert Result<(), DagError> to () for consistency
        let dag_handle_wrapped = tokio::spawn(async move {
            if let Err(e) = dag_handle.await {
                warn!("DAG service error: {:?}", e);
            }
        });
        self.task_handles.push(dag_handle_wrapped);

        // Create REAL Bullshark BFT service  
        let bft_config = BftConfig::default();
        let storage = Arc::new(InMemoryConsensusStorage::new());
        let (bft_output_sender, bft_output_receiver) = mpsc::unbounded_channel();

        let bft_service = BftService::with_storage(
            bft_config,
            self.committee_receiver.borrow().clone(),
            certificate_receiver,
            bft_output_sender,
            storage,
        );

        info!("✅ Created real Bullshark BFT service");

        // Spawn BFT service
        let bft_handle = bft_service.spawn();
        self.bft_service = Some(bft_handle);

        // Spawn transaction bridge
        let tx_bridge_handle = self.spawn_transaction_bridge(dag_tx_sender).await?;
        self.task_handles.push(tx_bridge_handle);

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

    /// Bridge transactions from Reth to Narwhal
    async fn spawn_transaction_bridge(
        &mut self,
        tx_sender: mpsc::UnboundedSender<NarwhalTransaction>,
    ) -> Result<JoinHandle<()>> {
        let mut transaction_receiver = self.transaction_receiver
            .take()
            .ok_or_else(|| anyhow::anyhow!("Transaction receiver already taken"))?;

        let handle = tokio::spawn(async move {
            info!("🌉 Transaction bridge active: Reth → Narwhal");

            while let Some(reth_tx) = transaction_receiver.recv().await {
                // Convert Reth transaction to Narwhal transaction
                // Use RLP encoding for consensus
                let tx_bytes = alloy_rlp::encode(&reth_tx);
                let narwhal_tx = NarwhalTransaction::from_bytes(tx_bytes);

                debug!("🔄 Bridging transaction: {} -> {} bytes", 
                    reth_tx.hash(), narwhal_tx.as_bytes().len());

                if tx_sender.send(narwhal_tx).is_err() {
                    warn!("Failed to send transaction to Narwhal - channel closed");
                    break;
                }
            }

            info!("🔚 Transaction bridge stopped");
        });

        Ok(handle)
    }

    /// Process finalized batches from Bullshark to Reth
    async fn spawn_batch_processor(
        &self,
        mut batch_receiver: mpsc::UnboundedReceiver<FinalizedBatchInternal>,
    ) -> Result<JoinHandle<()>> {
        let finalized_sender = self.finalized_batch_sender.clone();
        let mut committee_receiver = self.committee_receiver.clone();

        let handle = tokio::spawn(async move {
            info!("🏭 Batch processor active: Bullshark → Reth");

            while let Some(internal_batch) = batch_receiver.recv().await {
                // Convert Bullshark batch to Reth format
                // Decode Narwhal transactions back to Reth transactions
                let mut reth_transactions = Vec::new();
                let mut decode_errors = 0;
                
                for narwhal_tx in &internal_batch.transactions {
                    // Decode RLP encoded transaction
                    let mut tx_bytes = narwhal_tx.as_bytes();
                    match RethTransaction::decode(&mut tx_bytes) {
                        Ok(tx) => reth_transactions.push(tx),
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
                    alloy_primitives::B256::from(digest_bytes)
                } else {
                    // Fallback if no certificates
                    alloy_primitives::B256::ZERO
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
                
                let finalized_batch = FinalizedBatch {
                    block_number: internal_batch.block_number,
                    parent_hash: internal_batch.parent_hash,
                    transactions: reth_transactions,
                    timestamp: internal_batch.timestamp,
                    consensus_round,
                    certificate_digest,
                    validator_signatures,
                };

                info!("✅ Finalized batch {} with {}/{} transactions (decode errors: {})",
                     finalized_batch.block_number,
                     finalized_batch.transactions.len(),
                     internal_batch.transactions.len(),
                     decode_errors);

                if finalized_sender.send(finalized_batch).is_err() {
                    warn!("Failed to send finalized batch to Reth - channel closed");
                    break;
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

            while let Some(dag_message) = outbound_receiver.recv().await {
                if let Some(network) = &network {
                    match dag_message {
                        DagMessage::Header(header) => {
                            info!("📤 Broadcasting header {} for round {}", header.id, header.round);
                            if let Err(e) = network.broadcast_header(header).await {
                                warn!("Failed to broadcast header: {:?}", e);
                            }
                        }
                        DagMessage::Vote(vote) => {
                            info!("📤 Broadcasting vote for round {}", vote.round);
                            if let Err(e) = network.broadcast_vote(vote).await {
                                warn!("Failed to broadcast vote: {:?}", e);
                            }
                        }
                        DagMessage::Certificate(certificate) => {
                            info!("📤 Broadcasting certificate for round {}", certificate.header.round);
                            if let Err(e) = network.broadcast_certificate(certificate).await {
                                warn!("Failed to broadcast certificate: {:?}", e);
                            }
                        }
                    }
                } else {
                    // ✅ FIX: Don't break - just skip broadcasting and continue processing
                    debug!("No network handle available for broadcasting, skipping message");
                    // Continue to next message instead of breaking
                }
            }

            info!("🔚 Outbound network bridge stopped");
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
        *self.is_running.blocking_read()
    }

    /// Configure RPC server (must be called before spawn)
    pub fn with_rpc(mut self, config: ConsensusRpcConfig) -> Self {
        self.rpc_config = Some(config);
        self
    }

    /// Start the RPC server if configured
    async fn start_rpc_server(&mut self) -> Result<()> {
        if let Some(rpc_config) = &self.rpc_config {
            info!("Starting consensus RPC server on {}:{}", rpc_config.host, rpc_config.port);
            
            // Create validator registry if we have storage
            let validator_registry = if let Some(ref _storage) = self.storage {
                // Create a new empty validator registry
                let registry = crate::narwhal_bullshark::validator_keys::ValidatorRegistry::new();
                Some(Arc::new(RwLock::new(registry)))
            } else {
                None
            };
            
            // Start the standalone RPC server
            match super::service_rpc::start_service_rpc_server(
                rpc_config.clone(),
                self.node_config.clone(),
                self.current_committee.clone(),
                validator_registry,
                self.storage.clone(),
                self.is_running.clone(),
            ).await {
                Ok(handle) => {
                    self.rpc_server_handle = Some(handle);
                    info!("✅ Consensus RPC server started successfully");
                    info!("📡 RPC endpoints available at http://{}:{}/", rpc_config.host, rpc_config.port);
                }
                Err(e) => {
                    warn!("Failed to start consensus RPC server: {}", e);
                    return Err(e);
                }
            }
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
}

impl Drop for NarwhalBullsharkService {
    fn drop(&mut self) {
        if *self.is_running.blocking_read() {
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