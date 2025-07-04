//! Worker node implementation for Narwhal
//! 
//! The worker is responsible for:
//! - Receiving transactions from clients
//! - Creating batches of transactions
//! - Replicating batches to other workers
//! - Notifying the primary of batch digests

use crate::{
    NarwhalConfig, WorkerId, types::*, Transaction, Batch, BatchDigest,
    batch_maker::{BatchMaker, BatchMakerConfig},
    quorum_waiter::{QuorumWaiter, BatchAck},
    worker_handlers::{WorkerReceiverHandler, PrimaryReceiverHandler},
    worker_network::{WorkerNetwork, WorkerInfo},
    batch_store::{InMemoryBatchStore, MdbxBatchStore},
    storage_trait::BatchStore,
    DagError, DagResult,
};
use anemo::{Network, PeerId};
use anemo_tower::{callback::CallbackLayer, trace::TraceLayer};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{info, debug, warn};
use std::{sync::Arc, time::Duration, net::{SocketAddr, Ipv4Addr}};
use tower::ServiceBuilder;

/// Channels for worker components
pub struct WorkerChannels {
    /// Send transactions to the worker
    pub tx_transaction: mpsc::UnboundedSender<Transaction>,
    /// Receive batch digests from the worker
    pub rx_batch_digest: mpsc::UnboundedReceiver<(BatchDigest, WorkerId)>,
    /// Send batch acknowledgments to the worker
    pub tx_batch_ack: mpsc::UnboundedSender<BatchAck>,
    /// Committee updates
    pub tx_reconfigure: watch::Sender<Committee>,
}

impl WorkerChannels {
    /// Create new worker channels
    pub fn new(committee: Committee) -> (Self, WorkerChannelReceivers) {
        let (tx_transaction, rx_transaction) = mpsc::unbounded_channel();
        let (tx_batch, rx_batch) = mpsc::unbounded_channel();
        let (tx_batch_digest, rx_batch_digest) = mpsc::unbounded_channel();
        let (tx_batch_ack, rx_batch_ack) = mpsc::unbounded_channel();
        let (tx_reconfigure, rx_reconfigure) = watch::channel(committee);
        
        let channels = Self {
            tx_transaction,
            rx_batch_digest,
            tx_batch_ack,
            tx_reconfigure,
        };
        
        let receivers = WorkerChannelReceivers {
            rx_transaction,
            tx_batch,
            rx_batch,
            tx_batch_digest,
            rx_batch_ack,
            rx_reconfigure,
        };
        
        (channels, receivers)
    }
}

/// Internal channel receivers for worker components
pub struct WorkerChannelReceivers {
    pub rx_transaction: mpsc::UnboundedReceiver<Transaction>,
    pub tx_batch: mpsc::UnboundedSender<Batch>,
    pub rx_batch: mpsc::UnboundedReceiver<Batch>,
    pub tx_batch_digest: mpsc::UnboundedSender<(BatchDigest, WorkerId)>,
    pub rx_batch_ack: mpsc::UnboundedReceiver<BatchAck>,
    pub rx_reconfigure: watch::Receiver<Committee>,
}

/// A worker node in the Narwhal network
pub struct Worker {
    /// Primary's public key this worker belongs to
    pub primary_name: PublicKey,
    /// Worker ID
    pub id: WorkerId,
    /// Committee information
    pub committee: Committee,
    /// Configuration
    pub config: NarwhalConfig,
    /// Batch maker configuration
    pub batch_config: BatchMakerConfig,
    /// Worker key pair for network identity
    pub keypair: crate::crypto::KeyPair,
    /// Worker address for incoming connections
    pub worker_address: SocketAddr,
    /// Batch storage
    pub store: Arc<dyn BatchStore>,
}

impl Worker {
    /// Create a new worker node
    pub fn new(
        primary_name: PublicKey,
        id: WorkerId,
        committee: Committee,
        config: NarwhalConfig,
        keypair: crate::crypto::KeyPair,
        worker_address: SocketAddr,
    ) -> Self {
        // Default to in-memory storage
        let store: Arc<dyn BatchStore> = Arc::new(InMemoryBatchStore::new());
        Self::with_store(primary_name, id, committee, config, keypair, worker_address, store)
    }
    
    /// Create a new worker node with custom batch store
    pub fn with_store(
        primary_name: PublicKey,
        id: WorkerId,
        committee: Committee,
        config: NarwhalConfig,
        keypair: crate::crypto::KeyPair,
        worker_address: SocketAddr,
        store: Arc<dyn BatchStore>,
    ) -> Self {
        let batch_config = BatchMakerConfig {
            max_batch_size: config.max_batch_size,
            max_batch_delay: config.max_batch_delay,
        };
        
        
        Self {
            primary_name,
            id,
            committee,
            config,
            batch_config,
            keypair,
            worker_address,
            store,
        }
    }

    /// Spawn the worker node with the provided channels
    pub fn spawn(self, channels: WorkerChannelReceivers) -> (Vec<JoinHandle<DagResult<()>>>, WorkerNetwork) {
        info!(
            "Starting Narwhal Worker {} for primary {} on {}",
            self.id, self.primary_name, self.worker_address
        );
        
        let mut handles = vec![];
        
        // Set up Anemo network
        let network = self.setup_network(&channels);
        
        // Create worker network handle
        let worker_info = WorkerInfo {
            primary: self.primary_name.clone(),
            worker_id: self.id,
            worker_address: self.worker_address,
        };
        let worker_network = WorkerNetwork::new(worker_info, network.clone());
        
        // Spawn batch maker
        let batch_maker = BatchMaker::new(
            self.id,
            self.batch_config.clone(),
            self.committee.clone(),
            channels.rx_transaction,
            channels.tx_batch,
            channels.rx_reconfigure.clone(),
        );
        handles.push(batch_maker.spawn());
        
        // Create quorum waiter with network support and batch store
        let mut quorum_waiter = QuorumWaiter::new(
            self.id,
            self.primary_name.clone(),
            self.committee.clone(),
            channels.rx_batch,
            channels.tx_batch_digest,
            channels.rx_batch_ack,
            channels.rx_reconfigure,
            self.config.worker.batch_timeout,
            self.store.clone(),
        );
        
        // Set the worker network on the quorum waiter
        quorum_waiter.set_worker_network(worker_network.clone());
        handles.push(quorum_waiter.spawn());
        
        // TODO: Spawn batch synchronizer that uses rx_synchronizer and rx_primary_batch_req
        
        info!(
            "Worker {} spawned {} tasks with network support",
            self.id,
            handles.len()
        );
        
        (handles, worker_network)
    }
    
    /// Set up the Anemo network with RPC services
    fn setup_network(&self, channels: &WorkerChannelReceivers) -> Network {
        // Create RPC handlers
        let worker_handler = WorkerReceiverHandler::new(
            channels.tx_batch.clone(),
            self.store.clone(),
        );
        
        // Create channels for primary handler
        let (tx_synchronizer, rx_synchronizer) = mpsc::unbounded_channel();
        let (tx_primary_batch_req, rx_primary_batch_req) = mpsc::unbounded_channel();
        
        let primary_handler = PrimaryReceiverHandler::new(
            self.id,
            self.store.clone(),
            Some(tx_synchronizer),
            Some(tx_primary_batch_req),
        );
        
        // Create RPC services
        let worker_service = crate::rpc::worker::worker_to_worker_server::WorkerToWorkerServer::new(worker_handler);
        let primary_service = crate::rpc::primary::primary_to_worker_server::PrimaryToWorkerServer::new(primary_handler);
        
        // Set up router
        let routes = anemo::Router::new()
            .add_rpc_service(worker_service)
            .add_rpc_service(primary_service);
        
        // Build service with tracing
        let service = ServiceBuilder::new()
            .layer(TraceLayer::new())
            .service(routes);
        
        // Start network
        let network = Network::bind(self.worker_address)
            .server_name("narwhal-worker")
            .private_key(self.keypair.private_key_bytes())
            .start(service)
            .expect("Failed to start worker network");
        
        info!("Worker {} network started on {}", self.id, self.worker_address);
        
        // Add known peers (other workers and primary)
        self.add_known_peers(&network);
        
        network
    }
    
    /// Add known peers to the network
    fn add_known_peers(&self, network: &Network) {
        // Add our primary as a known peer
        let primary_info = self.committee.authority(&self.primary_name)
            .expect("Our primary not in committee");
        
        if let Ok(primary_addr) = primary_info.primary_address.parse::<SocketAddr>() {
            // Convert public key to bytes for PeerId
            use fastcrypto::traits::ToFromBytes;
            let peer_id = PeerId(primary_info.network_key.as_bytes().to_vec().try_into().unwrap_or([0u8; 32]));
            let peer_info = anemo::types::PeerInfo {
                peer_id,
                affinity: anemo::types::PeerAffinity::High,
                address: vec![anemo::types::Address::from(primary_addr)],
            };
            network.known_peers().insert(peer_info);
            info!("Added primary {} as known peer", self.primary_name);
        }
        
        // Add other workers as known peers
        for (name, authority) in &self.committee.authorities {
            if name == &self.primary_name {
                continue; // Skip our own primary's workers
            }
            
            for (worker_id, worker_addr) in authority.workers.get_all_worker_addresses() {
                if let Ok(addr) = worker_addr.parse::<SocketAddr>() {
                    // Use the authority's network key for worker peer ID
                    use fastcrypto::traits::ToFromBytes;
                    let peer_id = PeerId(authority.network_key.as_bytes().to_vec().try_into().unwrap_or([0u8; 32]));
                    let peer_info = anemo::types::PeerInfo {
                        peer_id,
                        affinity: anemo::types::PeerAffinity::High,
                        address: vec![anemo::types::Address::from(addr)],
                    };
                    network.known_peers().insert(peer_info);
                    debug!("Added worker {} of {} as known peer", worker_id, name);
                }
            }
        }
    }
    
    /// Create channels and spawn the worker
    pub fn create_and_spawn(self) -> (WorkerChannels, Vec<JoinHandle<DagResult<()>>>, WorkerNetwork) {
        let (channels, receivers) = WorkerChannels::new(self.committee.clone());
        let (handles, network) = self.spawn(receivers);
        (channels, handles, network)
    }
}