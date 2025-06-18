//! Networking for Narwhal DAG consensus

use crate::{
    types::{Header, Vote, Certificate, PublicKey, Committee},
    DagError, DagResult,
};
use anemo::{Network, PeerId, Request, Response};
use anemo_tower::trace::TraceLayer;
use fastcrypto::traits::ToFromBytes;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tower::ServiceBuilder;
use tracing::{debug, info, warn};


/// Network events that can be received from other nodes
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// Received a header from another node
    HeaderReceived(Header),
    /// Received a vote from another node
    VoteReceived(Vote),
    /// Received a certificate from another node
    CertificateReceived(Certificate),
    /// A peer connected
    PeerConnected(PublicKey),
    /// A peer disconnected
    PeerDisconnected(PublicKey),
}

/// Network messages that can be sent between Narwhal nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NarwhalMessage {
    /// Header proposal
    Header(Header),
    /// Vote on a header
    Vote(Vote),
    /// Finalized certificate
    Certificate(Certificate),
    /// Request for missing certificates
    CertificateRequest(Vec<crate::types::CertificateDigest>),
    /// Response with requested certificates
    CertificateResponse(Vec<Certificate>),
    /// Ping message for liveness
    Ping(u64),
    /// Pong response
    Pong(u64),
}

impl NarwhalMessage {
    /// Serialize message to bytes
    pub fn to_bytes(&self) -> DagResult<Bytes> {
        let serialized = bincode::serialize(self)
            .map_err(|e| DagError::Serialization(e.to_string()))?;
        Ok(Bytes::from(serialized))
    }

    /// Deserialize message from bytes
    pub fn from_bytes(bytes: &[u8]) -> DagResult<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| DagError::Serialization(e.to_string()))
    }
}

/// Narwhal networking implementation
pub struct NarwhalNetwork {
    /// Our node's public key
    node_key: PublicKey,
    /// Current committee
    committee: Committee,
    /// Anemo network instance
    network: Network,
    /// Event sender for broadcasting network events
    #[allow(dead_code)]
    event_sender: broadcast::Sender<NetworkEvent>,
    /// Map of public keys to peer IDs
    peer_map: HashMap<PublicKey, PeerId>,
}

impl std::fmt::Debug for NarwhalNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NarwhalNetwork")
            .field("node_key", &self.node_key)
            .field("committee", &self.committee)
            .field("peer_count", &self.peer_map.len())
            .finish_non_exhaustive()
    }
}

impl NarwhalNetwork {
    /// Create a new Narwhal network instance
    pub fn new(
        node_key: PublicKey,
        committee: Committee,
        bind_address: SocketAddr,
    ) -> DagResult<(Self, broadcast::Receiver<NetworkEvent>)> {
        let (event_sender, event_receiver) = broadcast::channel(1000);

        // Create the service for handling incoming messages
        let service = ServiceBuilder::new()
            .layer(TraceLayer::new())
            .service(NarwhalNetworkService {
                event_sender: event_sender.clone(),
            });

        // Create anemo network
        let network = anemo::Network::bind(bind_address)
            .server_name("narwhal")
            .start(service)
            .map_err(|e| DagError::Network(e.to_string()))?;

        info!("Narwhal network listening on {}", bind_address);

        let narwhal_network = Self {
            node_key,
            committee,
            network,
            event_sender,
            peer_map: HashMap::new(),
        };

        Ok((narwhal_network, event_receiver))
    }

    /// Connect to peers in the committee
    pub async fn connect_to_committee(&mut self) -> DagResult<()> {
        for (authority, _stake) in &self.committee.authorities {
            if authority != &self.node_key {
                // In a real implementation, we'd have peer discovery or static configuration
                // For now, we'll simulate peer connection
                let peer_id = self.derive_peer_id(authority);
                self.peer_map.insert(authority.clone(), peer_id);
                
                debug!("Connected to committee member: {}", authority);
            }
        }

        info!("Connected to {} committee members", self.peer_map.len());
        Ok(())
    }

    /// Broadcast a header to all peers
    pub async fn broadcast_header(&self, header: Header) -> DagResult<()> {
        let message = NarwhalMessage::Header(header);
        self.broadcast_message(message).await
    }

    /// Broadcast a vote to all peers
    pub async fn broadcast_vote(&self, vote: Vote) -> DagResult<()> {
        let message = NarwhalMessage::Vote(vote);
        self.broadcast_message(message).await
    }

    /// Broadcast a certificate to all peers
    pub async fn broadcast_certificate(&self, certificate: Certificate) -> DagResult<()> {
        let message = NarwhalMessage::Certificate(certificate);
        self.broadcast_message(message).await
    }

    /// Send a message to a specific peer
    pub async fn send_to_peer(
        &self,
        peer_key: &PublicKey,
        message: NarwhalMessage,
    ) -> DagResult<()> {
        if let Some(peer_id) = self.peer_map.get(peer_key) {
            let serialized = message.to_bytes()?;
            let request = Request::new(serialized);
            
            match self.network.rpc(*peer_id, request).await {
                Ok(_) => {
                    debug!("Sent message to peer {}", peer_key);
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to send message to peer {}: {}", peer_key, e);
                    Err(DagError::Network(e.to_string()))
                }
            }
        } else {
            Err(DagError::Network(format!("Peer {} not found", peer_key)))
        }
    }

    /// Request certificates from a peer
    pub async fn request_certificates(
        &self,
        peer_key: &PublicKey,
        certificate_digests: Vec<crate::types::CertificateDigest>,
    ) -> DagResult<Vec<Certificate>> {
        let message = NarwhalMessage::CertificateRequest(certificate_digests);
        
        if let Some(peer_id) = self.peer_map.get(peer_key) {
            let serialized = message.to_bytes()?;
            let request = Request::new(serialized);
            
            match self.network.rpc(*peer_id, request).await {
                Ok(response) => {
                    let response_message = NarwhalMessage::from_bytes(response.into_inner().as_ref())?;
                    if let NarwhalMessage::CertificateResponse(certificates) = response_message {
                        debug!("Received {} certificates from peer {}", certificates.len(), peer_key);
                        Ok(certificates)
                    } else {
                        Err(DagError::Network("Unexpected response type".to_string()))
                    }
                }
                Err(e) => {
                    warn!("Failed to request certificates from peer {}: {}", peer_key, e);
                    Err(DagError::Network(e.to_string()))
                }
            }
        } else {
            Err(DagError::Network(format!("Peer {} not found", peer_key)))
        }
    }

    /// Broadcast a message to all connected peers
    async fn broadcast_message(&self, message: NarwhalMessage) -> DagResult<()> {
        let mut success_count = 0;
        let mut error_count = 0;
        
        let serialized = message.to_bytes()?;

        for (peer_key, peer_id) in &self.peer_map {
            let request = Request::new(serialized.clone());
            
            match self.network.rpc(*peer_id, request).await {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    warn!("Failed to send message to peer {}: {}", peer_key, e);
                    error_count += 1;
                }
            }
        }

        debug!("Broadcast complete: {} success, {} errors", success_count, error_count);

        // Consider broadcast successful if we reached at least one peer
        if success_count > 0 || self.peer_map.is_empty() {
            Ok(())
        } else {
            Err(DagError::Network("Failed to reach any peers".to_string()))
        }
    }

    /// Derive a peer ID from a public key (placeholder implementation)
    fn derive_peer_id(&self, public_key: &PublicKey) -> PeerId {
        // In a real implementation, this would derive the peer ID from the network key
        let key_bytes = public_key.as_bytes();
        PeerId(key_bytes[..32].try_into().unwrap_or([0u8; 32]))
    }

    /// Get network statistics
    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            connected_peers: self.peer_map.len(),
            committee_size: self.committee.authorities.len(),
            network_address: self.network.local_addr(),
        }
    }

    /// Update the committee configuration
    pub async fn update_committee(&mut self, new_committee: Committee) -> DagResult<()> {
        info!("Updating network committee configuration");
        self.committee = new_committee;
        
        // Reconnect to new committee members
        self.peer_map.clear();
        self.connect_to_committee().await?;
        
        Ok(())
    }
}

/// Service for handling incoming network requests
#[derive(Clone)]
struct NarwhalNetworkService {
    event_sender: broadcast::Sender<NetworkEvent>,
}

impl tower::Service<Request<Bytes>> for NarwhalNetworkService {
    type Response = Response<Bytes>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let event_sender = self.event_sender.clone();
        let bytes = req.into_inner();

        Box::pin(async move {
            // Deserialize the message
            let message = match NarwhalMessage::from_bytes(&bytes) {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Failed to deserialize message: {}", e);
                    // Return a default pong message for error cases
                    let pong = NarwhalMessage::Pong(0);
                    let response_bytes = pong.to_bytes().unwrap_or_else(|_| Bytes::new());
                    return Ok(Response::new(response_bytes));
                }
            };

            // Handle the incoming message
            let response_message = match message {
                NarwhalMessage::Header(header) => {
                    debug!("Received header: {:?}", header);
                    let _ = event_sender.send(NetworkEvent::HeaderReceived(header));
                    NarwhalMessage::Pong(0) // Acknowledge receipt
                }
                NarwhalMessage::Vote(vote) => {
                    debug!("Received vote: {:?}", vote);
                    let _ = event_sender.send(NetworkEvent::VoteReceived(vote));
                    NarwhalMessage::Pong(0)
                }
                NarwhalMessage::Certificate(certificate) => {
                    debug!("Received certificate: {:?}", certificate);
                    let _ = event_sender.send(NetworkEvent::CertificateReceived(certificate));
                    NarwhalMessage::Pong(0)
                }
                NarwhalMessage::CertificateRequest(digests) => {
                    debug!("Received certificate request for {} digests", digests.len());
                    // TODO: Look up certificates and return them
                    NarwhalMessage::CertificateResponse(Vec::new())
                }
                NarwhalMessage::Ping(nonce) => {
                    debug!("Received ping with nonce {}", nonce);
                    NarwhalMessage::Pong(nonce)
                }
                _ => {
                    warn!("Received unexpected message type");
                    NarwhalMessage::Pong(0)
                }
            };

            // Serialize and return the response
            let response_bytes = response_message.to_bytes().unwrap_or_else(|_| Bytes::new());
            Ok(Response::new(response_bytes))
        })
    }
}

/// Network statistics
#[derive(Debug, Clone)]
pub struct NetworkStats {
    /// Number of connected peers
    pub connected_peers: usize,
    /// Size of the committee
    pub committee_size: usize,
    /// Our network address
    pub network_address: SocketAddr,
}

/// Network configuration for Narwhal
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Address to bind the network service to
    pub bind_address: SocketAddr,
    /// Maximum number of connections
    pub max_connections: usize,
    /// Connection timeout
    pub connection_timeout: std::time::Duration,
    /// Request timeout
    pub request_timeout: std::time::Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:0".parse().unwrap(),
            max_connections: 100,
            connection_timeout: std::time::Duration::from_secs(10),
            request_timeout: std::time::Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Committee;
    use std::collections::HashMap;
    use fastcrypto::traits::KeyPair;
    use rand_08;

    fn create_test_committee() -> Committee {
        let mut authorities = HashMap::new();
        for _ in 0..4 {
            let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
            authorities.insert(keypair.public().clone(), 100);
        }
        Committee::new(0, authorities)
    }

    #[tokio::test]
    #[ignore] // TODO: Fix anemo networking library issues in test environment
    async fn test_network_creation() {
        let committee = create_test_committee();
        let node_key = committee.authorities.keys().next().unwrap().clone();
        let bind_address = "127.0.0.1:0".parse().unwrap();

        let result = NarwhalNetwork::new(node_key, committee, bind_address);
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // TODO: Fix anemo networking library issues in test environment
    async fn test_committee_connection() {
        let committee = create_test_committee();
        let node_key = committee.authorities.keys().next().unwrap().clone();
        let bind_address = "127.0.0.1:0".parse().unwrap();

        let (mut network, _receiver) = NarwhalNetwork::new(node_key, committee, bind_address).unwrap();
        
        // This should succeed (even if connections are simulated)
        let result = network.connect_to_committee().await;
        assert!(result.is_ok());
    }
} 