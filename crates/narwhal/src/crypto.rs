//! Cryptographic utilities for Narwhal

use crate::types::PublicKey;
use serde::{Deserialize, Serialize};

/// A key pair for network identity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyPair {
    /// Private key bytes
    private_key: Vec<u8>,
    /// Public key
    public_key: PublicKey,
}

impl KeyPair {
    /// Create a new key pair from bytes
    pub fn from_bytes(private_key: Vec<u8>, public_key: PublicKey) -> Self {
        Self {
            private_key,
            public_key,
        }
    }
    
    /// Get the private key bytes for Anemo
    pub fn private_key_bytes(&self) -> [u8; 32] {
        self.private_key.clone().try_into().unwrap_or([0u8; 32])
    }
    
    /// Get the public key
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

impl KeyPair {
    /// Derive a worker keypair from a primary key and worker ID
    pub fn derive_worker_keypair(primary_key: &PublicKey, worker_id: u32) -> Self {
        use blake2::{Blake2b, Digest};
        use fastcrypto::traits::{ToFromBytes, KeyPair as _};
        
        // Create deterministic seed from primary key + worker ID
        let mut hasher = Blake2b::new();
        hasher.update(b"NARWHAL_WORKER_KEY_DERIVATION_V1");
        hasher.update(primary_key.as_bytes());
        hasher.update(worker_id.to_le_bytes());
        
        let hash = hasher.finalize();
        let seed_bytes: [u8; 32] = hash.as_slice()[..32].try_into().unwrap();
        
        // Generate deterministic keypair from seed
        use rand_08::{SeedableRng, rngs::StdRng};
        let mut rng = StdRng::from_seed(seed_bytes);
        
        // Generate BLS keypair for worker
        let bls_keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rng);
        
        // Extract private key bytes and reconstruct to avoid borrow issues
        let private_key_obj = bls_keypair.private();
        let private_key_bytes = private_key_obj.as_bytes().to_vec();
        
        // Reconstruct keypair from private key to get public key
        use fastcrypto::bls12381::BLS12381PrivateKey;
        let private_key_reconstructed = BLS12381PrivateKey::from_bytes(&private_key_bytes).unwrap();
        let keypair_reconstructed = fastcrypto::bls12381::BLS12381KeyPair::from(private_key_reconstructed);
        let public_key = keypair_reconstructed.public().clone();
        
        Self {
            private_key: private_key_bytes,
            public_key,
        }
    }
    
    /// Derive a network (ed25519) keypair for a primary from consensus key and network address
    pub fn derive_network_keypair(consensus_key: &PublicKey, network_address: &std::net::SocketAddr) -> [u8; 32] {
        use blake2::{Blake2b, Digest};
        use fastcrypto::traits::ToFromBytes;
        
        // Create a deterministic seed from consensus key + network address
        // CRITICAL: Use normalized address for consistent key generation
        let normalized_addr = Self::normalize_socket_addr(network_address);
        
        let mut hasher = Blake2b::new();
        hasher.update(b"narwhal_network_key:");
        hasher.update(consensus_key.as_bytes());
        hasher.update(normalized_addr.as_bytes());
        
        let hash = hasher.finalize();
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&hash[..32]);
        
        key_bytes
    }

    /// Derive a network (ed25519) keypair for a worker from primary key and worker ID
    pub fn derive_worker_network_keypair(primary_key: &PublicKey, worker_id: u32, worker_address: &std::net::SocketAddr) -> [u8; 32] {
        use blake2::{Blake2b, Digest};
        use fastcrypto::traits::ToFromBytes;
        
        // Create a deterministic seed from primary key + worker ID + network address
        // CRITICAL: Use normalized address representation for consistent hashing
        let normalized_addr = Self::normalize_socket_addr(worker_address);
        
        let mut hasher = Blake2b::new();
        hasher.update(b"narwhal_worker_key:");
        hasher.update(primary_key.as_bytes());
        hasher.update(&worker_id.to_le_bytes());
        hasher.update(normalized_addr.as_bytes());
        
        let hash = hasher.finalize();
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&hash[..32]);
        
        key_bytes
    }
    
    /// Normalize socket address to ensure consistent string representation
    fn normalize_socket_addr(addr: &std::net::SocketAddr) -> String {
        // Always use the IP:port format, converting IPv4-mapped IPv6 to IPv4
        match addr {
            std::net::SocketAddr::V4(v4) => v4.to_string(),
            std::net::SocketAddr::V6(v6) => {
                // Check if this is an IPv4-mapped IPv6 address
                if let Some(ipv4) = v6.ip().to_ipv4_mapped() {
                    format!("{}:{}", ipv4, v6.port())
                } else {
                    v6.to_string()
                }
            }
        }
    }
}