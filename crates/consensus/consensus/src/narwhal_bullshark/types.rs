use reth_primitives::{TransactionSigned};
use alloy_primitives::{B256, Bytes};
use narwhal::{NarwhalConfig, types::PublicKey};
use bullshark::BftConfig;
use serde::{Deserialize, Serialize};
use fastcrypto::traits::KeyPair;
use rand_08;
use std::collections::HashMap;
use std::net::SocketAddr;

/// Configuration for consensus RPC server
#[derive(Debug, Clone)]
pub struct ConsensusRpcConfig {
    /// Port to bind the RPC server to
    pub port: u16,
    /// Host to bind the RPC server to
    pub host: String,
    /// Whether to enable admin endpoints
    pub enable_admin: bool,
}

impl Default for ConsensusRpcConfig {
    fn default() -> Self {
        Self {
            port: 9999,
            host: "127.0.0.1".to_string(),
            enable_admin: false,
        }
    }
}

/// A finalized batch of transactions from Narwhal + Bullshark consensus
#[derive(Debug, Clone)]
pub struct FinalizedBatch {
    /// Block number this batch represents
    pub block_number: u64,
    /// Parent hash of the previous block
    pub parent_hash: B256,
    /// Transactions in this batch
    pub transactions: Vec<TransactionSigned>,
    /// Timestamp for the block
    pub timestamp: u64,
    /// Consensus round that finalized this batch
    pub consensus_round: u64,
    /// Certificate that led to finalization
    pub certificate_digest: B256,
    /// Validator signatures that formed consensus
    pub validator_signatures: Vec<(PublicKey, Vec<u8>)>,
}

/// Configuration for Narwhal + Bullshark consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarwhalBullsharkConfig {
    /// This node's public key
    pub node_public_key: PublicKey,
    /// Narwhal DAG configuration
    pub narwhal: NarwhalConfig,
    /// Bullshark BFT configuration  
    pub bullshark: BftConfig,
    /// Network configuration
    pub network: NetworkConfig,
}

/// Network configuration for consensus nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Address this node binds to for consensus networking
    pub bind_address: SocketAddr,
    /// Addresses of other validators in the committee (PublicKey -> SocketAddr)
    pub peer_addresses: HashMap<String, SocketAddr>, // Using String for serialization
}

impl Default for NarwhalBullsharkConfig {
    fn default() -> Self {
        // Generate a dummy key for testing
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        
        Self {
            node_public_key: keypair.public().clone(),
            narwhal: NarwhalConfig::default(),
            bullshark: BftConfig::default(),
            network: NetworkConfig::default(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:9000".parse().expect("Valid address"),
            peer_addresses: HashMap::new(),
        }
    }
}

/// Consensus seal data for Narwhal+Bullshark blocks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusSeal {
    /// The consensus round that finalized this block
    pub round: u64,
    /// The certificate digest that led to finalization
    pub certificate_digest: B256,
    /// Aggregated BLS signature from validators
    pub aggregated_signature: Bytes,
    /// Bitmap indicating which validators participated
    pub signers_bitmap: Bytes,
}

impl ConsensusSeal {
    /// Encode the consensus seal to bytes for inclusion in block header
    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::new();
        // Encode round as 8 bytes
        encoded.extend_from_slice(&self.round.to_be_bytes());
        // Encode certificate digest as 32 bytes
        encoded.extend_from_slice(self.certificate_digest.as_ref());
        // Encode signature length and signature
        encoded.extend_from_slice(&(self.aggregated_signature.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.aggregated_signature);
        // Encode bitmap length and bitmap
        encoded.extend_from_slice(&(self.signers_bitmap.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.signers_bitmap);
        Bytes::from(encoded)
    }

    /// Decode consensus seal from bytes
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 44 { // Minimum size: 8 (round) + 32 (digest) + 4 (sig len)
            return None;
        }

        let mut cursor = 0;
        
        // Decode round
        let round = u64::from_be_bytes(data[cursor..cursor+8].try_into().ok()?);
        cursor += 8;
        
        // Decode certificate digest
        let certificate_digest = B256::from_slice(&data[cursor..cursor+32]);
        cursor += 32;
        
        // Decode signature
        let sig_len = u32::from_be_bytes(data[cursor..cursor+4].try_into().ok()?) as usize;
        cursor += 4;
        if data.len() < cursor + sig_len {
            return None;
        }
        let aggregated_signature = Bytes::copy_from_slice(&data[cursor..cursor+sig_len]);
        cursor += sig_len;
        
        // Decode bitmap
        if data.len() < cursor + 4 {
            return None;
        }
        let bitmap_len = u32::from_be_bytes(data[cursor..cursor+4].try_into().ok()?) as usize;
        cursor += 4;
        if data.len() < cursor + bitmap_len {
            return None;
        }
        let signers_bitmap = Bytes::copy_from_slice(&data[cursor..cursor+bitmap_len]);
        
        Some(ConsensusSeal {
            round,
            certificate_digest,
            aggregated_signature,
            signers_bitmap,
        })
    }
    
    /// Verify the consensus seal against a committee
    pub fn verify(&self, committee: &narwhal::types::Committee) -> bool {
        use fastcrypto::traits::{ToFromBytes, VerifyingKey, AggregateAuthenticator};
        use fastcrypto::bls12381::{BLS12381AggregateSignature, BLS12381PublicKey};
        
        // Parse the aggregated signature
        let agg_sig = match BLS12381AggregateSignature::from_bytes(&self.aggregated_signature) {
            Ok(sig) => sig,
            Err(_) => return false,
        };
        
        // Collect public keys of signers based on bitmap
        let mut pubkeys = Vec::new();
        let authorities: Vec<_> = committee.authorities.keys().collect();
        
        for (i, authority) in authorities.iter().enumerate() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            
            if byte_idx < self.signers_bitmap.len() && (self.signers_bitmap[byte_idx] & (1 << bit_idx)) != 0 {
                pubkeys.push((*authority).clone());
            }
        }
        
        // Verify we have enough stake
        let total_stake: u64 = pubkeys.iter()
            .map(|pk| committee.stake(pk))
            .sum();
            
        if total_stake < committee.validity_threshold() {
            return false;
        }
        
        // Create message to verify (certificate digest)
        let message = self.certificate_digest.as_ref();
        
        // Verify aggregate signature
        // Note: In real implementation, this would verify against the actual message signed
        agg_sig.verify(&pubkeys, message).is_ok()
    }
}
