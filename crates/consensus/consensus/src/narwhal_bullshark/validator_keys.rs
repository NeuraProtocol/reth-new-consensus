//! Validator key management for EVM-compatible Narwhal + Bullshark consensus
//!
//! This module handles the dual-key architecture where validators have:
//! 1. EVM addresses (secp256k1) for staking, rewards, and chain interactions
//! 2. Consensus keys (BLS12-381) for participating in Narwhal + Bullshark BFT

use alloy_primitives::{Address, B256};
use narwhal::types::{PublicKey as ConsensusPublicKey, Committee};
use fastcrypto::traits::{KeyPair, EncodeDecodeBase64};
use secp256k1::{SecretKey as EvmSecretKey, PublicKey as EvmPublicKey, Secp256k1};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use tracing::{info, warn};
use rand_08::RngCore;
use sha2::{Sha256, Digest};

/// Validator identity combining EVM address and consensus key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorIdentity {
    /// EVM address (derived from secp256k1 key) 
    pub evm_address: Address,
    /// Consensus public key (BLS12-381) for consensus participation
    pub consensus_public_key: ConsensusPublicKey,
    /// Optional validator metadata
    pub metadata: ValidatorMetadata,
}

/// Additional validator metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidatorMetadata {
    /// Validator name or identifier
    pub name: Option<String>,
    /// Validator description
    pub description: Option<String>,
    /// Contact information (website, email, etc.)
    pub contact: Option<String>,
}

/// Validator key pair containing both EVM and consensus keys
#[derive(Debug)]
pub struct ValidatorKeyPair {
    /// EVM private key (secp256k1)
    pub evm_private_key: EvmSecretKey,
    /// EVM public key (secp256k1)
    pub evm_public_key: EvmPublicKey,
    /// EVM address (20 bytes derived from secp256k1 public key)
    pub evm_address: Address,
    /// Consensus key pair (BLS12-381)
    pub consensus_keypair: fastcrypto::bls12381::BLS12381KeyPair,
}

impl ValidatorKeyPair {
    /// Generate a new validator key pair with both EVM and consensus keys
    pub fn generate() -> Result<Self> {
        // Generate EVM key (secp256k1)
        let mut rng = rand_08::thread_rng();
        let mut key_bytes = [0u8; 32];
        rng.fill_bytes(&mut key_bytes);
        let evm_private_key = EvmSecretKey::from_byte_array(&key_bytes)
            .map_err(|e| anyhow!("Failed to generate EVM private key: {}", e))?;
        let secp = Secp256k1::new();
        let evm_public_key = EvmPublicKey::from_secret_key(&secp, &evm_private_key);
        let evm_address = public_key_to_address(&evm_public_key);
        
        // Generate consensus key (BLS12-381)
        let consensus_keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rng);
        
        Ok(Self {
            evm_private_key,
            evm_public_key,
            evm_address,
            consensus_keypair,
        })
    }
    
    /// Create validator key pair deterministically from a string seed
    /// This ensures all nodes generate the same validator keys for shared committee
    pub fn from_seed(seed: &str) -> Result<Self> {
        use rand_08::SeedableRng;
        use sha2::{Sha256, Digest};
        
        // Hash the string seed to get deterministic 32-byte seed
        let mut hasher = Sha256::new();
        hasher.update(b"RETH_NARWHAL_VALIDATOR_SEED_V1:");
        hasher.update(seed.as_bytes());
        let seed_bytes: [u8; 32] = hasher.finalize().into();
        
        // Create deterministic RNG from seed
        let mut rng = rand_08::rngs::StdRng::from_seed(seed_bytes);
        
        // Generate deterministic EVM private key
        let mut evm_key_bytes = [0u8; 32];
        rng.fill_bytes(&mut evm_key_bytes);
        let evm_private_key = EvmSecretKey::from_byte_array(&evm_key_bytes)
            .map_err(|e| anyhow!("Failed to generate deterministic EVM private key: {}", e))?;
        
        let secp = Secp256k1::new();
        let evm_public_key = EvmPublicKey::from_secret_key(&secp, &evm_private_key);
        let evm_address = public_key_to_address(&evm_public_key);
        
        // Generate deterministic consensus key
        let consensus_keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rng);
        
        info!(
            "Generated deterministic validator from seed '{}': EVM {:?}, Consensus {}",
            seed,
            evm_address,
            consensus_keypair.public().encode_base64()
        );
        
        Ok(Self {
            evm_private_key,
            evm_public_key,
            evm_address,
            consensus_keypair,
        })
    }
    
    /// Create validator key pair from existing EVM private key, generating new consensus key
    pub fn from_evm_key(evm_private_key: EvmSecretKey) -> Result<Self> {
        let secp = Secp256k1::new();
        let evm_public_key = EvmPublicKey::from_secret_key(&secp, &evm_private_key);
        let evm_address = public_key_to_address(&evm_public_key);
        
        // Generate new consensus key
        let mut rng = rand_08::thread_rng();
        let consensus_keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rng);
        
        Ok(Self {
            evm_private_key,
            evm_public_key,
            evm_address,
            consensus_keypair,
        })
    }
    
    /// Create validator key pair with deterministic consensus key derived from EVM key
    pub fn from_evm_key_deterministic(evm_private_key: EvmSecretKey) -> Result<Self> {
        let secp = Secp256k1::new();
        let evm_public_key = EvmPublicKey::from_secret_key(&secp, &evm_private_key);
        let evm_address = public_key_to_address(&evm_public_key);
        
        // Derive consensus key deterministically from EVM private key
        let consensus_seed = derive_consensus_seed(&evm_private_key)?;
        
        // Use the seed to create a deterministic BLS key
        let consensus_keypair = derive_bls_keypair_from_seed(consensus_seed)?;
        
        Ok(Self {
            evm_private_key,
            evm_public_key,
            evm_address,
            consensus_keypair,
        })
    }
    
    /// Get the validator identity (public information only)
    pub fn identity(&self, metadata: ValidatorMetadata) -> ValidatorIdentity {
        ValidatorIdentity {
            evm_address: self.evm_address,
            consensus_public_key: self.consensus_keypair.public().clone(),
            metadata,
        }
    }
    
    /// Sign a message with the consensus key
    pub fn consensus_sign(&self, message: &[u8]) -> Result<fastcrypto::bls12381::BLS12381Signature> {
        use fastcrypto::traits::Signer;
        self.consensus_keypair.try_sign(message)
            .map_err(|e| anyhow!("Consensus signing failed: {}", e))
    }
    
    /// Sign a transaction hash with the EVM key  
    pub fn evm_sign(&self, hash: B256) -> Result<alloy_primitives::Signature> {
        use reth_primitives_traits::crypto::secp256k1::sign_message;
        sign_message(
            B256::from_slice(&self.evm_private_key.secret_bytes()),
            hash
        ).map_err(|e| anyhow!("EVM signing failed: {}", e))
    }
}

/// Committee manager that maps EVM addresses to consensus keys
#[derive(Debug, Clone)]
pub struct ValidatorRegistry {
    /// Mapping from EVM address to validator identity
    validators: HashMap<Address, ValidatorIdentity>,
    /// Reverse mapping from consensus key to EVM address
    consensus_to_evm: HashMap<ConsensusPublicKey, Address>,
}

impl ValidatorRegistry {
    /// Create a new empty validator registry
    pub fn new() -> Self {
        Self {
            validators: HashMap::new(),
            consensus_to_evm: HashMap::new(),
        }
    }
    
    /// Register a validator with both EVM and consensus identities
    pub fn register_validator(&mut self, identity: ValidatorIdentity) -> Result<()> {
        let evm_address = identity.evm_address;
        let consensus_key = identity.consensus_public_key.clone();
        
        // Check for conflicts
        if self.validators.contains_key(&evm_address) {
            return Err(anyhow!("EVM address {:?} already registered", evm_address));
        }
        
        if self.consensus_to_evm.contains_key(&consensus_key) {
            return Err(anyhow!("Consensus key already registered to different EVM address"));
        }
        
        // Register both mappings
        self.validators.insert(evm_address, identity);
        self.consensus_to_evm.insert(consensus_key.clone(), evm_address);
        
        info!("Registered validator: EVM {:?} <-> Consensus {:?}", 
              evm_address, consensus_key.encode_base64());
        
        Ok(())
    }
    
    /// Get validator identity by EVM address
    pub fn get_by_evm_address(&self, address: &Address) -> Option<&ValidatorIdentity> {
        self.validators.get(address)
    }
    
    /// Get EVM address by consensus public key
    pub fn get_evm_address(&self, consensus_key: &ConsensusPublicKey) -> Option<&Address> {
        self.consensus_to_evm.get(consensus_key)
    }
    
    /// Create a Narwhal committee from registered validators with stakes
    pub fn create_committee(&self, epoch: u64, stakes: &HashMap<Address, u64>) -> Result<Committee> {
        let mut authorities = HashMap::new();
        
        for (evm_address, stake) in stakes {
            if let Some(identity) = self.validators.get(evm_address) {
                authorities.insert(identity.consensus_public_key.clone(), *stake);
            } else {
                warn!("Validator with EVM address {:?} not found in registry", evm_address);
            }
        }
        
        if authorities.is_empty() {
            return Err(anyhow!("No registered validators found for committee creation"));
        }
        
        Ok(Committee::new(epoch, authorities))
    }
    
    /// Get all registered validators
    pub fn all_validators(&self) -> Vec<&ValidatorIdentity> {
        self.validators.values().collect()
    }
    
    /// Get count of registered validators
    pub fn validator_count(&self) -> usize {
        self.validators.len()
    }
}

/// Convert secp256k1 public key to Ethereum address (20 bytes)
fn public_key_to_address(public_key: &EvmPublicKey) -> Address {
    use sha3::{Keccak256, Digest};
    
    let public_key_bytes = public_key.serialize_uncompressed();
    let hash = Keccak256::digest(&public_key_bytes[1..]);  // Skip the 0x04 prefix
    Address::from_slice(&hash[12..])  // Take last 20 bytes
}

/// Derive a deterministic consensus key seed from EVM private key
/// 
/// This uses HKDF-like derivation with domain separation to ensure:
/// - Same EVM key always produces same consensus key
/// - Different EVM keys produce different consensus keys
/// - Cannot reverse-engineer EVM key from consensus key
fn derive_consensus_seed(evm_key: &EvmSecretKey) -> Result<[u8; 32]> {
    use sha2::{Sha256, Digest};
    
    // Use HKDF-like derivation with domain separation
    let mut hasher = Sha256::new();
    hasher.update(b"RETH_NARWHAL_CONSENSUS_KEY_DERIVATION_V1");
    hasher.update(&evm_key.secret_bytes());
    
    Ok(hasher.finalize().into())
}

/// Derive a BLS12-381 keypair deterministically from a seed
/// 
/// This ensures that the same seed always produces the same BLS keypair
/// by using a deterministic RNG seeded with the provided bytes
fn derive_bls_keypair_from_seed(seed: [u8; 32]) -> Result<fastcrypto::bls12381::BLS12381KeyPair> {
    use rand_08::SeedableRng;
    
    // Create a deterministic RNG from the seed
    let mut rng = rand_08::rngs::StdRng::from_seed(seed);
    
    // Generate BLS keypair using the deterministic RNG
    Ok(fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rng))
}

/// Configuration for validator key management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorKeyConfig {
    /// Strategy for validator key management
    pub key_strategy: KeyManagementStrategy,
    /// Path to validator key files (if using file-based management)
    pub key_directory: Option<std::path::PathBuf>,
    /// Whether to derive consensus keys deterministically from EVM keys
    pub deterministic_consensus_keys: bool,
}

/// Strategy for managing validator keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyManagementStrategy {
    /// Generate random keys for testing
    Random,
    /// Load keys from configuration files
    FileSystem,
    /// Use external key management service
    External,
    /// Use hardware security modules
    HSM,
}

impl Default for ValidatorKeyConfig {
    fn default() -> Self {
        Self {
            key_strategy: KeyManagementStrategy::Random,
            key_directory: None,
            deterministic_consensus_keys: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_generate_validator_keypair() {
        let keypair = ValidatorKeyPair::generate().unwrap();
        
        // Verify EVM address derivation
        let derived_address = public_key_to_address(&keypair.evm_public_key);
        assert_eq!(keypair.evm_address, derived_address);
    }
    
    #[test]
    fn test_deterministic_consensus_keys() {
        // Generate a test EVM private key
        let mut rng = rand_08::thread_rng();
        let mut key_bytes = [0u8; 32];
        rng.fill_bytes(&mut key_bytes);
        let evm_key = EvmSecretKey::from_byte_array(&key_bytes).unwrap();
        
        // Generate deterministic consensus keys twice from same EVM key
        let keypair1 = ValidatorKeyPair::from_evm_key_deterministic(evm_key).unwrap();
        let keypair2 = ValidatorKeyPair::from_evm_key_deterministic(evm_key).unwrap();
        
        // Should be identical - same EVM key → same consensus key
        assert_eq!(keypair1.evm_address, keypair2.evm_address);
        assert_eq!(
            keypair1.consensus_keypair.public().encode_base64(),
            keypair2.consensus_keypair.public().encode_base64()
        );
        
        // Generate a different EVM key
        let mut different_key_bytes = [0u8; 32];
        rng.fill_bytes(&mut different_key_bytes);
        let different_evm_key = EvmSecretKey::from_byte_array(&different_key_bytes).unwrap();
        let keypair3 = ValidatorKeyPair::from_evm_key_deterministic(different_evm_key).unwrap();
        
        // Should be different - different EVM key → different consensus key
        assert_ne!(keypair1.evm_address, keypair3.evm_address);
        assert_ne!(
            keypair1.consensus_keypair.public().encode_base64(),
            keypair3.consensus_keypair.public().encode_base64()
        );
    }
    
    #[test]
    fn test_validator_registry() {
        let mut registry = ValidatorRegistry::new();
        
        // Create test validator
        let keypair = ValidatorKeyPair::generate().unwrap();
        let identity = keypair.identity(ValidatorMetadata {
            name: Some("Test Validator".to_string()),
            ..Default::default()
        });
        
        // Register validator
        registry.register_validator(identity.clone()).unwrap();
        
        // Test lookups
        assert!(registry.get_by_evm_address(&identity.evm_address).is_some());
        assert!(registry.get_evm_address(&identity.consensus_public_key).is_some());
        assert_eq!(registry.validator_count(), 1);
        
        // Test committee creation
        let mut stakes = HashMap::new();
        stakes.insert(identity.evm_address, 100);
        
        let committee = registry.create_committee(0, &stakes).unwrap();
        assert_eq!(committee.authorities.len(), 1);
        assert!(committee.authorities.contains_key(&identity.consensus_public_key));
    }
} 