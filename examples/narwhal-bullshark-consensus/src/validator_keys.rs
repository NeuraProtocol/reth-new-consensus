//! Validator key management for Narwhal+Bullshark consensus

use alloy_primitives::Address;
use fastcrypto::{
    traits::{KeyPair as FastCryptoKeyPair, EncodeDecodeBase64},
    bls12381::{BLS12381KeyPair, BLS12381PublicKey, BLS12381PrivateKey},
};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;

/// Validator metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorMetadata {
    /// Human-readable name
    pub name: String,
    /// Description
    pub description: String,
    /// Contact info
    pub contact: String,
}

/// Validator key pair containing both EVM and consensus keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorKeyPair {
    /// Metadata about the validator
    #[serde(default)]
    pub metadata: ValidatorMetadata,
    /// EVM private key (hex string)
    pub evm_private_key: String,
    /// BLS private key for consensus (base64 encoded, can be null)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_private_key: Option<String>,
    /// BLS public key for consensus (base64 encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_public_key: Option<String>,
    /// Validator's stake
    pub stake: u64,
    /// Whether the validator is active
    #[serde(default = "default_active")]
    pub active: bool,
    /// Network address for primary
    pub network_address: String,
    /// Worker port range
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_port_range: Option<String>,
    /// Derived EVM address (populated at runtime)
    #[serde(skip)]
    pub evm_address: Address,
    /// Human-readable name (for backward compatibility)
    #[serde(skip)]
    pub name: String,
}

fn default_active() -> bool {
    true
}

impl Default for ValidatorMetadata {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            contact: String::new(),
        }
    }
}

impl ValidatorKeyPair {
    /// Load from a JSON file
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut key_pair: Self = serde_json::from_str(&content)?;
        
        // Derive EVM address from private key
        use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PrivateKey};
        use fastcrypto::traits::ToFromBytes;
        let key_bytes = alloy_primitives::hex::decode(key_pair.evm_private_key.trim_start_matches("0x"))?;
        let secp_private_key = Secp256k1PrivateKey::from_bytes(&key_bytes)?;
        let secp_keypair = Secp256k1KeyPair::from(secp_private_key);
        let public_key = secp_keypair.public();
        let public_key_bytes = public_key.as_bytes();
        // For secp256k1, we need to hash the uncompressed public key (without the 0x04 prefix)
        // fastcrypto returns compressed format, so we need to handle this differently
        let address_bytes = alloy_primitives::keccak256(&public_key_bytes[1..]);
        key_pair.evm_address = Address::from_slice(&address_bytes[12..]);
        
        // Set name from metadata for backward compatibility
        key_pair.name = key_pair.metadata.name.clone();
        
        // Generate consensus keys if not provided
        if key_pair.consensus_private_key.is_none() || key_pair.consensus_public_key.is_none() {
            // Generate deterministic BLS key from EVM private key
            // Use stable hashing for deterministic key derivation
            use std::hash::{Hash, Hasher};
            use std::collections::hash_map::DefaultHasher;
            
            // Create deterministic seed by hashing the key material
            let mut hasher = DefaultHasher::new();
            b"NEURA_BLS_KEY_DERIVATION_V1".hash(&mut hasher);
            key_bytes.hash(&mut hasher);
            let hash_value = hasher.finish();
            
            // Convert to 32 bytes
            let mut seed_bytes = [0u8; 32];
            seed_bytes[..8].copy_from_slice(&hash_value.to_le_bytes());
            seed_bytes[8..16].copy_from_slice(&hash_value.to_be_bytes());
            // Fill rest with more deterministic data
            for i in 16..32 {
                seed_bytes[i] = ((hash_value >> (i % 8)) & 0xff) as u8;
            }
            
            use rand::SeedableRng;
            use fastcrypto::traits::{KeyPair as FastCryptoKeyPair, ToFromBytes};
            let mut rng = rand::rngs::StdRng::from_seed(seed_bytes);
            let bls_keypair = BLS12381KeyPair::generate(&mut rng);
            
            // Extract private key and derive public key
            let private_key = bls_keypair.private();
            let private_key_bytes = private_key.as_bytes();
            let private_key_base64 = private_key.encode_base64();
            
            // Reconstruct keypair to get public key (avoids borrow issue)
            let private_key_reconstructed = BLS12381PrivateKey::from_bytes(private_key_bytes)?;
            let keypair_reconstructed = BLS12381KeyPair::from(private_key_reconstructed);
            let public_key_base64 = keypair_reconstructed.public().encode_base64();
            
            key_pair.consensus_private_key = Some(private_key_base64);
            key_pair.consensus_public_key = Some(public_key_base64);
        }
        
        Ok(key_pair)
    }

    /// Get the BLS key pair
    pub fn bls_keypair(&self) -> Result<BLS12381KeyPair> {
        let private_key_str = self.consensus_private_key.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No consensus private key available"))?;
        let private_key = BLS12381PrivateKey::decode_base64(private_key_str)
            .map_err(|e| anyhow::anyhow!("Failed to decode private key: {}", e))?;
        
        // Create keypair from private key (public key will be derived)
        Ok(BLS12381KeyPair::from(private_key))
    }
}

/// Registry of all validators
#[derive(Debug, Clone)]
pub struct ValidatorRegistry {
    /// Map from EVM address to validator info
    validators: HashMap<Address, ValidatorKeyPair>,
    /// Map from consensus public key to EVM address
    consensus_to_evm: HashMap<String, Address>,
}

impl ValidatorRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            validators: HashMap::new(),
            consensus_to_evm: HashMap::new(),
        }
    }

    /// Load validators from a directory
    pub fn from_directory(dir: &str) -> Result<Self> {
        let mut registry = Self::new();
        
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Ok(validator) = ValidatorKeyPair::from_file(path.to_str().unwrap()) {
                    registry.add_validator(validator);
                }
            }
        }
        
        Ok(registry)
    }

    /// Add a validator to the registry
    pub fn add_validator(&mut self, validator: ValidatorKeyPair) {
        if let Some(ref consensus_public_key) = validator.consensus_public_key {
            self.consensus_to_evm.insert(
                consensus_public_key.clone(),
                validator.evm_address,
            );
        }
        self.validators.insert(validator.evm_address, validator);
    }

    /// Get a validator by EVM address
    pub fn get_by_evm_address(&self, address: &Address) -> Option<&ValidatorKeyPair> {
        self.validators.get(address)
    }

    /// Get a validator by consensus public key
    pub fn get_by_consensus_key(&self, key: &str) -> Option<&ValidatorKeyPair> {
        self.consensus_to_evm.get(key)
            .and_then(|addr| self.validators.get(addr))
    }

    /// Get all validators
    pub fn all_validators(&self) -> Vec<&ValidatorKeyPair> {
        self.validators.values().collect()
    }

    /// Get total stake
    pub fn total_stake(&self) -> u64 {
        self.validators.values().map(|v| v.stake).sum()
    }
}