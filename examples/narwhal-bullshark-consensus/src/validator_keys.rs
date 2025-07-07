//! Validator key management for Narwhal+Bullshark consensus

use alloy_primitives::Address;
use fastcrypto::{
    traits::{KeyPair as FastCryptoKeyPair, EncodeDecodeBase64},
    bls12381::{BLS12381KeyPair, BLS12381PublicKey, BLS12381PrivateKey},
};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;

/// Validator key pair containing both EVM and consensus keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorKeyPair {
    /// EVM address for this validator
    pub evm_address: Address,
    /// BLS private key for consensus (base64 encoded)
    pub consensus_private_key: String,
    /// BLS public key for consensus (base64 encoded)
    pub consensus_public_key: String,
    /// Human-readable name
    pub name: String,
    /// Validator's stake
    pub stake: u64,
}

impl ValidatorKeyPair {
    /// Load from a JSON file
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let key_pair: Self = serde_json::from_str(&content)?;
        Ok(key_pair)
    }

    /// Get the BLS key pair
    pub fn bls_keypair(&self) -> Result<BLS12381KeyPair> {
        let private_key = BLS12381PrivateKey::decode_base64(&self.consensus_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to decode private key: {}", e))?;
        let public_key = BLS12381PublicKey::decode_base64(&self.consensus_public_key)
            .map_err(|e| anyhow::anyhow!("Failed to decode public key: {}", e))?;
        
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
        self.consensus_to_evm.insert(
            validator.consensus_public_key.clone(),
            validator.evm_address,
        );
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