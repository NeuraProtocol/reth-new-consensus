#!/usr/bin/env rust-script
//! ```cargo
//! [dependencies]
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! fastcrypto = "=0.1.2"
//! alloy-primitives = "0.8"
//! blake2 = "0.10"
//! rand_08 = { package = "rand", version = "0.8" }
//! secp256k1 = "0.28"
//! anyhow = "1.0"
//! ```

use serde::{Deserialize, Serialize};
use fastcrypto::{
    traits::{KeyPair as FastCryptoKeyPair, EncodeDecodeBase64},
    bls12381::{BLS12381KeyPair, BLS12381PrivateKey},
};
use alloy_primitives::{Address, hex};
use blake2::{Blake2b, Digest, digest::consts::U64};
use rand_08::{SeedableRng, rngs::StdRng};
use secp256k1::{SecretKey, PublicKey, Secp256k1};
use std::fs;
use anyhow::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidatorConfig {
    metadata: serde_json::Value,
    evm_private_key: String,
    consensus_private_key: Option<String>,
    consensus_public_key: Option<String>,
    evm_address: Option<String>,
    stake: u64,
    active: bool,
    network_address: String,
    worker_port_range: String,
}

/// Generate BLS keys deterministically from EVM private key
fn generate_bls_keys_from_evm(evm_private_key: &str) -> Result<(String, String, Address)> {
    // Parse EVM private key
    let key_bytes = hex::decode(evm_private_key.trim_start_matches("0x"))?;
    let mut key_array = [0u8; 32];
    key_array.copy_from_slice(&key_bytes);
    
    let secp_private_key = SecretKey::from_slice(&key_array)?;
    
    // Generate EVM address
    let secp = Secp256k1::new();
    let public_key = PublicKey::from_secret_key(&secp, &secp_private_key);
    let public_key_bytes = public_key.serialize_uncompressed();
    let public_key_hash = alloy_primitives::keccak256(&public_key_bytes[1..]);
    let evm_address = Address::from_slice(&public_key_hash[12..]);
    
    // Create deterministic seed for BLS key generation
    let mut hasher = Blake2b::<U64>::new();
    hasher.update(b"NEURA_CONSENSUS_KEY_DERIVATION_V1");
    hasher.update(&key_bytes);
    
    let hash = hasher.finalize();
    let seed_bytes: [u8; 32] = hash.as_slice()[..32].try_into().unwrap();
    
    // Generate deterministic BLS keypair from seed
    let mut rng = StdRng::from_seed(seed_bytes);
    let bls_keypair = BLS12381KeyPair::generate(&mut rng);
    
    // Extract keys
    let private_key = bls_keypair.private().encode_base64();
    
    // Recreate keypair from private key to get public key
    let private_key_obj = BLS12381PrivateKey::decode_base64(&private_key)
        .map_err(|e| anyhow::anyhow!("Failed to decode private key: {}", e))?;
    let keypair_reconstructed = BLS12381KeyPair::from(private_key_obj);
    let public_key = keypair_reconstructed.public().encode_base64();
    
    Ok((private_key, public_key, evm_address))
}

fn main() -> Result<()> {
    println!("🔑 Neura Validator Key Generator");
    println!("Generating BLS consensus keys from EVM private keys...");
    println!();
    
    let validator_dir = "test_validators";
    
    // Process each validator file
    for entry in fs::read_dir(validator_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                if filename.starts_with("validator-") {
                    println!("📄 Processing {}", filename);
                    
                    // Read current config
                    let content = fs::read_to_string(&path)?;
                    let mut config: ValidatorConfig = serde_json::from_str(&content)?;
                    
                    // Generate BLS keys if not already present
                    if config.consensus_private_key.is_none() {
                        let (private_key, public_key, evm_address) = 
                            generate_bls_keys_from_evm(&config.evm_private_key)?;
                        
                        config.consensus_private_key = Some(private_key);
                        config.consensus_public_key = Some(public_key);
                        config.evm_address = Some(format!("{:?}", evm_address));
                        
                        // Write updated config
                        let updated_content = serde_json::to_string_pretty(&config)?;
                        fs::write(&path, updated_content)?;
                        
                        println!("  ✅ Generated BLS keys for {}", filename);
                        println!("  📧 EVM Address: {:?}", evm_address);
                        println!("  🔐 BLS Public Key: {}", config.consensus_public_key.as_ref().unwrap());
                    } else {
                        println!("  ⏭️  Already has consensus keys, skipping");
                    }
                    println!();
                }
            }
        }
    }
    
    println!("✅ Validator key generation complete!");
    println!();
    println!("🚀 You can now run: ./start_multivalidator_test.sh");
    
    Ok(())
}