use serde::{Deserialize, Serialize};
use fastcrypto::traits::EncodeDecodeBase64;
use std::fs;
use anyhow::Result;

// Import the modules directly
use example_narwhal_bullshark_consensus::validator_keys::ValidatorKeyPair;
use secp256k1::SecretKey;

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

fn main() -> Result<()> {
    println!("🔑 Generating BLS consensus keys from EVM private keys");
    println!();
    
    let validator_dir = "test_validators";
    let mut updated_count = 0;
    
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
                        // Parse EVM private key
                        let key_bytes = alloy_primitives::hex::decode(config.evm_private_key.trim_start_matches("0x"))?;
                        let mut key_array = [0u8; 32];
                        key_array.copy_from_slice(&key_bytes);
                        
                        let evm_private_key = SecretKey::from_byte_array(&key_array)?;
                        let keypair = ValidatorKeyPair::from_evm_key_deterministic(evm_private_key)?;
                        
                        config.consensus_private_key = Some(keypair.consensus_private_key);
                        config.consensus_public_key = Some(keypair.consensus_public_key);
                        config.evm_address = Some(format!("{:?}", keypair.evm_address));
                        
                        // Write updated config
                        let updated_content = serde_json::to_string_pretty(&config)?;
                        fs::write(&path, updated_content)?;
                        
                        println!("  ✅ Generated BLS keys for {}", filename);
                        println!("  📧 EVM Address: {:?}", keypair.evm_address);
                        println!("  🔐 BLS Public Key: {}", keypair.consensus_public_key);
                        updated_count += 1;
                    } else {
                        println!("  ⏭️  Already has consensus keys, skipping");
                    }
                    println!();
                }
            }
        }
    }
    
    println!("✅ Updated {} validator files with BLS consensus keys!", updated_count);
    println!("🚀 You can now run: ./start_multivalidator_test.sh");
    
    Ok(())
}