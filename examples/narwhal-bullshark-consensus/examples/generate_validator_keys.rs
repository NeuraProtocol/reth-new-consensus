use alloy_primitives::{hex, Address};
use fastcrypto::{
    bls12381::BLS12381KeyPair,
    traits::{EncodeDecodeBase64, KeyPair},
};
use rand_08::{rngs::StdRng, SeedableRng};
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use blake2::{Blake2b, Digest, digest::consts::U32};
use std::{env, fs};

/// Validator data structure matching the expected format
#[derive(Debug, Serialize, Deserialize)]
struct ValidatorData {
    name: String,
    evm_address: String,
    evm_private_key: String,
    consensus_private_key: String,
    consensus_public_key: String,
    stake: u64,
    network_address: String,
    worker_port_range: String,
}

/// Committee structure
#[derive(Debug, Serialize, Deserialize)]
struct Committee {
    epoch: u64,
    validators: Vec<CommitteeValidator>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitteeValidator {
    name: String,
    evm_address: String,
    consensus_public_key: String,
    stake: u64,
    network_address: String,
    worker_port_range: String,
}

/// Derive consensus key seed from EVM private key
/// This matches the validator_keys.rs implementation
fn derive_consensus_seed(evm_key: &SecretKey) -> [u8; 32] {
    let mut hasher = Blake2b::<U32>::new();
    hasher.update(b"NEURA_CONSENSUS_KEY_DERIVATION_V1");
    hasher.update(&evm_key.secret_bytes());
    hasher.finalize().into()
}

/// Generate BLS keypair from seed
fn generate_bls_keypair(seed: [u8; 32]) -> BLS12381KeyPair {
    let mut rng = StdRng::from_seed(seed);
    BLS12381KeyPair::generate(&mut rng)
}

/// Convert secp256k1 public key to Ethereum address
fn public_key_to_address(public_key: &PublicKey) -> Address {
    let public_key_bytes = public_key.serialize_uncompressed();
    
    // Skip the first byte (0x04 prefix) and hash the rest
    let hash = alloy_primitives::keccak256(&public_key_bytes[1..]);
    
    // Take the last 20 bytes of the hash
    Address::from_slice(&hash[12..])
}

/// Generate validator data from Ethereum private key
fn generate_validator_data(
    eth_private_key_hex: &str,
    name: &str,
    stake: u64,
    network_address: &str,
    worker_port_range: &str,
) -> Result<ValidatorData, Box<dyn std::error::Error>> {
    // Parse Ethereum private key
    let eth_key_hex = if eth_private_key_hex.starts_with("0x") {
        &eth_private_key_hex[2..]
    } else {
        eth_private_key_hex
    };
    
    let eth_key_bytes = hex::decode(eth_key_hex)?;
    let eth_private_key = SecretKey::from_slice(&eth_key_bytes)?;
    
    // Generate Ethereum address
    let secp = Secp256k1::new();
    let eth_public_key = PublicKey::from_secret_key(&secp, &eth_private_key);
    let eth_address = public_key_to_address(&eth_public_key);
    
    // Generate deterministic BLS keys
    let consensus_seed = derive_consensus_seed(&eth_private_key);
    let bls_keypair = generate_bls_keypair(consensus_seed);
    
    // Get public key first (before moving keypair)
    let consensus_public_key = bls_keypair.public().encode_base64();
    let public_key_len = bls_keypair.public().as_ref().len();
    
    // Get private key (this consumes keypair)
    let consensus_private_key = bls_keypair.private().encode_base64();
    
    // Verify the encoding (debug)
    println!("Generated BLS public key length: {} bytes", public_key_len);
    
    Ok(ValidatorData {
        name: name.to_string(),
        evm_address: format!("0x{}", hex::encode(eth_address)),
        evm_private_key: if eth_private_key_hex.starts_with("0x") {
            eth_private_key_hex.to_string()
        } else {
            format!("0x{}", eth_private_key_hex)
        },
        consensus_private_key,
        consensus_public_key,
        stake,
        network_address: network_address.to_string(),
        worker_port_range: worker_port_range.to_string(),
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} <command> [args...]", args[0]);
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  single <eth_private_key> [name] [stake] [network_address] [worker_port_range]");
        eprintln!("  batch - Generate test validators");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} single 0x1111111111111111111111111111111111111111111111111111111111111111", args[0]);
        eprintln!("  {} batch", args[0]);
        return Ok(());
    }
    
    match args[1].as_str() {
        "single" => {
            if args.len() < 3 {
                eprintln!("Error: Missing Ethereum private key");
                return Ok(());
            }
            
            let eth_private_key = &args[2];
            let name = args.get(3).map(|s| s.as_str()).unwrap_or("Validator");
            let stake = args.get(4)
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000);
            let network_address = args.get(5)
                .map(|s| s.as_str())
                .unwrap_or("127.0.0.1:9001");
            let worker_port_range = args.get(6)
                .map(|s| s.as_str())
                .unwrap_or("19000:19003");
            
            let validator_data = generate_validator_data(
                eth_private_key,
                name,
                stake,
                network_address,
                worker_port_range,
            )?;
            
            // Print JSON
            println!("{}", serde_json::to_string_pretty(&validator_data)?);
            
            // Save to file
            let filename = format!("{}.json", name.to_lowercase().replace(' ', "_"));
            fs::write(&filename, serde_json::to_string_pretty(&validator_data)?)?;
            println!("\nSaved to {}", filename);
        }
        
        "batch" => {
            let test_validators = vec![
                ("0x1111111111111111111111111111111111111111111111111111111111111111", "Test Validator 0", 1000, "127.0.0.1:9001", "19000:19003"),
                ("0x2222222222222222222222222222222222222222222222222222222222222222", "Test Validator 1", 1000, "127.0.0.1:9002", "19004:19007"),
                ("0x3333333333333333333333333333333333333333333333333333333333333333", "Test Validator 2", 1000, "127.0.0.1:9003", "19008:19011"),
                ("0x4444444444444444444444444444444444444444444444444444444444444444", "Test Validator 3", 1000, "127.0.0.1:9004", "19012:19015"),
            ];
            
            let mut validators = Vec::new();
            let mut committee_validators = Vec::new();
            
            for (i, (eth_key, name, stake, network_addr, worker_ports)) in test_validators.iter().enumerate() {
                println!("Generating validator {}...", i);
                
                let validator_data = generate_validator_data(
                    eth_key,
                    name,
                    *stake,
                    network_addr,
                    worker_ports,
                )?;
                
                // Save individual validator file
                let filename = format!("validator-{}.json", i);
                fs::write(&filename, serde_json::to_string_pretty(&validator_data)?)?;
                println!("  Saved {}", filename);
                println!("  EVM Address: {}", validator_data.evm_address);
                println!("  Consensus Public Key: {}...", &validator_data.consensus_public_key[..32]);
                
                // Prepare committee entry
                committee_validators.push(CommitteeValidator {
                    name: validator_data.name.clone(),
                    evm_address: validator_data.evm_address.clone(),
                    consensus_public_key: validator_data.consensus_public_key.clone(),
                    stake: validator_data.stake,
                    network_address: validator_data.network_address.clone(),
                    worker_port_range: validator_data.worker_port_range.clone(),
                });
                
                validators.push(validator_data);
            }
            
            // Create committee.json
            let committee = Committee {
                epoch: 0,
                validators: committee_validators,
            };
            
            fs::write("committee.json", serde_json::to_string_pretty(&committee)?)?;
            println!("\nGenerated committee.json");
            
            println!("\nAll files generated successfully!");
        }
        
        _ => {
            eprintln!("Unknown command: {}", args[1]);
        }
    }
    
    Ok(())
}