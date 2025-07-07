use example_narwhal_bullshark_consensus::validator_keys::ValidatorKeyPair;
use secp256k1::SecretKey;
use fastcrypto::traits::EncodeDecodeBase64;

fn main() {
    println!("Testing consensus key generation from EVM private keys:");
    println!();
    
    let test_keys = vec![
        ("validator-0", "0x1111111111111111111111111111111111111111111111111111111111111111"),
        ("validator-1", "0x2222222222222222222222222222222222222222222222222222222222222222"),
        ("validator-2", "0x3333333333333333333333333333333333333333333333333333333333333333"),
        ("validator-3", "0x4444444444444444444444444444444444444444444444444444444444444444"),
    ];
    
    for (name, hex_key) in test_keys {
        let key_bytes = alloy_primitives::hex::decode(hex_key.trim_start_matches("0x")).unwrap();
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&key_bytes);
        
        let evm_private_key = SecretKey::from_byte_array(&key_array).unwrap();
        let keypair = ValidatorKeyPair::from_evm_key_deterministic(evm_private_key).unwrap();
        
        println!("{}: ", name);
        println!("  EVM Address: {:?}", keypair.evm_address);
        println!("  Consensus Public Key: {}", keypair.consensus_keypair.public().encode_base64());
        println!();
    }
    
    // Also check if the problematic key matches any of these
    let problematic_key = "o48QpRa9zKKjrMlLKmmB38P+WA/1M8ywRgju7kev/P5aiSosnm49I9GlM7hazlwLGXjqv9eS5MKqFPa5sXvj1lF1isg3BhFLhq1AS77HJQE+gOYOTWyMy31csVNJl1I9";
    println!("Problematic key: {}", problematic_key);
}