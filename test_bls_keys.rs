use fastcrypto::traits::EncodeDecodeBase64;
use fastcrypto::bls12381::BLS12381PublicKey;

fn main() {
    let key_str = "9Jui0JjZbmG6mF4JZ8kYBFgBQyUXyUACcWIdSZzKF3mgHbccqLN2SlYv+ePI3LrB";
    match BLS12381PublicKey::decode_base64(key_str) {
        Ok(key) => println!("✅ BLS key decode succeeded: {:?}", key),
        Err(e) => println!("❌ BLS key decode failed: {}", e),
    }
}