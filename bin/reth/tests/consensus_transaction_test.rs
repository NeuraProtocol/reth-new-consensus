//! Transaction serialization tests for Narwhal + Bullshark consensus
//! 
//! Tests for:
//! - Basic transaction serialization functionality
//! - JSON serialization fallback
//! - Error handling

use alloy_primitives::{Address, U256, TxKind, Signature};
use reth_primitives::{TransactionSigned, Transaction};
use alloy_consensus::TxLegacy;
use serde_json;

/// Test basic transaction creation and hashing
fn create_test_transaction() -> TransactionSigned {
    // Create a simple legacy transaction using the builder pattern
    
    let tx = Transaction::Legacy(TxLegacy {
        chain_id: Some(1),
        nonce: 1,
        gas_price: 20_000_000_000u128,
        gas_limit: 21_000u64,
        to: TxKind::Call(Address::random()),
        value: U256::from(1000000000000000000u64),
        input: Default::default(),
    });

    let signature = Signature::new(U256::from(1), U256::from(1), false);
    TransactionSigned::new_unhashed(tx, signature)
}

/// Test versioned serialization approach
fn serialize_transaction_versioned(transaction: &TransactionSigned) -> eyre::Result<Vec<u8>> {
    // Try JSON serialization (version 0x02)
    if let Ok(json_data) = serde_json::to_vec(transaction) {
        let mut result = vec![0x02]; // Version 2: JSON
        result.extend_from_slice(&json_data);
        return Ok(result);
    }

    // Final fallback: hash only (version 0x00)
    let hash = transaction.hash();
    let mut result = vec![0x00]; // Version 0: hash only
    result.extend_from_slice(hash.as_slice());
    Ok(result)
}

/// Test versioned deserialization
fn deserialize_transaction_versioned(data: &[u8]) -> eyre::Result<TransactionSigned> {
    if data.is_empty() {
        return Err(eyre::eyre!("Empty serialized data"));
    }

    let version = data[0];
    let payload = &data[1..];

    match version {
        0x02 => {
            // JSON deserialization
            serde_json::from_slice(payload)
                .map_err(|e| eyre::eyre!("JSON deserialization failed: {}", e))
        }
        0x00 => {
            // Hash-only fallback (can't reconstruct full transaction)
            Err(eyre::eyre!("Cannot reconstruct transaction from hash-only serialization"))
        }
        _ => {
            Err(eyre::eyre!("Unknown serialization version: {}", version))
        }
    }
}

#[tokio::test]
async fn test_basic_transaction_creation() -> eyre::Result<()> {
    let transaction = create_test_transaction();
    
    // Verify basic properties
    let hash = transaction.hash();
    assert!(!hash.is_zero(), "Transaction hash should not be zero");
    
    println!("✅ Basic transaction creation test passed - Hash: {:?}", hash);
    Ok(())
}

#[tokio::test]
async fn test_json_serialization() -> eyre::Result<()> {
    let transaction = create_test_transaction();
    
    // Test JSON serialization directly
    let json_result = serde_json::to_vec(&transaction);
    assert!(json_result.is_ok(), "JSON serialization should work for TransactionSigned");
    
    let json_bytes = json_result?;
    assert!(!json_bytes.is_empty(), "JSON bytes should not be empty");
    
    // Test JSON deserialization
    let deserialized: TransactionSigned = serde_json::from_slice(&json_bytes)?;
    assert_eq!(deserialized.hash(), transaction.hash(), "Hash mismatch after JSON roundtrip");
    
    println!("✅ JSON serialization test passed");
    Ok(())
}

#[tokio::test]
async fn test_versioned_serialization() -> eyre::Result<()> {
    let transaction = create_test_transaction();
    let original_hash = transaction.hash();
    
    // Test versioned serialization
    let serialized = serialize_transaction_versioned(&transaction)?;
    
    // Verify version byte
    assert!(!serialized.is_empty());
    let version = serialized[0];
    assert_eq!(version, 0x02, "Expected JSON serialization version");
    
    // Test versioned deserialization
    let deserialized = deserialize_transaction_versioned(&serialized)?;
    assert_eq!(deserialized.hash(), original_hash, "Hash mismatch after versioned roundtrip");
    
    println!("✅ Versioned serialization test passed");
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> eyre::Result<()> {
    // Test empty data
    let empty_result = deserialize_transaction_versioned(&[]);
    assert!(empty_result.is_err(), "Should fail on empty data");
    
    // Test unknown version
    let unknown_version_data = vec![0xFF, 1, 2, 3, 4];
    let unknown_result = deserialize_transaction_versioned(&unknown_version_data);
    assert!(unknown_result.is_err(), "Should fail on unknown version");
    
    // Test corrupted JSON data
    let corrupted_json = vec![0x02, b'{', b'c', b'o', b'r', b'r', b'u', b'p', b't'];
    let corrupted_result = deserialize_transaction_versioned(&corrupted_json);
    assert!(corrupted_result.is_err(), "Should fail on corrupted JSON");
    
    // Test hash-only deserialization (should fail as expected)
    let hash_only = vec![0x00; 33]; // Version + 32 byte hash
    let hash_result = deserialize_transaction_versioned(&hash_only);
    assert!(hash_result.is_err(), "Should fail on hash-only deserialization");
    
    println!("✅ Error handling test passed");
    Ok(())
}

#[tokio::test]
async fn test_serialization_performance() -> eyre::Result<()> {
    let transaction = create_test_transaction();
    const NUM_ITERATIONS: usize = 1000;
    
    // Test JSON serialization performance
    let start = std::time::Instant::now();
    for _ in 0..NUM_ITERATIONS {
        let _serialized = serde_json::to_vec(&transaction)?;
    }
    let json_duration = start.elapsed();
    
    // Test versioned serialization performance
    let start = std::time::Instant::now();
    for _ in 0..NUM_ITERATIONS {
        let _serialized = serialize_transaction_versioned(&transaction)?;
    }
    let versioned_duration = start.elapsed();
    
    println!("📊 Serialization performance ({} iterations):", NUM_ITERATIONS);
    println!("   - JSON serialization: {:?}", json_duration);
    println!("   - Versioned serialization: {:?}", versioned_duration);
    
    // Versioned should be comparable to JSON
    let overhead_ratio = versioned_duration.as_nanos() as f64 / json_duration.as_nanos() as f64;
    assert!(overhead_ratio < 2.0, "Versioned serialization overhead too high: {:.2}x", overhead_ratio);
    
    println!("✅ Serialization performance test passed - overhead: {:.2}x", overhead_ratio);
    Ok(())
} 