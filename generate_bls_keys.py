#!/usr/bin/env python3
"""
Generate BLS consensus keys for Neura validators from their EVM private keys.
This script updates the validator JSON files with the generated BLS keys.
"""

import json
import os
import hashlib
import secrets
from typing import Dict, Any, Tuple

# We'll use a simple implementation that generates deterministic keys
def generate_bls_keys_from_evm(evm_private_key: str) -> Tuple[str, str, str]:
    """
    Generate BLS keys deterministically from EVM private key.
    Returns (private_key_b64, public_key_b64, evm_address)
    
    Note: This is a simplified version. The actual implementation should use
    the fastcrypto BLS12-381 library with proper deterministic derivation.
    """
    # Remove 0x prefix if present
    hex_key = evm_private_key.replace('0x', '')
    key_bytes = bytes.fromhex(hex_key)
    
    # Generate EVM address (simplified - normally needs secp256k1 point decompression)
    # This is just for demonstration - real implementation needs proper secp256k1
    address_hash = hashlib.sha256(key_bytes).digest()
    evm_address = "0x" + address_hash[-20:].hex()
    
    # Create deterministic seed for BLS key generation
    seed_input = b"NEURA_CONSENSUS_KEY_DERIVATION_V1" + key_bytes
    seed = hashlib.blake2b(seed_input, digest_size=32).digest()
    
    # Generate pseudo-BLS keys (deterministic from seed)
    # In real implementation, this would use fastcrypto BLS12-381
    private_key_hash = hashlib.blake2b(seed + b"PRIVATE", digest_size=32).digest()
    public_key_hash = hashlib.blake2b(seed + b"PUBLIC", digest_size=48).digest()
    
    # Convert to base64 (simulating BLS key format)
    import base64
    private_key_b64 = base64.b64encode(private_key_hash).decode('utf-8')
    public_key_b64 = base64.b64encode(public_key_hash).decode('utf-8')
    
    return private_key_b64, public_key_b64, evm_address


def update_validator_file(filepath: str) -> bool:
    """Update a validator JSON file with generated BLS keys."""
    try:
        with open(filepath, 'r') as f:
            config = json.load(f)
        
        # Check if keys already exist
        if config.get('consensus_private_key') is not None:
            print(f"  ⏭️  Already has consensus keys, skipping")
            return False
        
        # Generate BLS keys
        evm_private_key = config['evm_private_key']
        private_key_b64, public_key_b64, evm_address = generate_bls_keys_from_evm(evm_private_key)
        
        # Update config
        config['consensus_private_key'] = private_key_b64
        config['consensus_public_key'] = public_key_b64
        config['evm_address'] = evm_address
        
        # Write back to file
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"  ✅ Generated BLS keys")
        print(f"  📧 EVM Address: {evm_address}")
        print(f"  🔐 BLS Public Key: {public_key_b64}")
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def main():
    print("🔑 Neura Validator Key Generator")
    print("⚠️  WARNING: This uses simplified BLS key generation for testing.")
    print("⚠️  For production, use the Rust implementation with fastcrypto.")
    print()
    
    validator_dir = "test_validators"
    updated_count = 0
    
    if not os.path.exists(validator_dir):
        print(f"❌ Directory {validator_dir} not found!")
        return
    
    # Process each validator file
    for filename in os.listdir(validator_dir):
        if filename.startswith("validator-") and filename.endswith(".json"):
            filepath = os.path.join(validator_dir, filename)
            print(f"📄 Processing {filename}")
            
            if update_validator_file(filepath):
                updated_count += 1
            print()
    
    print(f"✅ Updated {updated_count} validator files with BLS consensus keys!")
    print()
    print("🚀 You can now run: ./start_multivalidator_test.sh")
    print()
    print("📝 NOTE: These are simplified keys for testing. For production,")
    print("   compile and run the Rust implementation with proper BLS12-381 keys.")


if __name__ == "__main__":
    main()