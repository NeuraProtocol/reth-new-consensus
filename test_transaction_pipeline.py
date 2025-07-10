#!/usr/bin/env python3
"""
Test script to verify transaction pipeline: mempool -> consensus -> block creation
"""

import json
import time
import requests
from typing import Dict, Any

def rpc_call(method: str, params: list = None, port: int = 8545) -> Dict[str, Any]:
    """Make an RPC call to the node"""
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or [],
        "id": 1
    }
    
    try:
        response = requests.post(f"http://localhost:{port}", json=payload, timeout=5)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def test_basic_rpc():
    """Test basic RPC connectivity"""
    print("=== Testing Basic RPC ===")
    
    # Test node connectivity
    result = rpc_call("net_version")
    print(f"Net version: {result}")
    
    # Test current block number
    result = rpc_call("eth_blockNumber")
    print(f"Block number: {result}")
    
    # Test chain ID
    result = rpc_call("eth_chainId")
    print(f"Chain ID: {result}")
    
    # Test peer count
    result = rpc_call("net_peerCount")
    print(f"Peer count: {result}")

def test_mempool_status():
    """Test mempool and pending transactions"""
    print("\n=== Testing Mempool Status ===")
    
    # Check pending transactions
    result = rpc_call("txpool_status")
    print(f"Txpool status: {result}")
    
    # Check mempool content
    result = rpc_call("txpool_content")
    print(f"Txpool content: {result}")

def test_consensus_status():
    """Test consensus-specific RPC methods"""
    print("\n=== Testing Consensus Status ===")
    
    # Try consensus-specific methods if available
    try:
        # Test if custom consensus RPC methods exist
        result = rpc_call("consensus_status")
        print(f"Consensus status: {result}")
    except:
        print("No custom consensus RPC methods available")

def send_test_transaction():
    """Send a test transaction to validate pipeline"""
    print("\n=== Sending Test Transaction ===")
    
    # Create a simple transaction (will fail due to no account/balance, but should enter mempool)
    tx_data = {
        "from": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "to": "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        "value": "0x16345785d8a0000",  # 0.1 ETH
        "gas": "0x5208",
        "gasPrice": "0x4a817c800"
    }
    
    result = rpc_call("eth_sendTransaction", [tx_data])
    print(f"Transaction result: {result}")
    
    # Also try sending raw transaction
    # Note: This will fail but should show us the error handling
    raw_tx = "0x02f86e0108168405f5e10082520894742d35cc6634c0532925a3b8d68f22b7cbfc682f870DE0B6B3A764000080c080a0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0a0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"
    
    result = rpc_call("eth_sendRawTransaction", [raw_tx])
    print(f"Raw transaction result: {result}")

def monitor_block_production():
    """Monitor for block production over time"""
    print("\n=== Monitoring Block Production ===")
    
    initial_block = rpc_call("eth_blockNumber")
    print(f"Initial block: {initial_block}")
    
    print("Waiting 10 seconds for block production...")
    time.sleep(10)
    
    final_block = rpc_call("eth_blockNumber")
    print(f"Final block: {final_block}")
    
    if initial_block.get("result") == final_block.get("result"):
        print("⚠️  No new blocks produced during monitoring period")
    else:
        print("✅ Block production detected!")

def main():
    print("🧪 Testing Narwhal+Bullshark Transaction Pipeline")
    print("=" * 60)
    
    test_basic_rpc()
    test_mempool_status()
    test_consensus_status()
    send_test_transaction()
    monitor_block_production()
    
    print("\n" + "=" * 60)
    print("✅ Pipeline test completed")

if __name__ == "__main__":
    main()