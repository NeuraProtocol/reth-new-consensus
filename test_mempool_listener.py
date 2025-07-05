#!/usr/bin/env python3
"""
Test script to monitor mempool activity
"""

import json
import time
import requests
from web3 import Web3

# Connect to the node
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))

print("Testing mempool listener...")
print(f"Connected: {w3.is_connected()}")
print(f"Chain ID: {w3.eth.chain_id}")
print(f"Current block: {w3.eth.block_number}")

# Check mempool content via RPC
def check_mempool():
    response = requests.post('http://localhost:8545', json={
        'jsonrpc': '2.0',
        'method': 'txpool_content',
        'params': [],
        'id': 1
    })
    data = response.json()
    if 'result' in data:
        pending = data['result']['pending']
        queued = data['result']['queued']
        
        pending_count = sum(len(txs) for txs in pending.values())
        queued_count = sum(len(txs) for txs in queued.values())
        
        return pending_count, queued_count, pending
    return 0, 0, {}

# Initial state
pending_count, queued_count, pending_txs = check_mempool()
print(f"\nInitial mempool state:")
print(f"  Pending: {pending_count}")
print(f"  Queued: {queued_count}")

if pending_count > 0:
    print("\nPending transactions:")
    for addr, txs in list(pending_txs.items())[:2]:  # Show first 2 addresses
        print(f"  From {addr}:")
        for nonce, tx in list(txs.items())[:3]:  # Show first 3 txs
            print(f"    Nonce {nonce}: hash={tx['hash']}, to={tx['to']}, value={tx['value']}")

print("\nMonitoring for changes...")
last_pending = pending_count
last_block = w3.eth.block_number

for i in range(30):  # Monitor for 30 seconds
    time.sleep(1)
    
    # Check mempool
    current_pending, current_queued, _ = check_mempool()
    current_block = w3.eth.block_number
    
    # Report changes
    if current_pending != last_pending:
        print(f"[{i+1}s] Mempool change: {last_pending} -> {current_pending} pending txs")
        last_pending = current_pending
        
    if current_block != last_block:
        print(f"[{i+1}s] New block! #{current_block}")
        block = w3.eth.get_block(current_block)
        print(f"      Transactions: {len(block['transactions'])}")
        print(f"      Parent hash: {block['parentHash'].hex()}")
        last_block = current_block
        
    # Show status every 5 seconds
    if (i + 1) % 5 == 0:
        print(f"[{i+1}s] Status: {current_pending} pending, block #{current_block}")

print("\nTest complete.")