#!/usr/bin/env python3
"""
Send a fresh transaction to test mempool listener
"""

import time
from web3 import Web3
from eth_account import Account

# Connect to the node
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))

# Use a different private key to avoid nonce issues
private_key = '0x2222222222222222222222222222222222222222222222222222222222222222'
account = Account.from_key(private_key)

print(f"Sending fresh transaction from new account...")
print(f"Account: {account.address}")
print(f"Balance: {w3.eth.get_balance(account.address)} wei")

# Get current state
nonce = w3.eth.get_transaction_count(account.address)
print(f"Nonce: {nonce}")

# Send a simple transaction
tx = {
    'nonce': nonce,
    'to': '0x3333333333333333333333333333333333333333',
    'value': 0,
    'gas': 100000,
    'gasPrice': w3.to_wei(1, 'gwei'),
    'chainId': 266,
    'data': '0x' + 'FRESH_TX_TEST'.encode().hex()
}

print(f"\nSending transaction...")
signed_tx = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
print(f"Transaction sent: {tx_hash.hex()}")

# Wait a bit
time.sleep(2)

# Check if it's in the mempool
try:
    response = w3.provider.make_request('txpool_content', [])
    pending = response['result']['pending']
    
    # Count total pending
    total_pending = sum(len(txs) for txs in pending.values())
    
    # Check if our transaction is there
    our_txs = pending.get(account.address.lower(), {})
    
    print(f"\nTotal pending transactions: {total_pending}")
    print(f"Our transactions: {len(our_txs)}")
    
    if our_txs:
        print("Transaction successfully added to mempool!")
except Exception as e:
    print(f"Error checking mempool: {e}")

# Monitor for block creation
print("\nMonitoring for block creation...")
initial_block = w3.eth.block_number
for i in range(20):
    time.sleep(1)
    current_block = w3.eth.block_number
    if current_block > initial_block:
        print(f"✅ New block created! Block #{current_block}")
        block = w3.eth.get_block(current_block)
        print(f"   Transactions: {len(block['transactions'])}")
        print(f"   Parent hash: {block['parentHash'].hex()}")
        break
    else:
        print(f"   [{i+1}s] Still at block {current_block}...")

print("\nChecking logs for transaction forwarding...")
import subprocess
result = subprocess.run(['grep', '-n', 'Forwarding transaction', '/home/peastew/.neura/node1/node.log'], 
                       capture_output=True, text=True)
if result.stdout:
    print("Found transaction forwarding logs:")
    print(result.stdout)
else:
    print("No transaction forwarding logs found")