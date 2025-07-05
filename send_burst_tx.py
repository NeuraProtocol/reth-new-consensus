#!/usr/bin/env python3
"""
Send a burst of transactions to trigger consensus
"""

import time
from web3 import Web3
from eth_account import Account

# Connect to the node
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
private_key = '0x1111111111111111111111111111111111111111111111111111111111111111'
account = Account.from_key(private_key)

print(f"Sending burst of 100 transactions to trigger consensus...")
print(f"Account: {account.address}")

# Get initial state
initial_block = w3.eth.block_number
initial_pending = w3.eth.get_transaction_count(account.address, 'pending')
nonce = w3.eth.get_transaction_count(account.address)

print(f"Initial block: {initial_block}, Nonce: {nonce}, Pending nonce: {initial_pending}")

# Send 100 transactions rapidly
successful = 0
for i in range(100):
    try:
        tx = {
            'nonce': nonce + i,
            'to': '0x0000000000000000000000000000000000000001',
            'value': 0,
            'gas': 100000,
            'gasPrice': w3.to_wei(1, 'gwei'),
            'chainId': 266,
            'data': f'0x{i:064x}'
        }
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        successful += 1
        
        if i % 10 == 0:
            print(f"Sent {i+1}/100 transactions...")
            
    except Exception as e:
        print(f"Tx {i+1} failed: {e}")
        break

print(f"\nSuccessfully sent {successful} transactions")

# Check mempool
try:
    response = w3.provider.make_request('txpool_content', [])
    pending_count = len(response['result']['pending'].get(account.address, {}))
    print(f"Pending transactions in mempool: {pending_count}")
except:
    pass

# Wait for blocks
print("\nWaiting for consensus to process transactions...")
for i in range(10):
    time.sleep(1)
    new_block = w3.eth.block_number
    if new_block > initial_block:
        print(f"✅ New block created! Block #{new_block}")
        block = w3.eth.get_block(new_block)
        print(f"   Transactions in block: {len(block['transactions'])}")
        print(f"   Parent hash: {block['parentHash'].hex()}")
        break
    else:
        print(f"   Still at block {new_block}...")

if w3.eth.block_number == initial_block:
    print("\n❌ No new blocks created. Check logs for issues.")