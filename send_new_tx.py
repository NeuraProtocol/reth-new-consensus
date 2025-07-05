#!/usr/bin/env python3
"""
Send a new transaction with higher nonce to trigger listener
"""

import time
from web3 import Web3
from eth_account import Account

# Connect to the node
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
private_key = '0x1111111111111111111111111111111111111111111111111111111111111111'
account = Account.from_key(private_key)

# Get current nonce (should be 10 since we have 10 pending)
nonce = w3.eth.get_transaction_count(account.address, 'pending')
print(f"Account: {account.address}")
print(f"Current pending nonce: {nonce}")

# Send a new transaction with next nonce
tx = {
    'nonce': nonce,
    'to': '0x2222222222222222222222222222222222222222',  # Different address
    'value': w3.to_wei(0.001, 'ether'),
    'gas': 100000,
    'gasPrice': w3.to_wei(2, 'gwei'),  # Higher gas price
    'chainId': 266,
    'data': '0x' + 'NEW_TRANSACTION_TEST'.encode().hex()
}

print(f"\nSending new transaction with nonce {nonce}...")
signed_tx = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
print(f"Transaction sent: {tx_hash.hex()}")

# Check mempool
time.sleep(1)
try:
    response = w3.provider.make_request('txpool_content', [])
    pending_count = sum(len(txs) for txs in response['result']['pending'].values())
    print(f"\nPending transactions in mempool: {pending_count}")
except Exception as e:
    print(f"Error checking mempool: {e}")

# Wait and check for blocks
print("\nWaiting for consensus to process transaction...")
initial_block = w3.eth.block_number
for i in range(10):
    time.sleep(1)
    current_block = w3.eth.block_number
    if current_block > initial_block:
        print(f"✅ New block created! Block #{current_block}")
        break
    else:
        print(f"   Still at block {current_block}...")