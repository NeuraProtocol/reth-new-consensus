#!/usr/bin/env python3
"""
Simple transaction sender for testing Neura consensus
"""

import time
from web3 import Web3
from eth_account import Account

# Connect to the node
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))

# Use the test private key from validator-0
# This is 0x1111111111111111111111111111111111111111111111111111111111111111
private_key = '0x1111111111111111111111111111111111111111111111111111111111111111'
account = Account.from_key(private_key)

print(f"Connected: {w3.is_connected()}")
print(f"Chain ID: {w3.eth.chain_id}")
print(f"Current block: {w3.eth.block_number}")
print(f"Using account: {account.address}")

# Check balance
balance = w3.eth.get_balance(account.address)
print(f"Balance: {w3.from_wei(balance, 'ether')} ANKR")

# Send 10 test transactions
print("\nSending test transactions...")
nonce = w3.eth.get_transaction_count(account.address)

for i in range(10):
    try:
        tx = {
            'nonce': nonce + i,
            'to': '0x0000000000000000000000000000000000000001',  # Precompile address
            'value': 0,
            'gas': 100000,  # Higher gas limit for data
            'gasPrice': w3.to_wei(1, 'gwei'),
            'chainId': 266,
            'data': f'0x{i:064x}'  # Some data to make tx unique
        }
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"Tx {i+1}: {tx_hash.hex()}")
        
    except Exception as e:
        print(f"Tx {i+1} failed: {e}")
    
    time.sleep(0.1)

# Check if block number increased
print("\nWaiting for blocks...")
time.sleep(5)
new_block = w3.eth.block_number
print(f"New block number: {new_block}")

# Try to get the latest block
if new_block > 0:
    block = w3.eth.get_block(new_block)
    print(f"Block {new_block} has {len(block['transactions'])} transactions")
    print(f"Parent hash: {block['parentHash'].hex()}")