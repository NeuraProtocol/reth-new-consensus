#!/usr/bin/env python3
"""
Send test transactions to Reth node
"""

import json
import time
import sys
from web3 import Web3
from eth_account import Account

# Connection
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))

# Test account (from validator-0.json)
private_key = '0xb71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291'
account = Account.from_key(private_key)

# Check connection
if not w3.is_connected():
    print("Failed to connect to node")
    sys.exit(1)

print(f"Connected to node. Chain ID: {w3.eth.chain_id}")
print(f"Current block: {w3.eth.block_number}")
print(f"Account: {account.address}")

# Get balance
balance = w3.eth.get_balance(account.address)
print(f"Balance: {w3.from_wei(balance, 'ether')} ETH")

# For Neura (chain 266), the genesis allocates funds to validator addresses
# If balance is 0, that's expected - we'll try anyway as consensus might accept it

# Build transaction
nonce = w3.eth.get_transaction_count(account.address)
to_address = Web3.to_checksum_address('0x1563915e194d8cfba1943570603f7606a3115508')  # validator-1

tx = {
    'nonce': nonce,
    'to': to_address,
    'value': w3.to_wei(0.001, 'ether'),
    'gas': 21000,
    'gasPrice': w3.to_wei(1, 'gwei'),
    'chainId': 266,  # Neura chain ID
}

# Sign transaction
signed_tx = account.sign_transaction(tx)

# Send transaction
try:
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    print(f"Transaction sent! Hash: {tx_hash.hex()}")
    
    # Wait for receipt
    print("Waiting for transaction receipt...")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    print(f"Transaction mined in block {receipt['blockNumber']}")
    print(f"Gas used: {receipt['gasUsed']}")
except Exception as e:
    print(f"Transaction failed: {e}")
    print("This is expected if the account has no balance in genesis")
    print("Let's try sending multiple test transactions to trigger batch creation...")
    
    # Send multiple dummy transactions to trigger batching
    print("\nSending test transactions to trigger consensus...")
    for i in range(10):
        test_tx = {
            'nonce': nonce + i,
            'to': to_address,
            'value': 0,  # 0 value transfer
            'gas': 21000,
            'gasPrice': 0,  # 0 gas price for testing
            'chainId': 266,
            'data': f'0x{i:064x}'.encode().hex()  # Add some data to make each tx unique
        }
        
        signed = account.sign_transaction(test_tx)
        try:
            hash = w3.eth.send_raw_transaction(signed.rawTransaction)
            print(f"Test tx {i+1}/10 sent: {hash.hex()}")
        except Exception as e:
            print(f"Test tx {i+1}/10 failed: {e}")
        
        time.sleep(0.1)

print("\nDone sending test transactions")
print("Check if block number increased...")
time.sleep(5)
new_block = w3.eth.block_number
print(f"New block number: {new_block}")