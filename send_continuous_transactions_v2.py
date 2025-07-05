#!/usr/bin/env python3
"""
Send continuous test transactions to Reth/Neura node to test consensus
Version 2: Handles pending transactions and sends continuously
"""

import json
import time
import sys
import argparse
from web3 import Web3
from eth_account import Account
import threading
import random

def load_validator_key(validator_file):
    """Load private key from validator JSON file"""
    with open(validator_file, 'r') as f:
        data = json.load(f)
    return data['evm_private_key']

def clear_pending_transactions(w3, account):
    """Send replacement transactions to clear pending ones"""
    print(f"Checking for pending transactions...")
    
    # Get current nonce (executed transactions)
    current_nonce = w3.eth.get_transaction_count(account.address)
    # Get pending nonce (including pending transactions)
    pending_nonce = w3.eth.get_transaction_count(account.address, 'pending')
    
    if pending_nonce > current_nonce:
        print(f"Found {pending_nonce - current_nonce} pending transactions, clearing...")
        
        for nonce in range(current_nonce, pending_nonce):
            try:
                # Send a replacement transaction with higher gas price
                tx = {
                    'nonce': nonce,
                    'to': account.address,  # Send to self
                    'value': 0,
                    'gas': 21000,
                    'gasPrice': w3.to_wei(10, 'gwei'),  # Higher gas price to replace
                    'chainId': 266,
                }
                
                signed_tx = account.sign_transaction(tx)
                tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
                print(f"Sent replacement tx for nonce {nonce}: {tx_hash.hex()[:10]}...")
                
            except Exception as e:
                print(f"Failed to replace nonce {nonce}: {str(e)[:50]}")
        
        # Wait a bit for transactions to be processed
        time.sleep(2)
        print("Pending transactions cleared")
    else:
        print("No pending transactions found")

def send_continuous_transactions(node_url, private_key, target_address, batch_size, batch_delay, total_batches):
    """Send transactions in batches to ensure continuous flow through consensus rounds"""
    
    # Connect to node
    w3 = Web3(Web3.HTTPProvider(node_url))
    if not w3.is_connected():
        print(f"Failed to connect to {node_url}")
        return
    
    # Setup account
    account = Account.from_key(private_key)
    print(f"Using account {account.address}")
    
    # Clear any pending transactions first
    clear_pending_transactions(w3, account)
    
    # Get fresh nonce
    nonce = w3.eth.get_transaction_count(account.address)
    print(f"Starting with nonce: {nonce}")
    
    total_sent = 0
    total_failed = 0
    
    for batch_num in range(total_batches):
        print(f"\n--- Batch {batch_num + 1}/{total_batches} ---")
        batch_sent = 0
        batch_failed = 0
        
        for i in range(batch_size):
            try:
                # Create transaction with unique data
                tx = {
                    'nonce': nonce,
                    'to': target_address,
                    'value': 0,
                    'gas': 100000,
                    'gasPrice': w3.to_wei(1, 'gwei'),
                    'chainId': 266,
                    'data': f'0x{batch_num:04x}{i:04x}{random.randint(0, 0xFFFF):04x}'.encode().hex()
                }
                
                # Sign and send
                signed_tx = account.sign_transaction(tx)
                tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
                
                print(f"Sent tx {i+1}/{batch_size} - nonce {nonce} - {tx_hash.hex()[:10]}...")
                batch_sent += 1
                nonce += 1
                
            except Exception as e:
                print(f"Failed tx {i+1}/{batch_size} - {str(e)[:50]}...")
                batch_failed += 1
                
                # Try to recover correct nonce
                try:
                    new_nonce = w3.eth.get_transaction_count(account.address, 'pending')
                    if new_nonce > nonce:
                        nonce = new_nonce
                        print(f"Recovered nonce: {nonce}")
                except:
                    pass
        
        total_sent += batch_sent
        total_failed += batch_failed
        
        print(f"Batch {batch_num + 1} complete: {batch_sent} sent, {batch_failed} failed")
        
        # Check current block number
        try:
            block_num = w3.eth.block_number
            print(f"Current block number: {block_num}")
        except:
            pass
        
        # Delay between batches to allow consensus rounds to progress
        if batch_num < total_batches - 1:
            print(f"Waiting {batch_delay}s before next batch...")
            time.sleep(batch_delay)
    
    print(f"\n=== Summary ===")
    print(f"Total sent: {total_sent}")
    print(f"Total failed: {total_failed}")
    
    # Final block check
    try:
        final_block = w3.eth.block_number
        print(f"Final block number: {final_block}")
    except:
        pass

def main():
    parser = argparse.ArgumentParser(description='Send continuous transactions to test Neura consensus (v2)')
    parser.add_argument('--node', default='http://localhost:8545', help='Node RPC URL')
    parser.add_argument('--batch-size', type=int, default=20, help='Transactions per batch')
    parser.add_argument('--batch-delay', type=float, default=3.0, help='Delay between batches (seconds)')
    parser.add_argument('--total-batches', type=int, default=10, help='Number of batches to send')
    parser.add_argument('--validator', default='test_validators/validator-0.json', help='Validator key file')
    parser.add_argument('--target', help='Target address (default: random)')
    
    args = parser.parse_args()
    
    # Connect to check status
    w3 = Web3(Web3.HTTPProvider(args.node))
    if not w3.is_connected():
        print(f"Failed to connect to {args.node}")
        sys.exit(1)
    
    print(f"Connected to Neura node at {args.node}")
    print(f"Chain ID: {w3.eth.chain_id}")
    print(f"Current block: {w3.eth.block_number}")
    
    # Load private key
    private_key = load_validator_key(args.validator)
    account = Account.from_key(private_key)
    
    # Use random target if not specified
    if args.target:
        target_address = Web3.to_checksum_address(args.target)
    else:
        # Generate random address
        random_account = Account.create()
        target_address = random_account.address
    
    print(f"Target address: {target_address}")
    print(f"Will send {args.total_batches} batches of {args.batch_size} transactions")
    print(f"Total transactions: {args.total_batches * args.batch_size}")
    print(f"Delay between batches: {args.batch_delay}s")
    
    # Send continuous transactions
    send_continuous_transactions(
        args.node,
        private_key,
        target_address,
        args.batch_size,
        args.batch_delay,
        args.total_batches
    )

if __name__ == '__main__':
    main()