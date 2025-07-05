#!/usr/bin/env python3
"""
Send continuous test transactions to Reth/Neura node to test consensus
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

def send_transactions(node_url, private_key, target_address, count, delay, thread_id):
    """Send transactions from a single thread"""
    # Connect to node
    w3 = Web3(Web3.HTTPProvider(node_url))
    if not w3.is_connected():
        print(f"Thread {thread_id}: Failed to connect to {node_url}")
        return
    
    # Setup account
    account = Account.from_key(private_key)
    print(f"Thread {thread_id}: Using account {account.address}")
    
    # Get initial nonce
    nonce = w3.eth.get_transaction_count(account.address)
    
    successful = 0
    failed = 0
    
    for i in range(count):
        try:
            # Create transaction with some random data
            tx = {
                'nonce': nonce,
                'to': target_address,
                'value': 0,  # 0 value for testing
                'gas': 100000,  # Higher gas limit for data
                'gasPrice': w3.to_wei(1, 'gwei'),
                'chainId': 266,  # Neura chain ID
                'data': f'0x{thread_id:02x}{i:06x}{random.randint(0, 0xFFFFFF):06x}'.encode().hex()
            }
            
            # Sign and send
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            print(f"Thread {thread_id}: Sent tx {i+1}/{count} - {tx_hash.hex()[:10]}...")
            successful += 1
            nonce += 1
            
        except Exception as e:
            print(f"Thread {thread_id}: Failed tx {i+1}/{count} - {str(e)[:50]}...")
            failed += 1
            # Try to recover nonce
            try:
                nonce = w3.eth.get_transaction_count(account.address)
            except:
                pass
        
        time.sleep(delay)
    
    print(f"Thread {thread_id}: Complete - {successful} successful, {failed} failed")

def main():
    parser = argparse.ArgumentParser(description='Send continuous transactions to test Neura consensus')
    parser.add_argument('--node', default='http://localhost:8545', help='Node RPC URL')
    parser.add_argument('--count', type=int, default=100, help='Number of transactions per thread')
    parser.add_argument('--threads', type=int, default=1, help='Number of parallel threads')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between transactions (seconds)')
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
    print(f"Using account: {account.address}")
    
    # Check balance
    balance = w3.eth.get_balance(account.address)
    print(f"Balance: {w3.from_wei(balance, 'ether')} ANKR")
    
    if balance == 0:
        print("\nWARNING: Account has 0 balance. Transactions may fail.")
        print("Continuing anyway to test consensus with 0-value transactions...\n")
    
    # Target address
    if args.target:
        target = Web3.to_checksum_address(args.target)
    else:
        # Generate random target
        target = Account.create().address
    print(f"Target address: {target}")
    
    # Monitor block production in background
    initial_block = w3.eth.block_number
    
    def monitor_blocks():
        last_block = initial_block
        while True:
            try:
                current_block = w3.eth.block_number
                if current_block > last_block:
                    block = w3.eth.get_block(current_block)
                    tx_count = len(block['transactions'])
                    print(f"\n🔵 New block #{current_block} with {tx_count} transactions (parent: {block['parentHash'].hex()[:10]}...)")
                    last_block = current_block
            except:
                pass
            time.sleep(1)
    
    monitor_thread = threading.Thread(target=monitor_blocks, daemon=True)
    monitor_thread.start()
    
    print(f"\nStarting {args.threads} threads sending {args.count} transactions each...")
    print("Press Ctrl+C to stop\n")
    
    # Start transaction threads
    threads = []
    for i in range(args.threads):
        t = threading.Thread(
            target=send_transactions,
            args=(args.node, private_key, target, args.count, args.delay, i)
        )
        threads.append(t)
        t.start()
        time.sleep(0.1)  # Stagger thread starts
    
    # Wait for completion
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\nStopping...")
    
    # Final status
    time.sleep(2)
    final_block = w3.eth.block_number
    print(f"\nFinal block: {final_block}")
    print(f"Blocks created: {final_block - initial_block}")

if __name__ == '__main__':
    main()