#!/usr/bin/env python3
"""
Send transactions to ALL validators to ensure full consensus participation
"""

import json
import time
import sys
import argparse
from web3 import Web3
from eth_account import Account
import threading
import random
import concurrent.futures

def load_validator_key(validator_file):
    """Load private key from validator JSON file"""
    with open(validator_file, 'r') as f:
        data = json.load(f)
    return data['evm_private_key']

def send_transactions_to_node(node_url, private_key, target_address, count, node_id):
    """Send transactions to a specific node"""
    
    # Connect to node
    w3 = Web3(Web3.HTTPProvider(node_url))
    if not w3.is_connected():
        print(f"Node {node_id}: Failed to connect to {node_url}")
        return 0
    
    # Setup account
    account = Account.from_key(private_key)
    
    # Get current nonce for this account
    nonce = w3.eth.get_transaction_count(account.address, 'pending')
    
    successful = 0
    failed = 0
    
    print(f"Node {node_id}: Sending {count} transactions from {account.address}")
    
    for i in range(count):
        try:
            # Create transaction with unique data
            tx = {
                'nonce': nonce,
                'to': target_address,
                'value': 0,
                'gas': 100000,
                'gasPrice': w3.to_wei(1, 'gwei'),
                'chainId': 266,
                'data': f'0x{node_id:02x}{i:04x}{random.randint(0, 0xFFFF):04x}'.encode().hex()
            }
            
            # Sign and send
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            print(f"Node {node_id}: Sent tx {i+1}/{count} - nonce {nonce} - {tx_hash.hex()[:10]}...")
            successful += 1
            nonce += 1
            
        except Exception as e:
            print(f"Node {node_id}: Failed tx {i+1}/{count} - {str(e)[:50]}...")
            failed += 1
            # Try to recover nonce
            try:
                nonce = w3.eth.get_transaction_count(account.address, 'pending')
            except:
                pass
    
    print(f"Node {node_id}: Complete - {successful} successful, {failed} failed")
    return successful

def main():
    parser = argparse.ArgumentParser(description='Send transactions to all validators')
    parser.add_argument('--txs-per-node', type=int, default=10, help='Transactions to send per node')
    parser.add_argument('--rounds', type=int, default=5, help='Number of rounds to repeat')
    parser.add_argument('--round-delay', type=float, default=3.0, help='Delay between rounds (seconds)')
    
    args = parser.parse_args()
    
    # Define all validator nodes
    nodes = [
        {'id': 0, 'url': 'http://localhost:8545', 'key_file': 'test_validators/validator-0.json'},
        {'id': 1, 'url': 'http://localhost:8546', 'key_file': 'test_validators/validator-1.json'},
        {'id': 2, 'url': 'http://localhost:8547', 'key_file': 'test_validators/validator-2.json'},
        {'id': 3, 'url': 'http://localhost:8548', 'key_file': 'test_validators/validator-3.json'},
    ]
    
    # Generate random target address
    target_account = Account.create()
    target_address = target_account.address
    print(f"Target address: {target_address}")
    
    # Check connectivity to all nodes
    print("\nChecking node connectivity...")
    for node in nodes:
        w3 = Web3(Web3.HTTPProvider(node['url']))
        if w3.is_connected():
            block = w3.eth.block_number
            print(f"Node {node['id']}: Connected - Block #{block}")
        else:
            print(f"Node {node['id']}: FAILED to connect to {node['url']}")
    
    print(f"\nWill send {args.txs_per_node} transactions to each of {len(nodes)} nodes")
    print(f"Total transactions per round: {args.txs_per_node * len(nodes)}")
    print(f"Number of rounds: {args.rounds}")
    print(f"Delay between rounds: {args.round_delay}s")
    
    # Send transactions in rounds
    for round_num in range(args.rounds):
        print(f"\n{'='*60}")
        print(f"ROUND {round_num + 1}/{args.rounds}")
        print(f"{'='*60}")
        
        # Use thread pool to send to all nodes in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            futures = []
            
            for node in nodes:
                # Load private key for this validator
                private_key = load_validator_key(node['key_file'])
                
                # Submit transaction sending task
                future = executor.submit(
                    send_transactions_to_node,
                    node['url'],
                    private_key,
                    target_address,
                    args.txs_per_node,
                    node['id']
                )
                futures.append(future)
            
            # Wait for all nodes to complete
            results = [future.result() for future in futures]
            total_sent = sum(results)
            print(f"\nRound {round_num + 1} complete: {total_sent} total transactions sent")
        
        # Check block numbers after round
        print("\nBlock numbers after round:")
        for node in nodes:
            try:
                w3 = Web3(Web3.HTTPProvider(node['url']))
                if w3.is_connected():
                    block = w3.eth.block_number
                    print(f"Node {node['id']}: Block #{block}")
            except:
                pass
        
        # Delay before next round
        if round_num < args.rounds - 1:
            print(f"\nWaiting {args.round_delay}s before next round...")
            time.sleep(args.round_delay)
    
    print("\n" + "="*60)
    print("All rounds complete!")
    
    # Final block check
    print("\nFinal block numbers:")
    for node in nodes:
        try:
            w3 = Web3(Web3.HTTPProvider(node['url']))
            if w3.is_connected():
                block = w3.eth.block_number
                print(f"Node {node['id']}: Block #{block}")
        except:
            pass

if __name__ == '__main__':
    main()