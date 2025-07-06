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

def send_transactions_to_node(node_url, private_key, target_address, count, node_id, value_wei=0, include_invalid=False):
    """Send transactions to a specific node, optionally including some invalid ones"""
    
    # Connect to node
    w3 = Web3(Web3.HTTPProvider(node_url))
    if not w3.is_connected():
        print(f"Node {node_id}: Failed to connect to {node_url}")
        return 0
    
    # Setup account
    account = Account.from_key(private_key)
    
    # Check account balance
    balance = w3.eth.get_balance(account.address)
    balance_eth = w3.from_wei(balance, 'ether')
    print(f"Node {node_id}: Account {account.address} balance: {balance_eth} ETH")
    
    if balance == 0:
        print(f"Node {node_id}: ERROR - Account has no balance! Cannot send transactions.")
        return 0
    
    # Get current nonce for this account
    nonce = w3.eth.get_transaction_count(account.address, 'latest')  # Use 'latest' instead of 'pending'
    print(f"Node {node_id}: Starting nonce: {nonce}")
    
    successful = 0
    failed = 0
    
    print(f"Node {node_id}: Sending {count} transactions from {account.address}")
    
    for i in range(count):
        try:
            # Determine if this should be an invalid transaction
            make_invalid = include_invalid and i % 5 == 4  # Every 5th transaction is invalid
            
            if make_invalid:
                # Create various types of invalid transactions
                invalid_type = i % 3  # Rotate through different invalid types
                
                if invalid_type == 0:
                    # Transaction with insufficient gas
                    print(f"Node {node_id}: Creating tx {i+1}/{count} with insufficient gas...")
                    tx = {
                        'nonce': nonce,
                        'to': target_address,
                        'value': 0,
                        'gas': 100,  # Way too low
                        'gasPrice': w3.to_wei(1, 'gwei'),
                        'chainId': 266,
                        'data': '0x1234'
                    }
                elif invalid_type == 1:
                    # Transaction with wrong nonce (skip ahead)
                    print(f"Node {node_id}: Creating tx {i+1}/{count} with future nonce...")
                    tx = {
                        'nonce': nonce + 10,  # Jump ahead
                        'to': target_address,
                        'value': 0,
                        'gas': 50000,
                        'gasPrice': w3.to_wei(1, 'gwei'),
                        'chainId': 266,
                        'data': '0x'
                    }
                else:
                    # Transaction trying to spend more than balance
                    print(f"Node {node_id}: Creating tx {i+1}/{count} with excessive value...")
                    tx = {
                        'nonce': nonce,
                        'to': target_address,
                        'value': w3.to_wei(1000, 'ether'),  # Way more than account has
                        'gas': 21000,
                        'gasPrice': w3.to_wei(1, 'gwei'),
                        'chainId': 266,
                        'data': '0x'
                    }
            else:
                # Create normal valid transaction
                # Generate data payload
                data_hex = f'0x{node_id:02x}{i:04x}{random.randint(0, 0xFFFF):04x}'.encode().hex()
                
                # Calculate intrinsic gas for transaction with data
                # Base cost: 21000
                # Data cost: 4 gas per zero byte, 16 gas per non-zero byte
                # For our hex data, estimate ~100 bytes of non-zero data
                intrinsic_gas = 21000 + (len(data_hex) // 2) * 16  # Conservative estimate
                gas_limit = max(intrinsic_gas + 10000, 50000)  # Add buffer and set minimum
                
                tx = {
                    'nonce': nonce,
                    'to': target_address,
                    'value': value_wei,  # Can send value if needed
                    'gas': gas_limit,
                    'gasPrice': w3.to_wei(1, 'gwei'),
                    'chainId': 266,
                    'data': data_hex
                }
                
                # Check if account has enough balance for gas
                gas_cost = tx['gas'] * tx['gasPrice']
                total_cost = gas_cost + tx['value']
                if total_cost > balance:
                    print(f"Node {node_id}: Insufficient balance for tx {i+1} (need {w3.from_wei(total_cost, 'ether')} ETH)")
                    break
            
            # Sign and send
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            if make_invalid:
                print(f"Node {node_id}: Sent INVALID tx {i+1}/{count} - nonce {tx['nonce']} - {tx_hash.hex()[:10]}... (should fail)")
            else:
                print(f"Node {node_id}: Sent tx {i+1}/{count} - nonce {nonce} - {tx_hash.hex()[:10]}...")
            
            successful += 1
            
            # Only increment nonce for valid transactions
            if not make_invalid or invalid_type != 1:  # Don't increment for future nonce txs
                nonce += 1
            
            # Small delay to avoid overwhelming the node
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Node {node_id}: Failed tx {i+1}/{count} - {str(e)[:100]}...")
            failed += 1
            # Try to recover nonce
            try:
                new_nonce = w3.eth.get_transaction_count(account.address, 'latest')
                if new_nonce > nonce:
                    nonce = new_nonce
                    print(f"Node {node_id}: Recovered nonce: {nonce}")
            except:
                pass
    
    print(f"Node {node_id}: Complete - {successful} successful, {failed} failed")
    return successful

def main():
    parser = argparse.ArgumentParser(description='Send transactions to all validators')
    parser.add_argument('--txs-per-node', type=int, default=10, help='Transactions to send per node')
    parser.add_argument('--rounds', type=int, default=5, help='Number of rounds to repeat')
    parser.add_argument('--round-delay', type=float, default=3.0, help='Delay between rounds (seconds)')
    parser.add_argument('--include-invalid', action='store_true', help='Include some invalid transactions to test error handling')
    
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
    if args.include_invalid:
        print(f"Including invalid transactions: ~20% will be invalid to test error handling")
    
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
                    node['id'],
                    0,  # value_wei = 0 for simple data transactions
                    args.include_invalid  # Pass the flag to include invalid transactions
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