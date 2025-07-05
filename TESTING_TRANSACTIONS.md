# Testing Transactions with Neura Consensus

This document explains how to send test transactions to verify that the Narwhal + Bullshark consensus is working correctly.

## Prerequisites

1. Multi-validator network running (use `./start_multivalidator_test.sh`)
2. Python 3 with web3.py installed: `pip install web3 eth-account`

## Quick Test

For a quick test with 50 transactions:
```bash
./send_test_transactions.sh
```

This will send 50 transactions with a 0.05s delay between each.

## Continuous Testing

For more comprehensive testing with multiple options:

```bash
# Send 100 transactions from a single thread
./send_continuous_transactions.py --count 100 --delay 0.1

# Send transactions from multiple threads in parallel
./send_continuous_transactions.py --count 50 --threads 4 --delay 0.2

# Use a different node (e.g., node2 on port 8546)
./send_continuous_transactions.py --node http://localhost:8546 --count 100

# Use a different validator key
./send_continuous_transactions.py --validator test_validators/validator-1.json --count 50
```

## What to Expect

1. **Transaction Batching**: Narwhal workers will batch transactions together
2. **Header Creation**: When enough transactions accumulate or timer expires (with transactions)
3. **Certificate Formation**: When a header receives f+1 votes from validators
4. **Block Creation**: When Bullshark consensus finalizes a certificate with transactions

## Monitoring

The transaction script will show:
- Transaction send status
- New blocks as they're created
- Parent hash of each block (should link correctly)

You can also monitor logs:
```bash
# Watch all node logs
tail -f ~/.neura/node*/node.log

# Watch specific node
tail -f ~/.neura/node1/node.log | grep -E "(block|certificate|transaction)"
```

## Expected Behavior After Changes

With the recent changes to prevent empty block creation:
- ✅ No blocks created when no transactions are sent
- ✅ Blocks only created when transactions are in the mempool
- ✅ Each block should contain the transactions that triggered its creation
- ✅ Parent hash should be correct (not 0x0000...)
- ✅ Block numbers should increment properly

## Troubleshooting

1. **"Account has 0 balance"**: This is normal if the account isn't funded in genesis. The script will try 0-value transactions.

2. **"Failed to connect"**: Make sure the node is running on the expected port.

3. **No blocks created**: Check that:
   - Transactions are being accepted into mempool
   - All 4 validators are running
   - Network connectivity between validators

4. **Too many blocks**: This should be fixed now - blocks should only be created when transactions exist.

## Python Script Options

```
Options:
  --node NODE          Node RPC URL (default: http://localhost:8545)
  --count COUNT        Number of transactions per thread (default: 100)
  --threads THREADS    Number of parallel threads (default: 1)
  --delay DELAY        Delay between transactions in seconds (default: 0.1)
  --validator FILE     Validator key file (default: test_validators/validator-0.json)
  --target ADDRESS     Target address for transfers (default: random)
```

## Example Test Sequence

```bash
# 1. Start the network
./start_multivalidator_test.sh

# 2. Wait for validators to connect (about 10 seconds)
sleep 10

# 3. Send test transactions
./send_continuous_transactions.py --count 200 --delay 0.05

# 4. Check results
grep "New block" ~/.neura/node1/node.log | tail -20
```