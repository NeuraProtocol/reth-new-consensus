#!/bin/bash
# Quick script to send test transactions to trigger consensus

echo "Sending test transactions to Neura network..."

# Check if Python script exists
if [ ! -f "send_continuous_transactions.py" ]; then
    echo "Error: send_continuous_transactions.py not found!"
    exit 1
fi

# Default to node1 port
NODE_PORT=${1:-8545}
NODE_URL="http://localhost:${NODE_PORT}"

echo "Using node at ${NODE_URL}"

# Send 50 transactions with 0.05s delay to trigger batching
python3 send_continuous_transactions.py \
    --node "${NODE_URL}" \
    --count 50 \
    --delay 0.05 \
    --threads 1 \
    --validator test_validators/validator-0.json

echo "Done sending test transactions"