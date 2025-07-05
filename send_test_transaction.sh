#!/bin/bash

# Send a test transaction to the first node

# Test accounts from validator files
FROM_ADDR="0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a"
FROM_KEY="0xb71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"
TO_ADDR="0x1563915e194d8cfba1943570603f7606a3115508"

# Create a simple transfer transaction
VALUE="0x1000000000000000"  # 0.001 ETH in wei
GAS="0x5208"                # 21000 gas for simple transfer
GAS_PRICE="0x3b9aca00"      # 1 gwei

# Get current nonce
NONCE=$(curl -s -X POST -H "Content-Type: application/json" \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionCount\",\"params\":[\"$FROM_ADDR\",\"latest\"],\"id\":1}" \
  http://localhost:8545 | jq -r '.result' || echo "0x0")

echo "Current nonce: $NONCE"

# Create the transaction
TX_DATA=$(cat <<EOF
{
  "jsonrpc": "2.0",
  "method": "eth_sendTransaction",
  "params": [{
    "from": "$FROM_ADDR",
    "to": "$TO_ADDR",
    "value": "$VALUE",
    "gas": "$GAS",
    "gasPrice": "$GAS_PRICE",
    "nonce": "$NONCE"
  }],
  "id": 1
}
EOF
)

# For Reth, we need to use eth_sendRawTransaction with a signed transaction
# Let's use cast from foundry to sign and send

# First check if cast is available
if ! command -v cast &> /dev/null; then
    echo "Installing foundry (includes cast)..."
    curl -L https://foundry.paradigm.xyz | bash
    source ~/.bashrc
    foundryup
fi

# Sign and send the transaction
echo "Sending transaction from $FROM_ADDR to $TO_ADDR..."
TX_HASH=$(cast send --rpc-url http://localhost:8545 \
  --private-key $FROM_KEY \
  $TO_ADDR \
  --value 0.001ether \
  --gas-price 1gwei \
  --gas-limit 21000 \
  2>&1 | grep "transactionHash" | awk '{print $2}' || echo "")

if [ -n "$TX_HASH" ]; then
    echo "Transaction sent! Hash: $TX_HASH"
    echo "Waiting for transaction receipt..."
    sleep 5
    
    # Check transaction receipt
    RECEIPT=$(curl -s -X POST -H "Content-Type: application/json" \
      --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$TX_HASH\"],\"id\":1}" \
      http://localhost:8545 | jq '.')
    
    echo "Receipt: $RECEIPT"
else
    echo "Failed to send transaction. Trying raw transaction API..."
    
    # Alternative: Create raw transaction manually
    # This is more complex but doesn't require external tools
    
    # Create transaction hex (simplified - would need proper RLP encoding)
    echo "Please install 'cast' tool for easier transaction sending:"
    echo "curl -L https://foundry.paradigm.xyz | bash && foundryup"
fi