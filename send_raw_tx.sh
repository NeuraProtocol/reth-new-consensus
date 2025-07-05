#!/bin/bash
# Send raw transactions using curl (no Python dependencies)

NODE_URL=${1:-"http://localhost:8545"}
echo "Sending transactions to ${NODE_URL}"

# Pre-signed transactions from validator-0 (0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a)
# These are simple value transfers with incrementing nonces

# Function to send a transaction
send_tx() {
    local raw_tx=$1
    local tx_num=$2
    
    response=$(curl -s -X POST ${NODE_URL} \
        -H "Content-Type: application/json" \
        -d '{
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": ["'${raw_tx}'"],
            "id": '${tx_num}'
        }')
    
    echo "Tx ${tx_num}: ${response}"
}

# Get current block number
echo "Current block number:"
curl -s -X POST ${NODE_URL} \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | jq .

echo -e "\nSending test transactions..."

# These are pre-signed transactions with nonce 0-9
# To: 0x1563915e194d8cfba1943570603f7606a3115508, Value: 1 gwei, Gas: 21000, GasPrice: 1 gwei
# You can generate more using web3.py or ethers.js

# Example transactions (you'll need to generate actual signed transactions based on current nonce)
TXNS=(
    "0xf86a808504a817c800825208941563915e194d8cfba1943570603f7606a311550801808202aaa00123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefa00123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    "0xf86a018504a817c800825208941563915e194d8cfba1943570603f7606a311550801808202aaa00123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefa00123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

# Note: The above transactions are dummy examples and won't work
# In practice, you need to sign real transactions

echo "NOTE: This script needs real signed transactions to work properly."
echo "Use send_continuous_transactions.py for actual testing."

# Check mempool
echo -e "\nChecking mempool:"
curl -s -X POST ${NODE_URL} \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":1}' | jq '.result.pending | length'

echo -e "\nFor proper testing, use:"
echo "  ./send_continuous_transactions.py --count 100 --delay 0.05"