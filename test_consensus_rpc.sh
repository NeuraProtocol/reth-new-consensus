#!/bin/bash
# Test script to verify consensus RPC functionality

echo "Testing consensus RPC server..."

# Function to test RPC endpoint
test_rpc() {
    local port=$1
    local method=$2
    echo -n "Testing $method on port $port... "
    
    response=$(curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":[],\"id\":1}" \
        http://127.0.0.1:$port)
    
    if [[ $? -eq 0 && $response == *"result"* ]]; then
        echo "✅ Success"
        echo "Response: $response"
    else
        echo "❌ Failed"
        echo "Error: $response"
    fi
}

# Test consensus endpoints
echo "Available consensus RPC endpoints:"
echo "  - consensus_getStatus"
echo "  - consensus_getCommittee"
echo "  - consensus_listValidators"
echo "  - consensus_getMetrics"
echo "  - consensus_getConfig"
echo ""

# Example: Test on port 9999 (default consensus RPC port)
PORT=${1:-9999}
echo "Testing consensus RPC on port $PORT..."
echo ""

test_rpc $PORT "consensus_getStatus"
echo ""
test_rpc $PORT "consensus_getCommittee"
echo ""
test_rpc $PORT "consensus_getMetrics"