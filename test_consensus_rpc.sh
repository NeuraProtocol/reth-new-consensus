#!/bin/bash

# Test script for Consensus RPC endpoints
# Usage: ./test_consensus_rpc.sh [PORT]
# Default port: 10001

PORT=${1:-10001}
echo "🔍 Testing Consensus RPC on port $PORT"
echo "========================================="

# Function to make RPC call and pretty print
rpc_call() {
    local method=$1
    local params=$2
    local desc=$3
    
    echo -e "\n📡 $desc"
    echo "Method: $method"
    echo "Request:"
    local request="{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}"
    echo "$request" | jq .
    
    echo -e "\nResponse:"
    curl -s -X POST -H 'Content-Type: application/json' \
        --data "$request" \
        http://localhost:$PORT | jq . || echo "Failed to connect to port $PORT"
}

# Test consensus API endpoints
echo -e "\n=== CONSENSUS API ENDPOINTS ==="

rpc_call "consensus_getStatus" "[]" "Get consensus status"
sleep 1

rpc_call "consensus_getCommittee" "[]" "Get committee information"
sleep 1

rpc_call "consensus_listValidators" "[{\"active_only\": true, \"limit\": 10}]" "List active validators"
sleep 1

rpc_call "consensus_getValidator" "[\"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\"]" "Get specific validator (example address)"
sleep 1

rpc_call "consensus_getValidatorMetrics" "[\"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\"]" "Get validator metrics"
sleep 1

rpc_call "consensus_getMetrics" "[]" "Get consensus metrics"
sleep 1

rpc_call "consensus_getConfig" "[]" "Get consensus configuration"
sleep 1

rpc_call "consensus_getRecentBatches" "[5]" "Get recent finalized batches"
sleep 1

# Test admin API endpoints
echo -e "\n\n=== CONSENSUS ADMIN API ENDPOINTS ==="
echo "(These require --consensus-rpc-enable-admin flag)"

rpc_call "consensus_admin_getDagInfo" "[]" "Get DAG information"
sleep 1

rpc_call "consensus_admin_getStorageStats" "[]" "Get storage statistics"
sleep 1

rpc_call "consensus_admin_getInternalState" "[]" "Get internal consensus state"
sleep 1

echo -e "\n\n✅ RPC test completed!"
echo "========================================="
echo "💡 Tips:"
echo "  - If you see connection errors, check that the node is running"
echo "  - Admin endpoints return errors if --consensus-rpc-enable-admin is not set"
echo "  - Use different ports for different nodes (10001, 10002, 10003, 10004)"
echo "  - Monitor logs: tail -f ~/.neura/node*/node.log | grep -i rpc"