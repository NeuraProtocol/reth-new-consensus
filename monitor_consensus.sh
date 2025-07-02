#!/bin/bash
# Monitor consensus status across all nodes

echo "🔍 Monitoring Consensus Status Across All Nodes"
echo "=============================================="

# Function to query consensus status
check_node() {
    local node_num=$1
    local rpc_port=$((10000 + $node_num))
    local http_port=$((8544 + $node_num))
    
    echo ""
    echo "📊 Node $node_num Status (ConsensusRPC: $rpc_port, HTTP: $http_port):"
    echo "----------------------------------------"
    
    # Check if consensus RPC is responding
    consensus_status=$(curl -s -X POST -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"consensus_getStatus","params":[],"id":1}' \
        http://localhost:$rpc_port 2>/dev/null)
    
    if [[ $? -eq 0 && $consensus_status == *"result"* ]]; then
        # Extract key fields from consensus status
        healthy=$(echo $consensus_status | jq -r '.result.healthy')
        epoch=$(echo $consensus_status | jq -r '.result.epoch')
        round=$(echo $consensus_status | jq -r '.result.round')
        active_validators=$(echo $consensus_status | jq -r '.result.active_validators')
        last_finalized=$(echo $consensus_status | jq -r '.result.last_finalized_batch // "none"')
        is_producing=$(echo $consensus_status | jq -r '.result.is_producing')
        
        echo "  ✅ Consensus RPC: Online"
        echo "  📌 Healthy: $healthy"
        echo "  📅 Epoch: $epoch"
        echo "  🔄 Round: $round"
        echo "  👥 Active Validators: $active_validators"
        echo "  📦 Last Finalized Batch: $last_finalized"
        echo "  ⚙️  Producing: $is_producing"
    else
        echo "  ❌ Consensus RPC: Offline or not responding"
    fi
    
    # Check standard RPC block number
    block_response=$(curl -s -X POST -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        http://localhost:$http_port 2>/dev/null)
    
    if [[ $? -eq 0 && $block_response == *"result"* ]]; then
        block_hex=$(echo $block_response | jq -r '.result')
        block_num=$((16#${block_hex:2}))
        echo "  🔢 Current Block: $block_num"
    fi
}

# Monitor all 4 nodes
for i in {1..4}; do
    check_node $i
done

echo ""
echo "=============================================="
echo "💡 Tips:"
echo "  • All nodes should show 'healthy: true'"
echo "  • All nodes should be in the same epoch"
echo "  • Rounds should be progressing (increasing)"
echo "  • Active validators should be 4"
echo "  • Block numbers should be synchronized"
echo ""
echo "📡 For detailed committee info:"
echo "  curl -X POST -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getCommittee\",\"params\":[],\"id\":1}' \\"
echo "    http://localhost:10001 | jq ."
echo ""
echo "📊 For consensus metrics:"
echo "  curl -X POST -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getMetrics\",\"params\":[],\"id\":1}' \\"
echo "    http://localhost:10001 | jq ."