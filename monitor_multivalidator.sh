#!/bin/bash

# Neura Multivalidator Monitoring Script
# Checks status and health of all 4 validator nodes

echo "🔍 Neura Multivalidator Network Status"
echo "======================================"

# Function to check if a port is listening
check_port() {
    local port=$1
    local name=$2
    if netstat -an | grep ":$port " | grep LISTEN > /dev/null; then
        echo "✅ $name (port $port): LISTENING"
    else
        echo "❌ $name (port $port): NOT LISTENING"
    fi
}

# Function to check RPC status
check_rpc() {
    local port=$1
    local node=$2
    local response=$(curl -s -X POST -H 'Content-Type: application/json' \
        --data '{"jsonrpc":"2.0","method":"consensus_getStatus","params":[],"id":1}' \
        http://localhost:$port 2>/dev/null)
    
    if [[ $? -eq 0 && -n "$response" ]]; then
        echo "✅ Node $node RPC (port $port): RESPONDING"
        echo "   Response: $response"
    else
        echo "❌ Node $node RPC (port $port): NOT RESPONDING"
    fi
}

# Check process status
echo ""
echo "📊 Process Status:"
echo "=================="
if pgrep -f "reth.*node.*narwhal" > /dev/null; then
    echo "✅ Reth processes running:"
    pgrep -f "reth.*node.*narwhal" | while read pid; do
        echo "   PID: $pid"
    done
else
    echo "❌ No Reth processes found"
fi

echo ""
echo "🌐 Network Ports Status:"
echo "========================"
check_port 8545 "Node 1 HTTP"
check_port 8546 "Node 2 HTTP" 
check_port 8547 "Node 3 HTTP"
check_port 8548 "Node 4 HTTP"
check_port 30303 "Node 1 P2P"
check_port 30304 "Node 2 P2P"
check_port 30305 "Node 3 P2P"
check_port 30306 "Node 4 P2P"

echo ""
echo "🤝 Consensus RPC Status:"
echo "========================"
check_rpc 8545 1
check_rpc 8546 2
check_rpc 8547 3  
check_rpc 8548 4

echo ""
echo "📝 Recent Log Activity:"
echo "======================="
for i in {1..4}; do
    if [[ -f ~/.neura/node$i/node.log ]]; then
        echo "Node $i (last 3 lines):"
        tail -3 ~/.neura/node$i/node.log | sed 's/^/  /'
        echo ""
    else
        echo "❌ Node $i: No log file found"
    fi
done

echo ""
echo "🎯 Quick Commands:"
echo "=================="
echo "  View Node 1 logs: tail -f ~/.neura/node1/node.log"
echo "  View Node 2 logs: tail -f ~/.neura/node2/node.log" 
echo "  View Node 3 logs: tail -f ~/.neura/node3/node.log"
echo "  View Node 4 logs: tail -f ~/.neura/node4/node.log"
echo "  Stop all nodes: pkill -f 'reth.*node.*narwhal'"
echo "  Check validator info: curl -s -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_listValidators\",\"params\":[],\"id\":1}' http://localhost:8545 | jq" 