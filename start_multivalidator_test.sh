#!/bin/bash

# Neura Multivalidator Test Script with REAL Validator Key Management
# Starts 4 validator nodes with their own private keys and shared committee configuration

echo "🚀 Starting Neura Multivalidator Test (Chain ID: 266, Coin: ANKR)"
echo "🔑 Using REAL Validator Key Management (No Random Keys!)"
echo "🏛️ Committee loaded from test_validators/ directory"
echo "=================================================================="

# Kill any existing reth processes
pkill -f "reth.*node.*narwhal" || true
sleep 2

# Build the binary once (release mode for performance)
echo "🔨 Building Reth with Narwhal + Bullshark consensus..."
cargo build --release --bin reth
if [ $? -ne 0 ]; then
    echo "❌ Build failed! Exiting."
    exit 1
fi
echo "✅ Build completed successfully"
echo ""

# Clean up any previous data to ensure fresh start
echo "Cleaning up previous blockchain data..."
for i in {1..4}; do
    echo "Cleaning node $i..."
    rm -rf /home/peastew/.neura/node$i/db || true
    rm -rf /home/peastew/.neura/node$i/static_files || true
done

echo ""
echo "Starting validator nodes with REAL key management..."

# Use the pre-built binary
RETH_BINARY="./target/release/reth"

# Start Node 1 (Validator-001) with Real Key Management
echo "Starting Node 1 (Validator-001) on ports: P2P=30303, HTTP=8545, Auth=8551, Narwhal=9001"
echo "  🔑 Using validator key file: test_validators/validator-0.json"
echo "  🏛️ Loading committee from: test_validators/"
echo "  Binding to: 127.0.0.1:9001"  
echo "  Connecting to peers: 127.0.0.1:9002, 127.0.0.1:9003, 127.0.0.1:9004"
$RETH_BINARY node \
  --narwhal.enable \
  --chain neura-mainnet \
  --datadir /home/peastew/.neura/node1 \
  --port 30303 \
  --discovery.port 30303 \
  --http --http.port 8545 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8551 \
  --narwhal.network-addr 127.0.0.1:9001 \
  --narwhal.committee-size 4 \
  --narwhal.peers 127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004 \
  --narwhal.max-batch-delay-ms 1000 \
  --validator.key-file test_validators/validator-0.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --validator.index 0 \
  > /home/peastew/.neura/node1/node.log 2>&1 &

NODE1_PID=$!
echo "Node 1 started with PID: $NODE1_PID"

sleep 1

# Start Node 2 (Validator-002) with Real Key Management
echo "Starting Node 2 (Validator-002) on ports: P2P=30304, HTTP=8546, Auth=8552, Narwhal=9002"
echo "  🔑 Using validator key file: test_validators/validator-1.json"
echo "  🏛️ Loading committee from: test_validators/"
echo "  Binding to: 127.0.0.1:9002"
echo "  Connecting to peers: 127.0.0.1:9001, 127.0.0.1:9003, 127.0.0.1:9004"
$RETH_BINARY node \
  --narwhal.enable \
  --chain neura-mainnet \
  --datadir /home/peastew/.neura/node2 \
  --port 30304 \
  --discovery.port 30304 \
  --http --http.port 8546 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8552 \
  --narwhal.network-addr 127.0.0.1:9002 \
  --narwhal.committee-size 4 \
  --narwhal.peers 127.0.0.1:9001,127.0.0.1:9003,127.0.0.1:9004 \
  --narwhal.max-batch-delay-ms 1000 \
  --validator.key-file test_validators/validator-1.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --validator.index 1 \
  > /home/peastew/.neura/node2/node.log 2>&1 &

NODE2_PID=$!
echo "Node 2 started with PID: $NODE2_PID"

sleep 1

# Start Node 3 (Validator-003) with Real Key Management
echo "Starting Node 3 (Validator-003) on ports: P2P=30305, HTTP=8547, Auth=8553, Narwhal=9003"
echo "  🔑 Using validator key file: test_validators/validator-2.json"
echo "  🏛️ Loading committee from: test_validators/"
echo "  Binding to: 127.0.0.1:9003"
echo "  Connecting to peers: 127.0.0.1:9001, 127.0.0.1:9002, 127.0.0.1:9004"
$RETH_BINARY node \
  --narwhal.enable \
  --chain neura-mainnet \
  --datadir /home/peastew/.neura/node3 \
  --port 30305 \
  --discovery.port 30305 \
  --http --http.port 8547 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8553 \
  --narwhal.network-addr 127.0.0.1:9003 \
  --narwhal.committee-size 4 \
  --narwhal.peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9004 \
  --narwhal.max-batch-delay-ms 1000 \
  --validator.key-file test_validators/validator-2.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --validator.index 2 \
  > /home/peastew/.neura/node3/node.log 2>&1 &

NODE3_PID=$!
echo "Node 3 started with PID: $NODE3_PID"

sleep 1

# Start Node 4 (Validator-004) with Real Key Management
echo "Starting Node 4 (Validator-004) on ports: P2P=30306, HTTP=8548, Auth=8554, Narwhal=9004"
echo "  🔑 Using validator key file: test_validators/validator-3.json"
echo "  🏛️ Loading committee from: test_validators/"
echo "  Binding to: 127.0.0.1:9004"
echo "  Connecting to peers: 127.0.0.1:9001, 127.0.0.1:9002, 127.0.0.1:9003"
$RETH_BINARY node \
  --narwhal.enable \
  --chain neura-mainnet \
  --datadir /home/peastew/.neura/node4 \
  --port 30306 \
  --discovery.port 30306 \
  --http --http.port 8548 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8554 \
  --narwhal.network-addr 127.0.0.1:9004 \
  --narwhal.committee-size 4 \
  --narwhal.peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --narwhal.max-batch-delay-ms 1000 \
  --validator.key-file test_validators/validator-3.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --validator.index 3 \
  > /home/peastew/.neura/node4/node.log 2>&1 &

NODE4_PID=$!
echo "Node 4 started with PID: $NODE4_PID"

sleep 3

echo ""
echo "✅ All 4 Neura validator nodes started with REAL validator key management!"
echo "========================================================================="
echo "📊 Network: Neura (Chain ID: 266, Coin: ANKR)"
echo "🔗 Consensus: Narwhal + Bullshark BFT with REAL distributed consensus"  
echo "💾 Storage: Independent MDBX per node"
echo "🔑 Keys: Each validator uses its own private key from JSON file"
echo "🏛️ Committee: Shared configuration derived from test_validators/ directory"
echo ""
echo "📍 Node Configuration:"
echo "  Node 1 PID: $NODE1_PID - Key: validator-0.json - Bind: 9001 - HTTP: 8545 - Logs: /home/peastew/.neura/node1/node.log"
echo "  Node 2 PID: $NODE2_PID - Key: validator-1.json - Bind: 9002 - HTTP: 8546 - Logs: /home/peastew/.neura/node2/node.log"  
echo "  Node 3 PID: $NODE3_PID - Key: validator-2.json - Bind: 9003 - HTTP: 8547 - Logs: /home/peastew/.neura/node3/node.log"
echo "  Node 4 PID: $NODE4_PID - Key: validator-3.json - Bind: 9004 - HTTP: 8548 - Logs: /home/peastew/.neura/node4/node.log"
echo ""
echo "🔧 New Validator Key Management Features:"
echo "  --validator.key-file: Load private key from JSON file"
echo "  --validator.config-dir: Load committee from directory containing all validator files"
echo "  --validator.deterministic-consensus-key: Derive consensus key from EVM private key"
echo "  --validator.index: Specify which validator position this node represents"
echo ""
echo "🔧 Monitoring Commands:"
echo "  Monitor all logs: tail -f /home/peastew/.neura/node*/node.log"
echo "  Monitor node 1: tail -f /home/peastew/.neura/node1/node.log"
echo "  Stop all nodes: pkill -f 'reth.*node.*narwhal'"
echo "  Check processes: ps aux | grep reth"
echo ""
echo "🌐 RPC Test Commands:"
echo "  Node 1 version: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"web3_clientVersion\",\"params\":[],\"id\":1}' http://localhost:8545"
echo "  Node 2 block#:  curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[],\"id\":1}' http://localhost:8546"
echo "  Node 3 peers:   curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"net_peerCount\",\"params\":[],\"id\":1}' http://localhost:8547"
echo "  Node 4 syncing: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_syncing\",\"params\":[],\"id\":1}' http://localhost:8548"
echo ""
echo "🎯 Expected Behavior with REAL Key Management:"
echo "  • Each node loads its own unique private key from JSON file"
echo "  • Committee built from ALL validator files in test_validators/ directory"
echo "  • Consensus keys derived deterministically from EVM private keys"
echo "  • Nodes should recognize and validate each other's signatures"
echo "  • No more 'Unknown authority' errors - validators know each other's public keys"
echo "  • Headers and votes should flow between nodes and be accepted"
echo ""
echo "🔍 Quick Verification:"
echo "  Check all nodes started: ps aux | grep 'reth.*node.*narwhal' | wc -l  # Should show 4"
echo "  Check for port conflicts: netstat -tlnp | grep -E ':(9001|9002|9003|9004)' | wc -l  # Should show 4"
echo "  Check consensus working: grep -l 'Creating.*header.*heartbeat' /home/peastew/.neura/node*/node.log | wc -l  # Should show 4"
echo "  Check NO 'Unknown authority' errors: grep -c 'Unknown authority' /home/peastew/.neura/node*/node.log  # Should show 0"
echo "  Check validator key loading: grep -c 'Loading validator key from file' /home/peastew/.neura/node*/node.log  # Should show 4"
echo ""
echo "💡 If nodes fail to start, check for:"
echo "  • Missing validator key files in test_validators/ directory"
echo "  • Invalid JSON format in validator key files"
echo "  • Port conflicts (netstat -tlnp | grep 900[1-4])"
echo "  • Build issues (cargo build --release --bin reth)" 
echo ""
echo "🔍 Validator Key Files:"
echo "  • test_validators/validator-0.json -> Node 1 (EVM key: 0x1111...)"
echo "  • test_validators/validator-1.json -> Node 2 (EVM key: 0x2222...)"
echo "  • test_validators/validator-2.json -> Node 3 (EVM key: 0x3333...)"
echo "  • test_validators/validator-3.json -> Node 4 (EVM key: 0x4444...)" 