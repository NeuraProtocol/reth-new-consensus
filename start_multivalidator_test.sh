#!/bin/bash

# Neura Multivalidator Test Script
# Starts 4 validator nodes with separate data directories and Neura custom chain

echo "🚀 Starting Neura Multivalidator Test (Chain ID: 266, Coin: ANKR)"
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
echo "Starting validator nodes..."

# Use the pre-built binary
RETH_BINARY="./target/release/reth"

# Start Node 1 (Validator-001)
echo "Starting Node 1 (Validator-001) on ports: P2P=30303, HTTP=8545, Auth=8551, Narwhal=9001"
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
  > /home/peastew/.neura/node1/node.log 2>&1 &

NODE1_PID=$!
echo "Node 1 started with PID: $NODE1_PID"

sleep 1

# Start Node 2 (Validator-002)  
echo "Starting Node 2 (Validator-002) on ports: P2P=30304, HTTP=8546, Auth=8552, Narwhal=9002"
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
  > /home/peastew/.neura/node2/node.log 2>&1 &

NODE2_PID=$!
echo "Node 2 started with PID: $NODE2_PID"

sleep 1

# Start Node 3 (Validator-003)
echo "Starting Node 3 (Validator-003) on ports: P2P=30305, HTTP=8547, Auth=8553, Narwhal=9003"
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
  > /home/peastew/.neura/node3/node.log 2>&1 &

NODE3_PID=$!
echo "Node 3 started with PID: $NODE3_PID"

sleep 1

# Start Node 4 (Validator-004)
echo "Starting Node 4 (Validator-004) on ports: P2P=30306, HTTP=8548, Auth=8554, Narwhal=9004"
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
  > /home/peastew/.neura/node4/node.log 2>&1 &

NODE4_PID=$!
echo "Node 4 started with PID: $NODE4_PID"

sleep 3

echo ""
echo "✅ All 4 Neura validator nodes started!"
echo "======================================"
echo "📊 Network: Neura (Chain ID: 266, Coin: ANKR)"
echo "🔗 Consensus: Narwhal + Bullshark BFT"  
echo "💾 Storage: Independent MDBX per node"
echo ""
echo "Node 1 PID: $NODE1_PID - HTTP: http://localhost:8545 - Logs: /home/peastew/.neura/node1/node.log"
echo "Node 2 PID: $NODE2_PID - HTTP: http://localhost:8546 - Logs: /home/peastew/.neura/node2/node.log"  
echo "Node 3 PID: $NODE3_PID - HTTP: http://localhost:8547 - Logs: /home/peastew/.neura/node3/node.log"
echo "Node 4 PID: $NODE4_PID - HTTP: http://localhost:8548 - Logs: /home/peastew/.neura/node4/node.log"
echo ""
echo "🔧 Test Commands:"
echo "  Monitor logs: tail -f /home/peastew/.neura/node1/node.log"
echo "  Stop all nodes: pkill -f 'reth.*node.*narwhal'"
echo "  Monitor status: ./monitor_multivalidator.sh"
echo ""
echo "🌐 RPC Test Commands:"
echo "  Test Node 1: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"web3_clientVersion\",\"params\":[],\"id\":1}' http://localhost:8545"
echo "  Test Node 2: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[],\"id\":1}' http://localhost:8546" 