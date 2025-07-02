#!/bin/bash

echo "🚀 Starting 2-node test to debug Unknown Authority error"

# Kill any existing reth processes
pkill -f "reth.*node.*narwhal" || true
sleep 2

# Use debug binary for better error messages
RETH_BINARY="./target/debug/reth"

# Clean up previous data
rm -rf /home/peastew/.neura/node1/db /home/peastew/.neura/node2/db

# Start Node 1
echo "Starting Node 1..."
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
  --narwhal.peers 127.0.0.1:9002 \
  --narwhal.max-batch-delay-ms 100 \
  --validator.key-file test_validators/validator-0.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10001 \
  > /home/peastew/.neura/node1/debug.log 2>&1 &

NODE1_PID=$!
echo "Node 1 started with PID: $NODE1_PID"
sleep 3

# Start Node 2
echo "Starting Node 2..."
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
  --narwhal.peers 127.0.0.1:9001 \
  --narwhal.max-batch-delay-ms 100 \
  --validator.key-file test_validators/validator-1.json \
  --validator.config-dir test_validators \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10002 \
  > /home/peastew/.neura/node2/debug.log 2>&1 &

NODE2_PID=$!
echo "Node 2 started with PID: $NODE2_PID"

echo ""
echo "✅ 2-node test started"
echo "Monitor logs:"
echo "  tail -f /home/peastew/.neura/node1/debug.log"
echo "  tail -f /home/peastew/.neura/node2/debug.log"
echo ""
echo "Check for Unknown Authority errors:"
echo "  grep -A 5 'Unknown authority' /home/peastew/.neura/node*/debug.log"