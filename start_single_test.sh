#!/bin/bash

echo "Starting single node for testing..."

# Clean up old data
rm -rf ~/.neura/test-node

# Start node with debug logging
RUST_LOG=debug,reth::narwhal_bullshark=trace \
./target/release/reth node \
    --chain neura-mainnet \
    --datadir ~/.neura/test-node \
    --http \
    --http.addr 0.0.0.0 \
    --http.port 8545 \
    --http.api all \
    --authrpc.addr 127.0.0.1 \
    --authrpc.port 8551 \
    --port 30303 \
    --narwhal.enable \
    --validator.key-file test_validators/validator-0.json \
    --narwhal.committee-size 1 \
    --consensus-rpc-port 10001 \
    --narwhal.network-addr 127.0.0.1:9001 \
    > test-node.log 2>&1 &

echo "Node started with PID $!"
echo "Logs: tail -f test-node.log"