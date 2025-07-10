#!/bin/bash
# Test script for the new payload builder integration

set -e

echo "Testing new Narwhal+Bullshark integration with Reth payload builder..."

# Clean up previous test data
rm -rf ~/.neura_test

# Create test directories
mkdir -p ~/.neura_test/node-0

# Create genesis file
cat > ~/.neura_test/neura-genesis.json << 'EOF'
{
  "chainId": 266,
  "homesteadBlock": 0,
  "eip150Block": 0,
  "eip155Block": 0,
  "eip158Block": 0,
  "byzantiumBlock": 0,
  "constantinopleBlock": 0,
  "petersburgBlock": 0,
  "istanbulBlock": 0,
  "berlinBlock": 0,
  "londonBlock": 0,
  "parisBlock": 0,
  "shanghaiTime": 0,
  "cancunTime": 0,
  "pragueTime": 1800000000,
  "osakaTime": 2000000000,
  "config": {
    "chainId": 266,
    "homesteadBlock": 0,
    "eip150Block": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "byzantiumBlock": 0,
    "constantinopleBlock": 0,
    "petersburgBlock": 0,
    "istanbulBlock": 0,
    "berlinBlock": 0,
    "londonBlock": 0,
    "parisBlock": 0,
    "shanghaiTime": 0,
    "cancunTime": 0,
    "pragueTime": 1800000000,
    "osakaTime": 2000000000
  },
  "alloc": {
    "0x0000000000000000000000000000000000000000": {
      "balance": "0x1"
    }
  },
  "difficulty": "0x1",
  "gasLimit": "0x8000000"
}
EOF

# Create validator key
cat > ~/.neura_test/node-0/validator.json << 'EOF'
{
  "name": "validator-0",
  "consensus_public_key": "mmgp1j+GZpLvbq1p4Y7OY3KPEcvsoxO7nCJLPQq8xpnPh0Oh2sWxG0FHLEJIFx4sAA==",
  "consensus_private_key": "nMstb4fgAU8MlYKUqC/K4J5fctizqe1biENW7l8pL3k="
}
EOF

# Create committee config
cat > ~/.neura_test/node-0/committee.json << 'EOF'
{
  "epoch": 0,
  "validators": [
    {
      "name": "validator-0",
      "evm_address": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
      "consensus_public_key": "mmgp1j+GZpLvbq1p4Y7OY3KPEcvsoxO7nCJLPQq8xpnPh0Oh2sWxG0FHLEJIFx4sAA==",
      "stake": 1000000,
      "network_address": "127.0.0.1:18000",
      "worker_port_range": "19000:19003"
    }
  ]
}
EOF

echo "Starting single validator test node with new integration..."

# Run with USE_REAL_CONSENSUS=true and timeout after 30 seconds
USE_REAL_CONSENSUS=true timeout 30 ./target/release/reth node \
  --chain ~/.neura_test/neura-genesis.json \
  --datadir ~/.neura_test/node-0 \
  --port 30303 \
  --http --http.port 8545 \
  --authrpc.port 8551 \
  --ws --ws.port 8546 \
  --narwhal.enable \
  --validator.key-file ~/.neura_test/node-0/validator.json \
  --validator.committee-config ~/.neura_test/node-0/committee.json \
  --log.file.directory ~/.neura_test/node-0 \
  --debug.tip always \
  2>&1 | tee ~/.neura_test/test.log || true

echo "Test completed. Checking logs..."

# Check if consensus started
if grep -q "Starting REAL Narwhal+Bullshark consensus" ~/.neura_test/test.log; then
    echo "✅ Real consensus started successfully"
else
    echo "❌ Real consensus did not start"
fi

# Check if blocks were built
if grep -q "Built block.*with state_root" ~/.neura_test/test.log; then
    echo "✅ Blocks were built with state roots"
else
    echo "❌ No blocks built"
fi

# Check for errors
if grep -q "error" ~/.neura_test/test.log; then
    echo "⚠️  Errors found in logs:"
    grep "error" ~/.neura_test/test.log | head -5
fi

echo "Full log saved to ~/.neura_test/test.log"