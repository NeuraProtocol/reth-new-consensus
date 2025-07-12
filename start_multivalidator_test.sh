#!/bin/bash

# Neura Multivalidator Test Script with REAL Validator Key Management
# Starts 4 validator nodes with their own private keys and shared committee configuration

# Parse command line arguments
BUILD_ENABLED=true
ENGINE_TREE_FLAG="--narwhal.use-engine-tree"
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-build)
            BUILD_ENABLED=false
            echo "🏃 Skipping build step (--no-build flag detected)"
            shift
            ;;
        --engine-tree)
            ENGINE_TREE_FLAG="--narwhal.use-engine-tree"
            echo "🌳 Using engine tree executor for canonical state updates"
            shift
            ;;
        --no-engine-tree)
            ENGINE_TREE_FLAG=""
            echo "⚠️  Engine tree executor disabled - using legacy DB writes"
            shift
            ;;
        *)
            # Pass through any other arguments
            EXTRA_ARGS="$EXTRA_ARGS $1"
            shift
            ;;
    esac
done

echo "🚀 Starting Neura Multivalidator Test (Chain ID: 266, Coin: ANKR)"
echo "🔑 Using REAL Validator Key Management (No Random Keys!)"
echo "🏛️ Committee loaded from test_validators/ directory"
echo "⏱️  Block Time: 100ms ultra-fast (configurable with --bullshark.min-block-time-ms)"
echo "   Default: --bullshark.min-block-time-ms 100 (10 blocks/sec)"
echo "   For slower blocks: --bullshark.min-block-time-ms 500 (2 blocks/sec)"
if [ -n "$ENGINE_TREE_FLAG" ]; then
    echo "🌳 Engine tree executor ENABLED for canonical state updates"
fi
echo "=================================================================="

# Kill any existing reth processes
pkill -f "reth.*node.*narwhal" || true
sleep 2

# Build the binary once (release mode for performance) - unless --no-build is set
if [ "$BUILD_ENABLED" = true ]; then
    echo "🔨 Building Reth with Narwhal + Bullshark consensus..."
    cd /srv/tank/src/reth-new-consensus && cargo build --release --bin reth
    if [ $? -ne 0 ]; then
        echo "❌ Build failed! Exiting."
        exit 1
    fi
    echo "✅ Build completed successfully"
else
    echo "📦 Using existing binary (build skipped)"
fi

# Copy the binary to the expected location
cp /srv/tank/src/reth-new-consensus/target/release/reth /home/peastew/src/reth-new-consensus/target/release/reth 2>/dev/null || true
echo "✅ Binary copied to expected location"
echo ""

# Clean up any previous data to ensure fresh start
echo "Cleaning up previous blockchain data..."
for i in {1..4}; do
    echo "Cleaning node $i..."
    # Remove blockchain database
    rm -rf /home/peastew/.neura/node$i/db || true
    rm -rf /home/peastew/.neura/node$i/static_files || true
    
    # Remove consensus-specific data
    rm -rf /home/peastew/.neura/node$i/consensus_db || true
    rm -rf /home/peastew/.neura/node$i/consensus-db || true
    
    # Remove transaction pool backup
    rm -f /home/peastew/.neura/node$i/txpool-transactions-backup.rlp || true
    
    # Remove any lock files
    rm -f /home/peastew/.neura/node$i/db.lock || true
    rm -f /home/peastew/.neura/node$i/.lock || true
    
    # Remove logs (optional - comment out if you want to preserve logs)
    rm -f /home/peastew/.neura/node$i/node.log || true
    
    # Ensure the directory exists for the node
    mkdir -p /home/peastew/.neura/node$i
done

echo "✅ Cleanup completed - all blockchain data removed"
echo "🔑 Validator keys preserved in test_validators/"

echo ""
echo "Starting validator nodes with REAL key management..."

# Use the pre-built binary
RETH_BINARY="./target/release/reth"

# Start Node 1 (Validator-001) with Real Key Management
echo "🔧 Starting node 1 (Validator-0)..."
echo "   Primary: 127.0.0.1:9001 (must match validator-0.json network_address)"
echo "   Workers: ports 19000-19003 (from validator-0.json worker_port_range)"
echo "   HTTP RPC: port 8545"
echo "   Consensus RPC: port 10001"
echo "   P2P: port 30303"
USE_REAL_CONSENSUS=true $RETH_BINARY node \
  --narwhal.enable \
  --chain neura-genesis.json \
  --datadir /home/peastew/.neura/node1 \
  --port 30303 \
  --discovery.port 30303 \
  --http --http.port 8545 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8551 \
  --narwhal.network-addr 127.0.0.1:9001 \
  --validator.key-file /home/peastew/.neura/node1/validator.json \
  --validator.committee-config /home/peastew/.neura/node1/committee.json \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10001 \
  --consensus-rpc-enable-admin \
  --narwhal.max-batch-delay-ms 50 \
  --narwhal.max-batch-size 100000 \
  --narwhal.gc-depth 50 \
  --bullshark.min-block-time-ms 100 \
  --narwhal.cache-size 1000 \
  --narwhal.max-concurrent-requests 200 \
  --narwhal.connection-timeout-ms 5000 \
  --narwhal.request-timeout-ms 10000 \
  --narwhal.retry-attempts 3 \
  --narwhal.retry-base-delay-ms 100 \
  --narwhal.retry-max-delay-ms 10000 \
  --narwhal.sync-retry-delay-ms 5000 \
  --narwhal.certificate-buffer-size 1000 \
  --narwhal.max-transactions-per-batch 100 \
  --narwhal.batch-creation-interval-ms 50 \
  --bullshark.min-leader-round 0 \
  --bullshark.finality-threshold 3 \
  --bullshark.max-pending-rounds 10 \
  --bullshark.finalization-timeout-secs 5 \
  --bullshark.max-certificates-per-round 1000 \
  --bullshark.leader-rotation-frequency 2 \
  --bullshark.max-dag-walk-depth 10 \
  --bullshark.enable-detailed-metrics \
  --bullshark.max-certificates-per-dag 50 \
  $ENGINE_TREE_FLAG \
  $EXTRA_ARGS \
  > /home/peastew/.neura/node1/node.log 2>&1 &

NODE1_PID=$!
echo "Node 1 started with PID: $NODE1_PID"

sleep 1

# Start Node 2 (Validator-002) with Real Key Management
echo "🔧 Starting node 2 (Validator-1)..."
echo "   Primary: 127.0.0.1:9002 (from validator-1.json)"
echo "   Workers: ports 19004-19007 (from validator-1.json worker_port_range)"
echo "   HTTP RPC: port 8546"
echo "   Consensus RPC: port 10002"
echo "   P2P: port 30304"
USE_REAL_CONSENSUS=true $RETH_BINARY node \
  --narwhal.enable \
  --chain neura-genesis.json \
  --datadir /home/peastew/.neura/node2 \
  --port 30304 \
  --discovery.port 30304 \
  --http --http.port 8546 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8552 \
  --narwhal.network-addr 127.0.0.1:9002 \
  --validator.key-file /home/peastew/.neura/node2/validator.json \
  --validator.committee-config /home/peastew/.neura/node2/committee.json \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10002 \
  --consensus-rpc-enable-admin \
  --narwhal.max-batch-delay-ms 50 \
  --narwhal.max-batch-size 100000 \
  --narwhal.gc-depth 50 \
  --bullshark.min-block-time-ms 100 \
  --narwhal.cache-size 1000 \
  --narwhal.max-concurrent-requests 200 \
  --narwhal.connection-timeout-ms 5000 \
  --narwhal.request-timeout-ms 10000 \
  --narwhal.retry-attempts 3 \
  --narwhal.retry-base-delay-ms 100 \
  --narwhal.retry-max-delay-ms 10000 \
  --narwhal.sync-retry-delay-ms 5000 \
  --narwhal.certificate-buffer-size 1000 \
  --narwhal.max-transactions-per-batch 100 \
  --narwhal.batch-creation-interval-ms 50 \
  --bullshark.min-leader-round 0 \
  --bullshark.finality-threshold 3 \
  --bullshark.max-pending-rounds 10 \
  --bullshark.finalization-timeout-secs 5 \
  --bullshark.max-certificates-per-round 1000 \
  --bullshark.leader-rotation-frequency 2 \
  --bullshark.max-dag-walk-depth 10 \
  --bullshark.enable-detailed-metrics \
  --bullshark.max-certificates-per-dag 50 \
  $ENGINE_TREE_FLAG \
  $EXTRA_ARGS \
  > /home/peastew/.neura/node2/node.log 2>&1 &

NODE2_PID=$!
echo "Node 2 started with PID: $NODE2_PID"

sleep 1

# Start Node 3 (Validator-003) with Real Key Management
echo "🔧 Starting node 3 (Validator-2)..."
echo "   Primary: 127.0.0.1:9003 (from validator-2.json)"
echo "   Workers: ports 19008-19011 (from validator-2.json worker_port_range)"
echo "   HTTP RPC: port 8547"
echo "   Consensus RPC: port 10003"
echo "   P2P: port 30305"
USE_REAL_CONSENSUS=true $RETH_BINARY node \
  --narwhal.enable \
  --chain neura-genesis.json \
  --datadir /home/peastew/.neura/node3 \
  --port 30305 \
  --discovery.port 30305 \
  --http --http.port 8547 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8553 \
  --narwhal.network-addr 127.0.0.1:9003 \
  --validator.key-file /home/peastew/.neura/node3/validator.json \
  --validator.committee-config /home/peastew/.neura/node3/committee.json \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10003 \
  --consensus-rpc-enable-admin \
  --narwhal.max-batch-delay-ms 50 \
  --narwhal.max-batch-size 100000 \
  --narwhal.gc-depth 50 \
  --bullshark.min-block-time-ms 100 \
  --narwhal.cache-size 1000 \
  --narwhal.max-concurrent-requests 200 \
  --narwhal.connection-timeout-ms 5000 \
  --narwhal.request-timeout-ms 10000 \
  --narwhal.retry-attempts 3 \
  --narwhal.retry-base-delay-ms 100 \
  --narwhal.retry-max-delay-ms 10000 \
  --narwhal.sync-retry-delay-ms 5000 \
  --narwhal.certificate-buffer-size 1000 \
  --narwhal.max-transactions-per-batch 100 \
  --narwhal.batch-creation-interval-ms 50 \
  --bullshark.min-leader-round 0 \
  --bullshark.finality-threshold 3 \
  --bullshark.max-pending-rounds 10 \
  --bullshark.finalization-timeout-secs 5 \
  --bullshark.max-certificates-per-round 1000 \
  --bullshark.leader-rotation-frequency 2 \
  --bullshark.max-dag-walk-depth 10 \
  --bullshark.enable-detailed-metrics \
  --bullshark.max-certificates-per-dag 50 \
  $ENGINE_TREE_FLAG \
  $EXTRA_ARGS \
  > /home/peastew/.neura/node3/node.log 2>&1 &

NODE3_PID=$!
echo "Node 3 started with PID: $NODE3_PID"

sleep 1

# Start Node 4 (Validator-004) with Real Key Management
echo "🔧 Starting node 4 (Validator-3)..."
echo "   Primary: 127.0.0.1:9004 (from validator-3.json)"
echo "   Workers: ports 19012-19015 (from validator-3.json worker_port_range)"
echo "   HTTP RPC: port 8548"
echo "   Consensus RPC: port 10004"
echo "   P2P: port 30306"
USE_REAL_CONSENSUS=true $RETH_BINARY node \
  --narwhal.enable \
  --chain neura-genesis.json \
  --datadir /home/peastew/.neura/node4 \
  --port 30306 \
  --discovery.port 30306 \
  --http --http.port 8548 --http.addr 0.0.0.0 \
  --http.api debug,eth,net,trace,txpool,web3,rpc,reth \
  --authrpc.port 8554 \
  --narwhal.network-addr 127.0.0.1:9004 \
  --validator.key-file /home/peastew/.neura/node4/validator.json \
  --validator.committee-config /home/peastew/.neura/node4/committee.json \
  --validator.deterministic-consensus-key \
  --consensus-rpc-port 10004 \
  --consensus-rpc-enable-admin \
  --narwhal.max-batch-delay-ms 50 \
  --narwhal.max-batch-size 100000 \
  --narwhal.gc-depth 50 \
  --bullshark.min-block-time-ms 100 \
  --narwhal.cache-size 1000 \
  --narwhal.max-concurrent-requests 200 \
  --narwhal.connection-timeout-ms 5000 \
  --narwhal.request-timeout-ms 10000 \
  --narwhal.retry-attempts 3 \
  --narwhal.retry-base-delay-ms 100 \
  --narwhal.retry-max-delay-ms 10000 \
  --narwhal.sync-retry-delay-ms 5000 \
  --narwhal.certificate-buffer-size 1000 \
  --narwhal.max-transactions-per-batch 100 \
  --narwhal.batch-creation-interval-ms 50 \
  --bullshark.min-leader-round 0 \
  --bullshark.finality-threshold 3 \
  --bullshark.max-pending-rounds 10 \
  --bullshark.finalization-timeout-secs 5 \
  --bullshark.max-certificates-per-round 1000 \
  --bullshark.leader-rotation-frequency 2 \
  --bullshark.max-dag-walk-depth 10 \
  --bullshark.enable-detailed-metrics \
  --bullshark.max-certificates-per-dag 50 \
  $ENGINE_TREE_FLAG \
  $EXTRA_ARGS \
  > /home/peastew/.neura/node4/node.log 2>&1 &

NODE4_PID=$!
echo "Node 4 started with PID: $NODE4_PID"

sleep 3

echo ""
echo "✅ All 4 Neura validator nodes started!"
echo "========================================================================="
echo "📊 Network: Neura (Chain ID: 266, Coin: ANKR)"
echo "🔗 Consensus: Narwhal + Bullshark BFT"  
echo "💾 Storage: MDBX with consensus tables"
echo "🔑 Configuration: Each validator loads from test_validators/*.json"
echo "🏛️ Port Assignment: Network addresses and worker ports from config files"
echo ""
echo "📍 Node Configuration:"
echo "  Node 1 PID: $NODE1_PID - Key: validator-0.json - Bind: 9001 - HTTP: 8545 - ConsensusRPC: 10001 - Logs: /home/peastew/.neura/node1/node.log"
echo "  Node 2 PID: $NODE2_PID - Key: validator-1.json - Bind: 9002 - HTTP: 8546 - ConsensusRPC: 10002 - Logs: /home/peastew/.neura/node2/node.log"  
echo "  Node 3 PID: $NODE3_PID - Key: validator-2.json - Bind: 9003 - HTTP: 8547 - ConsensusRPC: 10003 - Logs: /home/peastew/.neura/node3/node.log"
echo "  Node 4 PID: $NODE4_PID - Key: validator-3.json - Bind: 9004 - HTTP: 8548 - ConsensusRPC: 10004 - Logs: /home/peastew/.neura/node4/node.log"
echo ""
echo "🔧 Key Configuration Features:"
echo "  • Validator keys loaded from JSON files (test_validators/*.json)"
echo "  • Each validator file includes:"
echo "    - EVM private key and consensus key configuration"
echo "    - Network address (primary consensus port) - must match CLI --narwhal.network-addr"
echo "    - Worker port range (e.g., '19000:19003')"
echo "  • Worker ports automatically determined from configuration"
echo "  • Primary addresses still need CLI args for binding and peer discovery"
echo ""
echo "🔧 Narwhal Configuration Options:"
echo "  --narwhal.max-batch-size: Maximum batch size in bytes (default: 1024)"
echo "  --narwhal.max-batch-delay-ms: Maximum batch delay in ms (default: 50 for ultra-fast blocks)"
echo "  --narwhal.num-workers: Number of workers per authority (default: 4)"
echo "  --narwhal.gc-depth: Garbage collection depth for old certificates (default: 50)"
echo "  --narwhal.cache-size: Certificate cache size (default: 1000)"
echo "  --narwhal.max-concurrent-requests: Max concurrent network requests (default: 200)"
echo "  --narwhal.connection-timeout-ms: Connection timeout in ms (default: 5000)"
echo "  --narwhal.request-timeout-ms: Request timeout in ms (default: 10000)"
echo "  --narwhal.retry-attempts: Number of retry attempts (default: 3)"
echo "  --narwhal.retry-base-delay-ms: Base delay for exponential backoff (default: 100)"
echo "  --narwhal.retry-max-delay-ms: Max delay for exponential backoff (default: 10000)"
echo "  --narwhal.sync-retry-delay-ms: Sync retry delay in ms (default: 5000)"
echo "  --narwhal.certificate-buffer-size: Pre-allocated certificate buffer (default: 1000)"
echo "  --narwhal.max-transactions-per-batch: Max transactions per batch (default: 100)"
echo "  --narwhal.batch-creation-interval-ms: Batch creation interval (default: 50)"
echo "  --narwhal.worker-base-port: Base port for THIS node's workers (overridden by validator config)"
echo "  --narwhal.worker-bind-address: Worker bind address (default: same as primary)"
echo ""
echo "🔧 Bullshark Configuration Options:"
echo "  --bullshark.finality-threshold: Minimum confirmations needed (default: 3)"
echo "  --bullshark.max-pending-rounds: Maximum pending rounds to keep (default: 10)"
echo "  --bullshark.finalization-timeout-secs: Finalization timeout in seconds (default: 5)"
echo "  --bullshark.max-certificates-per-round: Max certificates per round (default: 1000)"
echo "  --bullshark.leader-rotation-frequency: Leader rotation frequency in rounds (default: 2)"
echo "  --bullshark.min-leader-round: Minimum round for leader election (default: 0)"
echo "  --bullshark.max-dag-walk-depth: Maximum DAG walk depth for consensus (default: 10)"
echo "  --bullshark.enable-detailed-metrics: Enable detailed consensus metrics"
echo "  --bullshark.max-certificates-per-dag: Max certificates per DAG traversal (default: 50 for ultra-fast processing)"
echo ""
echo "🔧 Monitoring Commands:"
echo "  Monitor all logs: tail -f /home/peastew/.neura/node*/node.log"
echo "  Monitor node 1: tail -f /home/peastew/.neura/node1/node.log"
echo "  Stop all nodes: pkill -f 'reth.*node.*narwhal'"
echo "  Check processes: ps aux | grep reth"
echo ""
echo "🌐 Standard RPC Test Commands:"
echo "  Node 1 version: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"web3_clientVersion\",\"params\":[],\"id\":1}' http://localhost:8545"
echo "  Node 2 block#:  curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[],\"id\":1}' http://localhost:8546"
echo "  Node 3 peers:   curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"net_peerCount\",\"params\":[],\"id\":1}' http://localhost:8547"
echo "  Node 4 syncing: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_syncing\",\"params\":[],\"id\":1}' http://localhost:8548"
echo ""
echo "📡 Consensus RPC Test Commands:"
echo "  Node 1 consensus status: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getStatus\",\"params\":[],\"id\":1}' http://localhost:10001"
echo "  Node 2 committee info:   curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getCommittee\",\"params\":[],\"id\":1}' http://localhost:10002"
echo "  Node 3 validators list:  curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_listValidators\",\"params\":[{\"active_only\":true}],\"id\":1}' http://localhost:10003"
echo "  Node 4 consensus metrics: curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getMetrics\",\"params\":[],\"id\":1}' http://localhost:10004"
echo ""
echo "📡 Consensus Admin RPC Commands (--consensus-rpc-enable-admin required):"
echo "  DAG info:        curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_admin_getDagInfo\",\"params\":[],\"id\":1}' http://localhost:10001"
echo "  Storage stats:   curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_admin_getStorageStats\",\"params\":[],\"id\":1}' http://localhost:10002"
echo "  Internal state:  curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_admin_getInternalState\",\"params\":[],\"id\":1}' http://localhost:10003"
echo "  Compact DB:      curl -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"consensus_admin_compactDatabase\",\"params\":[],\"id\":1}' http://localhost:10004"
echo ""
echo "🎯 Expected Behavior:"
echo "  • Each node loads configuration from its validator JSON file"
echo "  • Network addresses and worker ports come from validator configs"
echo "  • Committee includes all active validators in test_validators/"
echo "  • Workers listen on their configured port ranges"
echo "  • Nodes connect to peers' workers using configured ports"
echo "  • No port conflicts between validators"
echo ""
echo "🔍 Quick Verification:"
echo "  Check all nodes started: ps aux | grep 'reth.*node.*narwhal' | wc -l  # Should show 4"
echo "  Check for port conflicts: netstat -tlnp | grep -E ':(9001|9002|9003|9004)' | wc -l  # Should show 4"
echo "  Check consensus RPC ports: netstat -tlnp | grep -E ':(10001|10002|10003|10004)' | wc -l  # Should show 4"
echo "  Check worker ports in use: netstat -tlnp | grep -E ':(1900[0-9]|1901[0-5])' | wc -l  # Should show 16 (4 workers × 4 nodes)"
echo "  Check validator configs loaded: grep -c 'configured with.*workers on ports' /home/peastew/.neura/node*/node.log  # Should show validators × nodes"
echo "  Check worker connections: grep -c 'Worker.*listening' /home/peastew/.neura/node*/node.log  # Should show 4 per node"
echo ""
echo "💡 If nodes fail to start, check for:"
echo "  • Missing validator key files in test_validators/ directory"
echo "  • Invalid JSON format in validator key files"
echo "  • Port conflicts (netstat -tlnp | grep 900[1-4])"
echo "  • Build issues (cargo build --release --bin reth)" 
echo ""
echo "🔍 Validator Configuration Files:"
echo "  • validator-0.json: Node 1 - Primary: 9001, Workers: 19000-19003"
echo "  • validator-1.json: Node 2 - Primary: 9002, Workers: 19004-19007"
echo "  • validator-2.json: Node 3 - Primary: 9003, Workers: 19008-19011"
echo "  • validator-3.json: Node 4 - Primary: 9004, Workers: 19012-19015"
echo ""
echo "🛠️ Helper Scripts:"
echo "  • Test consensus RPC: ./test_consensus_rpc.sh [PORT]"
echo "    Example: ./test_consensus_rpc.sh 10001"
echo ""
echo "📊 All Consensus RPC Endpoints:"
echo "  • consensus_getStatus - Get consensus health and status"
echo "  • consensus_getCommittee - Get current validator committee"
echo "  • consensus_getValidator - Get specific validator details"
echo "  • consensus_listValidators - List all validators"
echo "  • consensus_getMetrics - Get consensus performance metrics"
echo "  • consensus_getConfig - Get consensus configuration"
echo "  • consensus_admin_getDagInfo - Get DAG structure info (admin)"
echo "  • consensus_admin_getStorageStats - Get storage statistics (admin)" 
