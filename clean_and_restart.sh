#!/bin/bash

echo "🧹 Cleaning all node data..."
pkill -f "reth.*node.*narwhal" || true
sleep 2

# Clean all database files
rm -rf ~/.neura/node*/db
rm -rf ~/.neura/node*/static_files
rm -f consensus_db_*.redb

echo "✅ Clean complete"
echo "🚀 Starting multi-validator test..."

# Start the test
./start_multivalidator_test.sh