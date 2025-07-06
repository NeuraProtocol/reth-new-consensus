#!/bin/bash

# Send test transaction using curl
echo "Sending test transaction to trigger block creation..."

curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_sendTransaction",
    "params": [{
      "from": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
      "to": "0x0000000000000000000000000000000000000002",
      "value": "0x1",
      "gas": "0x5208",
      "gasPrice": "0x3B9ACA00"
    }],
    "id": 1
  }'

echo
echo "Transaction sent!"