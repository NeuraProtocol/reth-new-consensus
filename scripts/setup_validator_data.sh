#!/bin/bash
# Script to set up validator data for multi-validator test
# This script transforms and copies the test validator files to the node directories

set -e

echo "Setting up validator data for 4-node test..."

# Create directories if they don't exist
for i in {1..4}; do
    mkdir -p ~/.neura/node$i
done

# Transform validator files from test_validators to the format expected by ValidatorKeyPair
echo "Creating validator files..."
for i in {0..3}; do
    node_num=$((i+1))
    src_file="/srv/tank/src/reth-new-consensus/test_validators/validator-$i.json"
    dest_file="$HOME/.neura/node$node_num/validator.json"
    
    # Read the source file and extract the fields
    if [ -f "$src_file" ]; then
        # Use jq to transform the JSON structure
        jq '{
            evm_address: .evm_address,
            evm_private_key: .evm_private_key,
            consensus_private_key: .consensus_private_key,
            consensus_public_key: .consensus_public_key,
            name: (.name // .metadata.name),
            stake: .stake
        }' "$src_file" > "$dest_file"
        echo "Created $dest_file"
    else
        echo "ERROR: $src_file not found!"
        exit 1
    fi
done

# Create committee.json with proper format
echo "Creating committee.json files..."
# Check if committee.json exists in test_validators
if [ -f "$SCRIPT_DIR/../test_validators/committee.json" ]; then
    # Use the generated committee.json
    cp "$SCRIPT_DIR/../test_validators/committee.json" ~/.neura/node1/committee.json
else
    echo "ERROR: test_validators/committee.json not found!"
    exit 1
fi

# Copy committee.json to all nodes
for i in {2..4}; do
    cp ~/.neura/node1/committee.json ~/.neura/node$i/committee.json
done

echo "✅ Validator data setup complete!"
echo ""
echo "Files created:"
echo "  - ~/.neura/node1/validator.json (from test_validators/validator-0.json)"
echo "  - ~/.neura/node2/validator.json (from test_validators/validator-1.json)"
echo "  - ~/.neura/node3/validator.json (from test_validators/validator-2.json)"
echo "  - ~/.neura/node4/validator.json (from test_validators/validator-3.json)"
echo "  - ~/.neura/node*/committee.json (same for all nodes)"
echo ""
echo "You can now run: ./start_multivalidator_test.sh"