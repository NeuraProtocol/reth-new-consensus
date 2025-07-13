# Validator Key Generation and Setup Scripts

This directory contains scripts for managing validator keys and configuration for the Neura blockchain (Narwhal + Bullshark consensus).

## Key Generation Tool

The key generation tool is a Rust program that creates BLS consensus keys deterministically from Ethereum private keys.

### Building the Key Generator

```bash
cargo build -p example-narwhal-bullshark-consensus --example generate_validator_keys --release
```

### Usage

#### Generate a Single Validator

```bash
cargo run -p example-narwhal-bullshark-consensus --example generate_validator_keys -- single <eth_private_key> [name] [stake] [network_address] [worker_port_range]
```

Example:
```bash
cargo run -p example-narwhal-bullshark-consensus --example generate_validator_keys -- single 0x1111111111111111111111111111111111111111111111111111111111111111 "Validator 1" 1000 "127.0.0.1:9001" "19000:19003"
```

#### Generate Test Validators (Batch Mode)

```bash
cargo run -p example-narwhal-bullshark-consensus --example generate_validator_keys -- batch
```

This generates 4 test validators with predefined private keys (0x111...111, 0x222...222, 0x333...333, 0x444...444).

### Key Format

The tool generates:
- **EVM Address**: Derived from the Ethereum private key
- **Consensus Private Key**: BLS12-381 private key (32 bytes, base64 encoded)
- **Consensus Public Key**: BLS12-381 public key (96 bytes uncompressed G1 point, base64 encoded)

### Key Derivation

Consensus keys are derived deterministically using:
1. Blake2b hash function with 32-byte output
2. Domain separation: `"NEURA_CONSENSUS_KEY_DERIVATION_V1" || evm_private_key`
3. The resulting hash is used as seed for BLS key generation

## Setup Scripts

### setup_validator_data.sh

Sets up validator configuration files for a 4-node test network.

```bash
./scripts/setup_validator_data.sh
```

This script:
1. Copies validator JSON files from `test_validators/` to `~/.neura/node[1-4]/`
2. Transforms the JSON structure to match the expected format
3. Creates committee.json files for all nodes

## Validator File Format

### validator.json
```json
{
  "evm_address": "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
  "evm_private_key": "0x1111111111111111111111111111111111111111111111111111111111111111",
  "consensus_private_key": "Zps/5zoe3TmK8gDCuAuQmJDghHfTQSDm7svIiKFd0eo=",
  "consensus_public_key": "lBUYJAfUwdHyezP/rfRCCy3UDrx7R2h/Xh0iskG5+wL3y3kE0uG6UTfKvfnje1XzFqmkmht2SARzZaBT6G+0JwQ9vphfHrrnCTTX6dqqFlB4g/BYUbBTaS3rrdR/JzB3",
  "name": "Test Validator 0",
  "stake": 1000
}
```

### committee.json
```json
{
  "epoch": 0,
  "validators": [
    {
      "name": "Test Validator 0",
      "evm_address": "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
      "consensus_public_key": "lBUYJAfUwdHyezP/rfRCCy3UDrx7R2h/Xh0iskG5+wL3y3kE0uG6UTfKvfnje1XzFqmkmht2SARzZaBT6G+0JwQ9vphfHrrnCTTX6dqqFlB4g/BYUbBTaS3rrdR/JzB3",
      "stake": 1000,
      "network_address": "127.0.0.1:9001",
      "worker_port_range": "19000:19003"
    }
  ]
}
```

## Important Notes

1. **Deterministic Keys**: The consensus keys are derived deterministically from the Ethereum private key, ensuring consistency across restarts.

2. **BLS Format**: The consensus uses BLS12-381 with 96-byte uncompressed G1 points for public keys.

3. **Network Ports**: Each validator needs:
   - 1 primary port (e.g., 9001)
   - 4 worker ports (e.g., 19000-19003)

4. **Committee Configuration**: All validators must have the same committee.json file listing all participants.