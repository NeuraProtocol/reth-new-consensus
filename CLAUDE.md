# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a fork of Reth (Rust Ethereum) that integrates Narwhal + Bullshark consensus, replacing Ethereum's standard consensus with a DAG-based BFT consensus protocol. The chain is called "Neura" (Chain ID: 266, Coin: ANKR).

### Architecture Flow
1. **Transaction Pool** → Transactions collected by Reth mempool
2. **Batching** → Narwhal creates batches from transactions
3. **DAG Construction** → Narwhal builds Certificate DAG via reliable broadcast
4. **Consensus** → Bullshark runs BFT consensus on the DAG
5. **Block Construction** → Consensus output becomes Reth blocks
6. **Execution** → Reth executes blocks with revm

## Build and Test Commands

```bash
# Build
make build                          # Release build
make build-debug                    # Debug build
cargo build --release --bin reth    # Direct cargo build

# Test
make test-unit                      # Unit tests with nextest
cargo test -p narwhal              # Test Narwhal crate
cargo test -p bullshark            # Test Bullshark crate
cargo test -p reth-consensus -- narwhal  # Integration tests

# Lint and Format
make lint                          # Run all lints
make fmt                           # Format code
make clippy                        # Run clippy

# Run Multi-Validator Test
./start_multivalidator_test.sh     # Starts 4 validator nodes
tail -f ~/.neura/node*/node.log   # Monitor logs
pkill -f "reth.*node.*narwhal"    # Stop all nodes
```

## Current Branch Status

Working on branch `v1.4.8-neura` with recent commits showing functional consensus and DAG implementation. The integration is working but several production features remain as TODOs.

## Important TODOs (Stubs to Implement)

1. **Persistent Storage** - Currently uses in-memory storage (see `crates/narwhal/src/dag_service.rs`)
2. **Cryptographic Keys** - Using placeholder keys (see `crates/consensus/consensus/src/narwhal_bullshark/validator_keys.rs`)
3. **Worker-Primary Communication** - Batch fetching not implemented (`crates/narwhal/src/primary.rs`)
4. **State Root Calculations** - Returns dummy values (`crates/consensus/consensus/src/narwhal_bullshark/service.rs`)
5. **Peer Discovery** - Static peer list only (`crates/node/core/src/args/narwhal_bullshark_args.rs`)

## Key Files to Understand

- `crates/consensus/consensus/src/narwhal_bullshark/` - Main integration layer
  - `service.rs` - Consensus service implementation
  - `integration.rs` - Reth integration hooks
  - `types.rs` - Type conversions between systems
- `crates/narwhal/src/` - Narwhal DAG implementation
  - `core.rs` - Core Narwhal primary logic
  - `dag_service.rs` - DAG construction and storage
- `crates/bullshark/src/` - Bullshark BFT consensus
- `bin/reth/src/narwhal_bullshark.rs` - CLI integration
- `test_validators/` - Validator configuration files

## Running the System

1. **Single Node**: See command examples in `start_multivalidator_test.sh`
2. **Multi-Validator**: Run `./start_multivalidator_test.sh` (requires 4 validators)
3. **RPC Endpoints**: Nodes expose HTTP RPC on ports 8545-8548

## Validator Configuration

Validators use JSON config files with format:
```json
{
  "address": "0x...",
  "public_key": "0x...",
  "private_key": "0x...",
  "stake": 1000
}
```

## Key Architectural Decisions

1. **Async Everything** - Heavy use of tokio for consensus operations
2. **Type Safety** - Conversion traits between Narwhal/Bullshark and Reth types
3. **Modular Design** - Clean separation between consensus layers
4. **In-Memory First** - Current implementation optimizes for testing over persistence

## Development Tips

- When modifying consensus, test with `cargo test -p narwhal -- --nocapture` for logs
- The multi-validator test script is the best way to test full integration
- Check `NARWHAL_BULLSHARK_INTEGRATION_SUMMARY.md` for detailed implementation status
- Most consensus logic is async - ensure proper tokio runtime handling
- Validator indices are 0-based and must match configuration order

## Byzantine Fault Tolerance

- Supports f Byzantine faults where f = ⌊(n-1)/3⌋
- Default test configuration: 4 validators (tolerates 1 Byzantine fault)
- Consensus requires 2f+1 votes to make progress

## Production Roadmap

To make this production-ready, focus on:
1. Replace in-memory storage with persistent DB
2. Implement proper key management (not hardcoded)
3. Add worker nodes for transaction processing
4. Implement proper state root calculations
5. Add dynamic peer discovery
6. Performance optimization and benchmarking