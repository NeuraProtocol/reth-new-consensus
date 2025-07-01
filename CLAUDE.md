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

### Critical (Blocking Functionality)
1. ✅ **MDBX Storage Integration** - COMPLETED (with workarounds)
   - Implemented trait-based approach with ConsensusDbOps
   - Real storage operations for certificates and votes
   - Note: Vote/round indexing uses DAG vertices table as workaround
   - TODO: Implement proper ConsensusVotes and ConsensusCertificatesByRound table access
   - Location: `crates/narwhal/src/storage_mdbx.rs`
   
2. **Network Broadcasting** - No actual P2P communication implemented
   - Headers, votes, certificates not broadcast to peers
   - Worker batch replication not implemented
   - Locations: `crates/narwhal/src/dag_service.rs:265`, `quorum_waiter.rs:149`
   
3. **Transaction Processing** - Cannot decode/extract real transactions
   - `Transaction::to_alloy_transaction()` returns error
   - Bullshark creates dummy transactions instead of real ones
   - Locations: `crates/narwhal/src/lib.rs:59`, `crates/bullshark/src/bft_service.rs:213-216`

### High Priority
4. **Consensus Seal Generation** - Using dummy seals instead of proper proofs
   - Location: `crates/consensus/consensus/src/narwhal_bullshark/integration.rs:374`
   
5. **Parent Hash Retrieval** - Using B256::ZERO instead of actual parent
   - Location: `crates/bullshark/src/bft_service.rs:242`
   
6. **Vote Signing** - Using default signatures instead of proper signing
   - Location: `crates/narwhal/src/types.rs:265`

### Medium Priority
7. **RPC Implementation** - All methods return hardcoded values
   - No connection to actual consensus state
   - Location: `crates/consensus/consensus/src/rpc.rs:570-651`
   
8. **Worker Batch Fetching** - Cannot retrieve batches from workers
   - Location: `crates/bullshark/src/bft_service.rs:213-216`
   
9. **Metrics Collection** - Placeholder metrics only
   - Location: `crates/consensus/consensus/src/rpc.rs:706`

### Configuration
10. **Hardcoded Values** - Many timeouts and parameters should be configurable
    - Worker timeout: `crates/narwhal/src/worker.rs:139`
    - Validator stakes: `crates/consensus/consensus/src/rpc.rs:593`

## Key Files to Understand

- `crates/consensus/consensus/src/narwhal_bullshark/` - Main integration layer
  - `service.rs` - Consensus service implementation
  - `integration.rs` - Reth integration hooks
  - `types.rs` - Type conversions between systems
  - `transaction_adapter.rs` - Transaction pool to worker connection
  - `dag_storage_adapter.rs` - Storage adapter (currently in-memory)
- `crates/narwhal/src/` - Narwhal DAG implementation
  - `dag_service.rs` - DAG construction and vote aggregation
  - `aggregators.rs` - Vote and certificate aggregation with BLS
  - `batch_maker.rs` - Transaction batching for workers
  - `quorum_waiter.rs` - Batch replication quorum logic
  - `storage_mdbx.rs` - Database storage (TODO: uses HashMap)
  - `types.rs` - Core types with BLS signature support
- `crates/bullshark/src/` - Bullshark BFT consensus
  - `bft_service.rs` - Main consensus service
  - `consensus.rs` - Core BFT algorithm
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

## Recent Implementation Progress

### Completed (from reference implementation)
- ✅ Vote aggregation with proper BLS signatures (`VotesAggregator`)
- ✅ Certificate formation with quorum verification
- ✅ Round advancement based on certificate collection
- ✅ Worker components: `BatchMaker` and `QuorumWaiter`
- ✅ Worker storage tables in `consensus_tables.rs`
- ✅ Transaction adapter for pool→worker connection
- ✅ Garbage collection for old DAG state

### Implementation Status
- **Narwhal Core**: ~85% complete (✅ MDBX storage integrated, ❌ network broadcast missing)
- **Bullshark**: ~60% complete (missing transaction extraction)
- **Workers**: ~70% complete (✅ transaction pool connection, ❌ network replication missing)
- **Integration**: ~65% complete (✅ storage integration done, ❌ chain state connection missing)

## Production Roadmap

To make this production-ready, priority order:
1. ✅ **Database Integration** - MDBX storage operations implemented
2. **Network Layer** - Add P2P broadcasting for consensus messages
3. **Transaction Decoding** - Implement proper RLP decoding  
4. **Chain State** - Connect to Reth's blockchain state
5. **Cryptographic Signing** - Proper vote/header signatures
6. **Worker Replication** - Batch distribution between workers
7. **Dynamic Configuration** - Replace hardcoded values
8. **Monitoring** - Real metrics and observability

## Development Principles

- Always prioritise implementing full implementation rather than stubs, simplified versions or mocking
- **New Memory**: Never claim success before reviewing the narwhal, bullshark and consensus crates for todos, mocks, stubs and simplified versions

## Reference Implementation Location

- Always remember that there is a reference implementation at `/home/peastew/src/reth-new-consensus/narwhal-reference-implementation`