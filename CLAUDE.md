# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Last Updated**: 2025-07-03 - Chain state integration completed, consensus now tracks actual blockchain state for parent hash and block numbers.

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

## Recent Updates (2025-07-03)

15. ✅ **Test Fixes** - COMPLETED
    - ✅ Updated all test helper functions to use new Authority struct format
    - ✅ Fixed compilation errors in narwhal, bullshark, and reth-consensus tests
    - ✅ All unit and integration tests now passing
    - ✅ Authority struct now includes: stake, primary_address, network_key, workers

16. ✅ **Binary Compilation Fixes** - COMPLETED
    - ✅ Fixed missing trait implementations in bin/reth for worker batch storage
    - ✅ Added get_worker_batch method to ConsensusDbTx implementations
    - ✅ Added put_worker_batch and delete_worker_batch to ConsensusDbTxMut
    - ✅ Added get_worker_batches to DatabaseOps implementation
    - ✅ Fixed all test MockDatabaseOps to include worker_batches field
    - ✅ Binary now compiles successfully with all consensus features

17. **Reference Implementation Comparison** - ANALYZED (2025-07-03)
    After comparing with reference implementation at `/home/peastew/src/reth-new-consensus/narwhal-reference-implementation`:
    
    **Architecture Alignment:**
    - ✅ Correct separation of primary/worker responsibilities
    - ✅ Vote aggregation logic is complete (just different error handling philosophy)
    - ✅ Bullshark consensus algorithm correctly implemented
    - ✅ Worker batch creation and replication functional
    
    **Missing Production Features:**
    - ✅ **Network retry mechanisms** - Exponential backoff and retry logic implemented
    - ✅ **Connection pooling** - Bounded executors for per-peer and global limits
    - ❌ **Metrics and observability** - All metrics return hardcoded/zero values
    - ❌ **Proper shutdown handling** - Missing graceful termination
    - ❌ **Performance optimizations** - No batch pre-allocation, yield points
    
    **Remaining Stubs/Simplifications:**
    - RPC methods return hardcoded values (stakes, metrics, config)
    - Consensus seal uses dummy seal instead of proper proof
    - Vote signing uses placeholder signatures
    - Worker batch retrieval falls back to dummy transactions
    - Storage uses workarounds for vote/round indexing

18. ✅ **Network Retry Mechanisms** - IMPLEMENTED (2025-07-03)
    - ✅ Added RetryConfig with exponential backoff support
    - ✅ Implemented retry logic for all network operations
    - ✅ Added BoundedExecutor to limit concurrent tasks per peer
    - ✅ Global executor limits total concurrent network operations
    - ✅ Connection attempts now retry with backoff on transient failures
    - ✅ Broadcasts use retry logic for resilient message delivery
    - ✅ Proper error classification (transient vs permanent)
    - Locations: `crates/narwhal/src/retry.rs`, `bounded_executor.rs`, updated `network.rs`

## Important TODOs (Stubs to Implement)

### Critical (Blocking Functionality)
1. ✅ **MDBX Storage Integration** - COMPLETED (with workarounds)
   - Implemented trait-based approach with ConsensusDbOps
   - Real storage operations for certificates and votes
   - Note: Vote/round indexing uses DAG vertices table as workaround
   - TODO: Implement proper ConsensusVotes and ConsensusCertificatesByRound table access
   - Location: `crates/narwhal/src/storage_mdbx.rs`
   
2. ✅ **Network Broadcasting** - PRODUCTION READY
   - ✅ Headers, votes, certificates ARE broadcast via Anemo RPC to connected peers
   - ✅ Full P2P transport layer exists with connection management
   - ✅ Outbound network bridge connects DAG service to network broadcasts
   - ✅ Worker batch replication implemented with full RPC services
   - ✅ WorkerToWorker and PrimaryToWorker RPC handlers implemented
   - ✅ Retry mechanisms with exponential backoff implemented
   - ✅ Connection pooling with bounded executors (per-peer and global)
   - ❌ Missing: Health monitoring, circuit breakers (lower priority)
   - Locations: `crates/narwhal/src/network.rs`, `worker_network.rs`, `worker_handlers.rs`
   
3. ✅ **Transaction Processing** - COMPLETED
   - ✅ `Transaction::to_alloy_transaction()` now properly decodes RLP transactions
   - ✅ Mempool bridge connected - transactions flow from pool to consensus
   - ✅ Batch storage implemented - Bullshark retrieves actual transactions from worker batches
   - ✅ Workers store batches persistently in MDBX via WorkerBatches table
   - ⚠️ Falls back to dummy transactions when batches not found (should error instead)
   - Locations: `crates/narwhal/src/batch_store.rs`, `crates/consensus/consensus/src/narwhal_bullshark/batch_storage_adapter.rs`

### High Priority
4. **Consensus Seal Generation** - Using dummy seals instead of proper proofs
   - Location: `crates/consensus/consensus/src/narwhal_bullshark/integration.rs:424`
   
5. ✅ **Parent Hash Retrieval** - COMPLETED
   - ✅ BftService now uses ChainStateProvider to get actual parent hash
   - ✅ Chain state synchronized between integration layer and consensus
   - ✅ BlockExecutor trait provides chain tip information
   - Locations: `crates/bullshark/src/chain_state.rs`, `crates/consensus/consensus/src/narwhal_bullshark/chain_state.rs`
   
6. **Vote Signing** - Using default signatures instead of proper signing
   - Location: `crates/narwhal/src/types.rs:295`

### Medium Priority
7. **RPC Implementation** - All methods return hardcoded values
   - Hardcoded stakes: lines 625, 672, 713
   - All metrics return 0 or hardcoded values: lines 873-895
   - Configuration uses hardcoded addresses and timeouts: lines 918-921
   - Certificate deserialization returns placeholder data: line 767
   - Location: `crates/consensus/consensus/src/rpc.rs`
   
8. ✅ **Worker Batch Fetching** - COMPLETED (with fallback)
   - ✅ BftService now retrieves actual batches from MDBX storage
   - ✅ MdbxBatchStore implementation with proper serialization/deserialization
   - ⚠️ Falls back to dummy transactions instead of erroring: lines 238-258
   - Locations: `crates/bullshark/src/bft_service.rs:234-244`, `crates/narwhal/src/batch_store.rs`
   
9. **Metrics Collection** - Placeholder metrics only
   - All throughput metrics return 0.0: lines 873-877
   - Latency metrics use hardcoded values: lines 880-884
   - Resource metrics return 0: lines 887-890
   - Network message rates return 0.0: lines 894-895
   - Location: `crates/consensus/consensus/src/rpc.rs:851-889`

### Configuration
10. **Hardcoded Values** - Many timeouts and parameters should be configurable
    - Worker timeout: `crates/narwhal/src/worker.rs` - hardcoded 10 second timeout
    - Validator stakes: `crates/consensus/consensus/src/rpc.rs:625,672,713` - all return 100
    - Network config: `crates/consensus/consensus/src/rpc.rs:918-921` - hardcoded addresses/timeouts
    - Performance config: `crates/consensus/consensus/src/rpc.rs:923-927` - hardcoded cache sizes

### Additional TODOs Found
11. ✅ **Worker Components** - COMPLETED
    - ✅ Worker network with full Anemo RPC implementation
    - ✅ WorkerToWorker and PrimaryToWorker RPC services
    - ✅ Worker batch replication across network
    - ✅ Proper worker key derivation from primary keys
    - ✅ Worker addresses from Authority configuration
    
12. ✅ **Certificate Storage** - FIXED WITH BATCH STORAGE
    - ✅ Certificates no longer have empty transaction vectors
    - ✅ Actual transactions now extracted from worker batches via MDBX storage
    - ✅ Batch digests in certificates are used to retrieve full batch data

13. ✅ **DAG Storage Implementation** - COMPLETED
    - ✅ Full MDBX implementation in consensus_db_ops_impl.rs
    - ✅ Certificates stored/retrieved from ConsensusDagVertices table
    - ✅ Votes stored/retrieved with proper serialization
    - ✅ Certificate indexing by round for efficient queries
    - ✅ Latest certificate cache for authorities
    - ✅ Garbage collection for old DAG data

14. ✅ **Chain State Integration** - COMPLETED
    - ✅ ChainStateTracker for managing blockchain state
    - ✅ ChainStateProvider trait in Bullshark for state access
    - ✅ Integration layer syncs state between Reth and consensus
    - ✅ BlockExecutor trait provides chain tip information
    - ✅ Finalized batches now use actual parent hash and block numbers
    - ✅ State updates propagated after block creation

## Key Files to Understand

- `crates/consensus/consensus/src/narwhal_bullshark/` - Main integration layer
  - `service.rs` - Consensus service implementation
  - `integration.rs` - Reth integration hooks
  - `types.rs` - Type conversions between systems
  - `transaction_adapter.rs` - Transaction pool to worker connection
  - `dag_storage_adapter.rs` - Storage adapter (MDBX implementation)
  - `consensus_db_ops_impl.rs` - MDBX DAG storage operations
- `crates/narwhal/src/` - Narwhal DAG implementation
  - `dag_service.rs` - DAG construction and vote aggregation
  - `aggregators.rs` - Vote and certificate aggregation with BLS
  - `batch_maker.rs` - Transaction batching for workers
  - `quorum_waiter.rs` - Batch replication quorum logic
  - `storage_mdbx.rs` - Database storage (implemented with MDBX, some workarounds)
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
- **Narwhal Core**: ~92% complete (✅ MDBX storage, ✅ network broadcast, ✅ worker replication, ✅ full RPC, ✅ DAG persistence, ✅ retry mechanisms, ❌ metrics)
- **Bullshark**: ~95% complete (✅ consensus algorithm, ✅ batch retrieval from MDBX, ✅ chain state tracking)
- **Workers**: ~85% complete (✅ transaction pool connection, ✅ network replication, ✅ RPC services, ✅ key derivation, ✅ batch storage, ❌ metrics, ❌ performance optimizations)
- **Integration**: ~92% complete (✅ storage, ✅ mempool bridge, ✅ network resilience, ✅ workers spawned, ✅ batch storage, ✅ DAG storage, ✅ chain state, ❌ production hardening)
- **Network Layer**: ~90% complete (✅ basic connectivity, ✅ RPC handlers, ✅ retry logic, ✅ connection pooling, ❌ health monitoring)

## Production Roadmap

To make this production-ready, priority order:
1. ✅ **Database Integration** - MDBX storage operations implemented
2. ✅ **Transaction Flow** - Mempool bridge connected, RLP decoding works
3. ⚠️ **Network Layer** - Basic P2P works, needs retry/resilience features
4. ✅ **Batch Storage** - Store/retrieve worker batches for transaction extraction
5. ✅ **Chain State** - Connect to Reth's blockchain state
6. **Cryptographic Signing** - Proper vote/header signatures (currently using placeholders)
7. ✅ **Worker Replication** - Batch distribution between workers via RPC
8. **Dynamic Configuration** - Replace all hardcoded values in RPC and workers
9. **Monitoring** - Implement real metrics collection (all currently return 0)
10. **Network Resilience** - Add retry mechanisms, connection pooling, health checks
11. **Performance** - Add batch pre-allocation, yield points, bounded executors
12. **Error Handling** - Remove dummy transaction fallbacks, proper error propagation

## Development Principles

- Always prioritise implementing full implementation rather than stubs, simplified versions or mocking
- **New Memory**: Never claim success before reviewing the narwhal, bullshark and consensus crates for todos, mocks, stubs and simplified versions

## Reference Implementation Location

- Always remember that there is a reference implementation for Narhwal + Bullshark at `/home/peastew/src/reth-new-consensus/narwhal-reference-implementation`
- Always remember that there is a reference implementation for Anemo at `/home/peastew/src/reth-new-consensus/anemo-reference`