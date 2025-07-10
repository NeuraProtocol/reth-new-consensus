# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Last Updated**: 2025-07-10 - Enabled real consensus and implemented base fee caching infrastructure

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
cargo build --release --bin reth

# Test Multi-Validator
./start_multivalidator_test.sh     # Starts 4 validator nodes
tail -f ~/.neura/node*/node.log   # Monitor logs
pkill -f "reth.*node.*narwhal"    # Stop all nodes
```

## Key Implementation Status

### ✅ Fully Working
- **Complete DAG Construction**: Headers, votes, certificates with proper flow
- **Full P2P Networking**: Anemo/QUIC with all RPC services
- **Worker System**: Batch creation, replication, and storage
- **BFT Consensus**: Leader election, commit rules, finalization
- **MDBX Storage**: Real database operations for all consensus data
- **Transaction Flow**: Mempool → Workers → DAG → BFT → Blocks
- **Multi-Validator**: 4 validators fully participating in real consensus
- **Real Consensus**: Enabled via USE_REAL_CONSENSUS=true environment variable

### ⚠️ Working with Known Issues
- **Block Production**: Works with transactions, produces blocks at regular intervals
- **Base Fee Synchronization**: Consensus and engine calculate different base fee values (persistence issue)
- **Block Validation**: Engine occasionally rejects blocks due to base fee mismatches

### ❌ TODO
- Fix base fee synchronization by investigating database cleanup during node restarts
- Complete BLS signature aggregation for consensus seals
- Vote signing with proper BLS signatures (currently using generated signatures)

## Project Structure

### Core Implementation
- **`examples/narwhal-bullshark-consensus/`** - Main consensus implementation
  - `src/node_integration.rs` - Connects consensus to Reth node
  - `src/reth_database_ops.rs` - **REAL MDBX database operations** (connects to Reth's database)
  - `src/consensus_storage.rs` - Storage interface with database operations injection
  - `src/complete_integration.rs` - Full working integration from pre-move version
  - `src/validator_keys.rs` - BLS key management
  - `src/types.rs` - Core type definitions

- **`crates/narwhal/`** - DAG construction and networking
  - `src/dag_service.rs` - Main DAG service
  - `src/worker.rs` - Transaction batching
  - `src/network.rs` - P2P networking
  - `src/storage_mdbx.rs` - MDBX storage implementation

- **`crates/bullshark/`** - BFT consensus algorithm
  - `src/bft_service.rs` - Main BFT service
  - `src/consensus.rs` - Consensus algorithm

- **`bin/reth/src/narwhal_bullshark.rs`** - Integration with Reth node

## Recent Critical Fixes

### 2025-07-10: Real Consensus Enabled + Base Fee Caching
**Problem**: Nodes were running MOCK consensus instead of REAL consensus
- Root cause: Missing USE_REAL_CONSENSUS=true environment variable in startup script
- Impact: Multi-validator setup was testing mock implementation, not real consensus

**Solution**: Updated startup script and implemented base fee caching
- Added USE_REAL_CONSENSUS=true to all node startup commands in `start_multivalidator_test.sh`
- Implemented thread-safe parent block caching with Arc<Mutex<Option<(u64, u64, u64)>>>
- Added `update_chain_state_with_block_info` method for proper chain state synchronization
- Files: `start_multivalidator_test.sh`, `complete_integration.rs`, `node_integration.rs`

**Result**: ✅ All 4 nodes now run real Narwhal+Bullshark consensus with active DAG participation
- Evidence: Logs show "Starting REAL Narwhal+Bullshark consensus"
- Evidence: Headers, votes, certificates created with 3/4 quorum achieved
- Evidence: Finalized batches processed and sent to Reth integration

### 2025-07-09: Database Operations Fixed
**Problem**: "Batch store not configured - cannot extract transactions from certificates"
- Root cause: MdbxConsensusStorage was created but database operations were not injected
- Impact: Consensus worked but couldn't extract transactions from certificates for block creation

**Solution**: Implemented proper MDBX database operations
- Created `RethDatabaseOps` that connects to Reth's actual database via `DatabaseProviderFactory`
- Used correct `DBProvider` trait methods (`tx_ref()`, `tx_mut()`, `commit()`)
- Injected real database operations into `MdbxConsensusStorage` during initialization
- Files: `reth_database_ops.rs`, `node_integration.rs`

**Result**: ✅ Consensus now successfully processes certificates and creates finalized batches
- Evidence: `✅ Finalized batch 1 with 0/0 transactions (decode errors: 0)`
- Evidence: `✅ Successfully sent finalized batch #1 to Reth integration`

## Key Architecture Insights

### Database Integration Pattern
The consensus system uses a dependency injection pattern for database operations:
1. **Interface**: `DatabaseOps` trait in `consensus_storage.rs`
2. **Implementation**: `RethDatabaseOps` in `reth_database_ops.rs`
3. **Injection**: Database operations injected into `MdbxConsensusStorage` at startup
4. **Usage**: All consensus tables (certificates, batches, votes) use real MDBX operations

### Validator Setup
- Committee config file contains all validator public keys and network addresses
- Individual nodes only need their own private key file
- BLS keys generated deterministically from EVM private keys
- Test files: `test_validators/validator-*.json`, `committee.json`

## Important TODOs

### Critical (Next Steps)
1. **Fix Base Fee Synchronization** - Resolve database persistence issue
   - Issue: Consensus and engine calculate different base fee values after restarts
   - Root cause: Database cleanup script may not properly delete consensus database files
   - Investigation needed: Ensure consensus state is properly cleared between test runs
   - Current evidence: Base fee mismatch (consensus: 765625000 vs engine: 669921875)

2. **Complete BLS Signatures** - Replace placeholder signatures with real ones
   - Vote signing in `types.rs`
   - Certificate validation
   - Consensus seal generation

### Low Priority
- Optimize empty block production timing
- Complete RPC implementation (some methods return placeholder data)
- Metrics collection and monitoring

## Testing Commands

```bash
# Single node test
./target/release/reth node --datadir ~/.neura/node-0 --chain neura-mainnet \
  --narwhal.enable --validator.key-file ~/.neura/node-0/validator.json \
  --validator.committee-config ~/.neura/node-0/committee.json

# Multi-validator test (recommended)
./start_multivalidator_test.sh
```

## Lessons Learned

- **No Simplified Solutions**: Always implement real, robust solutions
- **Database Operations**: Use proper dependency injection for database operations
- **Committee Architecture**: Shared committee config + individual private keys
- **BLS Key Generation**: Use deterministic generation from EVM keys for consistency
- **Consensus State**: Load last consensus index from storage to avoid replay
- **Error Debugging**: Focus on root cause analysis rather than workarounds

## Working Features Summary

The system now successfully:
- ✅ Runs 4-validator REAL Narwhal+Bullshark consensus (not mock)
- ✅ Establishes full peer-to-peer connections between all validators
- ✅ Creates headers, processes votes, and achieves 3/4 quorum for certificates
- ✅ Processes transactions from mempool through DAG consensus
- ✅ Creates and broadcasts certificates with proper certificate digests
- ✅ Reaches consensus on transaction ordering via Bullshark BFT
- ✅ Extracts transactions from certificates for block creation
- ✅ Creates finalized batches for block building with real database operations
- ✅ Stores all consensus data in real MDBX database with proper transaction lifetimes
- ✅ Thread-safe base fee caching infrastructure implemented

Known issues:
- ⚠️ Base fee synchronization between consensus and engine (database persistence issue)

Next step: Investigate database cleanup to fix base fee synchronization between consensus and engine layers.