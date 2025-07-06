# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Last Updated**: 2025-07-06 - BFT consensus is working! The system processes 72,230+ consensus outputs and creates finalized batches with transactions. However, blocks remain at 0 because the BlockExecutor trait needs to be implemented to actually execute transactions and persist blocks to Reth's database.

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

## Recent Updates

### 2025-07-06

22. ✅ **BFT Consensus Processing** - WORKING
    - ✅ BFT service processes 72,230+ consensus outputs from accumulated rounds
    - ✅ Consensus outputs are properly ordered and processed
    - ✅ Finalized batches created with transactions (323 transactions in batch #1)
    - ✅ Batch processor receives finalized batches successfully
    - ✅ Fixed BFT initialization to start at current block (was incorrectly incrementing)
    - ❌ Blocks still at 0 - BlockExecutor trait not implemented
    - Issue: BFT service updates `current_block_number` before confirmation, causing subsequent certificates to be skipped
    - Location: `crates/bullshark/src/bft_service.rs:276`

23. ❌ **Block Persistence** - NOT IMPLEMENTED
    - ❌ Finalized batches are created but not executed
    - ❌ BlockExecutor trait stub returns error "not implemented"
    - ❌ No actual block creation or state updates in Reth database
    - ❌ Chain state remains at block 0 despite finalized batches
    - Next step: Implement BlockExecutor trait to execute transactions and persist blocks

### 2025-07-05

19. ✅ **Mempool Listener Fix** - COMPLETED
    - ✅ Fixed mempool listener to forward existing transactions to consensus
    - ✅ The `new_transactions_listener_for` method only yields NEW transactions
    - ✅ Transactions loaded from backup file were never being forwarded
    - ✅ Added code to send all `pending_transactions()` before starting listener
    - ✅ Now forwards 12 existing transactions on startup (from backup file)
    - Location: `bin/reth/src/narwhal_bullshark.rs:73-96`

20. ✅ **Worker-Primary Integration** - COMPLETED
    - ✅ Fixed critical bug: `rx_primary` channel was dropped immediately (line 295 in service.rs)
    - ✅ Worker batch digests now properly forwarded to DAG service
    - ✅ DAG service receives batch digests via `rx_batch_digests` channel
    - ✅ Modified `create_and_propose_header` to include worker batches in header payload
    - ✅ Headers now contain both primary batches and worker batches
    - ✅ Worker batches cleared after header creation to prevent duplicates
    - ✅ DAG service spawned after connecting batch digest receiver
    - Location: `crates/consensus/consensus/src/narwhal_bullshark/service.rs:289-307`

21. ✅ **Multi-Validator Consensus** - COMPLETED
    - ✅ Fixed consensus stuck at round 1 - needed ALL validators participating
    - ✅ Created `send_transactions_all_validators.py` to distribute transactions
    - ✅ All 4 validators now create headers, vote, and form certificates
    - ✅ DAG advances through thousands of rounds (2258+) instead of stuck at round 1
    - ✅ Bullshark produces finalized batches with transactions
    - ❌ Finalized batches not yet converted to blocks (BlockExecutor needed)
    - Key insight: Bullshark needs multiple validators creating certificates each round for leader election

### 2025-07-04

17. ✅ **Empty Block Production** - DESIGN DECISION
    - ✅ System designed to only produce blocks when there are transactions
    - ✅ BFT service skips certificates without transactions (intentional)
    - ✅ Resolved runtime panic caused by `block_on` within async context
    - ✅ Replaced async RwLock with sync Mutex in chain state adapter
    - Note: This is a design choice, not a bug
    
18. ✅ **Block Number Incrementing** - PARTIALLY FIXED
    - ✅ Fixed BFT service to use `chain_state.block_number + 1` for new blocks
    - ✅ Chain state initialization properly sets starting block number
    - ✅ Consensus creates block 1 after genesis (block 0)
    - ❌ Parent hash tracking still shows 0x0000...0000 (needs block executor)
    - ❌ Blocks not persisting to main chain (integration layer issue)
    - Note: Full fix requires proper BlockExecutor trait implementation

## Lessons Learned and Reminders

- **Multi-Validator Testing**: Always use the `/home/peastew/src/reth-new-consensus/start_multivalidator_test.sh` script for consistent multi-node test setup
- **Genesis Configuration**: The chain uses `neura-mainnet` which points to `/srv/tank/src/reth-new-consensus/crates/chainspec/res/genesis/neura-mainnet.json`
- **Command Line Arguments**: Use `--narwhal.enable` not `--narwhal-bullshark`, and `--validator.key-file` for JSON files
- **Empty Block Production**: Blockchains should produce empty blocks for consistent block times, not wait for transactions
- **Async Context Issues**: Cannot use `block_on` within async context - use sync primitives (Mutex) instead of async (RwLock) when needed
- **Chain State Synchronization**: BFT service needs chain state for block numbers; integration layer must update chain state after blocks
- **Mempool Transaction Listeners**: The `new_transactions_listener_for` trait method only yields transactions that arrive AFTER the listener is created, NOT existing transactions
- **Transaction Backup Files**: Transactions loaded from backup (`txpool-transactions-backup.rlp`) won't trigger the new transaction listener - must send them explicitly
- **Worker-Primary Architecture**: Workers create batches and send digests to primary via channels - these MUST be connected to DAG service or headers won't include worker batches
- **DAG Service Spawning**: Must configure DAG service with batch digest receiver BEFORE spawning it
- Ensure to review existing implementation thoroughly before making changes
- Cross-reference with reference implementations before claiming completeness
- **Reference Implementation**: Always consult `/home/peastew/src/reth-new-consensus/narwhal-reference-implementation` for guidance on implementation details and design choices

## Current Branch Status

Working on branch `v1.4.8-neura` with functional Narwhal+Bullshark consensus. All 4 validators participate in consensus, creating certificates and advancing through thousands of rounds. Bullshark produces finalized batches, but full blockchain integration requires BlockExecutor trait implementation to persist blocks.

## Important TODOs (Stubs to Implement)

### Critical (Blocking Functionality)
1. ✅ **MDBX Storage Integration** - COMPLETED (with workarounds)
   - Implemented trait-based approach with ConsensusDbOps
   - Real storage operations for certificates and votes
   - Note: Vote/round indexing uses DAG vertices table as workaround
   
2. ✅ **Network Broadcasting** - COMPLETED
   - ✅ Headers, votes, certificates ARE broadcast via Anemo RPC to connected peers
   - ✅ Full P2P transport layer exists with connection management
   - ✅ Outbound network bridge connects DAG service to network broadcasts
   - ✅ Worker batch replication implemented with full RPC services
   
3. ✅ **Transaction Processing** - COMPLETED
   - ✅ `Transaction::to_alloy_transaction()` now properly decodes RLP transactions
   - ✅ Mempool bridge connected - transactions flow from pool to consensus
   - ✅ Batch storage implemented - Bullshark retrieves actual transactions from worker batches
   - ✅ Workers store batches persistently in MDBX via WorkerBatches table

### High Priority
4. **Consensus Seal Generation** - Using aggregated BLS signatures from validators
   - Location: `crates/consensus/consensus/src/narwhal_bullshark/integration.rs:465`
   
5. ⚠️ **Parent Hash Retrieval** - PARTIALLY COMPLETED
   - ✅ BftService now uses ChainStateProvider to get parent hash
   - ✅ Chain state synchronized between integration layer and consensus
   - ❌ Requires BlockExecutor trait implementation to get genesis hash
   - ❌ Currently all blocks show parent 0x0000...0000
   - Locations: `crates/bullshark/src/chain_state.rs`, `crates/consensus/consensus/src/narwhal_bullshark/chain_state.rs`
   
6. **Vote Signing** - Using BLS signatures with SignatureService
   - Location: `crates/narwhal/src/types.rs:295`

### Medium Priority
7. **RPC Implementation** - Most methods implemented, some return placeholder data
   - Mostly connected to actual consensus state
   - Locations: `crates/consensus/consensus/src/rpc.rs`, `service_rpc.rs`
   
8. ✅ **Worker Batch Fetching** - COMPLETED
   - ✅ BftService now retrieves actual batches from MDBX storage
   - ✅ MdbxBatchStore implementation with proper serialization/deserialization
   
9. **Metrics Collection** - Real metrics via Prometheus
   - Location: `crates/consensus/consensus/src/rpc.rs:851-889`

### Configuration
10. **Hardcoded Values** - Most are now configurable via CLI arguments
    - See `start_multivalidator_test.sh` for all available options

### Additional TODOs Found
11. ✅ **Worker Components** - COMPLETED
    - ✅ Worker network with full Anemo RPC implementation
    - ✅ WorkerToWorker and PrimaryToWorker RPC services
    - ✅ Worker batch replication across network
    
12. ✅ **Certificate Storage** - COMPLETED
    - ✅ Certificates have actual transaction data
    - ✅ Transactions extracted from worker batches via MDBX storage
    - ✅ Batch digests in certificates used to retrieve full batch data

13. ✅ **DAG Storage Implementation** - COMPLETED
    - ✅ Full MDBX implementation in consensus_db_ops_impl.rs
    - ✅ Certificates stored/retrieved from ConsensusDagVertices table
    - ✅ Votes stored/retrieved with proper serialization
    - ✅ Certificate indexing by round for efficient queries
    - ✅ Garbage collection for old DAG data

14. ✅ **Chain State Integration** - MOSTLY COMPLETED
    - ✅ ChainStateTracker for managing blockchain state
    - ✅ ChainStateProvider trait in Bullshark for state access
    - ✅ Integration layer syncs state between Reth and consensus
    - ❌ BlockExecutor trait needed for proper genesis hash initialization
    - ✅ Finalized batches use block numbers (incrementing from 0 to 1)
    - ❌ Parent hash tracking requires BlockExecutor connection

15. ❌ **Empty Block Production** - NOT WORKING
    - ❌ BFT service skips empty certificates (no transactions = no block)
    - ❌ Workers don't create batches without transactions
    - ❌ DAG service doesn't create certificates without batches
    - ✅ Fixed runtime panic in async context
    - ❌ Blocks are NOT being produced at regular intervals

## Key Files to Understand
```