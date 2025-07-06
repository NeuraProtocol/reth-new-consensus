# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Last Updated**: 2025-07-06 - Fixed DAG output limiting and made block time configurable:
- Limited DAG traversal output to prevent 5200+ certificate batches that hang the system
- Made block time configurable via --bullshark.min-block-time-ms (default: 500ms)
- Added --bullshark.max-certificates-per-dag to limit certificates per traversal
- Increased certificate channel buffer to 50K for better burst handling
- System now handles catching up after being offline without overwhelming execution layer

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

### 2025-07-06 (Latest)

27. ✅ **DAG Output Limiting** - COMPLETED
    - ✅ Fixed system hang when consensus outputs 5200+ certificates
    - ✅ Added max_certificates_per_dag configuration (default: 500)
    - ✅ Created order_dag_with_limit function to cap DAG traversal
    - ✅ Increased certificate channel buffer from 10K to 50K
    - ✅ Command line arg: --bullshark.max-certificates-per-dag
    - ✅ Test script uses 200 for faster catchup
    - Prevents overwhelming system when catching up after being offline
    - Location: `crates/bullshark/src/utils.rs:140-157`

28. ✅ **Configurable Block Time** - COMPLETED
    - ✅ Made block time configurable via --bullshark.min-block-time-ms
    - ✅ Changed default from 2000ms to 500ms for sub-second blocks
    - ✅ Updated all configuration files and scripts
    - ✅ Added helpful documentation in CLI args
    - Location: `crates/node/core/src/args/narwhal_bullshark_args.rs:210-214`

### 2025-07-06 (Earlier)

25. ✅ **Consensus Replay Fix** - COMPLETED
    - ✅ Fixed BFT service to load last consensus index from storage on startup
    - ✅ Added filtering to skip already-processed certificates 
    - ✅ Consensus was producing 6000+ outputs due to replaying entire DAG history
    - ✅ Now properly continues from where it left off after restart
    - Location: `crates/bullshark/src/bft_service.rs:158-177, 250-268`

26. ✅ **Block Production Timing** - COMPLETED  
    - ✅ Increased min_block_time from 100ms to 2000ms
    - ✅ Increased max_batch_delay and max_header_delay to 2000ms
    - ✅ Prevents excessive block production and consensus getting stuck
    - ✅ Ensures consistent 2-second block times
    - Locations:
      - `crates/node/core/src/args/narwhal_bullshark_args.rs`
      - `crates/narwhal/src/config.rs`
      - `crates/narwhal/src/batch_maker.rs`

### 2025-07-06 (Earlier)

22. ✅ **BFT Consensus Processing** - WORKING
    - ✅ BFT service processes 72,230+ consensus outputs from accumulated rounds
    - ✅ Consensus outputs are properly ordered and processed
    - ✅ Finalized batches created with transactions (323 transactions in batch #1)
    - ✅ Batch processor receives finalized batches successfully
    - ✅ Fixed BFT initialization to start at current block (was incorrectly incrementing)
    - ❌ Blocks still at 0 - BlockExecutor trait not implemented
    - Issue: BFT service updates `current_block_number` before confirmation, causing subsequent certificates to be skipped
    - Location: `crates/bullshark/src/bft_service.rs:276`

23. ✅ **Chain State Synchronization** - COMPLETED
    - ✅ Fixed BFT service to NOT update current_block_number when sending finalized batch
    - ✅ BFT service now checks chain state from provider on each certificate
    - ✅ Added update_chain_state method to NarwhalRethBridge for post-persistence updates
    - ✅ Chain state flows: Block persisted → Bridge updated → Service updated → BFT synced
    - ✅ Prevents multiple block 1s being created due to state mismatch
    - ✅ BFT service only advances block number after confirmation from chain
    - Locations: 
      - `crates/bullshark/src/bft_service.rs:257-302` (state sync logic)
      - `crates/consensus/consensus/src/narwhal_bullshark/integration.rs:653-660` (update method)
      - `bin/reth/src/narwhal_bullshark.rs:371-376` (persistence callback)

24. ✅ **Block Execution Implementation** - COMPLETED
    - ✅ Created RethBlockExecutor that integrates with Reth's execution engine
    - ✅ Executes transactions using EVM with full state transitions
    - ✅ Calculates state root, receipts root, and logs bloom
    - ✅ Persists complete block data including receipts to database
    - ✅ Updates chain state with proper parent tracking
    - ✅ Integrated executor with consensus bridge via set_block_executor
    - ✅ Chain tip retrieval from database for genesis hash
    - Location: `bin/reth/src/block_executor.rs`

### 2025-01-06

21. ✅ **Full Block Execution Implementation** - COMPLETED
    - ✅ Created `block_executor_batch.rs` using Reth's batch executor pattern
    - ✅ Integrated with `BasicBlockExecutor` and the `Executor` trait
    - ✅ Proper EVM execution with state changes
    - ✅ State persistence with merkle tree calculations
    - ✅ Fixed empty block handling to avoid "IntegerList must be pre-sorted" panic
    - ✅ Blocks are now executed and persisted to the database
    - Location: `bin/reth/src/block_executor_batch.rs`

22. ✅ **Invalid Transaction Handling** - COMPLETED
    - ✅ Graceful handling of invalid transactions (insufficient funds, wrong nonce, etc.)
    - ✅ Automatically creates empty blocks when transactions fail
    - ✅ Block production continues even with invalid transactions
    - ✅ Added test script with `--include-invalid` flag to test error handling
    - Location: `bin/reth/src/narwhal_bullshark.rs` (handle_finalized_batch)

### 2025-01-05

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
- **Consensus State Persistence**: Always load last consensus index from storage to avoid replaying entire DAG history
- **Block Timing**: Use appropriate block times (500ms default) with configurable --bullshark.min-block-time-ms
- **Use Reth Examples**: The `/home/peastew/src/reth-new-consensus/examples` directory has many integration examples - use them!
- **DAG Output Limits**: When catching up, the DAG can contain thousands of certificates - limit output with --bullshark.max-certificates-per-dag
- **Channel Buffer Sizes**: Use large buffers (50K+) for certificate channels to handle burst traffic when catching up

## Current Branch Status

Working on branch `v1.4.8-neura` with functional Narwhal+Bullshark consensus producing blocks. All 4 validators participate in consensus, creating certificates and executing transactions. The system now:
- ✅ Executes transactions with full EVM support
- ✅ Persists blocks with state changes to the database
- ✅ Handles invalid transactions gracefully by creating empty blocks
- ✅ Properly loads consensus state on restart (no more replaying thousands of certificates)
- ✅ Maintains consistent 2-second block times
- ❌ Empty block production at regular intervals needs testing

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

4. ✅ **Block Execution** - COMPLETED
   - ✅ Full EVM execution using Reth's batch executor pattern
   - ✅ State persistence with merkle tree calculations
   - ✅ Invalid transaction handling with graceful fallback to empty blocks
   - ✅ Blocks are executed and persisted to the database

### High Priority
5. **Consensus Seal Generation** - Using aggregated BLS signatures from validators
   - Location: `crates/consensus/consensus/src/narwhal_bullshark/integration.rs:465`
   
6. ⚠️ **Parent Hash Retrieval** - MOSTLY COMPLETED
   - ✅ BftService now uses ChainStateProvider to get parent hash
   - ✅ Chain state synchronized between integration layer and consensus
   - ✅ BlockExecutor provides chain tip with correct parent hash
   - ✅ Blocks are created with proper parent hash linking
   - ❌ Genesis initialization could be improved
   - Locations: `crates/bullshark/src/chain_state.rs`, `crates/consensus/consensus/src/narwhal_bullshark/chain_state.rs`
   
7. **Vote Signing** - Using BLS signatures with SignatureService
   - Location: `crates/narwhal/src/types.rs:295`

8. ❌ **Empty Block Production** - NOT WORKING
   - ❌ BFT service skips empty certificates (no transactions = no block)
   - ❌ Workers don't create batches without transactions
   - ❌ DAG service doesn't create certificates without batches
   - ❌ Blocks are NOT being produced at regular intervals
   - Need to implement timer-based batch/certificate creation

### Medium Priority
9. **RPC Implementation** - Most methods implemented, some return placeholder data
   - Mostly connected to actual consensus state
   - Locations: `crates/consensus/consensus/src/rpc.rs`, `service_rpc.rs`
   
10. ✅ **Worker Batch Fetching** - COMPLETED
    - ✅ BftService now retrieves actual batches from MDBX storage
    - ✅ MdbxBatchStore implementation with proper serialization/deserialization
   
11. **Metrics Collection** - Real metrics via Prometheus
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

15. ⚠️ **Empty Block Production** - NEEDS TESTING
    - ✅ Updated batch_maker to always seal batches on timer expiry
    - ✅ DAG service creates headers when timer expires even without transactions
    - ✅ BFT service processes empty certificates for consistent block times
    - ✅ Fixed timing configuration (2 second intervals)
    - ⚠️ Needs testing to verify empty blocks are produced at regular intervals

## Summary of Working Features

The Narwhal + Bullshark integration with Reth now has:

### ✅ Consensus Layer
- Multi-validator BFT consensus with 4 validators
- Full P2P networking with Anemo RPC
- Certificate DAG construction and voting
- Transaction batching by workers
- Finalized batch production

### ✅ Execution Layer  
- Full EVM execution using Reth's engine
- State persistence with merkle trees
- Invalid transaction handling
- Block persistence to database
- Chain state synchronization

### ✅ Integration
- Mempool to consensus transaction flow
- Consensus to execution block creation
- MDBX storage for all consensus data
- Worker batch storage and retrieval
- Chain state tracking between layers

### ❌ Still TODO
- Test empty block production at regular intervals
- Proper BLS signature aggregation for consensus seals
- Vote signing with validator keys (currently using generated signatures)
- Complete RPC implementation (some methods return placeholder data)
- Metrics collection and monitoring

## Key Files to Understand
```