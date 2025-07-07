# Consensus Integration Options for Fixing eth_blockNumber

## The Problem

Blocks are being created and persisted to the database, but `eth_blockNumber` returns 0x0 because:
- The RPC reads from `BlockchainProvider.canonical_in_memory_state`
- Our custom consensus only writes to the database, not the in-memory state
- The canonical in-memory state is normally updated through Reth's engine API

## Option 2: Using the Canonical State Notification System

### How it works:

The canonical state notification system is Reth's internal pub/sub mechanism for notifying components when the canonical chain changes:

```rust
// From crates/chain-state/src/notifications.rs
pub enum CanonStateNotification<N> {
    /// The canonical chain was extended.
    Commit {
        /// The newly added chain segment.
        new: Arc<Chain<N>>,
    },
    /// A chain segment was reverted or reorged.
    Reorg {
        /// The chain segment that was reverted.
        old: Arc<Chain<N>>,
        /// The new chain segment.
        new: Arc<Chain<N>>,
    },
}
```

### Implementation approach:

1. **Get access to the notification sender**:
   ```rust
   // The BlockchainProvider has an internal CanonicalInMemoryState
   // which contains a canon_state_notification_sender
   // But it's not exposed publicly!
   ```

2. **Create a Chain object for new blocks**:
   ```rust
   use reth_execution_types::Chain;
   
   // After executing and persisting a block:
   let chain = Chain::new(
       vec![recovered_block],  // The new block
       execution_outcome,      // Execution results
       None,                  // No trie updates for now
   );
   ```

3. **Send the notification**:
   ```rust
   // If we had access to the sender:
   canon_state_notification_sender.send(
       CanonStateNotification::Commit {
           new: Arc::new(chain),
       }
   ).ok();
   ```

### Problems with this approach:

1. **No public access**: The `canon_state_notification_sender` is inside `CanonicalInMemoryStateInner`, which is not publicly accessible
2. **Need execution outcomes**: We'd need to properly track `ExecutionOutcome` for all blocks
3. **Complex state management**: Would need to handle reorgs, reverts, etc.

## Option 3: Using Reth's Engine API Properly

### How it works:

Reth's engine API is the standard interface for consensus clients (like lighthouse, prysm) to interact with execution clients. It follows the Ethereum Engine API specification:

1. **newPayload**: Submit a new block for execution
2. **forkchoiceUpdated**: Update which block is the head/safe/finalized
3. **getPayload**: Get a new block to propose (for validators)

### Normal flow in Reth:

```rust
// 1. Consensus client calls newPayload with a new block
engine_api.new_payload(execution_payload)
    -> Validates block
    -> Executes block  
    -> Stores in tree (not yet canonical)
    -> Returns VALID/INVALID/SYNCING

// 2. Consensus client calls forkchoiceUpdated to make it canonical
engine_api.forkchoice_updated(ForkchoiceState {
    head_block_hash: block_hash,
    safe_block_hash: safe_hash,
    finalized_block_hash: finalized_hash,
})
    -> Updates canonical chain
    -> Persists to database
    -> Updates canonical_in_memory_state
    -> Sends CanonStateNotification
    -> Returns forkchoice status
```

### Implementation approach:

Instead of directly executing blocks, we would:

1. **Create an engine API client in our consensus**:
   ```rust
   // In narwhal_bullshark integration
   struct EngineApiIntegration {
       engine_api: EngineApi,
   }
   ```

2. **Convert consensus output to engine payloads**:
   ```rust
   // When Bullshark produces a block
   let payload = ExecutionPayloadV3 {
       parent_hash: block.parent_hash,
       fee_recipient: validator_address,
       state_root: B256::ZERO, // Engine will calculate
       receipts_root: B256::ZERO, // Engine will calculate
       logs_bloom: Bloom::ZERO,
       timestamp: block.timestamp,
       block_number: block.number,
       gas_limit: block.gas_limit,
       gas_used: 0, // Engine will calculate
       extra_data: Bytes::from("Narwhal+Bullshark"),
       base_fee_per_gas: block.base_fee,
       block_hash: B256::ZERO, // Engine will calculate
       transactions: block.transactions,
       // ... other fields
   };
   ```

3. **Submit through engine API**:
   ```rust
   // Submit for execution
   let status = engine_api.new_payload(payload).await?;
   
   if status == PayloadStatus::VALID {
       // Make it canonical
       engine_api.forkchoice_updated(ForkchoiceState {
           head_block_hash: status.latest_valid_hash,
           safe_block_hash: status.latest_valid_hash,
           finalized_block_hash: finalized_hash,
       }).await?;
   }
   ```

### Benefits:

1. **Proper integration**: Uses Reth's designed flow
2. **Automatic state updates**: Engine handles all state management
3. **Supports all features**: Reorgs, finality, safe blocks, etc.
4. **Future-proof**: Works with any Reth updates

### Challenges:

1. **Major refactoring**: Need to restructure consensus integration
2. **Payload building**: Need to properly construct execution payloads
3. **Async complexity**: Engine API is async, consensus might not be
4. **Testing**: Need to test the full flow

## Current Workaround

Since both Option 2 and 3 require significant refactoring or access to internal APIs, I've implemented a temporary workaround in `canonical_state_fix.rs`:

### The Hack: Provider Reloading

```rust
// Reload the BlockchainProvider with the latest block from DB
let updater = CanonicalStateUpdater::new(provider, chain_spec);
let new_provider = updater.reload_canonical_state()?;
// Replace the old provider with the new one
```

This works by:
1. Reading the latest block from the database
2. Creating a new `BlockchainProvider` with that block as the canonical head
3. Replacing the old provider instance

**Pros:**
- Works with current architecture
- Makes blocks visible to RPC immediately
- Minimal code changes

**Cons:**
- Inefficient (recreates provider)
- Doesn't handle concurrent access well
- Not suitable for production
- Breaks subscriptions/streams

## Recommendation

**Immediate fix**: Use the provider reloading hack to make `eth_blockNumber` work for testing.

**Short term**: Investigate if we can expose the canonical state notification sender through a custom build of Reth.

**Long term**: Refactor to use Reth's engine tree properly:
1. Implement a proper `Consensus` trait for Narwhal+Bullshark
2. Feed blocks through the engine tree's insert/validate/execute flow
3. Let the engine handle canonical state updates naturally

The current approach of directly writing to the database bypasses too much of Reth's state management and will cause ongoing issues beyond just `eth_blockNumber`.