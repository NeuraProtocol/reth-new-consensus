# TODO/FIXME and Incomplete Implementations in Consensus Code

## Summary
This document lists all TODO, FIXME, stub, mock, placeholder, dummy, and incomplete implementations found in the Narwhal/Bullshark consensus implementation.

## Critical TODOs

### 1. Transaction Conversion Issue
**File**: `/srv/tank/src/reth-new-consensus/crates/consensus/consensus/src/narwhal_bullshark/service.rs`
**Line**: 260
```rust
transactions: Vec::new(), // TODO: Convert narwhal::Transaction back to TransactionSigned
```
**Issue**: Finalized batches are not properly converting Narwhal transactions back to Reth TransactionSigned format. This means consensus is working but transactions are not being included in blocks.

### 2. DAG Storage Adapter - Multiple Issues
**File**: `/srv/tank/src/reth-new-consensus/crates/consensus/consensus/src/narwhal_bullshark/dag_storage_adapter.rs`

#### a. Certificate Storage Not Implemented
**Lines**: 44-57
```rust
async fn store_certificate(&self, certificate: Certificate) -> DagResult<()> {
    // TODO: Use proper certificate ID from consensus tables
    let cert_id = 0u64; // Placeholder - need proper ID generation
    // TODO: Implement actual storage using consensus_storage
    Ok(())
}
```

#### b. Certificate Retrieval Not Implemented
**Lines**: 59-62
```rust
async fn get_certificate(&self, digest: &CertificateDigest) -> Option<Certificate> {
    // TODO: Implement retrieval from consensus storage
    None
}
```

#### c. Get Certificates by Round Returns Empty
**Lines**: 64-67
```rust
async fn get_certificates_by_round(&self, round: Round) -> Vec<Certificate> {
    // TODO: Implement retrieval from consensus storage
    Vec::new()
}
```

#### d. Get Parents for Round Returns Empty
**Lines**: 97-100
```rust
async fn get_parents_for_round(&self, round: Round) -> Vec<CertificateDigest> {
    // TODO: Implement using consensus storage
    Vec::new()
}
```

#### e. Garbage Collection Not Implemented
**Lines**: 102-105
```rust
async fn garbage_collect(&self, cutoff_round: Round) -> DagResult<()> {
    // TODO: Implement garbage collection
    Ok(())
}
```

#### f. Cache Issue Note
**Line**: 22
```rust
/// TODO: Extend consensus tables to include all needed DAG data
```
The adapter is using in-memory caches instead of persistent storage for votes and latest certificates.

### 3. Integration Layer Issues
**File**: `/srv/tank/src/reth-new-consensus/crates/consensus/consensus/src/narwhal_bullshark/integration.rs`

#### a. Mempool Bridge Not Set
**Line**: 244
```rust
mempool_bridge: None, // TODO: Implement with_pool constructor
```

#### b. Outbound Bridge Note
**Lines**: 303-306
```rust
// TODO: Start outbound bridge to handle broadcasting from service
// Since we can't clone the network, this would need to be implemented
// as a separate task that receives outbound messages and forwards them
```

### 4. Block Hash Calculation
**File**: `/srv/tank/src/reth-new-consensus/crates/consensus/consensus/src/narwhal_bullshark/mempool_bridge.rs`
**Line**: 211
```rust
/// TODO: This should compute the actual block hash based on the block header
fn calculate_block_hash(&self, batch: &FinalizedBatch) -> B256 {
```

## Non-Critical Placeholders

### 1. Gossip Service - Debug Messages
**File**: `/srv/tank/src/reth-new-consensus/crates/narwhal/src/gossip.rs`
**Lines**: 104, 111, 118
```rust
debug!("Would broadcast header {} to all peers", header.id);
debug!("Would broadcast vote for header {} to all peers", vote.id);
debug!("Would broadcast certificate {} to all peers", certificate.digest());
```
These are debug messages, not actual unimplemented functionality.

### 2. Storage In-Memory Placeholder Comment
**File**: `/srv/tank/src/reth-new-consensus/crates/narwhal/src/storage_inmemory.rs`
**Line**: 74
```rust
// For now, return Ok(()) as a placeholder
```

### 3. Primary Placeholder Comment
**File**: `/srv/tank/src/reth-new-consensus/crates/narwhal/src/primary.rs`
**Line**: 69
```rust
// This is just a placeholder
```

## Priority Fixes

1. **CRITICAL**: Transaction conversion in service.rs - Without this, blocks have no transactions
2. **HIGH**: DAG storage adapter implementation - Currently using in-memory caches only
3. **MEDIUM**: Proper block hash calculation in mempool_bridge.rs
4. **LOW**: Mempool bridge constructor and outbound bridge improvements

## Notes
- The consensus system is mostly functional but has critical gaps in transaction handling and persistent storage
- Most of the "Would" messages are just debug logs, not actual missing implementations
- The DAG storage adapter is the biggest area needing work - it's mostly using in-memory caches instead of the MDBX database