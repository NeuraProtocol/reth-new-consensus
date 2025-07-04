# SealedBlock Implementation for Narwhal/Bullshark

## Overview

The Narwhal/Bullshark consensus implementation in Reth properly seals blocks with consensus proof data. This document describes how blocks are sealed with validator signatures.

## Implementation Details

### 1. **ConsensusSeal Structure**
Located in `crates/consensus/consensus/src/narwhal_bullshark/types.rs`:
```rust
pub struct ConsensusSeal {
    /// The consensus round that finalized this block
    pub round: u64,
    /// The certificate digest that led to finalization
    pub certificate_digest: B256,
    /// Aggregated BLS signature from validators
    pub aggregated_signature: Bytes,
    /// Bitmap indicating which validators participated
    pub signers_bitmap: Bytes,
}
```

### 2. **Block Sealing Process**
In `integration.rs`, the `batch_to_block()` method:
1. Creates a `ConsensusSeal` from the `FinalizedBatch` data
2. Encodes the seal into the block header's `extra_data` field
3. Uses the certificate digest as the `mix_hash` field
4. Properly seals the block with `SealedBlock::seal_slow()`

### 3. **Consensus Proof Components**
- **Round**: The consensus round number when the block was finalized
- **Certificate Digest**: The hash of the certificate that achieved consensus
- **Aggregated Signature**: BLS12-381 aggregated signature from all participating validators
- **Signers Bitmap**: Bitmap indicating which validators signed (for verification)

### 4. **Header Fields Used**
- `extra_data`: Contains the encoded `ConsensusSeal` with all consensus proof data
- `mix_hash`: Set to the certificate digest for quick reference
- `difficulty`: Always 0 (no PoW)
- `nonce`: Always 0 (no PoW)

### 5. **Signature Aggregation**
The implementation:
1. Collects individual BLS signatures from validators
2. Creates a bitmap tracking which validators participated
3. Aggregates signatures using BLS12-381 aggregate signatures
4. Falls back to empty signature (96 zero bytes) if aggregation fails

## Usage

When a batch is finalized by Bullshark consensus:
1. The `FinalizedBatch` contains validator signatures
2. `batch_to_block()` creates a properly sealed block
3. The seal can be verified by:
   - Decoding the `extra_data` field to get the `ConsensusSeal`
   - Verifying the aggregated BLS signature
   - Checking the signers bitmap against the validator set

## Verification

To verify a sealed block:
1. Extract `ConsensusSeal` from `extra_data`: `ConsensusSeal::decode(&header.extra_data)`
2. Verify `mix_hash` matches `certificate_digest`
3. Verify the aggregated BLS signature against the validator public keys
4. Check that sufficient validators signed (based on the bitmap)

## Future Improvements

1. Add proper beneficiary address configuration
2. Make gas limit configurable
3. Add seal verification in the consensus engine
4. Implement proper slashing for invalid signatures