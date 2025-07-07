# Testing Narwhal+Bullshark Block Submission

This document describes how to test the block submission integration with Reth's engine API.

## Architecture

We've successfully restructured the Narwhal+Bullshark implementation to avoid circular dependencies:

1. **TestIntegration** - Simplified integration that builds and submits blocks to the engine API
2. **MockBlockBuilder** - Creates blocks without full state execution (for testing)
3. **NodeIntegration** - Connects to a running Reth node and produces blocks periodically

## Key Achievement

The integration now properly uses Reth's `PayloadTypes` trait to convert blocks to execution payloads, solving the original block hash mismatch issue.

## Running Tests

### 1. Start a Reth dev node

```bash
# Start a development node that accepts engine API calls
reth node --dev --http --http.api all
```

### 2. Run the test integration

The `TestIntegration` module can be used to submit test blocks:

```rust
use example_narwhal_bullshark_consensus::{
    types::FinalizedBatch,
    test_integration::TestIntegration,
};

// Create integration with provider and engine handle from running node
let integration = TestIntegration::new(provider, engine_handle, batch_receiver);

// Run the integration
integration.run().await?;
```

### 3. Mock block submission

For testing without full state execution:

```rust
use example_narwhal_bullshark_consensus::mock_block_builder::MockBlockBuilder;

let builder = MockBlockBuilder::new(provider);
let block = builder.build_block(batch)?;
// Block can now be submitted to engine API
```

## Current Status

✅ **Working modules that compile:**
- `TestIntegration` - Can submit blocks to engine API
- `MockBlockBuilder` - Creates valid block structure
- `NodeIntegration` - Framework for continuous block production

⚠️ **Remaining work:**
- Full state execution in block builder (requires fixing EVM integration)
- Actual Narwhal+Bullshark consensus integration
- Transaction pool integration for including transactions

## Next Steps

1. Test with a running Reth node to verify blocks are accepted
2. Implement proper state execution once EVM API issues are resolved
3. Integrate actual Narwhal+Bullshark consensus protocol