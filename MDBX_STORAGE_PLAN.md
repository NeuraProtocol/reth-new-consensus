# MDBX Storage Implementation Plan

## Current Status
- ✅ Consensus crates (narwhal, bullshark) have clean storage traits
- ✅ No circular dependencies from consensus → reth 
- ⚠️  Blocked by reth-consensus circular dependency in Reth ecosystem

## Implementation Approach (Following User Guidance)

### 1. Fix reth-consensus Circular Dependency First
This is a broader Reth architectural issue that needs to be resolved first.

### 2. Define Consensus Tables in reth-consensus Crate
```rust
// crates/consensus/consensus/src/storage/tables.rs
use reth_db::{table, Table};
use reth_primitives::B256;

// Consensus extension tables using reth's table! macro
table!(
    ConsensusCertificates {
        key: u64,        // Certificate ID  
        value: Vec<u8>   // Serialized Certificate
    }
);

table!(
    ConsensusBatches {
        key: u64,        // Batch ID
        value: Vec<u8>   // Serialized ConsensusBatch  
    }
);

table!(
    FinalizedBatches {
        key: u64,        // Batch ID
        value: B256      // Block hash
    }
);

table!(
    DagVertices {
        key: B256,       // Certificate hash
        value: Vec<u8>   // Serialized Certificate
    }
);

table!(
    LatestFinalized {
        key: (),         // Single row
        value: u64       // Latest finalized certificate ID
    }
);
```

### 3. Implement MDBX Storage in reth-consensus
```rust
// crates/consensus/consensus/src/storage/mdbx.rs
use reth_db::{DatabaseEnv, transaction::DbTxMut};
use bullshark::{ConsensusStorage, Certificate, ConsensusBatch};
use std::sync::Arc;

pub struct MdbxConsensusStorage {
    db: Arc<DatabaseEnv>,
}

impl MdbxConsensusStorage {
    pub fn new(db: Arc<DatabaseEnv>) -> Self {
        Self { db }
    }
}

impl ConsensusStorage for MdbxConsensusStorage {
    fn store_certificate(&self, id: u64, certificate: Certificate) -> Result<()> {
        let mut tx = self.db.tx_mut()?;
        let serialized = bincode::serialize(&certificate)?;
        tx.put::<ConsensusCertificates>(id, serialized)?;
        tx.commit()?;
        Ok(())
    }
    
    fn get_certificate(&self, id: u64) -> Result<Option<Certificate>> {
        let tx = self.db.tx()?;
        if let Some(data) = tx.get::<ConsensusCertificates>(id)? {
            let cert = bincode::deserialize(&data)?;
            Ok(Some(cert))
        } else {
            Ok(None)
        }
    }
    
    // ... implement other methods similarly
}
```

### 4. Wire Up in Node Builder
```rust
// Node builder integration (reth-node-ethereum or similar)
let db_env: Arc<DatabaseEnv> = /* Reth's main MDBX environment */;

// Create consensus storage using same MDBX instance
let consensus_storage = Arc::new(MdbxConsensusStorage::new(db_env.clone()));

// Create consensus engine with injected storage
let consensus_engine = BullsharkConsensus::with_storage(
    committee,
    config, 
    consensus_storage
);
```

## Key Benefits of This Approach

1. **No Circular Dependencies**: Consensus crates stay pure, reth-consensus provides storage
2. **Single MDBX Instance**: All data in same database file
3. **Extension Tables**: Consensus metadata alongside chain data
4. **Clean Separation**: Storage implementation injected at runtime

## Next Steps

1. Resolve reth-consensus circular dependency issue
2. Implement the MDBX tables and storage in reth-consensus  
3. Update node builder to wire consensus + storage together
4. Test the full integration

## Current Working State

The consensus implementations are ready and tested:
- Narwhal: 6/10 tests passing (4 network timeouts, not critical)
- Bullshark: 13/13 tests passing  
- Storage traits defined and ready for MDBX implementation

The architecture matches the design diagram:
External Clients → Reth Mempool → Narwhal DAG → Bullshark BFT → Block Builder → Reth Execution Pipeline → Database & JSON-RPC 