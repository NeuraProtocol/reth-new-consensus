use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// A certificate in the Bullshark consensus protocol
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Certificate {
    pub batch_id: u64,
    pub transactions: Vec<Vec<u8>>,
    pub block_hash: B256,
    pub timestamp: u64,
    pub signature: Vec<u8>,
}

/// A batch of transactions in the consensus protocol
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusBatch {
    pub id: u64,
    pub transactions: Vec<Vec<u8>>,
    pub timestamp: u64,
}

/// Certificate storage table: certificate_id -> Certificate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertificateTable;

/// Finalized batches table: batch_id -> block_hash
/// Maps consensus batches to the blocks they were finalized in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedBatchTable;

/// Consensus batches table: batch_id -> ConsensusBatch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusBatchTable;

/// DAG vertices table: vertex_id -> Certificate
/// For storing the DAG structure
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagVertexTable;

/// Latest finalized certificate ID
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestFinalizedTable; 