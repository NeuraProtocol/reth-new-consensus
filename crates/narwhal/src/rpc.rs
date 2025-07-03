//! RPC types for Narwhal networking

use crate::types::{Header, Vote, Certificate, CertificateDigest};
use crate::{Batch, BatchDigest};
use serde::{Deserialize, Serialize};

// Type alias for BatchId
pub type BatchId = BatchDigest;

/// Response to header submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderResponse {
    pub accepted: bool,
    pub error: Option<String>,
}

/// Response to vote submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub accepted: bool,
    pub error: Option<String>,
}

/// Response to certificate submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateResponse {
    pub accepted: bool,
    pub error: Option<String>,
}

/// Request for certificates by digest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCertificatesRequest {
    pub digests: Vec<CertificateDigest>,
}

/// Response with requested certificates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCertificatesResponse {
    pub certificates: Vec<Certificate>,
    pub missing: Vec<CertificateDigest>,
}

/// Response to batch submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponse {
    pub accepted: bool,
    pub batch_id: Option<BatchId>,
    pub error: Option<String>,
}

/// Request for a specific batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBatchRequest {
    pub batch_id: BatchId,
}

/// Response with requested batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBatchResponse {
    pub batch: Option<Batch>,
    pub error: Option<String>,
}

/// Message sent between workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    /// A batch to replicate
    Batch(Batch),
}

/// Request for multiple batches from a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBatchRequest {
    pub digests: Vec<BatchDigest>,
}

/// Response with requested batches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBatchResponse {
    pub batches: Vec<Batch>,
}

/// Message from primary to worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that a batch has been processed
    /// and can be deleted from storage
    DeleteBatches(Vec<BatchDigest>),
}

/// Synchronization request from primary to worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSynchronizeMessage {
    pub digests: Vec<BatchDigest>,
    pub target: crate::types::PublicKey,
}

/// Request a specific batch from worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestBatchRequest {
    pub digest: BatchDigest,
}

/// Response with requested batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestBatchResponse {
    pub batch: Option<Batch>,
}

// Include the generated RPC services
pub mod consensus {
    include!(concat!(env!("OUT_DIR"), "/narwhal.NarwhalConsensus.rs"));
}

pub mod dag {
    include!(concat!(env!("OUT_DIR"), "/narwhal.dag.NarwhalDag.rs"));
}

pub mod worker {
    include!(concat!(env!("OUT_DIR"), "/narwhal.worker.WorkerToWorker.rs"));
}

pub mod primary {
    include!(concat!(env!("OUT_DIR"), "/narwhal.primary.PrimaryToWorker.rs"));
} 