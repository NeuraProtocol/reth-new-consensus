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

// Include the generated RPC services
pub mod consensus {
    include!(concat!(env!("OUT_DIR"), "/narwhal.NarwhalConsensus.rs"));
}

pub mod dag {
    include!(concat!(env!("OUT_DIR"), "/narwhal.dag.NarwhalDag.rs"));
} 