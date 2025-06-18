//! Error types for Narwhal DAG

use thiserror::Error;

/// Result type for Narwhal operations
pub type DagResult<T> = Result<T, DagError>;

/// Errors that can occur in Narwhal DAG
#[derive(Debug, Error)]
pub enum DagError {
    /// Header has an invalid ID
    #[error("Invalid header ID")]
    InvalidHeaderId,
    
    /// Epoch mismatch between expected and received values
    #[error("Invalid epoch: expected {expected}, received {received}")]
    InvalidEpoch { 
        /// Expected epoch number
        expected: u64, 
        /// Received epoch number
        received: u64 
    },
    
    /// Authority is not known to the committee
    #[error("Unknown authority: {0}")]
    UnknownAuthority(String),
    
    /// Header is malformed
    #[error("Malformed header: {0}")]
    MalformedHeader(String),
    
    /// Cryptographic signature is invalid
    #[error("Invalid signature: {0}")]
    InvalidSignature(#[from] fastcrypto::traits::Error),
    
    /// Transaction data is invalid
    #[error("Invalid transaction: {0}")]
    InvalidTransaction(String),
    
    /// Certificate does not have sufficient votes for quorum
    #[error("Certificate requires quorum")]
    CertificateRequiresQuorum,
    
    /// Bitmap encoding/decoding error
    #[error("Invalid bitmap: {0}")]
    InvalidBitmap(String),
    
    /// Storage backend error
    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
    
    /// Network communication error
    #[error("Network error: {0}")]
    Network(String),
    
    /// Message serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// System is in shutdown state
    #[error("System is shutting down")]
    ShuttingDown,
    
    /// Operation timed out
    #[error("Timeout occurred")]
    Timeout,
    
    /// Configuration is invalid
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    /// Consensus protocol error
    #[error("Consensus error: {0}")]
    Consensus(String),
} 