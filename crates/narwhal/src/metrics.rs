//! Metrics for Narwhal

use prometheus::{Counter, Gauge, Registry};

/// Metrics for Narwhal nodes
#[derive(Clone, Debug)]
pub struct NarwhalMetrics {
    /// Number of transactions processed
    pub transactions_processed: Counter,
    /// Number of headers created
    pub headers_created: Counter,
    /// Number of votes cast
    pub votes_cast: Counter,
    /// Number of certificates created
    pub certificates_created: Counter,
    /// Current round number
    pub current_round: Gauge,
    /// DAG size in bytes
    pub dag_size_bytes: Gauge,
}

impl NarwhalMetrics {
    /// Create new metrics instance
    pub fn new(registry: &Registry) -> Self {
        let transactions_processed = Counter::new(
            "narwhal_transactions_processed_total",
            "Total number of transactions processed"
        ).unwrap();
        
        let headers_created = Counter::new(
            "narwhal_headers_created_total", 
            "Total number of headers created"
        ).unwrap();
        
        let votes_cast = Counter::new(
            "narwhal_votes_cast_total",
            "Total number of votes cast"
        ).unwrap();
        
        let certificates_created = Counter::new(
            "narwhal_certificates_created_total",
            "Total number of certificates created"
        ).unwrap();
        
        let current_round = Gauge::new(
            "narwhal_current_round",
            "Current consensus round"
        ).unwrap();
        
        let dag_size_bytes = Gauge::new(
            "narwhal_dag_size_bytes",
            "Size of the DAG in bytes"
        ).unwrap();

        registry.register(Box::new(transactions_processed.clone())).unwrap();
        registry.register(Box::new(headers_created.clone())).unwrap();
        registry.register(Box::new(votes_cast.clone())).unwrap();
        registry.register(Box::new(certificates_created.clone())).unwrap();
        registry.register(Box::new(current_round.clone())).unwrap();
        registry.register(Box::new(dag_size_bytes.clone())).unwrap();

        Self {
            transactions_processed,
            headers_created,
            votes_cast,
            certificates_created,
            current_round,
            dag_size_bytes,
        }
    }
} 