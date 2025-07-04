//! Metrics collection for Narwhal consensus
//!
//! This module provides comprehensive metrics collection for monitoring
//! the health and performance of the Narwhal DAG consensus system.

use prometheus::{
    register_counter_vec_with_registry, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, CounterVec, GaugeVec, HistogramVec, Registry,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics collector for Narwhal consensus
#[derive(Clone, Debug)]
pub struct NarwhalMetrics {
    // Network metrics
    pub messages_sent: CounterVec,
    pub messages_received: CounterVec,
    pub message_send_duration: HistogramVec,
    pub active_connections: GaugeVec,
    pub connection_errors: CounterVec,
    pub retry_attempts: CounterVec,
    
    // DAG metrics
    pub headers_created: CounterVec,
    pub votes_cast: CounterVec,
    pub certificates_formed: CounterVec,
    pub dag_rounds_advanced: CounterVec,
    pub dag_depth: GaugeVec,
    pub pending_headers: GaugeVec,
    
    // Transaction metrics
    pub transactions_received: CounterVec,
    pub transactions_batched: CounterVec,
    pub batch_size: HistogramVec,
    pub batch_creation_duration: HistogramVec,
    pub transactions_in_flight: GaugeVec,
    
    // Storage metrics
    pub storage_operations: CounterVec,
    pub storage_operation_duration: HistogramVec,
    pub storage_size_bytes: GaugeVec,
    pub certificates_stored: CounterVec,
    pub certificates_gc_removed: CounterVec,
    
    // Performance metrics
    pub consensus_latency: HistogramVec,
    pub throughput_tps: GaugeVec,
    pub memory_usage_bytes: GaugeVec,
    pub cpu_usage_percent: GaugeVec,
    
    // Byzantine fault tolerance metrics
    pub byzantine_faults_detected: CounterVec,
    pub vote_equivocations: CounterVec,
    pub invalid_signatures: CounterVec,
}

impl NarwhalMetrics {
    /// Create a new metrics collector with the given registry
    pub fn new(registry: &Registry) -> Self {
        Self {
            // Network metrics
            messages_sent: register_counter_vec_with_registry!(
                "narwhal_messages_sent_total",
                "Total number of messages sent",
                &["message_type", "peer"],
                registry
            ).unwrap(),
            
            messages_received: register_counter_vec_with_registry!(
                "narwhal_messages_received_total",
                "Total number of messages received",
                &["message_type", "peer"],
                registry
            ).unwrap(),
            
            message_send_duration: register_histogram_vec_with_registry!(
                "narwhal_message_send_duration_seconds",
                "Time taken to send messages",
                &["message_type"],
                registry
            ).unwrap(),
            
            active_connections: register_gauge_vec_with_registry!(
                "narwhal_active_connections",
                "Number of active peer connections",
                &["connection_type"],
                registry
            ).unwrap(),
            
            connection_errors: register_counter_vec_with_registry!(
                "narwhal_connection_errors_total",
                "Total number of connection errors",
                &["error_type", "peer"],
                registry
            ).unwrap(),
            
            retry_attempts: register_counter_vec_with_registry!(
                "narwhal_retry_attempts_total",
                "Total number of retry attempts",
                &["operation", "peer"],
                registry
            ).unwrap(),
            
            // DAG metrics
            headers_created: register_counter_vec_with_registry!(
                "narwhal_headers_created_total",
                "Total number of headers created",
                &["author"],
                registry
            ).unwrap(),
            
            votes_cast: register_counter_vec_with_registry!(
                "narwhal_votes_cast_total",
                "Total number of votes cast",
                &["voter", "round"],
                registry
            ).unwrap(),
            
            certificates_formed: register_counter_vec_with_registry!(
                "narwhal_certificates_formed_total",
                "Total number of certificates formed",
                &["author", "round"],
                registry
            ).unwrap(),
            
            dag_rounds_advanced: register_counter_vec_with_registry!(
                "narwhal_dag_rounds_advanced_total",
                "Total number of DAG rounds advanced",
                &["author"],
                registry
            ).unwrap(),
            
            dag_depth: register_gauge_vec_with_registry!(
                "narwhal_dag_depth",
                "Current depth of the DAG",
                &["author"],
                registry
            ).unwrap(),
            
            pending_headers: register_gauge_vec_with_registry!(
                "narwhal_pending_headers",
                "Number of headers pending votes",
                &["round"],
                registry
            ).unwrap(),
            
            // Transaction metrics
            transactions_received: register_counter_vec_with_registry!(
                "narwhal_transactions_received_total",
                "Total number of transactions received",
                &["source"],
                registry
            ).unwrap(),
            
            transactions_batched: register_counter_vec_with_registry!(
                "narwhal_transactions_batched_total",
                "Total number of transactions batched",
                &["worker_id"],
                registry
            ).unwrap(),
            
            batch_size: register_histogram_vec_with_registry!(
                "narwhal_batch_size",
                "Size of transaction batches",
                &["worker_id"],
                vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0],
                registry
            ).unwrap(),
            
            batch_creation_duration: register_histogram_vec_with_registry!(
                "narwhal_batch_creation_duration_seconds",
                "Time taken to create batches",
                &["worker_id"],
                registry
            ).unwrap(),
            
            transactions_in_flight: register_gauge_vec_with_registry!(
                "narwhal_transactions_in_flight",
                "Number of transactions currently being processed",
                &["stage"],
                registry
            ).unwrap(),
            
            // Storage metrics
            storage_operations: register_counter_vec_with_registry!(
                "narwhal_storage_operations_total",
                "Total number of storage operations",
                &["operation", "table"],
                registry
            ).unwrap(),
            
            storage_operation_duration: register_histogram_vec_with_registry!(
                "narwhal_storage_operation_duration_seconds",
                "Time taken for storage operations",
                &["operation", "table"],
                registry
            ).unwrap(),
            
            storage_size_bytes: register_gauge_vec_with_registry!(
                "narwhal_storage_size_bytes",
                "Size of storage in bytes",
                &["table"],
                registry
            ).unwrap(),
            
            certificates_stored: register_counter_vec_with_registry!(
                "narwhal_certificates_stored_total",
                "Total number of certificates stored",
                &["round"],
                registry
            ).unwrap(),
            
            certificates_gc_removed: register_counter_vec_with_registry!(
                "narwhal_certificates_gc_removed_total",
                "Total number of certificates removed by GC",
                &["round"],
                registry
            ).unwrap(),
            
            // Performance metrics
            consensus_latency: register_histogram_vec_with_registry!(
                "narwhal_consensus_latency_seconds",
                "Time from transaction submission to consensus",
                &["percentile"],
                vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
                registry
            ).unwrap(),
            
            throughput_tps: register_gauge_vec_with_registry!(
                "narwhal_throughput_tps",
                "Transactions per second throughput",
                &["window"],
                registry
            ).unwrap(),
            
            memory_usage_bytes: register_gauge_vec_with_registry!(
                "narwhal_memory_usage_bytes",
                "Memory usage in bytes",
                &["component"],
                registry
            ).unwrap(),
            
            cpu_usage_percent: register_gauge_vec_with_registry!(
                "narwhal_cpu_usage_percent",
                "CPU usage percentage",
                &["component"],
                registry
            ).unwrap(),
            
            // Byzantine fault tolerance metrics
            byzantine_faults_detected: register_counter_vec_with_registry!(
                "narwhal_byzantine_faults_detected_total",
                "Total number of Byzantine faults detected",
                &["fault_type", "author"],
                registry
            ).unwrap(),
            
            vote_equivocations: register_counter_vec_with_registry!(
                "narwhal_vote_equivocations_total",
                "Total number of vote equivocations detected",
                &["voter"],
                registry
            ).unwrap(),
            
            invalid_signatures: register_counter_vec_with_registry!(
                "narwhal_invalid_signatures_total",
                "Total number of invalid signatures detected",
                &["signature_type", "author"],
                registry
            ).unwrap(),
        }
    }
    
    /// Record a message being sent
    pub fn record_message_sent(&self, message_type: &str, peer: &str) {
        self.messages_sent
            .with_label_values(&[message_type, peer])
            .inc();
    }
    
    /// Record a message being received
    pub fn record_message_received(&self, message_type: &str, peer: &str) {
        self.messages_received
            .with_label_values(&[message_type, peer])
            .inc();
    }
    
    /// Record message send duration
    pub fn record_message_send_duration(&self, message_type: &str, duration: Duration) {
        self.message_send_duration
            .with_label_values(&[message_type])
            .observe(duration.as_secs_f64());
    }
    
    /// Update active connections gauge
    pub fn set_active_connections(&self, connection_type: &str, count: i64) {
        self.active_connections
            .with_label_values(&[connection_type])
            .set(count as f64);
    }
    
    /// Record a connection error
    pub fn record_connection_error(&self, error_type: &str, peer: &str) {
        self.connection_errors
            .with_label_values(&[error_type, peer])
            .inc();
    }
    
    /// Record a retry attempt
    pub fn record_retry_attempt(&self, operation: &str, peer: &str) {
        self.retry_attempts
            .with_label_values(&[operation, peer])
            .inc();
    }
    
    /// Record header creation
    pub fn record_header_created(&self, author: &str) {
        self.headers_created
            .with_label_values(&[author])
            .inc();
    }
    
    /// Record vote cast
    pub fn record_vote_cast(&self, voter: &str, round: u64) {
        self.votes_cast
            .with_label_values(&[voter, &round.to_string()])
            .inc();
    }
    
    /// Record certificate formation
    pub fn record_certificate_formed(&self, author: &str, round: u64) {
        self.certificates_formed
            .with_label_values(&[author, &round.to_string()])
            .inc();
    }
    
    /// Record DAG round advancement
    pub fn record_dag_round_advanced(&self, author: &str) {
        self.dag_rounds_advanced
            .with_label_values(&[author])
            .inc();
    }
    
    /// Update DAG depth gauge
    pub fn set_dag_depth(&self, author: &str, depth: i64) {
        self.dag_depth
            .with_label_values(&[author])
            .set(depth as f64);
    }
    
    /// Update pending headers gauge
    pub fn set_pending_headers(&self, round: u64, count: i64) {
        self.pending_headers
            .with_label_values(&[&round.to_string()])
            .set(count as f64);
    }
    
    /// Record transaction received
    pub fn record_transaction_received(&self, source: &str) {
        self.transactions_received
            .with_label_values(&[source])
            .inc();
    }
    
    /// Record transactions batched
    pub fn record_transactions_batched(&self, worker_id: &str, count: u64) {
        self.transactions_batched
            .with_label_values(&[worker_id])
            .inc_by(count as f64);
    }
    
    /// Record batch size
    pub fn record_batch_size(&self, worker_id: &str, size: f64) {
        self.batch_size
            .with_label_values(&[worker_id])
            .observe(size);
    }
    
    /// Record batch creation duration
    pub fn record_batch_creation_duration(&self, worker_id: &str, duration: Duration) {
        self.batch_creation_duration
            .with_label_values(&[worker_id])
            .observe(duration.as_secs_f64());
    }
    
    /// Update transactions in flight gauge
    pub fn set_transactions_in_flight(&self, stage: &str, count: i64) {
        self.transactions_in_flight
            .with_label_values(&[stage])
            .set(count as f64);
    }
    
    /// Record storage operation
    pub fn record_storage_operation(&self, operation: &str, table: &str) {
        self.storage_operations
            .with_label_values(&[operation, table])
            .inc();
    }
    
    /// Record storage operation duration
    pub fn record_storage_operation_duration(&self, operation: &str, table: &str, duration: Duration) {
        self.storage_operation_duration
            .with_label_values(&[operation, table])
            .observe(duration.as_secs_f64());
    }
    
    /// Update storage size gauge
    pub fn set_storage_size_bytes(&self, table: &str, size: i64) {
        self.storage_size_bytes
            .with_label_values(&[table])
            .set(size as f64);
    }
    
    /// Record certificate stored
    pub fn record_certificate_stored(&self, round: u64) {
        self.certificates_stored
            .with_label_values(&[&round.to_string()])
            .inc();
    }
    
    /// Record certificates removed by GC
    pub fn record_certificates_gc_removed(&self, round: u64, count: u64) {
        self.certificates_gc_removed
            .with_label_values(&[&round.to_string()])
            .inc_by(count as f64);
    }
    
    /// Record consensus latency
    pub fn record_consensus_latency(&self, latency: Duration) {
        self.consensus_latency
            .with_label_values(&["p50"])
            .observe(latency.as_secs_f64());
    }
    
    /// Update throughput gauge
    pub fn set_throughput_tps(&self, window: &str, tps: f64) {
        self.throughput_tps
            .with_label_values(&[window])
            .set(tps);
    }
    
    /// Update memory usage gauge
    pub fn set_memory_usage_bytes(&self, component: &str, bytes: i64) {
        self.memory_usage_bytes
            .with_label_values(&[component])
            .set(bytes as f64);
    }
    
    /// Update CPU usage gauge
    pub fn set_cpu_usage_percent(&self, component: &str, percent: f64) {
        self.cpu_usage_percent
            .with_label_values(&[component])
            .set(percent);
    }
    
    /// Record Byzantine fault detected
    pub fn record_byzantine_fault(&self, fault_type: &str, author: &str) {
        self.byzantine_faults_detected
            .with_label_values(&[fault_type, author])
            .inc();
    }
    
    /// Record vote equivocation
    pub fn record_vote_equivocation(&self, voter: &str) {
        self.vote_equivocations
            .with_label_values(&[voter])
            .inc();
    }
    
    /// Record invalid signature
    pub fn record_invalid_signature(&self, signature_type: &str, author: &str) {
        self.invalid_signatures
            .with_label_values(&[signature_type, author])
            .inc();
    }
}

/// Timer guard for recording operation duration
pub struct MetricTimer {
    start: Instant,
    metric: HistogramVec,
    labels: Vec<String>,
}

impl MetricTimer {
    /// Create a new timer for the given histogram metric
    pub fn new(metric: HistogramVec, labels: Vec<&str>) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Drop for MetricTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let label_refs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        self.metric
            .with_label_values(&label_refs)
            .observe(duration.as_secs_f64());
    }
}

/// Global metrics instance
static mut METRICS: Option<Arc<NarwhalMetrics>> = None;
static METRICS_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize global metrics with the given registry
pub fn init_metrics(registry: &Registry) -> Arc<NarwhalMetrics> {
    unsafe {
        METRICS_INIT.call_once(|| {
            METRICS = Some(Arc::new(NarwhalMetrics::new(registry)));
        });
        METRICS.as_ref().unwrap().clone()
    }
}

/// Get the global metrics instance
pub fn metrics() -> Option<Arc<NarwhalMetrics>> {
    unsafe { METRICS.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;
    
    #[test]
    fn test_metrics_creation() {
        let registry = Registry::new();
        let metrics = NarwhalMetrics::new(&registry);
        
        // Test recording some metrics
        metrics.record_message_sent("header", "peer1");
        metrics.record_header_created("validator1");
        metrics.set_active_connections("primary", 5);
        
        // Verify metrics can be gathered
        let metric_families = registry.gather();
        assert!(!metric_families.is_empty());
    }
    
    #[test]
    fn test_metric_timer() {
        let registry = Registry::new();
        let metrics = NarwhalMetrics::new(&registry);
        
        {
            let _timer = MetricTimer::new(
                metrics.storage_operation_duration.clone(),
                vec!["read", "certificates"]
            );
            // Simulate some work
            std::thread::sleep(Duration::from_millis(10));
        }
        
        // Timer should have recorded duration when dropped
        let metric_families = registry.gather();
        assert!(!metric_families.is_empty());
    }
}