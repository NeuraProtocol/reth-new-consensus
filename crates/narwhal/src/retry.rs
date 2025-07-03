//! Retry configuration and implementation for network operations
//!
//! This module provides exponential backoff retry logic for handling
//! transient network failures in a production environment.

use std::{future::Future, time::Duration};
use backoff::ExponentialBackoff;
use tracing::{debug, warn};
use std::sync::Arc;

/// Retry configurations for establishing connections and sending messages.
/// Determines the retry behaviour of requests by setting the backoff strategy used.
#[derive(Clone, Debug, Copy)]
pub struct RetryConfig {
    /// The initial retry interval.
    ///
    /// This is the first delay before a retry, for establishing connections and sending messages.
    /// The subsequent delay will be decided by the `retry_delay_multiplier`.
    pub initial_retry_interval: Duration,

    /// The maximum value of the backoff period. Once the retry interval reaches this
    /// value it stops increasing.
    ///
    /// This is the longest duration we will have,
    /// for establishing connections and sending messages.
    /// Retrying continues even after the duration times have reached this duration.
    /// The number of retries before that happens, will be decided by the `retry_delay_multiplier`.
    /// The number of retries after that, will be decided by the `retrying_max_elapsed_time`.
    pub max_retry_interval: Duration,

    /// The value to multiply the current interval with for each retry attempt.
    pub retry_delay_multiplier: f64,

    /// The randomization factor to use for creating a range around the retry interval.
    ///
    /// A randomization factor of 0.5 results in a random period ranging between 50% below and 50%
    /// above the retry interval.
    pub retry_delay_rand_factor: f64,

    /// The maximum elapsed time after instantiating
    ///
    /// Retrying continues until this time has elapsed.
    /// The number of retries before that happens, will be decided by the other retry config options.
    pub retrying_max_elapsed_time: Option<Duration>,
}

impl RetryConfig {
    // Together with the default max and multiplier,
    // default gives 5-6 retries in ~30 s total retry time.

    /// Default for [`RetryConfig::initial_retry_interval`] (500 ms).
    pub const DEFAULT_INITIAL_RETRY_INTERVAL: Duration = Duration::from_millis(500);

    /// Default for [`RetryConfig::max_retry_interval`] (15 s).
    pub const DEFAULT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(15);

    /// Default for [`RetryConfig::retry_delay_multiplier`] (x1.5).
    pub const DEFAULT_RETRY_INTERVAL_MULTIPLIER: f64 = 1.5;

    /// Default for [`RetryConfig::retry_delay_rand_factor`] (0.3).
    pub const DEFAULT_RETRY_DELAY_RAND_FACTOR: f64 = 0.3;

    /// Default for [`RetryConfig::retrying_max_elapsed_time`] (30 s).
    pub const DEFAULT_RETRYING_MAX_ELAPSED_TIME: Duration = Duration::from_secs(30);

    /// Creates a new retry configuration with infinite retries (no max elapsed time)
    pub fn new_infinite() -> Self {
        Self {
            retrying_max_elapsed_time: None,
            ..Default::default()
        }
    }

    /// Perform `op` and retry on errors as specified by this configuration.
    ///
    /// Note that `backoff::Error<E>` implements `From<E>` for any `E` by creating a
    /// `backoff::Error::Transient`, meaning that errors will be retried unless explicitly returning
    /// `backoff::Error::Permanent`.
    pub async fn retry<R, E, Fn, Fut>(&self, mut op: Fn) -> Result<R, E>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<R, backoff::Error<E>>>,
    {
        let backoff = ExponentialBackoff {
            initial_interval: self.initial_retry_interval,
            randomization_factor: self.retry_delay_rand_factor,
            multiplier: self.retry_delay_multiplier,
            max_interval: self.max_retry_interval,
            max_elapsed_time: self.retrying_max_elapsed_time,
            ..Default::default()
        };

        backoff::future::retry(backoff, op).await
    }

    /// Perform `op` and retry on errors, with logging of retry attempts
    pub async fn retry_with_logging<R, E, Fn, Fut>(
        &self,
        operation_name: &str,
        mut op: Fn,
    ) -> Result<R, E>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<R, backoff::Error<E>>>,
        E: std::fmt::Display,
    {
        let backoff = ExponentialBackoff {
            initial_interval: self.initial_retry_interval,
            randomization_factor: self.retry_delay_rand_factor,
            multiplier: self.retry_delay_multiplier,
            max_interval: self.max_retry_interval,
            max_elapsed_time: self.retrying_max_elapsed_time,
            ..Default::default()
        };

        // Use the simpler retry without notify for now
        let result = backoff::future::retry(backoff, op).await;
        
        match &result {
            Ok(_) => debug!("{} succeeded", operation_name),
            Err(e) => warn!("{} failed after retries: {}", operation_name, e),
        }
        
        result
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_retry_interval: RetryConfig::DEFAULT_INITIAL_RETRY_INTERVAL,
            max_retry_interval: RetryConfig::DEFAULT_MAX_RETRY_INTERVAL,
            retry_delay_multiplier: RetryConfig::DEFAULT_RETRY_INTERVAL_MULTIPLIER,
            retry_delay_rand_factor: RetryConfig::DEFAULT_RETRY_DELAY_RAND_FACTOR,
            retrying_max_elapsed_time: Some(RetryConfig::DEFAULT_RETRYING_MAX_ELAPSED_TIME),
        }
    }
}

/// Helper function to classify anemo network errors as transient or permanent
pub fn classify_error(error: anemo::Error) -> backoff::Error<anemo::Error> {
    let error_str = error.to_string();
    
    // Classify certain errors as permanent (non-retryable)
    if error_str.contains("invalid argument") ||
       error_str.contains("permission denied") ||
       error_str.contains("protocol error") ||
       error_str.contains("incompatible version") {
        backoff::Error::permanent(error)
    } else {
        // Most network errors are transient and should be retried
        backoff::Error::transient(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        
        let config = RetryConfig {
            initial_retry_interval: Duration::from_millis(10),
            max_retry_interval: Duration::from_millis(100),
            ..Default::default()
        };
        
        let result = config.retry(|| {
            let attempts = attempts_clone.clone();
            async move {
                let count = attempts.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    Err(backoff::Error::transient("temporary failure"))
                } else {
                    Ok("success")
                }
            }
        }).await;
        
        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_permanent_error() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        
        let config = RetryConfig::default();
        
        let result: Result<(), String> = config.retry(|| {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(backoff::Error::permanent("permanent failure".to_string()))
            }
        }).await;
        
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // Should not retry on permanent errors
    }

    #[tokio::test]
    async fn test_retry_max_elapsed_time() {
        let start = std::time::Instant::now();
        let config = RetryConfig {
            initial_retry_interval: Duration::from_millis(10),
            retrying_max_elapsed_time: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        
        let result: Result<(), String> = config.retry(|| async {
            Err(backoff::Error::transient("always fails".to_string()))
        }).await;
        
        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}