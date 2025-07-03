//! A bounded tokio executor for limiting concurrent tasks
//!
//! This module provides a semaphore-based executor that limits the number of
//! concurrent tasks, preventing resource exhaustion in high-load scenarios.

use futures::future::Future;
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tracing::{debug, warn};
use thiserror::Error;

/// Error type for bounded execution failures
#[derive(Error, Debug)]
pub enum BoundedExecutionError {
    /// The executor is at capacity and cannot accept new tasks
    #[error("Concurrent execution limit reached")]
    Full,
    
    /// The task could not be spawned
    #[error("Failed to spawn task: {0}")]
    SpawnError(String),
}

/// A bounded executor that limits concurrent task execution
#[derive(Clone, Debug)]
pub struct BoundedExecutor {
    /// Semaphore controlling concurrent task count
    semaphore: Arc<Semaphore>,
    /// Tokio runtime handle for spawning tasks
    executor: Handle,
    /// Human-readable name for debugging
    name: String,
}

impl BoundedExecutor {
    /// Create a new `BoundedExecutor` with a maximum concurrent task capacity
    pub fn new(capacity: usize, executor: Handle, name: impl Into<String>) -> Self {
        let semaphore = Arc::new(Semaphore::new(capacity));
        Self {
            semaphore,
            executor,
            name: name.into(),
        }
    }

    /// Create a new `BoundedExecutor` using the current runtime
    pub fn new_current(capacity: usize, name: impl Into<String>) -> Self {
        Self::new(capacity, Handle::current(), name)
    }

    /// Returns the executor's available capacity for running tasks
    pub fn available_capacity(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Returns the executor's total capacity
    pub fn total_capacity(&self) -> usize {
        self.semaphore.available_permits() + self.in_use_capacity()
    }

    /// Returns the number of permits currently in use
    pub fn in_use_capacity(&self) -> usize {
        // This is approximate since permits can be acquired/released concurrently
        let available = self.semaphore.available_permits();
        let total = self.semaphore.available_permits() + self.semaphore.available_permits();
        total.saturating_sub(available)
    }

    /// Acquires a permit, first trying non-blocking, then waiting with logging
    async fn acquire_permit(semaphore: Arc<Semaphore>, name: &str) -> OwnedSemaphorePermit {
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                debug!(
                    "BoundedExecutor '{}' at capacity, waiting for available slot...",
                    name
                );
                semaphore.acquire_owned().await.expect("Semaphore closed")
            }
        }
    }

    /// Spawn a future on the executor, waiting if at capacity
    pub async fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = Self::acquire_permit(self.semaphore.clone(), &self.name).await;
        self.spawn_with_permit(f, permit)
    }

    /// Try to spawn a future on the executor without waiting
    pub fn try_spawn<F>(&self, f: F) -> Result<JoinHandle<F::Output>, BoundedExecutionError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(self.spawn_with_permit(f, permit)),
            Err(_) => {
                warn!(
                    "BoundedExecutor '{}' is full ({} tasks running)",
                    self.name,
                    self.total_capacity()
                );
                Err(BoundedExecutionError::Full)
            }
        }
    }

    /// Spawn a future with an already-acquired permit
    fn spawn_with_permit<F>(
        &self,
        f: F,
        spawn_permit: OwnedSemaphorePermit,
    ) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Wrap the future to release the permit when done
        let wrapped = async move {
            let result = f.await;
            drop(spawn_permit); // Explicitly drop to release permit
            result
        };

        self.executor.spawn(wrapped)
    }

    /// Spawn a task with automatic retries using the provided retry configuration
    pub fn spawn_with_retries<F, Fut, T, E>(
        &self,
        retry_config: crate::retry::RetryConfig,
        operation_name: String,
        mut f: F,
    ) -> JoinHandle<Result<T, E>>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, backoff::Error<E>>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static + std::fmt::Display,
    {
        let semaphore = self.semaphore.clone();
        let executor_name = self.name.clone();
        
        // Spawn the retry driver as an unbounded task
        self.executor.spawn(async move {
            // Create a closure that acquires a permit before each attempt
            let mut attempt = 0;
            retry_config.retry_with_logging(&operation_name, || {
                let semaphore = semaphore.clone();
                let executor_name = executor_name.clone();
                let fut = f();
                attempt += 1;
                
                async move {
                    // Acquire permit for this attempt
                    let permit = Self::acquire_permit(semaphore, &executor_name).await;
                    
                    // Execute the operation while holding the permit
                    let result = fut.await;
                    
                    // Permit is automatically dropped here
                    drop(permit);
                    
                    result
                }
            }).await
        })
    }
}

/// A handle that cancels the associated task when dropped
pub struct CancelOnDropHandle<T>(pub JoinHandle<T>);

impl<T> Drop for CancelOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> CancelOnDropHandle<T> {
    /// Create a new cancel-on-drop handle
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }

    /// Wait for the task to complete
    pub async fn await_result(self) -> Result<T, tokio::task::JoinError> {
        // Extract the inner handle without dropping self
        let handle = unsafe {
            // This is safe because we immediately forget self after taking the handle
            let handle = std::ptr::read(&self.0);
            std::mem::forget(self);
            handle
        };
        handle.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_bounded_executor_capacity() {
        let executor = BoundedExecutor::new_current(2, "test");
        assert_eq!(executor.available_capacity(), 2);

        // Spawn a task that blocks
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle1 = executor.spawn(async move {
            rx.await.unwrap();
        }).await;

        // Should have one slot used
        assert_eq!(executor.available_capacity(), 1);

        // Try to spawn when not full - should succeed
        let handle2 = executor.try_spawn(async {
            sleep(Duration::from_millis(10)).await;
        }).unwrap();

        // Should be full now
        assert_eq!(executor.available_capacity(), 0);

        // Try to spawn when full - should fail
        let result = executor.try_spawn(async {});
        assert!(matches!(result, Err(BoundedExecutionError::Full)));

        // Complete first task
        tx.send(()).unwrap();
        handle1.await.unwrap();
        handle2.await.unwrap();

        // Should have capacity again
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(executor.available_capacity(), 2);
    }

    #[tokio::test]
    async fn test_concurrent_execution_limit() {
        const MAX_CONCURRENT: usize = 3;
        const TOTAL_TASKS: u32 = 10;
        
        let executor = BoundedExecutor::new_current(MAX_CONCURRENT, "test");
        let active_count = Arc::new(AtomicU32::new(0));
        let max_active = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();

        for _ in 0..TOTAL_TASKS {
            let active_count = active_count.clone();
            let max_active = max_active.clone();
            
            let handle = executor.spawn(async move {
                // Increment active count
                let current = active_count.fetch_add(1, Ordering::SeqCst) + 1;
                
                // Update max if needed
                max_active.fetch_max(current, Ordering::SeqCst);
                
                // Simulate work
                sleep(Duration::from_millis(50)).await;
                
                // Decrement active count
                active_count.fetch_sub(1, Ordering::SeqCst);
            }).await;
            
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify we never exceeded the limit
        assert!(max_active.load(Ordering::SeqCst) <= MAX_CONCURRENT as u32);
    }

    #[tokio::test]
    async fn test_cancel_on_drop() {
        let executor = BoundedExecutor::new_current(1, "test");
        
        let started = Arc::new(AtomicU32::new(0));
        let completed = Arc::new(AtomicU32::new(0));
        
        {
            let started = started.clone();
            let completed = completed.clone();
            
            let handle = executor.spawn(async move {
                started.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_secs(10)).await;
                completed.fetch_add(1, Ordering::SeqCst);
            }).await;
            
            let _cancel_handle = CancelOnDropHandle::new(handle);
            
            // Wait a bit to ensure task started
            sleep(Duration::from_millis(100)).await;
            
            // Handle will be dropped here, canceling the task
        }
        
        // Give time for cancellation
        sleep(Duration::from_millis(100)).await;
        
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
    }
}