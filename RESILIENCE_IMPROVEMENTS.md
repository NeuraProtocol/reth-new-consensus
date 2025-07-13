# Resilience Improvements for Narwhal+Bullshark Consensus

This document describes the resilience mechanisms added to improve consensus stability and network reliability.

## 1. Finalization Backpressure (COMPLETED)

**Problem**: DAG service continued creating new headers even when BFT was falling behind, leading to unbounded growth.

**Solution**: Added backpressure mechanism in `dag_service.rs`:
- Added `last_finalized_round` and `max_pending_rounds` fields
- DAG pauses header creation when pending rounds exceed threshold
- Added `update_last_finalized_round()` method for BFT to signal progress

**Implementation**:
```rust
// In dag_service.rs
let pending_rounds = self.current_round.saturating_sub(self.last_finalized_round);
if pending_rounds > self.max_pending_rounds {
    warn!("BACKPRESSURE: {} rounds pending finalization", pending_rounds);
    // Skip header creation until BFT catches up
}
```

## 2. Retry Mechanism for Leader Support (COMPLETED)

**Problem**: Leaders could fail to get support due to timing issues with no retry mechanism.

**Solution**: Added retry logic in BFT service:
- Added `max_leader_support_retries` and `leader_support_retry_delay` config options
- Consensus returns `LeaderLacksSupport` error for retry candidates
- BFT service tracks pending rounds and retries periodically
- Added `pending_leader_rounds` HashMap to track retry state

**Implementation**:
```rust
// In bft_service.rs
fn retry_pending_leader_rounds(&mut self) {
    // Check rounds pending support with exponential backoff
    // Retry leader support checks when appropriate
}
```

## 3. Network Authentication Fix (COMPLETED)

**Problem**: TLS certificate errors between nodes.

**Solution**: Already implemented deterministic key derivation:
- Network keys derived from consensus key + network address
- Ensures consistent peer IDs across restarts
- Uses Blake2b hash with domain separation

**Implementation**:
```rust
// In crypto.rs
pub fn derive_network_keypair(consensus_key: &PublicKey, network_address: &SocketAddr) -> [u8; 32]
```

## 4. Connection Monitoring and Retry (COMPLETED)

**Problem**: No visibility into connection health or automatic reconnection.

**Solution**: Added comprehensive connection monitoring:
- Added `connection_health` tracking with last success time and failure count
- Implemented exponential backoff for reconnection attempts
- Added `spawn_connection_monitor()` for periodic health checks
- Success/failure recording in `send_with_retry()`

**Implementation**:
```rust
// In network.rs
pub fn spawn_connection_monitor(self: Arc<Self>) -> JoinHandle<()> {
    // Periodic monitoring with exponential backoff
    // Logs connection health statistics
    // Automatically retries failed connections
}
```

## Configuration

Default settings in `config.rs`:
- `max_pending_rounds`: 50 (backpressure threshold)
- `max_leader_support_retries`: 5
- `leader_support_retry_delay`: 50ms
- Connection monitor interval: 30 seconds
- Connection retry backoff: 2^failures seconds (max 300s)

## Testing

To verify these improvements:

1. **Backpressure**: Monitor logs for "BACKPRESSURE" messages when BFT falls behind
2. **Leader Retry**: Look for "Leader X lacks support, retry Y/Z" messages
3. **Connection Health**: Check "Connection health: X/Y peers connected" logs
4. **Reconnection**: Watch for "Attempting to reconnect" and success messages

## Impact

These changes make the consensus more resilient to:
- Network delays and packet loss
- Timing variations between nodes
- Temporary network partitions
- Processing delays in BFT

The system now gracefully handles transient failures and maintains liveness even under adverse conditions.