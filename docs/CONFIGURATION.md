# Narwhal + Bullshark Configuration Guide

This guide explains all the configuration options available for Narwhal + Bullshark consensus in the Reth fork.

## Configuration Methods

You can configure Narwhal + Bullshark consensus using three methods:

1. **CLI Arguments** - Pass configuration directly as command-line arguments
2. **Configuration Files** - Use TOML or JSON configuration files
3. **Environment Variables** - Set configuration via environment variables

## CLI Arguments

All CLI arguments are prefixed with either `--narwhal.` or `--bullshark.` to indicate which component they configure.

### Basic Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--narwhal.enable` | Enable Narwhal + Bullshark consensus | `false` |
| `--narwhal.network-addr` | Network address for consensus | `127.0.0.1:9000` |
| `--narwhal.committee-size` | Number of validators in committee | `4` |
| `--narwhal.peers` | Comma-separated list of peer addresses | `[]` |
| `--narwhal.bootstrap` | Start without waiting for peers | `false` |

### Validator Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--validator.private-key` | Validator private key (hex) | - |
| `--validator.key-file` | Path to validator key JSON file | - |
| `--validator.config-dir` | Directory containing validator configs | - |
| `--validator.committee-config` | Committee configuration file | - |
| `--validator.deterministic-consensus-key` | Derive consensus key from EVM key | `false` |
| `--validator.index` | Validator index in committee (0-based) | - |

### Consensus RPC Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--consensus-rpc-port` | Port for standalone consensus RPC server (0=disabled) | `0` |
| `--consensus-rpc-enable-admin` | Enable admin endpoints for consensus RPC | `false` |

### Narwhal Performance Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--narwhal.max-batch-size` | Maximum batch size in bytes | `1024` |
| `--narwhal.max-batch-delay-ms` | Maximum batch delay in milliseconds | `100` |
| `--narwhal.num-workers` | Number of workers per authority | `4` |
| `--narwhal.gc-depth` | Garbage collection depth for old certificates | `50` |
| `--narwhal.cache-size` | Certificate cache size | `1000` |
| `--narwhal.max-concurrent-requests` | Maximum concurrent network requests | `200` |
| `--narwhal.certificate-buffer-size` | Pre-allocated certificate buffer size | `1000` |
| `--narwhal.max-transactions-per-batch` | Maximum transactions per batch | `100` |
| `--narwhal.batch-creation-interval-ms` | Batch creation interval in milliseconds | `50` |

### Narwhal Network Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--narwhal.connection-timeout-ms` | Connection timeout in milliseconds | `5000` |
| `--narwhal.request-timeout-ms` | Request timeout in milliseconds | `10000` |
| `--narwhal.retry-attempts` | Number of retry attempts for failed requests | `3` |
| `--narwhal.retry-base-delay-ms` | Base delay for exponential backoff in milliseconds | `100` |
| `--narwhal.retry-max-delay-ms` | Maximum delay for exponential backoff in milliseconds | `10000` |
| `--narwhal.sync-retry-delay-ms` | Sync retry delay in milliseconds | `5000` |

### Worker Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--narwhal.worker-base-port` | Base port for this node's worker services | `19000` |
| `--narwhal.worker-bind-address` | Worker bind address (defaults to primary address IP) | - |

**Note**: Worker port ranges for peer validators should be specified in their validator configuration files using the `worker_port_range` field (e.g., `"19000:19003"` for ports 19000-19003).

### Bullshark Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--bullshark.finality-threshold` | Minimum confirmations needed | `3` |
| `--bullshark.max-pending-rounds` | Maximum pending rounds to keep | `10` |
| `--bullshark.finalization-timeout-secs` | Finalization timeout in seconds | `5` |
| `--bullshark.max-certificates-per-round` | Maximum certificates per round | `1000` |
| `--bullshark.leader-rotation-frequency` | Leader rotation frequency in rounds | `2` |
| `--bullshark.min-leader-round` | Minimum round for leader election (must be even) | `0` |
| `--bullshark.max-dag-walk-depth` | Maximum DAG walk depth for consensus | `10` |
| `--bullshark.enable-detailed-metrics` | Enable detailed consensus metrics | `false` |

### HashiCorp Vault Integration (Optional)

| Argument | Description | Default |
|----------|-------------|---------|
| `--vault.enable` | Enable HashiCorp Vault for key management | `false` |
| `--vault.addr` | Vault server address | - |
| `--vault.mount-path` | Vault mount path for validator keys | `secret` |
| `--vault.key-path` | Vault key path for this validator's private key | - |
| `--vault.token` | Vault authentication token (use VAULT_TOKEN env var instead) | - |

## Validator Configuration Files

Each validator should have a JSON configuration file with the following structure:

```json
{
  "metadata": {
    "name": "Validator Name",
    "description": "Validator description",
    "contact": "contact@example.com"
  },
  "evm_private_key": "0x...",
  "consensus_private_key": null,  // Optional, derived from EVM key if null
  "stake": 1000,
  "active": true,
  "network_address": "127.0.0.1:9001",  // Primary consensus address
  "worker_port_range": "19000:19003"    // Worker ports (e.g., 4 workers use ports 19000-19003)
}
```

The `network_address` and `worker_port_range` fields tell other validators how to connect to this validator's consensus services.

### Worker Port Range Configuration

The `worker_port_range` field specifies the exact ports that a validator's workers will use. The format is `"start:end"` (inclusive). For example:
- `"19000:19003"` means 4 workers using ports 19000, 19001, 19002, and 19003
- `"20000:20004"` means 5 workers using ports 20000, 20001, 20002, 20003, and 20004

The number of workers is automatically calculated from the port range. This approach ensures:
- Each validator can have a different number of workers
- Port assignments are explicit and predictable
- No port conflicts between validators
- Easy to configure in deployment environments

## Configuration Files

You can use configuration files instead of CLI arguments. The configuration can be in TOML or JSON format.

### TOML Example

See `config/example-narwhal-config.toml` for a complete example:

```toml
# Enable Narwhal + Bullshark consensus
narwhal_enabled = true

# Network configuration
network_address = "127.0.0.1:9001"
committee_size = 4
peer_addresses = ["127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]

# Validator configuration
validator_index = 0
validator_key_file = "test_validators/validator-0.json"
validator_config_dir = "test_validators"
deterministic_consensus_key = true

# Consensus RPC
consensus_rpc_port = 10001
consensus_rpc_enable_admin = true

# Narwhal performance
max_batch_size = 100000
max_batch_delay_ms = 100
num_workers = 4
gc_depth = 50
cache_size = 1000

# Bullshark consensus
finality_threshold = 3
max_pending_rounds = 10
finalization_timeout_secs = 5
```

### JSON Example

See `config/example-narwhal-config.json` for a complete example:

```json
{
  "narwhal_enabled": true,
  "network_address": "127.0.0.1:9001",
  "committee_size": 4,
  "peer_addresses": ["127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"],
  "validator_index": 0,
  "validator_key_file": "test_validators/validator-0.json",
  "consensus_rpc_port": 10001,
  "consensus_rpc_enable_admin": true,
  "max_batch_size": 100000,
  "max_batch_delay_ms": 100,
  "num_workers": 4,
  "gc_depth": 50,
  "cache_size": 1000,
  "finality_threshold": 3,
  "max_pending_rounds": 10,
  "finalization_timeout_secs": 5
}
```

## Environment Variables

Some configuration options can also be set via environment variables:

| Environment Variable | Description |
|---------------------|-------------|
| `VALIDATOR_PRIVATE_KEY` | Validator private key (hex) |
| `VALIDATOR_KEY_FILE` | Path to validator key file |
| `VALIDATOR_CONFIG_DIR` | Directory containing validator configs |
| `COMMITTEE_CONFIG_FILE` | Committee configuration file |
| `VALIDATOR_INDEX` | Validator index in committee |
| `VAULT_ADDR` | HashiCorp Vault server address |
| `VAULT_KEY_PATH` | Vault key path |
| `VAULT_TOKEN` | Vault authentication token |

## Performance Tuning

### Network Performance

For production deployments, consider adjusting these parameters based on your network conditions:

- **`connection-timeout-ms`**: Increase for high-latency networks
- **`request-timeout-ms`**: Increase for slow nodes or large batches
- **`retry-attempts`**: Increase for unreliable networks
- **`max-concurrent-requests`**: Balance between throughput and resource usage

### Consensus Performance

- **`max-batch-size`**: Larger batches improve throughput but increase latency
- **`max-batch-delay-ms`**: Lower values reduce latency but may create smaller batches
- **`num-workers`**: More workers can improve throughput on multi-core systems
- **`gc-depth`**: Higher values keep more history but use more storage

### Byzantine Fault Tolerance

- **`finality-threshold`**: Higher values increase security but may slow finalization
- **`max-pending-rounds`**: Balance between memory usage and chain history
- **`leader-rotation-frequency`**: Lower values provide better fairness but more overhead

## Example Configurations

### High-Throughput Configuration

```bash
reth node \
  --narwhal.enable \
  --narwhal.max-batch-size 10000000 \
  --narwhal.max-batch-delay-ms 500 \
  --narwhal.num-workers 8 \
  --narwhal.max-concurrent-requests 500 \
  --narwhal.certificate-buffer-size 5000 \
  --bullshark.max-certificates-per-round 5000
```

### Low-Latency Configuration

```bash
reth node \
  --narwhal.enable \
  --narwhal.max-batch-size 50000 \
  --narwhal.max-batch-delay-ms 10 \
  --narwhal.batch-creation-interval-ms 10 \
  --narwhal.connection-timeout-ms 1000 \
  --narwhal.request-timeout-ms 2000
```

### High-Security Configuration

```bash
reth node \
  --narwhal.enable \
  --bullshark.finality-threshold 5 \
  --bullshark.max-pending-rounds 20 \
  --bullshark.finalization-timeout-secs 10 \
  --narwhal.retry-attempts 5 \
  --narwhal.gc-depth 100
```

## Monitoring

When `--bullshark.enable-detailed-metrics` is enabled, additional metrics are collected:

- Certificate production rate per validator
- Vote participation rates
- Round advancement times
- Network message latencies
- DAG growth statistics

Access these metrics via the consensus RPC endpoints:

```bash
# Get overall metrics
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"consensus_getMetrics","params":[],"id":1}' \
  http://localhost:10001

# Get validator-specific metrics
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"consensus_getValidatorMetrics","params":["0x..."],"id":1}' \
  http://localhost:10001
```

## Troubleshooting

### Common Issues

1. **"Unknown authority" errors**: Ensure all validators have the same committee configuration
2. **Slow consensus**: Check network latency and adjust timeout parameters
3. **High memory usage**: Reduce `gc-depth` and `max-pending-rounds`
4. **Failed connections**: Verify firewall rules and peer addresses

### Debug Commands

```bash
# Check consensus status
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"consensus_getStatus","params":[],"id":1}' \
  http://localhost:10001

# Get DAG information (requires --consensus-rpc-enable-admin)
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"consensus_admin_getDagInfo","params":[],"id":1}' \
  http://localhost:10001
```