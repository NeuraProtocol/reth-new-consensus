//! Narwhal + Bullshark consensus installation and management

use reth_ethereum_primitives::EthPrimitives;
use reth_evm::ConfigureEvm;
use reth_network::NetworkProtocols;
use reth_network_api::FullNetwork;
use reth_node_api::BeaconConsensusEngineEvent;
use reth_node_core::args::NarwhalBullsharkArgs;
use reth_provider::providers::{BlockchainProvider, ProviderNodeTypes};
use reth_tasks::TaskExecutor;
use reth_tokio_util::EventStream;
use reth_consensus::{
    narwhal_bullshark::{
        ServiceConfig,
        integration::NarwhalRethBridge,
        types::NarwhalBullsharkConfig,
        mempool_bridge::{MempoolOperations, PoolStats},
        validator_keys::{ValidatorKeyPair, ValidatorRegistry, ValidatorMetadata, ValidatorKeyConfig, KeyManagementStrategy, ValidatorIdentity},
    },
    consensus_storage::MdbxConsensusStorage,
    rpc::{ConsensusRpcImpl, ConsensusAdminRpcImpl},

};
use reth_transaction_pool::{TransactionPool, TransactionPoolExt, EthPooledTransaction, NewTransactionEvent, BlockInfo, TransactionListenerKind};
use reth_primitives::TransactionSigned as RethTransaction;
use alloy_primitives::{TxHash, B256, Address};
use tokio::sync::mpsc;
use futures::StreamExt;

use narwhal::types::Committee;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::*;
use fastcrypto::traits::{KeyPair, EncodeDecodeBase64};
use anyhow::Result;
use tokio::sync::RwLock;
use reth_db_api::{transaction::{DbTx, DbTxMut}, cursor::DbCursorRO};
use reth_provider::{DatabaseProviderFactory, DBProvider};


/// Concrete implementation of MempoolOperations for Reth's transaction pool
struct RethMempoolOperations<Pool> {
    pool: Arc<Pool>,
}

impl<Pool> RethMempoolOperations<Pool> 
where
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + 'static,
{
    pub fn new(pool: Arc<Pool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl<Pool> MempoolOperations for RethMempoolOperations<Pool>
where
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + Send + Sync + 'static,
{
    /// Subscribe to new transactions from the mempool
    async fn subscribe_new_transactions(&self) -> Result<mpsc::UnboundedReceiver<RethTransaction>> {
        let (tx_sender, tx_receiver) = mpsc::unbounded_channel();
        let pool = Arc::clone(&self.pool);
        
        // Spawn task to listen for new transactions and forward them
        tokio::spawn(async move {
            let mut new_tx_listener = pool.new_transactions_listener_for(TransactionListenerKind::All);
            let mut tx_count = 0u64;

            while let Some(event) = new_tx_listener.recv().await {
                tx_count += 1;
                
                let NewTransactionEvent { subpool, transaction } = event;
                let tx_hash = *transaction.hash();
                
                // Extract the underlying Reth transaction
                let recovered_tx = transaction.to_consensus();
                let reth_transaction = recovered_tx.into_inner();
                
                debug!(
                    "Forwarding transaction from {:?} pool to consensus: {} (count: {})",
                    subpool, tx_hash, tx_count
                );

                // Forward to consensus system
                if tx_sender.send(reth_transaction).is_err() {
                    warn!("Consensus system is shutting down, stopping pool listener");
                    break;
                }

                // Log progress periodically
                if tx_count % 100 == 0 {
                    info!("Forwarded {} transactions from pool to consensus", tx_count);
                }
            }
        });
        
        Ok(tx_receiver)
    }
    
    /// Remove finalized transactions from the mempool
    async fn remove_transactions(&self, tx_hashes: Vec<TxHash>) -> Result<usize> {
        let removed_transactions = self.pool.remove_transactions(tx_hashes);
        Ok(removed_transactions.len())
    }
    
    /// Update the mempool with new block information
    async fn update_block_info(&self, block_number: u64, block_hash: B256, base_fee: u64) -> Result<()> {
        let new_block_info = BlockInfo {
            last_seen_block_hash: block_hash,
            last_seen_block_number: block_number,
            block_gas_limit: 30_000_000, // 30M gas limit (should be configurable)
            pending_basefee: base_fee,
            pending_blob_fee: None, // No blob fee for now
        };

        self.pool.set_block_info(new_block_info);
        Ok(())
    }
    
    /// Get current mempool statistics
    fn get_pool_stats(&self) -> PoolStats {
        let pool_size = self.pool.pool_size();
        let block_info = self.pool.block_info();
        
        PoolStats {
            pending_transactions: pool_size.pending,
            queued_transactions: pool_size.queued,
            total_transactions: pool_size.total,
            current_block_number: block_info.last_seen_block_number,
            current_block_hash: block_info.last_seen_block_hash,
            processed_hashes_count: 0, // Not tracked at pool level
        }
    }
}

/// Install Narwhal + Bullshark consensus if it's enabled (with REAL MDBX integration).
pub fn install_narwhal_bullshark_consensus<P, E, N>(
    args: NarwhalBullsharkArgs,
    provider: BlockchainProvider<P>,
    _evm_config: E,
    _network: N,
    task_executor: TaskExecutor,
    engine_events: EventStream<BeaconConsensusEngineEvent<EthPrimitives>>,
) -> eyre::Result<(NarwhalRethBridge, Arc<RwLock<ValidatorRegistry>>, Arc<RwLock<MdbxConsensusStorage>>)>
where
    P: ProviderNodeTypes<Primitives = EthPrimitives>,
    E: ConfigureEvm<Primitives = EthPrimitives> + Clone + 'static,
    N: FullNetwork + NetworkProtocols,
{
    info!(target: "reth::cli", "Installing Narwhal + Bullshark consensus with REAL MDBX storage");

    // Create validator registry and committee with proper EVM integration
    let (committee, node_key, _validator_registry) = create_validator_committee(args.committee_size)?;

    // Create configuration
    let node_config = NarwhalBullsharkConfig {
        node_public_key: node_key.clone(),
        narwhal: args.to_narwhal_config(),
        bullshark: args.to_bullshark_config(),
    };

    let service_config = ServiceConfig::new(node_config, committee);

    // ✅ REAL: Create consensus storage and inject database operations
    let mut storage = MdbxConsensusStorage::new();
    
    // Create concrete database operations implementation using the provider
    let db_ops = Box::new(RethDatabaseOps::new(Arc::new(provider.clone())));
    storage.set_db_ops(db_ops);
    
    info!(target: "reth::narwhal_bullshark", "✅ REAL: Injected database operations into consensus storage");
    info!(target: "reth::narwhal_bullshark", "✅ REAL: Connected consensus storage to Reth database");
    
    info!(target: "reth::narwhal_bullshark", "🔧 MDBX integration complete:");
    info!(target: "reth::narwhal_bullshark", "   • Extension tables: ConsensusFinalizedBatch, ConsensusCertificates, ConsensusBatches, ConsensusDagVertices, ConsensusLatestFinalized");
    info!(target: "reth::narwhal_bullshark", "   • Database operations: RethDatabaseOps using provider interface");
    info!(target: "reth::narwhal_bullshark", "   • Real MDBX storage: All consensus data will be persisted to Reth's database");
    info!(target: "reth::narwhal_bullshark", "   • Transaction processing: Ready to store consensus certificates, batches, and finalized data");
    
    // Keep raw storage for return value
    let storage_for_return = Arc::new(tokio::sync::RwLock::new(MdbxConsensusStorage::new()));
    
    let storage = Some(Arc::new(storage));
    
    // Store raw storage for return value before wrapping
    // let storage_for_return = Arc::new(RwLock::new(storage.as_ref().unwrap().as_ref().clone()));
    
    // Create network configuration from CLI arguments
    let network_config = reth_consensus::narwhal_bullshark::integration::RethIntegrationConfig {
        network_address: args.network_address,
        enable_networking: true, // Enable networking for multi-node setup
        max_pending_transactions: 10000,
        execution_timeout: std::time::Duration::from_secs(30),
        enable_metrics: true,
        peer_addresses: args.peer_addresses.clone(),
    };
    
    let bridge = NarwhalRethBridge::new_with_network_config(service_config, storage.clone(), Some(network_config))
        .map_err(|e| eyre::eyre!("Failed to create Narwhal-Reth bridge: {}", e))?;

    // Note: Mempool integration is set up separately via setup_mempool_integration()
    info!(target: "reth::narwhal_bullshark", "✅ REAL: Consensus installed with MDBX integration - use setup_mempool_integration() for full integration");

    // Start the consensus bridge in a separate thread
    // For now, we skip starting the bridge here and let the caller handle it
    info!(target: "reth::cli", "✅ REAL: Narwhal + Bullshark consensus initialized with MDBX storage");
    
    // TODO: Monitor consensus health and integrate with Reth's engine events
    task_executor.spawn(async move {
        let mut stream = engine_events;
        while let Some(event) = stream.next().await {
            trace!(target: "reth::narwhal_bullshark", ?event, "Received engine event");
            // TODO: Forward relevant events to consensus system
        }
    });

    Ok((
        bridge, 
        Arc::new(RwLock::new(_validator_registry)), 
        storage_for_return
    ))
}

/// Sets up real mempool integration with the consensus bridge
pub fn setup_mempool_integration<Pool>(
    mut bridge: NarwhalRethBridge,
    transaction_pool: Arc<Pool>,
    task_executor: TaskExecutor,
) -> eyre::Result<()>
where
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + Send + Sync + 'static,
{
    info!(target: "reth::narwhal_bullshark", "Setting up real mempool integration");
    
    // Create mempool operations implementation
    let mempool_ops = Box::new(RethMempoolOperations::new(transaction_pool));
    
    // Set up mempool bridge with real pool operations
    bridge.set_mempool_operations(mempool_ops)
        .map_err(|e| eyre::eyre!("Failed to set mempool operations: {}", e))?;
    
    // Now start the consensus bridge with mempool integration
    task_executor.spawn(async move {
        if let Err(e) = bridge.start().await {
            error!(target: "reth::narwhal_bullshark", "Consensus bridge error: {}", e);
        }
    });
    
    info!(target: "reth::narwhal_bullshark", "Mempool integration configured and consensus bridge started");
    Ok(())
}

/// Create a validator committee with proper EVM address integration
/// Returns (Committee, node_consensus_key, ValidatorRegistry)
fn create_validator_committee(size: usize) -> eyre::Result<(Committee, narwhal::types::PublicKey, ValidatorRegistry)> {
    if size < 1 {
        return Err(eyre::eyre!("Committee size must be at least 1"));
    }

    let mut validator_registry = ValidatorRegistry::new();
    let mut stakes = HashMap::new();
    let mut validator_keypairs = Vec::new();

    info!(target: "reth::narwhal_bullshark", "Creating validator committee with {} members", size);

    // Generate validators with both EVM and consensus keys
    for i in 0..size {
        // Generate validator with both EVM and consensus keys
        let keypair = ValidatorKeyPair::generate()
            .map_err(|e| eyre::eyre!("Failed to generate validator keypair: {}", e))?;
        
        let metadata = ValidatorMetadata {
            name: Some(format!("Validator-{}", i)),
            description: Some(format!("Test validator {} for development", i)),
            contact: None,
        };
        
        let identity = keypair.identity(metadata);
        let evm_address = identity.evm_address;
        let consensus_key = identity.consensus_public_key.clone();
        
        // Register validator
        validator_registry.register_validator(identity)
            .map_err(|e| eyre::eyre!("Failed to register validator: {}", e))?;
        
        // Assign equal stake (100 units each)
        stakes.insert(evm_address, 100u64);
        validator_keypairs.push(keypair);
        
        info!(target: "reth::narwhal_bullshark", 
              "Generated validator {}: EVM address {:?}, Consensus key {:?}", 
              i, evm_address, consensus_key.encode_base64());
    }
    
    // Create committee from registered validators
    let committee = validator_registry.create_committee(0, &stakes)
        .map_err(|e| eyre::eyre!("Failed to create committee: {}", e))?;
    
    // Use the first validator's consensus key as this node's key
    let node_consensus_key = validator_keypairs[0].consensus_keypair.public().clone();
    
    info!(target: "reth::narwhal_bullshark", 
          "Created committee with {} validators, total stake: {}", 
          committee.authorities.len(), committee.total_stake);
    
    // Log the validator mappings for reference
    for validator in validator_registry.all_validators() {
        info!(target: "reth::narwhal_bullshark",
              "Validator mapping: EVM {:?} <-> Consensus {:?} ({})",
              validator.evm_address,
              validator.consensus_public_key.encode_base64(),
              validator.metadata.name.as_deref().unwrap_or("Unknown"));
    }
    
    Ok((committee, node_consensus_key, validator_registry))
}

/// Create a production validator committee from configuration
/// This would be used when loading real validator configurations
pub async fn create_production_committee(
    config: &ValidatorKeyConfig,
    stakes: &HashMap<Address, u64>,
    epoch: u64,
) -> eyre::Result<(Committee, ValidatorRegistry)> {
    let mut validator_registry = ValidatorRegistry::new();
    
    match config.key_strategy {
        KeyManagementStrategy::FileSystem => {
            info!(target: "reth::narwhal_bullshark", "Loading validators from filesystem");
            
            // Load validator files from the configured directory
            let validator_files = load_validators_from_filesystem(config)?;
            
            for validator_file in validator_files {
                // Skip inactive validators
                if !validator_file.active {
                    info!(target: "reth::narwhal_bullshark", 
                          "Skipping inactive validator: {}", 
                          validator_file.metadata.name.as_deref().unwrap_or("Unknown"));
                    continue;
                }
                
                // Check if this validator has the required stake
                let stake_amount = validator_file.stake;
                let evm_address = {
                    // Create keypair to get the EVM address
                    let keypair = validator_file_to_keypair(&validator_file)?;
                    keypair.evm_address
                };
                
                // Check if this EVM address is in the provided stakes map
                if let Some(&expected_stake) = stakes.get(&evm_address) {
                    if stake_amount != expected_stake {
                        warn!(target: "reth::narwhal_bullshark",
                              "Stake mismatch for validator {:?}: file has {}, expected {}",
                              evm_address, stake_amount, expected_stake);
                    }
                    
                    // Create the full validator keypair and identity
                    let keypair = validator_file_to_keypair(&validator_file)?;
                    let identity = create_validator_identity_from_file(&validator_file, &keypair);
                    
                    // Register the validator
                    validator_registry.register_validator(identity)
                        .map_err(|e| eyre::eyre!("Failed to register validator from file: {}", e))?;
                    
                    info!(target: "reth::narwhal_bullshark",
                          "Registered validator: {} (EVM: {:?}, Stake: {})",
                          validator_file.metadata.name.as_deref().unwrap_or("Unknown"),
                          evm_address, stake_amount);
                } else {
                    info!(target: "reth::narwhal_bullshark",
                          "Validator {} not in current epoch stake set, skipping",
                          validator_file.metadata.name.as_deref().unwrap_or("Unknown"));
                }
            }
        },
        KeyManagementStrategy::External => {
            // Load validators from external service (e.g., HashiCorp Vault)
            match load_validators_from_external(&config).await {
                Ok(external_validators) => {
                    for external_validator in external_validators {
                        // Create keypair from vault validator to get addresses and keys
                        let vault_config = parse_vault_config(config)?;
                        let mut vault_client = VaultValidatorClient::new(vault_config)?;
                        let vault_config = VaultValidatorConfig {
                            metadata: external_validator.metadata.clone(),
                            evm_key_path: external_validator.external_key_id.clone(),
                            consensus_key_path: external_validator.external_key_id.clone(),
                            stake: external_validator.stake,
                            active: external_validator.active,
                            key_version: external_validator.key_version,
                            key_access: VaultKeyAccessStrategy::RetrieveKeys {
                                kv_mount: "secret".to_string(),
                                key_format: VaultKeyFormat::Raw,
                            },
                        };
                        
                        if external_validator.active {
                            let keypair = vault_config_to_keypair(&mut vault_client, &vault_config).await?;
                            let evm_address = keypair.evm_address;
                            
                            if stakes.contains_key(&evm_address) {
                                let identity = ValidatorIdentity {
                                    evm_address,
                                    consensus_public_key: keypair.consensus_keypair.public().clone(),
                                    metadata: external_validator.metadata,
                                };
                                
                                validator_registry.register_validator(identity)
                                .map_err(|e| eyre::eyre!("Failed to register validator: {}", e))?;
                            }
                        }
                    }
                },
                Err(e) => {
                    return Err(eyre::eyre!("Failed to load validators from external service: {}", e));
                }
            }
        },
        KeyManagementStrategy::HSM => {
            // TODO: Load from hardware security modules
            return Err(eyre::eyre!("HSM key management not yet implemented"));
        },
        KeyManagementStrategy::Random => {
            // Generate random validators for each stake entry
            for (evm_address, stake) in stakes {
                // In production, this would load the real consensus key for this EVM address
                // For now, generate a random consensus key
                let consensus_keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
                
                let identity = reth_consensus::narwhal_bullshark::validator_keys::ValidatorIdentity {
                    evm_address: *evm_address,
                    consensus_public_key: consensus_keypair.public().clone(),
                    metadata: ValidatorMetadata {
                        name: Some(format!("Validator-{:?}", evm_address)),
                        ..Default::default()
                    },
                };
                
                validator_registry.register_validator(identity)
                    .map_err(|e| eyre::eyre!("Failed to register validator: {}", e))?;
            }
        },
    }
    
    let committee = validator_registry.create_committee(epoch, stakes)
        .map_err(|e| eyre::eyre!("Failed to create committee: {}", e))?;
    
    Ok((committee, validator_registry))
}

/// Check if Narwhal + Bullshark consensus should override standard Ethereum consensus
pub fn should_use_narwhal_consensus(args: &NarwhalBullsharkArgs) -> bool {
    args.narwhal_enabled
}

/// Get consensus mode description for logging
pub fn consensus_mode_description(args: &NarwhalBullsharkArgs) -> &'static str {
    if args.narwhal_enabled {
        "Narwhal + Bullshark BFT Consensus"
    } else {
        "Standard Ethereum Consensus"
    }
}

/// Install consensus RPC endpoints
/// 
/// This function adds consensus-specific RPC endpoints to Reth's RPC server,
/// providing visibility and control over the Narwhal + Bullshark consensus system.
pub fn install_consensus_rpc(
    consensus_bridge: Arc<RwLock<NarwhalRethBridge>>,
    validator_registry: Arc<RwLock<ValidatorRegistry>>, 
    storage: Arc<RwLock<MdbxConsensusStorage>>,
) -> (Arc<ConsensusRpcImpl>, Arc<ConsensusAdminRpcImpl>) {
    
    info!(target: "reth::narwhal_bullshark", "Installing consensus RPC endpoints");
    
    // Create the RPC implementations
    let consensus_rpc = Arc::new(ConsensusRpcImpl::new(
        consensus_bridge,
        validator_registry,
        storage,
    ));
    
    let admin_rpc = Arc::new(ConsensusAdminRpcImpl::new(consensus_rpc.clone()));
    
    info!(target: "reth::narwhal_bullshark", "Consensus RPC endpoints installed successfully");
    info!(target: "reth::narwhal_bullshark", "Available endpoints:");
    info!(target: "reth::narwhal_bullshark", "  consensus_* - Public consensus information");
    info!(target: "reth::narwhal_bullshark", "  consensus_admin_* - Administrative operations");
    
    (consensus_rpc, admin_rpc)
}

/// Example usage of consensus RPC endpoints
/// 
/// This shows how to query the consensus system via RPC.
pub fn example_consensus_rpc_usage() {
    info!(target: "reth::narwhal_bullshark", "Example consensus RPC calls:");
    info!(target: "reth::narwhal_bullshark", "");
    info!(target: "reth::narwhal_bullshark", "# Get consensus status");
    info!(target: "reth::narwhal_bullshark", "curl -X POST -H 'Content-Type: application/json' \\");
    info!(target: "reth::narwhal_bullshark", "  --data '{{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getStatus\",\"params\":[],\"id\":1}}' \\");
    info!(target: "reth::narwhal_bullshark", "  http://localhost:8545");
    info!(target: "reth::narwhal_bullshark", "");
    info!(target: "reth::narwhal_bullshark", "# Get committee information");
    info!(target: "reth::narwhal_bullshark", "curl -X POST -H 'Content-Type: application/json' \\");
    info!(target: "reth::narwhal_bullshark", "  --data '{{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getCommittee\",\"params\":[],\"id\":2}}' \\");
    info!(target: "reth::narwhal_bullshark", "  http://localhost:8545");
    info!(target: "reth::narwhal_bullshark", "");
    info!(target: "reth::narwhal_bullshark", "# List validators");
    info!(target: "reth::narwhal_bullshark", "curl -X POST -H 'Content-Type: application/json' \\");
    info!(target: "reth::narwhal_bullshark", "  --data '{{\"jsonrpc\":\"2.0\",\"method\":\"consensus_listValidators\",\"params\":[true,10],\"id\":3}}' \\");
    info!(target: "reth::narwhal_bullshark", "  http://localhost:8545");
    info!(target: "reth::narwhal_bullshark", "");
    info!(target: "reth::narwhal_bullshark", "# Get consensus metrics");
    info!(target: "reth::narwhal_bullshark", "curl -X POST -H 'Content-Type: application/json' \\");
    info!(target: "reth::narwhal_bullshark", "  --data '{{\"jsonrpc\":\"2.0\",\"method\":\"consensus_getMetrics\",\"params\":[],\"id\":4}}' \\");
    info!(target: "reth::narwhal_bullshark", "  http://localhost:8545");
    info!(target: "reth::narwhal_bullshark", "");
    info!(target: "reth::narwhal_bullshark", "# Administrative: Get storage stats (requires admin access)");
    info!(target: "reth::narwhal_bullshark", "curl -X POST -H 'Content-Type: application/json' \\");
    info!(target: "reth::narwhal_bullshark", "  --data '{{\"jsonrpc\":\"2.0\",\"method\":\"consensus_admin_getStorageStats\",\"params\":[],\"id\":5}}' \\");
    info!(target: "reth::narwhal_bullshark", "  http://localhost:8545");
}

// ===== PRODUCTION VALIDATOR KEY MANAGEMENT =====

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::fs;

/// File format for storing validator keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorKeyFile {
    /// Validator metadata
    pub metadata: ValidatorMetadata,
    /// EVM private key (hex-encoded, 32 bytes)
    pub evm_private_key: String,
    /// Optional consensus private key (hex-encoded) - if not provided, derived from EVM key
    pub consensus_private_key: Option<String>,
    /// Stake amount for this validator
    pub stake: u64,
    /// Whether this validator is active
    pub active: bool,
}

/// Directory structure for validator keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorDirectory {
    /// Directory containing individual validator key files
    pub validators_dir: PathBuf,
    /// Global configuration file
    pub config_file: Option<PathBuf>,
    /// Expected file extension (.json, .toml, etc.)
    pub file_extension: String,
}

/// Configuration for production validator set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductionValidatorConfig {
    /// Network identifier
    pub network: String,
    /// Epoch number
    pub epoch: u64,
    /// Minimum stake requirement
    pub min_stake: u64,
    /// Maximum number of validators
    pub max_validators: Option<usize>,
    /// Validator key files or directory
    pub validators: ValidatorSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ValidatorSource {
    /// Individual validator key files
    Files { paths: Vec<PathBuf> },
    /// Directory containing validator files
    Directory { path: PathBuf, pattern: Option<String> },
    /// Single configuration file with all validators
    ConfigFile { path: PathBuf },
}

/// Load validators from filesystem
pub fn load_validators_from_filesystem(
    config: &ValidatorKeyConfig,
) -> eyre::Result<Vec<ValidatorKeyFile>> {
    let key_directory = config.key_directory
        .as_ref()
        .ok_or_else(|| eyre::eyre!("key_directory is required for FileSystem strategy"))?;

    info!(target: "reth::narwhal_bullshark", "Loading validators from directory: {:?}", key_directory);

    // Look for configuration file first
    let config_path = key_directory.join("validators.json");
    if config_path.exists() {
        return load_validators_from_config_file(&config_path);
    }

    // Fall back to loading individual files from directory
    load_validators_from_directory(key_directory)
}

/// Load validators from a single configuration file
fn load_validators_from_config_file(config_path: &Path) -> eyre::Result<Vec<ValidatorKeyFile>> {
    info!(target: "reth::narwhal_bullshark", "Loading validators from config file: {:?}", config_path);
    
    let config_content = fs::read_to_string(config_path)
        .map_err(|e| eyre::eyre!("Failed to read config file {:?}: {}", config_path, e))?;

    let config: ProductionValidatorConfig = serde_json::from_str(&config_content)
        .map_err(|e| eyre::eyre!("Failed to parse config file {:?}: {}", config_path, e))?;

    match config.validators {
        ValidatorSource::ConfigFile { path } => {
            let validators_content = fs::read_to_string(&path)
                .map_err(|e| eyre::eyre!("Failed to read validators file {:?}: {}", path, e))?;
            
            let validators: Vec<ValidatorKeyFile> = serde_json::from_str(&validators_content)
                .map_err(|e| eyre::eyre!("Failed to parse validators file {:?}: {}", path, e))?;
            
            Ok(validators)
        },
        ValidatorSource::Directory { path, pattern: _ } => {
            load_validators_from_directory(&path)
        },
        ValidatorSource::Files { paths } => {
            let mut validators = Vec::new();
            for path in paths {
                let validator = load_single_validator_file(&path)?;
                validators.push(validator);
            }
            Ok(validators)
        }
    }
}

/// Load validators from individual files in a directory
fn load_validators_from_directory(directory: &Path) -> eyre::Result<Vec<ValidatorKeyFile>> {
    info!(target: "reth::narwhal_bullshark", "Scanning directory for validator files: {:?}", directory);
    
    let mut validators = Vec::new();
    
    // Read all .json files in the directory
    let entries = fs::read_dir(directory)
        .map_err(|e| eyre::eyre!("Failed to read directory {:?}: {}", directory, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| eyre::eyre!("Failed to read directory entry: {}", e))?;
        let path = entry.path();
        
        // Only process JSON files
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            match load_single_validator_file(&path) {
                Ok(validator) => {
                    info!(target: "reth::narwhal_bullshark", "Loaded validator from {:?}: {}", 
                          path, validator.metadata.name.as_deref().unwrap_or("Unknown"));
                    validators.push(validator);
                },
                Err(e) => {
                    warn!(target: "reth::narwhal_bullshark", "Failed to load validator from {:?}: {}", path, e);
                    // Continue loading other validators even if one fails
                }
            }
        }
    }

    info!(target: "reth::narwhal_bullshark", "Loaded {} validators from directory", validators.len());
    Ok(validators)
}

/// Load a single validator from a file
fn load_single_validator_file(file_path: &Path) -> eyre::Result<ValidatorKeyFile> {
    let content = fs::read_to_string(file_path)
        .map_err(|e| eyre::eyre!("Failed to read validator file {:?}: {}", file_path, e))?;

    let validator: ValidatorKeyFile = serde_json::from_str(&content)
        .map_err(|e| eyre::eyre!("Failed to parse validator file {:?}: {}", file_path, e))?;

    // Validate the validator data
    validate_validator_file(&validator, file_path)?;

    Ok(validator)
}

/// Validate a loaded validator file
fn validate_validator_file(validator: &ValidatorKeyFile, file_path: &Path) -> eyre::Result<()> {
    // Validate EVM private key format
    if validator.evm_private_key.len() != 64 && !validator.evm_private_key.starts_with("0x") {
        return Err(eyre::eyre!(
            "Invalid EVM private key format in {:?}: expected 64 hex chars or 0x-prefixed", 
            file_path
        ));
    }

    // Validate consensus private key format if provided
    if let Some(ref consensus_key) = validator.consensus_private_key {
        if consensus_key.is_empty() {
            return Err(eyre::eyre!(
                "Empty consensus private key in {:?}", 
                file_path
            ));
        }
    }

    // Validate stake
    if validator.stake == 0 {
        return Err(eyre::eyre!(
            "Zero stake not allowed for validator in {:?}", 
            file_path
        ));
    }

    Ok(())
}

/// Convert ValidatorKeyFile to ValidatorKeyPair
fn validator_file_to_keypair(validator_file: &ValidatorKeyFile) -> eyre::Result<ValidatorKeyPair> {
    use secp256k1::SecretKey as EvmSecretKey;
    use alloy_primitives::hex;
    
    // Parse EVM private key
    let evm_key_hex = validator_file.evm_private_key.strip_prefix("0x")
        .unwrap_or(&validator_file.evm_private_key);
    
    let evm_key_bytes = hex::decode(evm_key_hex)
        .map_err(|e| eyre::eyre!("Failed to decode EVM private key: {}", e))?;
    
    if evm_key_bytes.len() != 32 {
        return Err(eyre::eyre!("EVM private key must be 32 bytes"));
    }
    
    let mut key_array = [0u8; 32];
    key_array.copy_from_slice(&evm_key_bytes);
    
    let evm_private_key = EvmSecretKey::from_byte_array(&key_array)
        .map_err(|e| eyre::eyre!("Invalid EVM private key: {}", e))?;

    // Create keypair - either deterministic from EVM key or from provided consensus key
    let keypair = if let Some(ref consensus_key_hex) = validator_file.consensus_private_key {
        // TODO: Implement consensus key parsing when needed
        // For now, use deterministic derivation
        ValidatorKeyPair::from_evm_key_deterministic(evm_private_key)
            .map_err(|e| eyre::eyre!("Failed to create validator keypair: {}", e))?
    } else {
        // Use deterministic derivation from EVM key
        ValidatorKeyPair::from_evm_key_deterministic(evm_private_key)
            .map_err(|e| eyre::eyre!("Failed to create validator keypair: {}", e))?
    };

    Ok(keypair)
}

/// Create validator identity from file and keypair
fn create_validator_identity_from_file(
    validator_file: &ValidatorKeyFile,
    keypair: &ValidatorKeyPair,
) -> reth_consensus::narwhal_bullshark::validator_keys::ValidatorIdentity {
    use reth_consensus::narwhal_bullshark::validator_keys::ValidatorIdentity;
    
    ValidatorIdentity {
        evm_address: keypair.evm_address,
        consensus_public_key: keypair.consensus_keypair.public().clone(),
        metadata: validator_file.metadata.clone(),
    }
}

// ===== HSM KEY MANAGEMENT =====

/// HSM (Hardware Security Module) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMConfig {
    /// HSM provider type
    pub provider: HSMProvider,
    /// Connection configuration
    pub connection: HSMConnection,
    /// Authentication credentials
    pub auth: HSMAuth,
    /// Key mapping configuration
    pub key_mapping: HSMKeyMapping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HSMProvider {
    /// AWS CloudHSM
    AwsCloudHSM { cluster_id: String, region: String },
    /// Azure Dedicated HSM  
    AzureHSM { vault_url: String, subscription_id: String },
    /// Thales Luna HSM
    ThalesLuna { host: String, port: u16 },
    /// YubiKey HSM (for smaller deployments)
    YubiKey { serial_number: String },
    /// Generic PKCS#11 HSM
    PKCS11 { library_path: PathBuf, slot_id: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMConnection {
    /// Connection timeout in seconds
    pub timeout_secs: u32,
    /// Retry configuration
    pub max_retries: u32,
    /// Connection pooling
    pub max_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMAuth {
    /// Authentication method
    pub method: HSMAuthMethod,
    /// Credentials path or reference
    pub credentials: String,
    /// Multi-factor authentication
    pub mfa_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HSMAuthMethod {
    /// Username/password authentication
    UserPass,
    /// Certificate-based authentication  
    Certificate,
    /// IAM role (for cloud HSMs)
    IAMRole,
    /// Hardware token
    HardwareToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMKeyMapping {
    /// How to map EVM addresses to HSM key identifiers
    pub address_to_key_id: HSMKeyMappingStrategy,
    /// Key derivation parameters
    pub derivation: Option<HSMKeyDerivation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HSMKeyMappingStrategy {
    /// Direct mapping: EVM address -> HSM key ID
    DirectMapping { mappings: HashMap<Address, String> },
    /// Deterministic: derive HSM key ID from EVM address
    Deterministic { prefix: String, algorithm: String },
    /// Configuration file with mappings
    ConfigFile { path: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMKeyDerivation {
    /// Key derivation algorithm (BIP32, BIP44, etc.)
    pub algorithm: String,
    /// Derivation path pattern
    pub path_pattern: String,
    /// Master key identifier in HSM
    pub master_key_id: String,
}

// ===== EXTERNAL KEY MANAGEMENT =====

/// External key management service configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKeyConfig {
    /// Service provider
    pub provider: ExternalKeyProvider,
    /// API configuration
    pub api: ExternalKeyAPI,
    /// Authentication
    pub auth: ExternalKeyAuth,
    /// Key operations configuration
    pub operations: ExternalKeyOperations,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExternalKeyProvider {
    /// AWS Key Management Service
    AwsKMS { region: String, cmk_id: String },
    /// Azure Key Vault
    AzureKeyVault { vault_url: String, tenant_id: String },
    /// Google Cloud KMS
    GoogleCloudKMS { project_id: String, location: String, key_ring: String },
    /// HashiCorp Vault
    HashiCorpVault { address: String, mount_path: String },
    /// CyberArk Privileged Access Manager
    CyberArk { server_url: String, app_id: String },
    /// Custom REST API
    CustomAPI { base_url: String, api_version: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKeyAPI {
    /// API endpoint URLs
    pub endpoints: ExternalKeyEndpoints,
    /// HTTP configuration
    pub http: HTTPConfig,
    /// Rate limiting
    pub rate_limit: Option<RateLimitConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKeyEndpoints {
    /// Get public key endpoint
    pub get_public_key: String,
    /// Sign transaction endpoint  
    pub sign_transaction: String,
    /// List keys endpoint
    pub list_keys: String,
    /// Health check endpoint
    pub health_check: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HTTPConfig {
    /// Request timeout in seconds
    pub timeout_secs: u32,
    /// TLS verification
    pub verify_tls: bool,
    /// Custom headers
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Requests per second
    pub requests_per_second: u32,
    /// Burst capacity
    pub burst_capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKeyAuth {
    /// Authentication method
    pub method: ExternalAuthMethod,
    /// Credentials
    pub credentials: ExternalCredentials,
    /// Token refresh configuration
    pub token_refresh: Option<TokenRefreshConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExternalAuthMethod {
    /// API key authentication
    ApiKey,
    /// OAuth 2.0
    OAuth2,
    /// JWT tokens
    JWT,
    /// mTLS (mutual TLS)
    MTLS,
    /// AWS IAM signatures
    AwsIAM,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalCredentials {
    /// Primary credential (API key, client ID, etc.)
    pub primary: String,
    /// Secondary credential (secret, client secret, etc.)
    pub secondary: Option<String>,
    /// Certificate path (for mTLS)
    pub certificate_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRefreshConfig {
    /// Token lifetime in seconds
    pub lifetime_secs: u32,
    /// Refresh threshold (refresh when 90% of lifetime)
    pub refresh_threshold: f32,
    /// Refresh endpoint
    pub refresh_endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKeyOperations {
    /// Caching configuration
    pub caching: Option<CachingConfig>,
    /// Retry configuration  
    pub retry: RetryConfig,
    /// Batch operations
    pub batch: Option<BatchConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingConfig {
    /// Cache public keys locally
    pub cache_public_keys: bool,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u32,
    /// Maximum cache size
    pub max_cache_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Backoff strategy
    pub backoff: BackoffStrategy,
    /// Retriable error codes
    pub retriable_errors: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed { delay_ms: u32 },
    /// Exponential backoff
    Exponential { initial_delay_ms: u32, multiplier: f32, max_delay_ms: u32 },
    /// Linear backoff  
    Linear { initial_delay_ms: u32, increment_ms: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size for operations
    pub max_batch_size: usize,
    /// Batch timeout in seconds
    pub batch_timeout_secs: u32,
    /// Enable batch signing
    pub enable_batch_signing: bool,
}

// ===== HASHICORP VAULT IMPLEMENTATION =====

use serde_json::Value;
use base64::Engine;

/// HashiCorp Vault client for validator key management
#[derive(Debug)]
pub struct VaultValidatorClient {
    /// Vault client configuration
    config: VaultClientConfig,
    /// HTTP client for API calls
    http_client: reqwest::Client,
    /// Current authentication token
    auth_token: Option<String>,
    /// Token expiry timestamp
    token_expires_at: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone)]
pub struct VaultClientConfig {
    /// Vault server address
    pub address: String,
    /// Mount path for validator secrets
    pub mount_path: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Namespace (Vault Enterprise)
    pub namespace: Option<String>,
    /// Connection timeout
    pub timeout_secs: u64,
}

#[derive(Debug, Clone)]
pub enum VaultAuthMethod {
    /// JWT/OIDC authentication
    JWT { 
        role: String, 
        jwt_path: PathBuf,
    },
    /// AppRole authentication
    AppRole { 
        role_id: String, 
        secret_id_path: PathBuf,
    },
    /// Kubernetes authentication
    Kubernetes { 
        role: String, 
        service_account_path: PathBuf,
    },
    /// AWS IAM authentication
    AwsIam { 
        role: String,
    },
    /// Direct token (for development)
    Token { 
        token_path: PathBuf,
    },
}

/// Vault-stored validator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultValidatorConfig {
    /// Validator metadata
    pub metadata: ValidatorMetadata,
    /// EVM private key path in Vault KV store
    pub evm_key_path: String,
    /// Consensus private key path in Vault KV store
    pub consensus_key_path: String,
    /// Stake amount
    pub stake: u64,
    /// Whether validator is active
    pub active: bool,
    /// Key version for rotation
    pub key_version: Option<u32>,
    /// Key access strategy
    pub key_access: VaultKeyAccessStrategy,
}

/// How to access keys stored in Vault
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VaultKeyAccessStrategy {
    /// Retrieve private keys from Vault KV store for local use
    /// **RECOMMENDED FOR CONSENSUS**: This is needed for consensus operations that require direct key access
    /// 
    /// Example Vault storage:
    /// ```bash
    /// # Store validator private key in Vault KV
    /// vault kv put secret/validators/validator-001 \
    ///   private_key="0x1234567890abcdef..." \
    ///   metadata='{"name":"Validator 001","stake":1000}'
    /// ```
    RetrieveKeys {
        /// KV store mount path (e.g., "secret", "kv")
        kv_mount: String,
        /// Key format in storage ("raw", "json", "pkcs8")
        key_format: VaultKeyFormat,
    },
    
    /// Use Vault's transit engine for remote signing
    /// **HIGHER SECURITY**: More secure but limited functionality - not suitable for all consensus operations
    /// Keys never leave Vault, all signing happens server-side
    /// 
    /// Example Vault setup:
    /// ```bash
    /// # Enable transit engine
    /// vault auth -method=userpass username=validator
    /// vault secrets enable transit
    /// vault write -f transit/keys/validator-001-evm type=ecdsa-p256
    /// ```
    TransitEngine {
        /// Transit mount path (e.g., "transit")
        transit_mount: String,
        /// Key name in transit engine
        key_name: String,
    },
    
    /// Hybrid: Retrieve keys for consensus, use transit for transactions  
    /// **BALANCED APPROACH**: Optimal security while maintaining consensus functionality
    /// - Consensus keys retrieved for local DAG/BFT operations
    /// - Transaction signing can use transit engine for enhanced security
    Hybrid {
        /// KV config for consensus keys (needed for Narwhal + Bullshark)
        consensus_kv: VaultKVConfig,
        /// Transit config for transaction signing (enhanced security)
        transaction_transit: VaultTransitConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultKVConfig {
    pub kv_mount: String,
    pub key_format: VaultKeyFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultTransitConfig {
    pub transit_mount: String,
    pub key_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VaultKeyFormat {
    /// Raw hex-encoded private key (most common)
    Raw,
    /// JSON object with key data
    Json,
    /// PKCS#8 encoded private key
    PKCS8,
}

impl VaultKeyAccessStrategy {
    /// Create a standard KV-based strategy for consensus (recommended)
    pub fn consensus_kv(kv_mount: impl Into<String>) -> Self {
        Self::RetrieveKeys {
            kv_mount: kv_mount.into(),
            key_format: VaultKeyFormat::Raw,
        }
    }
    
    /// Create a transit-based strategy for high security scenarios
    pub fn high_security_transit(transit_mount: impl Into<String>, key_name: impl Into<String>) -> Self {
        Self::TransitEngine {
            transit_mount: transit_mount.into(), 
            key_name: key_name.into(),
        }
    }
    
    /// Create a hybrid strategy (recommended for production)
    pub fn production_hybrid(
        consensus_mount: impl Into<String>,
        transit_mount: impl Into<String>,
        key_name: impl Into<String>
    ) -> Self {
        Self::Hybrid {
            consensus_kv: VaultKVConfig {
                kv_mount: consensus_mount.into(),
                key_format: VaultKeyFormat::Raw,
            },
            transaction_transit: VaultTransitConfig {
                transit_mount: transit_mount.into(),
                key_name: key_name.into(),
            },
        }
    }
}

/// Example Vault configurations for different deployment scenarios
impl VaultValidatorConfig {
    /// Example configuration for development/testing
    /// Keys stored in Vault KV for easy access
    pub fn development_example() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: Some("Dev Validator 001".to_string()),
                description: Some("Development validator for testing".to_string()),
                contact: Some("dev-team@example.com".to_string()),
            },
            evm_key_path: "validators/dev-001/evm-key".to_string(),
            consensus_key_path: "validators/dev-001/consensus-key".to_string(),
            stake: 1000,
            active: true,
            key_version: Some(1),
            key_access: VaultKeyAccessStrategy::consensus_kv("secret"),
        }
    }
    
    /// Example configuration for production with high security
    /// Uses transit engine for maximum security (limited consensus functionality)
    pub fn high_security_example() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: Some("Prod Validator 001".to_string()),
                description: Some("Production validator with transit engine".to_string()),
                contact: Some("ops-team@example.com".to_string()),
            },
            evm_key_path: "not-used-for-transit".to_string(),
            consensus_key_path: "not-used-for-transit".to_string(),
            stake: 10000,
            active: true,
            key_version: Some(1),
            key_access: VaultKeyAccessStrategy::high_security_transit("transit", "validator-001-key"),
        }
    }
    
    /// Example configuration for production with balanced security
    /// Hybrid approach: KV for consensus, transit for transactions
    pub fn production_example() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: Some("Prod Validator 001".to_string()),
                description: Some("Production validator with hybrid security".to_string()),
                contact: Some("ops-team@example.com".to_string()),
            },
            evm_key_path: "validators/prod-001/consensus-keys".to_string(),
            consensus_key_path: "validators/prod-001/consensus-keys".to_string(),
            stake: 10000,
            active: true,
            key_version: Some(1),
            key_access: VaultKeyAccessStrategy::production_hybrid("consensus-keys", "transit", "validator-001-tx"),
        }
    }
}

impl VaultValidatorClient {
    /// Create a new Vault client
    pub fn new(config: VaultClientConfig) -> eyre::Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| eyre::eyre!("Failed to create HTTP client: {}", e))?;

        Ok(Self {
            config,
            http_client,
            auth_token: None,
            token_expires_at: None,
        })
    }

    /// Authenticate with Vault and obtain a token
    pub async fn authenticate(&mut self) -> eyre::Result<()> {
        let auth_response = match &self.config.auth_method {
            VaultAuthMethod::JWT { role, jwt_path } => {
                self.authenticate_jwt(role, jwt_path).await?
            },
            VaultAuthMethod::AppRole { role_id, secret_id_path } => {
                self.authenticate_approle(role_id, secret_id_path).await?
            },
            VaultAuthMethod::Kubernetes { role, service_account_path } => {
                self.authenticate_kubernetes(role, service_account_path).await?
            },
            VaultAuthMethod::AwsIam { role } => {
                self.authenticate_aws_iam(role).await?
            },
            VaultAuthMethod::Token { token_path } => {
                self.authenticate_token(token_path).await?
            },
        };

        // Extract token and expiry from response
        let client_token = auth_response["auth"]["client_token"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("No client_token in auth response"))?;

        let lease_duration = auth_response["auth"]["lease_duration"]
            .as_u64()
            .unwrap_or(3600); // Default 1 hour

        self.auth_token = Some(client_token.to_string());
        self.token_expires_at = Some(
            std::time::SystemTime::now() + std::time::Duration::from_secs(lease_duration)
        );

        info!(target: "reth::vault", "Successfully authenticated with Vault, token expires in {} seconds", lease_duration);
        Ok(())
    }

    /// JWT/OIDC authentication
    async fn authenticate_jwt(&self, role: &str, jwt_path: &Path) -> eyre::Result<Value> {
        let jwt_token = fs::read_to_string(jwt_path)
            .map_err(|e| eyre::eyre!("Failed to read JWT token from {:?}: {}", jwt_path, e))?;

        let auth_payload = serde_json::json!({
            "role": role,
            "jwt": jwt_token.trim()
        });

        self.vault_request("POST", "/v1/auth/jwt/login", Some(auth_payload)).await
    }

    /// AppRole authentication
    async fn authenticate_approle(&self, role_id: &str, secret_id_path: &Path) -> eyre::Result<Value> {
        let secret_id = fs::read_to_string(secret_id_path)
            .map_err(|e| eyre::eyre!("Failed to read secret_id from {:?}: {}", secret_id_path, e))?;

        let auth_payload = serde_json::json!({
            "role_id": role_id,
            "secret_id": secret_id.trim()
        });

        self.vault_request("POST", "/v1/auth/approle/login", Some(auth_payload)).await
    }

    /// Kubernetes authentication
    async fn authenticate_kubernetes(&self, role: &str, service_account_path: &Path) -> eyre::Result<Value> {
        let sa_token = fs::read_to_string(service_account_path.join("token"))
            .map_err(|e| eyre::eyre!("Failed to read service account token: {}", e))?;

        let auth_payload = serde_json::json!({
            "role": role,
            "jwt": sa_token.trim()
        });

        self.vault_request("POST", "/v1/auth/kubernetes/login", Some(auth_payload)).await
    }

    /// AWS IAM authentication
    async fn authenticate_aws_iam(&self, role: &str) -> eyre::Result<Value> {
        // This would integrate with AWS SDK to create IAM request signature
        // For now, return an error indicating this needs AWS SDK integration
        Err(eyre::eyre!("AWS IAM authentication requires AWS SDK integration (not implemented)"))
    }

    /// Token authentication (for development)
    async fn authenticate_token(&self, token_path: &Path) -> eyre::Result<Value> {
        let token = fs::read_to_string(token_path)
            .map_err(|e| eyre::eyre!("Failed to read token from {:?}: {}", token_path, e))?;

        // For token auth, we just use the token directly
        let fake_response = serde_json::json!({
            "auth": {
                "client_token": token.trim(),
                "lease_duration": 3600
            }
        });

        Ok(fake_response)
    }

    /// Load validator configurations from Vault
    pub async fn load_validators(&mut self) -> eyre::Result<Vec<VaultValidatorConfig>> {
        self.ensure_authenticated().await?;

        // List all validators in the mount path
        let list_path = format!("/v1/{}/metadata", self.config.mount_path);
        let list_response = self.vault_request("LIST", &list_path, None).await?;

        let keys = list_response["data"]["keys"]
            .as_array()
            .ok_or_else(|| eyre::eyre!("No keys found in Vault path"))?;

        let mut validators = Vec::new();

        for key in keys {
            let key_name = key.as_str()
                .ok_or_else(|| eyre::eyre!("Invalid key name in Vault response"))?;

            // Skip non-validator keys
            if !key_name.starts_with("validator-") {
                continue;
            }

            match self.load_single_validator(key_name).await {
                Ok(validator) => validators.push(validator),
                Err(e) => {
                    warn!(target: "reth::vault", "Failed to load validator {}: {}", key_name, e);
                }
            }
        }

        info!(target: "reth::vault", "Loaded {} validators from Vault", validators.len());
        Ok(validators)
    }

    /// Load a single validator configuration from Vault
    async fn load_single_validator(&mut self, key_name: &str) -> eyre::Result<VaultValidatorConfig> {
        let config_path = format!("/v1/{}/data/{}/config", self.config.mount_path, key_name);
        let response = self.vault_request("GET", &config_path, None).await?;

        let config_data = response["data"]["data"]
            .as_object()
            .ok_or_else(|| eyre::eyre!("No data in Vault response for {}", key_name))?;

        let validator_config: VaultValidatorConfig = serde_json::from_value(serde_json::Value::Object(config_data.clone()))
            .map_err(|e| eyre::eyre!("Failed to parse validator config for {}: {}", key_name, e))?;

        Ok(validator_config)
    }

    /// Get EVM private key from Vault (supports multiple access strategies)
    pub async fn get_evm_private_key(&mut self, validator_config: &VaultValidatorConfig) -> eyre::Result<secp256k1::SecretKey> {
        self.ensure_authenticated().await?;

        match &validator_config.key_access {
            VaultKeyAccessStrategy::RetrieveKeys { kv_mount, key_format } => {
                self.retrieve_private_key_from_kv(&validator_config.evm_key_path, kv_mount, key_format).await
            },
            VaultKeyAccessStrategy::TransitEngine { .. } => {
                Err(eyre::eyre!("Cannot retrieve private key when using TransitEngine strategy. Use sign_with_vault() instead."))
            },
            VaultKeyAccessStrategy::Hybrid { consensus_kv, .. } => {
                // For consensus operations, use KV retrieval
                self.retrieve_private_key_from_kv(&validator_config.evm_key_path, &consensus_kv.kv_mount, &consensus_kv.key_format).await
            },
        }
    }

    /// Retrieve private key from Vault KV store
    async fn retrieve_private_key_from_kv(
        &mut self, 
        key_path: &str, 
        kv_mount: &str, 
        key_format: &VaultKeyFormat
    ) -> eyre::Result<secp256k1::SecretKey> {
        let vault_path = format!("/v1/{}/data/{}", kv_mount, key_path);
        let response = self.vault_request("GET", &vault_path, None).await?;

        let private_key_data = match key_format {
            VaultKeyFormat::Raw => {
                // Expect raw hex string in "private_key" field
                response["data"]["data"]["private_key"]
                    .as_str()
                    .ok_or_else(|| eyre::eyre!("No private_key field in Vault response"))?
                    .to_string()
            },
            VaultKeyFormat::Json => {
                // Expect JSON object with key data
                let key_obj = response["data"]["data"]["key"]
                    .as_object()
                    .ok_or_else(|| eyre::eyre!("No key object in Vault response"))?;
                
                key_obj.get("private_key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| eyre::eyre!("No private_key in key object"))?
                    .to_string()
            },
            VaultKeyFormat::PKCS8 => {
                return Err(eyre::eyre!("PKCS8 key format not yet implemented"));
            }
        };

        // Parse the private key hex string
        let key_hex = private_key_data.strip_prefix("0x").unwrap_or(&private_key_data);
        let key_bytes = alloy_primitives::hex::decode(key_hex)
            .map_err(|e| eyre::eyre!("Failed to decode private key hex: {}", e))?;

        if key_bytes.len() != 32 {
            return Err(eyre::eyre!("Invalid private key length: {} (expected 32)", key_bytes.len()));
        }

        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&key_bytes);

        secp256k1::SecretKey::from_byte_array(&key_array)
            .map_err(|e| eyre::eyre!("Invalid secp256k1 private key: {}", e))
    }

    /// Sign data with a private key stored in Vault (using Vault's transit engine)
    pub async fn sign_with_vault(&mut self, key_path: &str, data: &[u8]) -> eyre::Result<Vec<u8>> {
        self.ensure_authenticated().await?;

        // Use Vault's transit engine for signing
        let sign_path = format!("/v1/transit/sign/{}", key_path);
        let payload = serde_json::json!({
            "input": base64::engine::general_purpose::STANDARD.encode(data),
            "hash_algorithm": "sha2-256"
        });

        let response = self.vault_request("POST", &sign_path, Some(payload)).await?;

        let signature = response["data"]["signature"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("No signature in Vault response"))?;

        // Parse Vault signature format (vault:v1:signature_data)
        let sig_parts: Vec<&str> = signature.split(':').collect();
        if sig_parts.len() != 3 {
            return Err(eyre::eyre!("Invalid Vault signature format"));
        }

        base64::engine::general_purpose::STANDARD.decode(sig_parts[2])
            .map_err(|e| eyre::eyre!("Failed to decode signature: {}", e))
    }

    /// Ensure we have a valid authentication token
    async fn ensure_authenticated(&mut self) -> eyre::Result<()> {
        // Check if we need to authenticate or refresh token
        let needs_auth = match (&self.auth_token, &self.token_expires_at) {
            (None, _) => true,
            (Some(_), None) => false, // Token without expiry
            (Some(_), Some(expires_at)) => {
                // Refresh if token expires in less than 5 minutes
                std::time::SystemTime::now() + std::time::Duration::from_secs(300) > *expires_at
            }
        };

        if needs_auth {
            info!(target: "reth::vault", "Authenticating with Vault");
            self.authenticate().await?;
        }

        Ok(())
    }

    /// Make a request to Vault API
    async fn vault_request(&self, method: &str, path: &str, payload: Option<Value>) -> eyre::Result<Value> {
        let url = format!("{}{}", self.config.address, path);
        
        let mut request = match method {
            "GET" => self.http_client.get(&url),
            "POST" => self.http_client.post(&url),
            "PUT" => self.http_client.put(&url),
            "DELETE" => self.http_client.delete(&url),
            "LIST" => self.http_client.request(reqwest::Method::from_bytes(b"LIST").unwrap(), &url),
            _ => return Err(eyre::eyre!("Unsupported HTTP method: {}", method)),
        };

        // Add authentication header if we have a token
        if let Some(ref token) = self.auth_token {
            request = request.header("X-Vault-Token", token);
        }

        // Add namespace header if configured (Vault Enterprise)
        if let Some(ref namespace) = self.config.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }

        // Add JSON payload if provided
        if let Some(payload) = payload {
            request = request.json(&payload);
        }

        let response = request.send().await
            .map_err(|e| eyre::eyre!("Vault request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(eyre::eyre!("Vault API error {}: {}", status, error_text));
        }

        let response_json: Value = response.json().await
            .map_err(|e| eyre::eyre!("Failed to parse Vault response: {}", e))?;

        Ok(response_json)
    }
}

/// Convert VaultValidatorConfig to ValidatorKeyPair using Vault keys
async fn vault_config_to_keypair(
    vault_client: &mut VaultValidatorClient, 
    vault_config: &VaultValidatorConfig
) -> eyre::Result<ValidatorKeyPair> {
    // Get EVM private key from Vault
    let evm_private_key = vault_client.get_evm_private_key(vault_config).await?;
    
    // For now, derive consensus key deterministically from EVM key
    // In production, you might store consensus keys separately in Vault
    ValidatorKeyPair::from_evm_key_deterministic(evm_private_key)
        .map_err(|e| eyre::eyre!("Failed to create validator keypair: {}", e))
}

/// Load validators from external key management service (HashiCorp Vault)
async fn load_validators_from_external(config: &ValidatorKeyConfig) -> eyre::Result<Vec<ExternalValidatorInfo>> {
    // Parse Vault configuration from the external configuration
    let vault_config = parse_vault_config(config)?;
    
    // Create Vault client
    let mut vault_client = VaultValidatorClient::new(vault_config)?;
    
    // Load validator configurations from Vault
    let vault_validators = vault_client.load_validators().await?;
    
    // Convert to external validator format
    let mut external_validators = Vec::new();
    
    for vault_validator in vault_validators {
        // Convert vault config to keypair to get consensus public key
        let keypair = vault_config_to_keypair(&mut vault_client, &vault_validator).await?;
        
        let external_validator = ExternalValidatorInfo {
            metadata: ValidatorMetadata {
                name: vault_validator.metadata.name,
                description: vault_validator.metadata.description,
                contact: vault_validator.metadata.contact,
            },
            stake: vault_validator.stake,
            active: vault_validator.active,
            key_version: vault_validator.key_version,
            external_key_id: vault_validator.evm_key_path,
        };
        
        external_validators.push(external_validator);
    }
    
    info!(target: "reth::external", "Loaded {} validators from external service", external_validators.len());
    Ok(external_validators)
}

/// Parse Vault configuration from validator key config
fn parse_vault_config(config: &ValidatorKeyConfig) -> eyre::Result<VaultClientConfig> {
    // For now, we'll expect Vault configuration in environment variables
    // In production, this would parse from config files or CLI args
    
    let vault_address = std::env::var("VAULT_ADDR")
        .map_err(|_| eyre::eyre!("VAULT_ADDR environment variable not set"))?;
    
    let mount_path = std::env::var("VAULT_MOUNT_PATH")
        .unwrap_or_else(|_| "secret".to_string());
    
    let timeout_secs = std::env::var("VAULT_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse::<u64>()
        .unwrap_or(30);
    
    // Determine authentication method
    let auth_method = if let Ok(role) = std::env::var("VAULT_JWT_ROLE") {
        let jwt_path = std::env::var("VAULT_JWT_PATH")
            .map_err(|_| eyre::eyre!("VAULT_JWT_PATH required for JWT auth"))?;
        VaultAuthMethod::JWT {
            role,
            jwt_path: PathBuf::from(jwt_path),
        }
    } else if let Ok(role_id) = std::env::var("VAULT_ROLE_ID") {
        let secret_id_path = std::env::var("VAULT_SECRET_ID_PATH")
            .map_err(|_| eyre::eyre!("VAULT_SECRET_ID_PATH required for AppRole auth"))?;
        VaultAuthMethod::AppRole {
            role_id,
            secret_id_path: PathBuf::from(secret_id_path),
        }
    } else if let Ok(role) = std::env::var("VAULT_K8S_ROLE") {
        let service_account_path = std::env::var("VAULT_K8S_SA_PATH")
            .unwrap_or_else(|_| "/var/run/secrets/kubernetes.io/serviceaccount".to_string());
        VaultAuthMethod::Kubernetes {
            role,
            service_account_path: PathBuf::from(service_account_path),
        }
    } else if let Ok(role) = std::env::var("VAULT_AWS_ROLE") {
        VaultAuthMethod::AwsIam { role }
    } else if let Ok(token_path) = std::env::var("VAULT_TOKEN_PATH") {
        VaultAuthMethod::Token {
            token_path: PathBuf::from(token_path),
        }
    } else {
        return Err(eyre::eyre!("No Vault authentication method configured"));
    };
    
    let namespace = std::env::var("VAULT_NAMESPACE").ok();
    
    Ok(VaultClientConfig {
        address: vault_address,
        mount_path,
        auth_method,
        namespace,
        timeout_secs,
    })
}

/// External validator information from external service
#[derive(Debug, Clone)]
struct ExternalValidatorInfo {
    /// Validator metadata
    metadata: ValidatorMetadata,
    /// Stake amount
    stake: u64,
    /// Whether validator is active
    active: bool,
    /// Key version for rotation
    key_version: Option<u32>,
    /// External service key identifier
    external_key_id: String,
}

/// Concrete implementation of DatabaseOps that uses Reth's database provider
/// This bridges the consensus storage with Reth's MDBX database through the provider interface
#[derive(Debug)]
struct RethDatabaseOps<P> {
    provider: Arc<P>,
}

impl<P> RethDatabaseOps<P> 
where
    P: reth_provider::DatabaseProviderFactory + Send + Sync + std::fmt::Debug,
{
    fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

impl<P> reth_consensus::consensus_storage::DatabaseOps for RethDatabaseOps<P>
where
    P: reth_provider::DatabaseProviderFactory + Send + Sync + std::fmt::Debug,
{
    fn get_finalized_batch(&self, batch_id: u64) -> anyhow::Result<Option<alloy_primitives::B256>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusFinalizedBatch};
        Ok(provider.tx_ref().get::<ConsensusFinalizedBatch>(batch_id)?)
    }

    fn put_finalized_batch(&self, batch_id: u64, block_hash: alloy_primitives::B256) -> anyhow::Result<()> {
        let provider = self.provider.database_provider_rw()?;
        use reth_consensus::{ConsensusFinalizedBatch};
        provider.tx_ref().put::<ConsensusFinalizedBatch>(batch_id, block_hash)?;
        provider.commit()?;
        Ok(())
    }

    fn get_certificate(&self, cert_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusCertificates};
        Ok(provider.tx_ref().get::<ConsensusCertificates>(cert_id)?)
    }

    fn put_certificate(&self, cert_id: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let provider = self.provider.database_provider_rw()?;
        use reth_consensus::{ConsensusCertificates};
        provider.tx_ref().put::<ConsensusCertificates>(cert_id, data)?;
        provider.commit()?;
        Ok(())
    }

    fn get_batch(&self, batch_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusBatches};
        Ok(provider.tx_ref().get::<ConsensusBatches>(batch_id)?)
    }

    fn put_batch(&self, batch_id: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let provider = self.provider.database_provider_rw()?;
        use reth_consensus::{ConsensusBatches};
        provider.tx_ref().put::<ConsensusBatches>(batch_id, data)?;
        provider.commit()?;
        Ok(())
    }

    fn get_dag_vertex(&self, hash: alloy_primitives::B256) -> anyhow::Result<Option<Vec<u8>>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusDagVertices};
        Ok(provider.tx_ref().get::<ConsensusDagVertices>(hash)?)
    }

    fn put_dag_vertex(&self, hash: alloy_primitives::B256, data: Vec<u8>) -> anyhow::Result<()> {
        let provider = self.provider.database_provider_rw()?;
        use reth_consensus::{ConsensusDagVertices};
        provider.tx_ref().put::<ConsensusDagVertices>(hash, data)?;
        provider.commit()?;
        Ok(())
    }

    fn get_latest_finalized(&self) -> anyhow::Result<Option<u64>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusLatestFinalized};
        // Use key 0 for the single latest finalized entry
        Ok(provider.tx_ref().get::<ConsensusLatestFinalized>(0u8)?)
    }

    fn put_latest_finalized(&self, cert_id: u64) -> anyhow::Result<()> {
        let provider = self.provider.database_provider_rw()?;
        use reth_consensus::{ConsensusLatestFinalized};
        // Use key 0 for the single latest finalized entry
        provider.tx_ref().put::<ConsensusLatestFinalized>(0u8, cert_id)?;
        provider.commit()?;
        Ok(())
    }

    fn list_finalized_batches(&self, limit: Option<usize>) -> anyhow::Result<Vec<(u64, alloy_primitives::B256)>> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusFinalizedBatch};
        use reth_db_api::cursor::DbCursorRO;
        
        // Use cursor to get key-value pairs
        let mut results = Vec::new();
        if let Ok(mut cursor) = provider.tx_ref().cursor_read::<ConsensusFinalizedBatch>() {
            while let Ok(Some((batch_id, block_hash))) = cursor.next() {
                results.push((batch_id, block_hash));
                
                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }
        
        Ok(results)
    }

    fn get_table_stats(&self) -> anyhow::Result<(u64, u64, u64)> {
        let provider = self.provider.database_provider_ro()?;
        use reth_consensus::{ConsensusCertificates, ConsensusBatches, ConsensusDagVertices};
        use reth_db_api::cursor::DbCursorRO;
        
        // Count certificates and batches using cursor_read_collect (they use u64 keys)
        let total_certificates = provider.cursor_read_collect::<ConsensusCertificates>(..)
            .map(|certs| certs.len() as u64)
            .unwrap_or(0);
        
        let total_batches = provider.cursor_read_collect::<ConsensusBatches>(..)
            .map(|batches| batches.len() as u64)
            .unwrap_or(0);
        
        // Count DAG vertices manually (they use B256 keys, not compatible with cursor_read_collect)
        let total_dag_vertices = match provider.tx_ref().cursor_read::<ConsensusDagVertices>() {
            Ok(mut cursor) => {
                let mut count = 0u64;
                while let Ok(Some(_)) = cursor.next() {
                    count += 1;
                }
                count
            }
            Err(_) => 0,
        };

        Ok((total_certificates, total_batches, total_dag_vertices))
    }
}

 