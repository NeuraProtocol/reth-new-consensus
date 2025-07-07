//! Narwhal + Bullshark consensus installation - redirects to the new implementation
//! that avoids circular dependencies by being in the examples directory

use example_narwhal_bullshark_consensus::{
    NarwhalRethIntegration, ConsensusConfig, NarwhalBullsharkEngine,
    ValidatorKeyPair, ValidatorRegistry, MempoolBridge, FinalizedBatch,
    service::Service as NarwhalBullsharkService,
};
use reth_provider::{
    providers::BlockchainProvider,
    DatabaseProviderFactory, DBProvider,
};
use reth_node_api::{BeaconConsensusEngineHandle, BeaconConsensusEngineEvent};
use reth_transaction_pool::{TransactionPool, TransactionPoolExt, EthPooledTransaction};
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_tasks::TaskExecutor;
use reth_node_core::args::NarwhalBullsharkArgs;
use reth_network::NetworkProtocols;
use reth_network_api::FullNetwork;
use reth_consensus::consensus_storage::MdbxConsensusStorage;
use reth_tokio_util::EventStream;
use reth_rpc_builder::RpcModuleBuilder;
use alloy_primitives::{Address, B256};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, error, warn};
use anyhow::Result;
use jsonrpsee::server::ServerBuilder;

/// Check if Narwhal consensus should be used
pub fn should_use_narwhal_consensus(args: &NarwhalBullsharkArgs) -> bool {
    args.enable
}

/// Get consensus mode description
pub fn consensus_mode_description(args: &NarwhalBullsharkArgs) -> &'static str {
    if args.enable {
        "Narwhal + Bullshark BFT consensus"
    } else {
        "Standard Ethereum consensus"
    }
}

/// Pool statistics wrapper
pub struct PoolStats {
    pub pending_count: usize,
    pub queued_count: usize,
    pub total_count: usize,
}

/// Install Narwhal+Bullshark consensus (main entry point)
pub fn install_narwhal_bullshark_consensus<Provider, EvmConfig, Net, Executor>(
    args: NarwhalBullsharkArgs,
    provider: Provider,
    evm_config: EvmConfig,
    network: Net,
    executor: Executor,
) -> (NarwhalRethBridge, ValidatorRegistry, Arc<MdbxConsensusStorage>)
where
    Provider: BlockchainProvider + DatabaseProviderFactory<DB = reth_db::DatabaseEnv> + Clone + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
    Net: FullNetwork + Clone + 'static,
    Executor: TaskExecutor + Clone + 'static,
{
    info!("Installing Narwhal+Bullshark consensus with new architecture");

    // Create consensus configuration
    let consensus_config = ConsensusConfig {
        network_addr: args.network_addr,
        peer_addresses: args.peers.clone(),
        validator_key_file: args.validator_key_file.clone(),
        validator_config_dir: args.validator_config_dir.clone(),
        max_batch_delay_ms: args.max_batch_delay_ms,
        max_batch_size: args.max_batch_size,
        min_block_time_ms: args.min_block_time_ms,
        consensus_rpc_port: args.consensus_rpc_port,
        enable_admin_api: args.consensus_rpc_enable_admin,
    };

    // Load validator registry
    let validator_registry = ValidatorRegistry::from_directory(&consensus_config.validator_config_dir)
        .expect("Failed to load validator registry");

    // Create storage
    let storage = Arc::new(MdbxConsensusStorage::new(provider.database()));

    // Create the bridge (simplified wrapper)
    let bridge = NarwhalRethBridge {
        config: consensus_config,
        registry: validator_registry.clone(),
        storage: storage.clone(),
        // Other fields would be initialized here
    };

    (bridge, validator_registry, storage)
}

/// Setup mempool integration with optional engine
pub async fn setup_mempool_integration_with_optional_engine<Pool, Provider, EvmConfig, Executor>(
    pool: Arc<Pool>,
    provider: Provider,
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
    executor: Executor,
    mut bridge: NarwhalRethBridge,
    engine_handle: Option<BeaconConsensusEngineHandle>,
) -> Result<()>
where
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + 'static,
    Provider: BlockchainProvider + Clone + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
    Executor: TaskExecutor + Clone + 'static,
{
    if let Some(engine) = engine_handle {
        info!("Setting up mempool integration with engine API");
        
        // Create the integration
        let integration = NarwhalRethIntegration::new(
            chain_spec,
            provider,
            pool.clone(),
            evm_config,
            engine,
        );

        // Start the consensus service
        executor.spawn_critical("narwhal-bullshark", Box::pin(async move {
            if let Err(e) = integration.start(bridge.config).await {
                error!("Consensus service failed: {}", e);
            }
        }));
    } else {
        warn!("No engine handle available - running in standalone mode");
    }

    Ok(())
}

/// Setup mempool integration (wrapper for compatibility)
pub async fn setup_mempool_integration<Pool, Provider, EvmConfig, Executor>(
    pool: Arc<Pool>,
    provider: Provider,
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
    executor: Executor,
    bridge: NarwhalRethBridge,
    events: EventStream<BeaconConsensusEngineEvent>,
    engine: BeaconConsensusEngineHandle,
) -> Result<()>
where
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + 'static,
    Provider: BlockchainProvider + Clone + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
    Executor: TaskExecutor + Clone + 'static,
{
    setup_mempool_integration_with_optional_engine(
        pool,
        provider,
        chain_spec,
        evm_config,
        executor,
        bridge,
        Some(engine),
    ).await
}

/// Install consensus RPC
pub fn install_consensus_rpc<Provider>(
    bridge: &NarwhalRethBridge,
    provider: Provider,
    module_builder: &mut RpcModuleBuilder<Provider>,
) where
    Provider: BlockchainProvider + Clone + 'static,
{
    info!("Installing consensus RPC endpoints");
    // In real implementation, would add RPC methods to the module builder
}

/// Start consensus RPC server
pub async fn start_consensus_rpc_server(
    port: u16,
    enable_admin: bool,
) -> Result<()> {
    info!("Starting consensus RPC server on port {}", port);
    
    let server = ServerBuilder::default()
        .build(format!("127.0.0.1:{}", port))
        .await?;
    
    let _handle = server.start(reth_rpc_builder::RpcModule::new(()))?;
    
    info!("Consensus RPC server started on port {}", port);
    
    // Keep the server running
    std::future::pending::<()>().await;
    
    Ok(())
}

/// Simplified bridge structure for compatibility
pub struct NarwhalRethBridge {
    config: ConsensusConfig,
    registry: ValidatorRegistry,
    storage: Arc<MdbxConsensusStorage>,
}

impl NarwhalRethBridge {
    pub fn is_running(&self) -> bool {
        true // Simplified
    }
    
    pub async fn get_next_block(&mut self) -> Result<Option<reth_primitives::SealedBlock>> {
        // Simplified - would get from actual consensus
        Ok(None)
    }
    
    pub async fn update_chain_state(&mut self, _block_number: u64, _block_hash: B256) {
        // Simplified
    }
    
    pub fn set_mempool_bridge(&mut self, _bridge: MempoolBridge<impl TransactionPool>) {
        // Simplified
    }
}

/// Mempool operations trait
#[async_trait::async_trait]
pub trait MempoolOperations: Send + Sync {
    async fn subscribe_new_transactions(&self) -> Result<mpsc::UnboundedReceiver<reth_primitives::TransactionSigned>>;
    async fn remove_transactions(&self, tx_hashes: &[alloy_primitives::TxHash]) -> Result<()>;
    async fn get_pool_stats(&self) -> Result<PoolStats>;
    async fn contains_transaction(&self, tx_hash: &alloy_primitives::TxHash) -> Result<bool>;
}