//! Narwhal + Bullshark consensus installation - redirects to the new implementation
//! that avoids circular dependencies by being in the examples directory

use example_narwhal_bullshark_consensus::{
    ConsensusConfig, NarwhalBullsharkEngine,
    ValidatorKeyPair, ValidatorRegistry, FinalizedBatch,
    node_integration_v2::NodeIntegrationV2,
};
use reth_provider::{DatabaseProviderFactory, StateProviderFactory, BlockReaderIdExt, HeaderProvider};
use reth_node_api::BeaconConsensusEngineHandle;
use reth_transaction_pool::TransactionPool;
use reth_chainspec::ChainSpec;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes};
use reth_primitives::EthPrimitives;
use reth_tasks::TaskSpawner;
use reth_node_core::args::NarwhalBullsharkArgs;
use reth_rpc_builder::RpcModuleBuilder;
use alloy_primitives::Address;
use reth_primitives::TransactionSigned;
use reth_node_ethereum::EthEngineTypes;
use std::sync::Arc;
use tracing::{info, error, warn};
use eyre::Result;

/// Check if Narwhal consensus should be used
pub fn should_use_narwhal_consensus(args: &NarwhalBullsharkArgs) -> bool {
    args.narwhal_enabled
}

/// Get consensus mode description
pub fn consensus_mode_description(args: &NarwhalBullsharkArgs) -> &'static str {
    if args.narwhal_enabled {
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

/// Get transaction pool statistics
pub fn get_pool_stats<Pool>(pool: &Pool) -> PoolStats 
where
    Pool: TransactionPool,
{
    // Get pool stats
    let pending = pool.pending_transactions().len();
    let queued = 0; // TODO: Get queued count  
    let total = pending + queued;
    
    PoolStats {
        pending_count: pending,
        queued_count: queued,
        total_count: total,
    }
}

/// Initialize Narwhal consensus
pub async fn initialize_narwhal_consensus<Pool, Provider, EvmConfig, Executor>(
    args: NarwhalBullsharkArgs,
    chain_spec: Arc<ChainSpec>,
    provider: Provider,
    pool: Pool,
    evm_config: EvmConfig,
    executor: Executor,
    engine_handle: BeaconConsensusEngineHandle<EthEngineTypes>,
) -> Result<()>
where
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    Provider: DatabaseProviderFactory + StateProviderFactory + BlockReaderIdExt + reth_provider::HeaderProvider<Header = alloy_consensus::Header> + Clone + Send + Sync + 'static + std::fmt::Debug,
    EvmConfig: ConfigureEvm<NextBlockEnvCtx = reth_evm::NextBlockEnvAttributes, Primitives = reth_primitives::EthPrimitives> + Clone + Send + Sync + 'static,
    Executor: TaskSpawner + Clone + 'static,
{
    info!("Initializing Narwhal + Bullshark consensus");

    // Load validator key
    let validator_key = if let Some(key_file) = args.validator_key_file {
        ValidatorKeyPair::from_file(&key_file.to_string_lossy())
            .map_err(|e| eyre::eyre!("Failed to load validator key: {}", e))?
    } else {
        warn!("No validator key file specified, running in non-validator mode");
        return Ok(());
    };

    // Create consensus config
    let consensus_datadir = args.consensus_datadir
        .clone()
        .unwrap_or_else(|| {
            // Default to a 'consensus' subdirectory under the current working directory
            // In production, this should be set explicitly via --consensus-datadir
            std::path::PathBuf::from("consensus_data")
        });
    
    // Ensure the consensus data directory exists
    std::fs::create_dir_all(&consensus_datadir)
        .map_err(|e| eyre::eyre!("Failed to create consensus data directory: {}", e))?;
    
    let config = ConsensusConfig {
        network_addr: args.network_address,
        validator_key_file: validator_key.name.clone(),
        validator_config_dir: args.validator_config_dir
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("validators"))
            .to_string_lossy()
            .to_string(),
        committee_config_file: args.committee_config_file
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        consensus_datadir,
        max_batch_delay_ms: args.max_batch_delay_ms,
        node_public_key: validator_key.public_key()
            .map_err(|e| eyre::eyre!("Failed to get public key: {}", e))?,
        consensus_private_key_bytes: validator_key.private_key_bytes()
            .map_err(|e| eyre::eyre!("Failed to get private key bytes: {}", e))?,
        max_batch_size: args.max_batch_size,
        min_block_time_ms: args.min_block_time_ms,
        consensus_rpc_port: args.consensus_rpc_port,
        consensus_port: args.network_address.port(),
        enable_admin_api: args.consensus_rpc_enable_admin,
    };

    // Create the node integration (V2 with Reth payload builder)
    let node_integration = NodeIntegrationV2::new(
        chain_spec.clone(),
        provider.clone(),
        pool,
        evm_config,
        engine_handle,
        validator_key,
        config,
    );
    
    // Check if we should use real consensus
    let use_real_consensus = std::env::var("USE_REAL_CONSENSUS")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
        
    if use_real_consensus {
        info!("Starting REAL Narwhal+Bullshark consensus");
    } else {
        info!("Starting MOCK consensus for testing (set USE_REAL_CONSENSUS=true for real consensus)");
    }
    
    let _handle = executor.spawn_critical("narwhal-consensus", Box::pin(async move {
        if let Err(e) = node_integration.run().await {
            error!("Consensus integration failed: {}", e);
        }
    }));

    Ok(())
}

/// Create consensus engine
pub fn create_consensus_engine(
    chain_spec: Arc<ChainSpec>,
) -> NarwhalBullsharkEngine {
    NarwhalBullsharkEngine::new(chain_spec.chain.id())
}

/// Start consensus service
pub async fn start_consensus_service<Pool, Provider, EvmConfig, Executor>(
    config: ConsensusConfig,
    chain_spec: Arc<ChainSpec>,
    provider: Provider,
    pool: Pool,
    evm_config: EvmConfig,
    _executor: Executor,
    _engine: BeaconConsensusEngineHandle<EthEngineTypes>,
) -> Result<()>
where
    Pool: TransactionPool + Clone + 'static,
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
    Executor: TaskSpawner + Clone + 'static,
{
    info!("Starting Narwhal + Bullshark consensus service");
    
    // For now, just log that we would start the service
    info!("Consensus service would start with config: {:?}", config);
    info!("Note: Full consensus service requires 'full' feature to be enabled");
    
    Ok(())
}

/// Setup RPC endpoints
pub fn setup_rpc_endpoints<N, Provider, Pool, Network, EvmConfig, Consensus>(
    _module_builder: &mut RpcModuleBuilder<N, Provider, Pool, Network, EvmConfig, Consensus>,
) -> Result<()>
where
    N: std::fmt::Debug,
    Pool: Clone + 'static,
    Provider: Clone + 'static,
    Network: Clone + 'static,
    EvmConfig: Clone + 'static,
    Consensus: Clone + 'static,
{
    info!("Setting up Narwhal + Bullshark RPC endpoints");
    
    // TODO: Add consensus-specific RPC methods when full feature is enabled
    
    Ok(())
}

/// Start standalone RPC server
pub async fn start_standalone_rpc(addr: std::net::SocketAddr) -> Result<()> {
    info!("Starting standalone Narwhal + Bullshark RPC server on {}", addr);
    
    let server = jsonrpsee::server::ServerBuilder::default()
        .build(addr)
        .await?;
    
    let _handle = server.start(jsonrpsee::RpcModule::new(()));
    
    info!("RPC server started successfully");
    
    // Keep server running
    std::future::pending::<()>().await;
    
    Ok(())
}