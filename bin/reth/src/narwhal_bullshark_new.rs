//! Narwhal + Bullshark consensus installation and management
//! 
//! This module uses the new example implementation that avoids circular dependencies

use example_narwhal_bullshark_consensus::{
    NarwhalRethIntegration, ConsensusConfig, NarwhalBullsharkEngine,
    ValidatorKeyPair, ValidatorRegistry, MempoolBridge,
};
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes},
    BlockWriter,
};
use reth_node_api::BeaconConsensusEngineHandle;
use reth_transaction_pool::{TransactionPool, TransactionPoolExt, EthPooledTransaction};
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_tasks::TaskExecutor;
use reth_node_core::args::NarwhalBullsharkArgs;
use std::sync::Arc;
use tracing::{info, error};
use anyhow::Result;

/// Install Narwhal+Bullshark consensus on a Reth node
pub async fn install_narwhal_bullshark_consensus<Provider, Pool, EvmConfig, Executor>(
    args: &NarwhalBullsharkArgs,
    chain_spec: Arc<ChainSpec>,
    provider: Provider,
    pool: Arc<Pool>,
    evm_config: EvmConfig,
    engine_handle: BeaconConsensusEngineHandle,
    executor: Executor,
) -> Result<()>
where
    Provider: BlockchainProvider + Clone + 'static,
    Pool: TransactionPool<Transaction = EthPooledTransaction> + TransactionPoolExt + 'static,
    EvmConfig: ConfigureEvm + Clone + 'static,
    Executor: TaskExecutor + Clone + 'static,
{
    info!("Installing Narwhal+Bullshark consensus");

    // Create consensus configuration from CLI args
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

    // Load validator key
    let validator_key = ValidatorKeyPair::from_file(&consensus_config.validator_key_file)?;
    info!("Loaded validator key: {}", validator_key.evm_address);

    // Create mempool bridge
    let mempool_bridge = MempoolBridge::new(pool.clone());
    let tx_receiver = mempool_bridge.start();
    info!("Started mempool bridge");

    // Create the integration
    let mut integration = NarwhalRethIntegration::new(
        chain_spec.clone(),
        provider,
        pool.clone(),
        evm_config,
        engine_handle,
    );

    // Connect transaction stream from mempool
    // (In real implementation, would connect tx_receiver to consensus workers)

    // Start the consensus service
    executor.spawn_critical("narwhal-bullshark-consensus", Box::pin(async move {
        if let Err(e) = integration.start(consensus_config).await {
            error!("Consensus service failed: {}", e);
        }
    }));

    info!("✅ Narwhal+Bullshark consensus installed and running");

    Ok(())
}

/// Create a custom consensus engine for Narwhal+Bullshark
pub fn create_consensus_engine(chain_id: u64) -> Arc<dyn reth_consensus::Consensus> {
    Arc::new(NarwhalBullsharkEngine::new(chain_id))
}