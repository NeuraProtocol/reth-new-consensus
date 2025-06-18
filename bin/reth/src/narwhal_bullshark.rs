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
use reth_consensus::narwhal_bullshark::{
    ServiceConfig,
    integration::NarwhalRethBridge,
    types::NarwhalBullsharkConfig,
};
use futures::StreamExt;

use narwhal::types::Committee;
use std::collections::HashMap;
use tracing::*;
use fastcrypto::traits::KeyPair;

/// Install Narwhal + Bullshark consensus if it's enabled.
pub fn install_narwhal_bullshark_consensus<P, E, N>(
    args: NarwhalBullsharkArgs,
    provider: BlockchainProvider<P>,
    evm_config: E,
    network: N,
    task_executor: TaskExecutor,
    engine_events: EventStream<BeaconConsensusEngineEvent<EthPrimitives>>,
) -> eyre::Result<()>
where
    P: ProviderNodeTypes<Primitives = EthPrimitives>,
    E: ConfigureEvm<Primitives = EthPrimitives> + Clone + 'static,
    N: FullNetwork + NetworkProtocols,
{
    info!(target: "reth::cli", "Installing Narwhal + Bullshark consensus");

    // Create test committee for now (TODO: proper committee management)
    let committee = create_test_committee(args.committee_size)?;
    let node_key = committee.authorities.keys().next().unwrap().clone();

    // Create configuration
    let node_config = NarwhalBullsharkConfig {
        node_public_key: node_key.clone(),
        narwhal: args.to_narwhal_config(),
        bullshark: args.to_bullshark_config(),
    };

    let service_config = ServiceConfig::new(node_config, committee);

    // Create and start the Narwhal-Reth bridge
    let mut bridge = NarwhalRethBridge::new(service_config)
        .map_err(|e| eyre::eyre!("Failed to create Narwhal-Reth bridge: {}", e))?;

    // Start the consensus bridge
    task_executor.spawn(async move {
        if let Err(e) = bridge.start() {
            error!(target: "reth::narwhal_bullshark", "Consensus bridge error: {}", e);
        }
    });

    info!(target: "reth::cli", "Narwhal + Bullshark consensus enabled");
    
    // TODO: Monitor consensus health and integrate with Reth's engine events
    task_executor.spawn(async move {
        let mut stream = engine_events;
        while let Some(event) = stream.next().await {
            trace!(target: "reth::narwhal_bullshark", ?event, "Received engine event");
            // TODO: Forward relevant events to consensus system
        }
    });

    Ok(())
}

/// Create a test committee for development/testing
/// TODO: Replace with proper committee management from configuration
fn create_test_committee(size: usize) -> eyre::Result<Committee> {
    if size < 1 {
        return Err(eyre::eyre!("Committee size must be at least 1"));
    }

    let mut authorities = HashMap::new();
    
    for i in 0..size {
        let keypair = fastcrypto::bls12381::BLS12381KeyPair::generate(&mut rand_08::thread_rng());
        let stake = 100; // Equal stake for all validators
        authorities.insert(keypair.public().clone(), stake);
        
        info!(target: "reth::narwhal_bullshark", 
              "Generated validator {} with public key: {:?}", i, keypair.public());
    }
    
    let committee = Committee::new(0, authorities);
    info!(target: "reth::narwhal_bullshark", 
          "Created test committee with {} validators", size);
    
    Ok(committee)
}

/// Check if Narwhal + Bullshark consensus should override standard Ethereum consensus
pub fn should_use_narwhal_consensus(args: &NarwhalBullsharkArgs) -> bool {
    args.enabled
}

/// Get consensus mode description for logging
pub fn consensus_mode_description(args: &NarwhalBullsharkArgs) -> &'static str {
    if args.enabled {
        "Narwhal + Bullshark BFT Consensus"
    } else {
        "Standard Ethereum Consensus"
    }
} 