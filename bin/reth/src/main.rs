#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

use clap::Parser;
use reth::{args::CombinedProtocolArgs, cli::Cli, ress::install_ress_subprotocol};
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_builder::NodeHandle;
use reth_node_ethereum::EthereumNode;
use tracing::{info, warn};
use std::sync::{Arc, RwLock};

mod narwhal_bullshark;
use narwhal_bullshark::{
    install_narwhal_bullshark_consensus, 
    setup_mempool_integration,
    should_use_narwhal_consensus,
    consensus_mode_description,
    install_consensus_rpc,
    start_consensus_rpc_server,
};

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<EthereumChainSpecParser, CombinedProtocolArgs>::parse().run(|builder, combined_args| async move {
            info!(target: "reth::cli", "Launching node");
            
            let NodeHandle { node, node_exit_future } = builder
                .node(EthereumNode::default())
                .launch_with_debug_capabilities()
                .await?;

            // Install ress subprotocol if enabled
            if combined_args.ress.enabled {
                install_ress_subprotocol(
                    combined_args.ress,
                    node.provider.clone(),
                    node.evm_config.clone(),
                    node.network.clone(),
                    node.task_executor.clone(),
                    node.add_ons_handle.engine_events.new_listener(),
                )?;
            }

            // Install Narwhal + Bullshark consensus if enabled
            if should_use_narwhal_consensus(&combined_args.narwhal_bullshark) {
                info!(target: "reth::cli", "Enabling Narwhal + Bullshark consensus instead of standard Ethereum consensus");
                
                // Step 1: Install basic consensus without mempool integration
                let (mut consensus_bridge, validator_registry, storage) = install_narwhal_bullshark_consensus(
                    combined_args.narwhal_bullshark.clone(),
                    node.provider.clone(),
                    node.evm_config.clone(),
                    node.network.clone(),
                    node.task_executor.clone(),
                    node.add_ons_handle.engine_events.new_listener(),
                )?;

                // Step 2: Configure RPC if port is specified
                let consensus_rpc_port = combined_args.narwhal_bullshark.consensus_rpc_port;
                if consensus_rpc_port > 0 {
                    info!(target: "reth::cli", "Configuring consensus RPC on port {}", consensus_rpc_port);
                    
                    let rpc_config = reth_consensus::narwhal_bullshark::ConsensusRpcConfig {
                        port: consensus_rpc_port,
                        host: "127.0.0.1".to_string(),
                        enable_admin: true, // Enable admin endpoints
                    };
                    
                    // Configure the bridge with RPC before starting
                    consensus_bridge.with_rpc(rpc_config)
                        .map_err(|e| eyre::eyre!("Failed to configure consensus RPC: {}", e))?;
                    info!(target: "reth::cli", "✅ Consensus RPC configured - will start when consensus service starts");
                } else {
                    info!(target: "reth::cli", "💡 Tip: Use --consensus-rpc-port <PORT> to enable consensus RPC endpoints");
                }

                // Step 3: Set up real mempool integration (needs ownership of bridge)
                setup_mempool_integration(
                    consensus_bridge,
                    Arc::new(node.pool.clone()),
                    node.task_executor.clone(),
                )?;

                info!(target: "reth::cli", "Narwhal + Bullshark consensus with mempool integration is now handling block production");
            }

            node_exit_future.await
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}