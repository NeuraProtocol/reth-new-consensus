#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

use clap::Parser;
use reth::{args::CombinedProtocolArgs, cli::Cli, ress::install_ress_subprotocol};
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_builder::NodeHandle;
use reth_node_ethereum::EthereumNode;
use tracing::info;
use std::sync::Arc;

mod narwhal_bullshark;
use narwhal_bullshark::{
    install_narwhal_bullshark_consensus, 
    setup_mempool_integration,
    should_use_narwhal_consensus,
    consensus_mode_description,
};

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<EthereumChainSpecParser, CombinedProtocolArgs>::parse().run(async move |builder, combined_args| {
            info!(target: "reth::cli", "Launching node");
            
            let NodeHandle { node, node_exit_future } =
                builder.node(EthereumNode::default()).launch_with_debug_capabilities().await?;

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
                let consensus_bridge = install_narwhal_bullshark_consensus(
                    combined_args.narwhal_bullshark,
                    node.provider.clone(),
                    node.evm_config.clone(),
                    node.network.clone(),
                    node.task_executor.clone(),
                    node.add_ons_handle.engine_events.new_listener(),
                )?;

                // Step 2: Set up real mempool integration
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
