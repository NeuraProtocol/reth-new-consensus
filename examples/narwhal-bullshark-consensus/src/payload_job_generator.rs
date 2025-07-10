//! Payload job generator for Narwhal+Bullshark consensus
//! 
//! This module provides a custom payload job generator that integrates with
//! Reth's payload building infrastructure to let Reth handle all block construction.

use crate::types::FinalizedBatch;
use reth_basic_payload_builder::{BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig};
use reth_chainspec::ChainSpec;
// use reth_payload_builder::PayloadBuilderHandle;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_transaction_pool::TransactionPool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

/// Create a basic payload job generator for Narwhal+Bullshark
/// 
/// This uses Reth's standard payload building infrastructure
pub fn create_narwhal_payload_generator<Provider, Pool, Builder>(
    provider: Provider,
    _pool: Pool,
    builder: Builder,
    _chain_spec: Arc<ChainSpec>,
    _batch_receiver: mpsc::UnboundedReceiver<FinalizedBatch>,
) -> BasicPayloadJobGenerator<Provider, Pool, Builder>
where
    Provider: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + Unpin + 'static,
    Builder: reth_basic_payload_builder::PayloadBuilder + Clone + Unpin + 'static,
{
    info!("Creating Narwhal payload job generator");
    
    // For now, we use the standard generator
    // In the future, we could customize this to handle finalized batches
    BasicPayloadJobGenerator::with_builder(
        provider,
        _pool,
        BasicPayloadJobGeneratorConfig::default(),
        builder,
    )
}

/// Create a channel for sending finalized batches from consensus to the payload builder
pub fn create_batch_channel() -> (mpsc::UnboundedSender<FinalizedBatch>, mpsc::UnboundedReceiver<FinalizedBatch>) {
    mpsc::unbounded_channel()
}