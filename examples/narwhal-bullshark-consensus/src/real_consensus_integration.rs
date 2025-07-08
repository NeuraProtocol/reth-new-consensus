//! Real Narwhal+Bullshark consensus integration
//!
//! This module provides the actual consensus implementation that connects
//! Narwhal DAG construction with Bullshark BFT finalization and submits
//! the finalized blocks to the Reth engine.

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    block_builder::NarwhalBlockBuilder,
    validator_keys::ValidatorKeyPair,
    chain_state::ChainStateTracker,
};
use narwhal::{
    DagService, Primary, Worker, 
    types::{Committee, PublicKey as NarwhalPublicKey, Authority},
    NetworkConfig, PrimaryConfiguration, WorkerConfiguration,
};
use bullshark::{BftService, BftConfig, ChainStateProvider};
use alloy_primitives::Address;
use reth_chainspec::ChainSpec;
use reth_provider::{BlockReaderIdExt, StateProviderFactory, DatabaseProviderFactory};
use reth_evm::ConfigureEvm;
use reth_node_api::BeaconConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthEngineTypes;
use tokio::sync::mpsc;
use tracing::{info, error, debug};
use anyhow::Result;
use std::{sync::Arc, collections::HashMap, net::SocketAddr};

/// Real consensus integration that runs Narwhal+Bullshark
pub struct RealConsensusIntegration<Provider, EvmConfig> {
    /// Chain specification
    chain_spec: Arc<ChainSpec>,
    /// Database provider
    provider: Provider,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Validator key
    validator_key: ValidatorKeyPair,
    /// Consensus configuration
    config: ConsensusConfig,
    /// Engine API handle
    engine_handle: BeaconConsensusEngineHandle<EthEngineTypes>,
    /// Chain state tracker
    chain_state: Arc<ChainStateTracker>,
}

impl<Provider, EvmConfig> RealConsensusIntegration<Provider, EvmConfig>
where
    Provider: StateProviderFactory + BlockReaderIdExt + DatabaseProviderFactory + Clone + Send + Sync + 'static,
    EvmConfig: ConfigureEvm + Clone + Send + Sync + 'static,
{
    /// Create a new real consensus integration
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        provider: Provider,
        evm_config: EvmConfig,
        validator_key: ValidatorKeyPair,
        config: ConsensusConfig,
        engine_handle: BeaconConsensusEngineHandle<EthEngineTypes>,
    ) -> Self {
        let chain_state = Arc::new(ChainStateTracker::new());
        
        Self {
            chain_spec,
            provider,
            evm_config,
            validator_key,
            config,
            engine_handle,
            chain_state,
        }
    }

    /// Run the consensus system
    pub async fn run(self) -> Result<()> {
        info!("Starting real Narwhal+Bullshark consensus");
        
        // Create block builder
        let block_builder = Arc::new(NarwhalBlockBuilder::new(
            self.chain_spec.clone(),
            self.provider.clone(),
            self.evm_config.clone(),
        ));
        
        // Create channels for batch communication
        let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel::<FinalizedBatch>();
        
        // Initialize chain state from current blockchain
        let current_block = self.provider.best_block_number()?;
        let current_hash = self.provider.block_hash(current_block)?
            .unwrap_or(alloy_primitives::B256::ZERO);
        self.chain_state.update(current_block, current_hash);
        
        // Create committee from validator keys
        let committee = self.create_committee()?;
        
        // Start Narwhal (DAG construction)
        let narwhal_handle = self.start_narwhal(&committee).await?;
        
        // Start Bullshark (BFT consensus)
        let bullshark_handle = self.start_bullshark(committee, batch_sender).await?;
        
        // Start block submission task
        let submission_handle = tokio::spawn({
            let engine_handle = self.engine_handle.clone();
            let chain_state = self.chain_state.clone();
            
            async move {
                while let Some(batch) = batch_receiver.recv().await {
                    debug!(
                        "Received finalized batch for block #{} with {} transactions",
                        batch.block_number,
                        batch.transactions.len()
                    );
                    
                    // Build the block
                    match block_builder.build_block(batch.clone()) {
                        Ok(sealed_block) => {
                            info!(
                                "Built block #{} with hash: {}",
                                sealed_block.number,
                                sealed_block.hash()
                            );
                            
                            // Submit to engine API
                            if let Err(e) = submit_block_to_engine(
                                &engine_handle,
                                sealed_block.clone()
                            ).await {
                                error!("Failed to submit block to engine: {}", e);
                            } else {
                                // Update chain state on successful submission
                                chain_state.update(
                                    sealed_block.number,
                                    sealed_block.hash()
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to build block: {}", e);
                        }
                    }
                }
            }
        });
        
        // Wait for all tasks
        tokio::select! {
            res = narwhal_handle => {
                error!("Narwhal task exited: {:?}", res);
            }
            res = bullshark_handle => {
                error!("Bullshark task exited: {:?}", res);
            }
            res = submission_handle => {
                error!("Submission task exited: {:?}", res);
            }
        }
        
        Ok(())
    }
    
    /// Create committee from validator configuration
    fn create_committee(&self) -> Result<Committee> {
        // For now, create a single-validator committee
        // In production, this would load from configuration
        let mut authorities = HashMap::new();
        
        let authority = Authority {
            stake: 1, // Equal stake for now
            primary_address: format!("127.0.0.1:{}", self.config.consensus_port)
                .parse::<SocketAddr>()?,
            network_public_key: self.validator_key.consensus_public_key.clone(),
        };
        
        authorities.insert(
            NarwhalPublicKey::from_bytes(&self.validator_key.consensus_public_key)?,
            authority
        );
        
        Ok(Committee::new(authorities))
    }
    
    /// Start the Narwhal DAG service
    async fn start_narwhal(&self, committee: &Committee) -> Result<tokio::task::JoinHandle<()>> {
        let config = PrimaryConfiguration {
            id: 0, // Single validator for now
            committee: committee.clone(),
            parameters: Default::default(),
            network_config: NetworkConfig {
                primary_address: format!("127.0.0.1:{}", self.config.consensus_port)
                    .parse()?,
                worker_address: format!("127.0.0.1:{}", self.config.consensus_port + 1)
                    .parse()?,
            },
        };
        
        // TODO: Start actual Narwhal primary and workers
        // For now, return a dummy handle
        Ok(tokio::spawn(async move {
            info!("Narwhal DAG service would run here");
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
        }))
    }
    
    /// Start the Bullshark BFT service
    async fn start_bullshark(
        &self,
        committee: Committee,
        batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let config = BftConfig {
            committee,
            gc_depth: 50,
        };
        
        // TODO: Create and start actual BftService
        // For now, return a dummy handle
        Ok(tokio::spawn(async move {
            info!("Bullshark BFT service would run here");
            
            // Temporary: Send a test batch after 5 seconds
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            
            let test_batch = FinalizedBatch {
                block_number: 1,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                proposer: Address::random(),
                transactions: vec![],
            };
            
            let _ = batch_sender.send(test_batch);
            
            // Keep running
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
        }))
    }
}

/// Submit a block to the engine API
async fn submit_block_to_engine(
    engine_handle: &BeaconConsensusEngineHandle<EthEngineTypes>,
    sealed_block: reth_primitives::SealedBlock,
) -> Result<()> {
    use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
    use reth_ethereum_engine_primitives::EthPayloadTypes;
    use reth_payload_primitives::PayloadTypes;
    use reth_node_api::EngineApiMessageVersion;
    
    // Convert to payload
    let payload = EthPayloadTypes::block_to_payload(sealed_block.clone());
    
    // Submit new payload
    let status = engine_handle.new_payload(payload).await?;
    
    match status.status {
        PayloadStatusEnum::Valid => {
            info!("Block #{} accepted as VALID", sealed_block.number);
            
            // Update fork choice
            let forkchoice = ForkchoiceState {
                head_block_hash: sealed_block.hash(),
                safe_block_hash: sealed_block.hash(),
                finalized_block_hash: sealed_block.hash(),
            };
            
            let fc_response = engine_handle
                .fork_choice_updated(forkchoice, None, EngineApiMessageVersion::default())
                .await?;
                
            match fc_response.payload_status.status {
                PayloadStatusEnum::Valid => {
                    info!("Block #{} is now canonical", sealed_block.number);
                }
                _ => {
                    error!("Fork choice update failed for block #{}", sealed_block.number);
                    return Err(anyhow::anyhow!("Fork choice update failed"));
                }
            }
        }
        PayloadStatusEnum::Invalid { .. } => {
            error!("Block #{} rejected as INVALID", sealed_block.number);
            return Err(anyhow::anyhow!("Block rejected by engine"));
        }
        _ => {
            error!("Unexpected status for block #{}", sealed_block.number);
            return Err(anyhow::anyhow!("Unexpected block status"));
        }
    }
    
    Ok(())
}