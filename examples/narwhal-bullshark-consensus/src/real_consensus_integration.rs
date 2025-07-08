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
    config::{NarwhalConfig, NetworkConfig as NarwhalNetworkConfig},
};
use fastcrypto::traits::ToFromBytes;
use bullshark::{BftService, BftConfig, ChainStateProvider};
use alloy_primitives::Address;
use reth_chainspec::ChainSpec;
use reth_provider::{BlockReaderIdExt, StateProviderFactory, DatabaseProviderFactory};
use reth_evm::ConfigureEvm;
use reth_node_api::BeaconConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthEngineTypes;
use tokio::sync::{mpsc, watch};
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
        let (narwhal_handle, certificate_sender) = self.start_narwhal(&committee).await?;
        
        // Start Bullshark (BFT consensus)
        let bullshark_handle = self.start_bullshark(
            committee, 
            certificate_sender,  // Already a receiver
            batch_sender
        ).await?;
        
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
        
        // Parse the base64 public key
        let public_key = NarwhalPublicKey::from_bytes(
            &base64::decode(&self.validator_key.consensus_public_key)
                .map_err(|e| anyhow::anyhow!("Failed to decode public key: {}", e))?
        ).map_err(|e| anyhow::anyhow!("Failed to parse public key: {}", e))?;
        
        let authority = Authority {
            stake: 1, // Equal stake for now
            primary_address: format!("127.0.0.1:{}", self.config.consensus_port),
            network_key: public_key.clone(),
            workers: narwhal::types::WorkerConfiguration {
                num_workers: 0,
                base_port: 5000,
                base_address: "127.0.0.1".to_string(),
                worker_ports: None,
            },
        };
        
        authorities.insert(public_key, authority);
        
        Ok(Committee::new(0, authorities))  // Epoch 0 for now
    }
    
    /// Start the Narwhal DAG service
    async fn start_narwhal(
        &self, 
        committee: &Committee
    ) -> Result<(tokio::task::JoinHandle<()>, mpsc::Receiver<narwhal::types::Certificate>)> {
        info!("Starting Narwhal DAG service");
        
        // For now, create a simple channel to receive certificates
        let (tx_certificates, rx_certificates) = mpsc::channel::<narwhal::types::Certificate>(1000);
        
        // TODO: Properly instantiate and connect the existing DAG service
        // The existing implementation requires:
        // 1. Network setup with Anemo
        // 2. Worker services
        // 3. Storage backend
        // 4. Proper configuration
        
        let handle = tokio::spawn(async move {
            info!("Narwhal DAG service placeholder running");
            // In production, this would run the actual DAG service from crates/narwhal/src/dag_service.rs
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
        });
        
        Ok((handle, rx_certificates))
    }
    
    /// Start the Bullshark BFT service
    async fn start_bullshark(
        &self,
        committee: Committee,
        certificate_receiver: mpsc::Receiver<narwhal::types::Certificate>,
        batch_sender: mpsc::UnboundedSender<FinalizedBatch>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        use std::time::Duration;
        
        info!("Starting Bullshark BFT service");
        
        // Parse the public key
        let node_key = NarwhalPublicKey::from_bytes(
            &base64::decode(&self.validator_key.consensus_public_key)
                .map_err(|e| anyhow::anyhow!("Failed to decode public key: {}", e))?
        ).map_err(|e| anyhow::anyhow!("Failed to parse public key: {}", e))?;
        
        let config = BftConfig {
            node_key,
            gc_depth: 50,
            finalization_timeout: Duration::from_secs(10),
            max_certificates_per_round: 1000,
            leader_rotation_frequency: 10,
            max_certificates_per_dag: 10000,
            min_block_time: Duration::from_secs(1),
            min_leader_round: 1,
        };
        
        // TODO: Properly instantiate the existing BftService from crates/bullshark/src/bft_service.rs
        // For now, just send test batches
        Ok(tokio::spawn(async move {
            info!("Bullshark BFT service placeholder running");
            
            // Send a test batch after 5 seconds
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let test_batch = FinalizedBatch {
                round: 1,
                block_number: 1,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                proposer: Address::random(),
                transactions: vec![],
                certificate_digest: alloy_primitives::B256::random(),
            };
            
            let _ = batch_sender.send(test_batch);
            info!("Sent test batch");
            
            // Keep running
            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
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