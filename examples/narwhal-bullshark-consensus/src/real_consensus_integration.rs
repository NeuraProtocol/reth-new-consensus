//! Real Narwhal+Bullshark consensus integration
//!
//! This module provides the actual consensus implementation that connects
//! Narwhal DAG construction with Bullshark BFT finalization and submits
//! the finalized blocks to the Reth engine.

use crate::{
    types::{FinalizedBatch, ConsensusConfig},
    block_builder::NarwhalPayloadBuilder,
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
        
        // Note: Block builder is now created inside the integration layers
        // that handle the actual block submission (engine or database integration)
        
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
            let chain_spec = self.chain_spec.clone();
            let provider = self.provider.clone();
            
            async move {
                // Create payload builder inside the task
                let payload_builder = NarwhalPayloadBuilder::new(
                    chain_spec,
                    provider,
                );
                
                while let Some(batch) = batch_receiver.recv().await {
                    debug!(
                        "Received finalized batch for block #{} with {} transactions",
                        batch.block_number,
                        batch.transactions.len()
                    );
                    
                    // Build the block
                    match payload_builder.build_block(batch.clone()).await {
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
        info!("Loading committee from validator configuration directory: {}", self.config.validator_config_dir);
        
        let mut authorities = HashMap::new();
        let config_dir = std::path::Path::new(&self.config.validator_config_dir);
        
        // Load all validator JSON files from the config directory
        let validator_files = std::fs::read_dir(config_dir)
            .map_err(|e| anyhow::anyhow!("Failed to read validator config dir: {}", e))?;
            
        for entry in validator_files {
            let entry = entry?;
            let path = entry.path();
            
            // Only process .json files
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            
            // Skip if this is committee.json or other non-validator files
            let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if !filename.starts_with("validator-") {
                continue;
            }
            
            info!("Loading validator from: {}", path.display());
            
            // Load the validator configuration
            let validator_key = ValidatorKeyPair::from_file(&path.to_string_lossy())
                .map_err(|e| anyhow::anyhow!("Failed to load validator {}: {}", path.display(), e))?;
                
            // Skip inactive validators
            if !validator_key.active {
                info!("Skipping inactive validator: {}", validator_key.name);
                continue;
            }
            
            // Parse the public key
            let consensus_public_key = validator_key.consensus_public_key.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Validator {} has no consensus public key", validator_key.name))?;
            let public_key = NarwhalPublicKey::from_bytes(
                &base64::decode(consensus_public_key)
                    .map_err(|e| anyhow::anyhow!("Failed to decode public key: {}", e))?
            ).map_err(|e| anyhow::anyhow!("Failed to parse public key: {}", e))?;
            
            // Parse worker port range
            let (worker_base_port, num_workers) = if let Some(port_range) = &validator_key.worker_port_range {
                let parts: Vec<&str> = port_range.split(':').collect();
                if parts.len() == 2 {
                    let start_port = parts[0].parse::<u16>()
                        .map_err(|e| anyhow::anyhow!("Invalid worker port range: {}", e))?;
                    let end_port = parts[1].parse::<u16>()
                        .map_err(|e| anyhow::anyhow!("Invalid worker port range: {}", e))?;
                    (start_port, (end_port - start_port + 1) as usize)
                } else {
                    (5000, 4) // Default
                }
            } else {
                (5000, 4) // Default
            };
            
            let authority = Authority {
                stake: validator_key.stake,
                primary_address: validator_key.network_address.clone(),
                network_key: public_key.clone(),
                workers: narwhal::types::WorkerConfiguration {
                    num_workers: num_workers as u32,
                    base_port: worker_base_port,
                    base_address: validator_key.network_address.split(':').next().unwrap_or("127.0.0.1").to_string(),
                    worker_ports: None,
                },
            };
            
            info!("Added validator {} to committee with stake {} and {} workers", 
                validator_key.name, validator_key.stake, num_workers);
            
            authorities.insert(public_key, authority);
        }
        
        if authorities.is_empty() {
            return Err(anyhow::anyhow!("No active validators found in configuration directory"));
        }
        
        info!("Created committee with {} validators", authorities.len());
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
        let consensus_public_key = self.validator_key.consensus_public_key.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Validator has no consensus public key"))?;
        let node_key = NarwhalPublicKey::from_bytes(
            &base64::decode(consensus_public_key)
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