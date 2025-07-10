//! Updated node integration using Reth's payload builder
//!
//! This version uses the approach where Reth handles all block construction.

use crate::{
    types::{ConsensusConfig, FinalizedBatch},
    complete_integration::{NarwhalRethBridge, ServiceConfig, RethIntegrationConfig},
    validator_keys::ValidatorKeyPair,
    working_validator_registry::ValidatorRegistry,
    reth_payload_builder_integration::RethPayloadBuilderIntegration,
    consensus_storage::MdbxConsensusStorage,
};
use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
use reth_ethereum_engine_primitives::EthPayloadTypes;
use reth_node_api::{BeaconConsensusEngineHandle, EngineApiMessageVersion};
use reth_payload_primitives::PayloadTypes;
use reth_provider::{BlockReaderIdExt, DatabaseProviderFactory, StateProviderFactory};
use reth_transaction_pool::TransactionPool;
use std::sync::Arc;
use tracing::{error, info};
use anyhow::Result;

/// Updated node integration that uses Reth's payload builder
pub struct NodeIntegrationV2<Provider, Pool, EvmConfig> {
    /// Chain spec
    chain_spec: Arc<reth_chainspec::ChainSpec>,
    /// Provider for reading blockchain data
    provider: Provider,
    /// Transaction pool
    pool: Pool,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Engine API handle
    engine_handle: BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Validator key
    validator_key: ValidatorKeyPair,
    /// Consensus configuration
    config: ConsensusConfig,
}

impl<Provider, Pool, EvmConfig> NodeIntegrationV2<Provider, Pool, EvmConfig>
where
    Provider: StateProviderFactory + DatabaseProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static + std::fmt::Debug,
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    EvmConfig: reth_evm::ConfigureEvm<NextBlockEnvCtx = reth_evm::NextBlockEnvAttributes> + Clone + Send + Sync + 'static,
{
    /// Create a new node integration
    pub fn new(
        chain_spec: Arc<reth_chainspec::ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        engine_handle: BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
        validator_key: ValidatorKeyPair,
        config: ConsensusConfig,
    ) -> Self {
        Self {
            chain_spec,
            provider,
            pool,
            evm_config,
            engine_handle,
            validator_key,
            config,
        }
    }

    /// Run the integration with Reth's payload builder
    pub async fn run(self) -> Result<()> {
        info!("Starting Narwhal+Bullshark node integration V2 (using Reth payload builder)");
        
        // Create the payload builder integration
        let payload_builder = Arc::new(RethPayloadBuilderIntegration::new(
            self.provider.clone(),
            self.evm_config.clone(),
            self.chain_spec.clone(),
        ));
        
        // Set up consensus (same as before)
        let committee = self.create_committee()?;
        let peer_addresses = self.get_peer_addresses()?;
        
        info!("Committee has {} members, {} peer addresses provided", 
            committee.authorities.len(), peer_addresses.len());
        
        // Create service configuration
        let service_config = ServiceConfig::new(self.config.clone(), committee.clone());
        
        // Create network configuration
        let network_config = RethIntegrationConfig {
            network_address: format!("127.0.0.1:{}", self.config.consensus_port).parse()?,
            enable_networking: true,
            max_pending_transactions: 10000,
            execution_timeout: std::time::Duration::from_secs(30),
            enable_metrics: true,
            peer_addresses,
        };
        
        // Create consensus storage
        let storage = Some({
            let mut storage = MdbxConsensusStorage::new();
            use crate::simple_consensus_db::OptimizedConsensusDb;
            let optimized_db = OptimizedConsensusDb::new(Arc::new(self.provider.clone()));
            let db_ops = Box::new(optimized_db);
            storage.set_db_ops(db_ops);
            Arc::new(storage)
        });
        
        // Create the Narwhal-Reth bridge
        let mut bridge = NarwhalRethBridge::new_with_network_config(
            service_config,
            storage,
            Some(network_config),
        )?;
        
        bridge.set_chain_spec(self.chain_spec.clone());
        bridge.start().await?;
        
        // Main loop: get finalized batches and build blocks using Reth
        loop {
            match bridge.get_next_finalized_batch().await {
                Ok(Some(batch)) => {
                    info!("Received finalized batch #{} with {} transactions", 
                        batch.block_number, batch.transactions.len());
                    
                    // Build block using Reth's execution engine
                    match self.build_and_submit_block(batch, &payload_builder).await {
                        Ok(()) => info!("Successfully built and submitted block"),
                        Err(e) => error!("Failed to build/submit block: {}", e),
                    }
                }
                Ok(None) => {
                    info!("Consensus stopped producing batches");
                    break;
                }
                Err(e) => {
                    error!("Error getting batch from consensus: {}", e);
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Build block from batch and submit to engine
    async fn build_and_submit_block(
        &self,
        batch: FinalizedBatch,
        payload_builder: &RethPayloadBuilderIntegration<Provider, EvmConfig>,
    ) -> Result<()> {
        // Use Reth's block executor to build the block
        let sealed_block = payload_builder.build_block_from_batch(batch).await?;
        
        info!(
            "Built block #{} with state_root: {}, receipts_root: {}",
            sealed_block.number,
            sealed_block.state_root,
            sealed_block.receipts_root
        );
        
        // Submit to engine API
        let payload = EthPayloadTypes::block_to_payload(sealed_block.clone());
        let status = self.engine_handle.new_payload(payload).await?;
        
        match status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} accepted as VALID", sealed_block.number);
                
                // Update fork choice
                let forkchoice = ForkchoiceState {
                    head_block_hash: sealed_block.hash(),
                    safe_block_hash: sealed_block.hash(),
                    finalized_block_hash: sealed_block.hash(),
                };
                
                let fc_response = self.engine_handle
                    .fork_choice_updated(forkchoice, None, EngineApiMessageVersion::default())
                    .await?;
                    
                match fc_response.payload_status.status {
                    PayloadStatusEnum::Valid => {
                        info!("Block #{} is now canonical", sealed_block.number);
                    }
                    _ => {
                        error!("Fork choice update failed for block #{}", sealed_block.number);
                    }
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                error!("Block #{} rejected: {}", sealed_block.number, validation_error);
            }
            _ => {
                error!("Unexpected status for block #{}", sealed_block.number);
            }
        }
        
        Ok(())
    }
    
    /// Create committee from committee config file
    fn create_committee(&self) -> Result<narwhal::types::Committee> {
        let committee_file = self.config.committee_config_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Committee config file is required"))?;
        
        self.create_committee_from_file(committee_file)
    }
    
    /// Create committee from shared committee configuration file
    fn create_committee_from_file(&self, committee_file: &str) -> Result<narwhal::types::Committee> {
        use std::fs;
        use std::collections::HashMap;
        use narwhal::types::PublicKey as NarwhalPublicKey;
        use fastcrypto::traits::EncodeDecodeBase64;
        use crate::types::{CommitteeConfig, CommitteeValidator};
        
        info!("Loading committee from configuration file: {}", committee_file);
        
        let committee_content = fs::read_to_string(committee_file)?;
        let committee_config: CommitteeConfig = serde_json::from_str(&committee_content)?;
        
        let mut registry = ValidatorRegistry::new();
        let mut stakes = HashMap::new();
        let mut worker_configs = HashMap::new();
        let mut network_addresses = HashMap::new();
        
        for validator in &committee_config.validators {
            let evm_address = validator.evm_address.parse::<alloy_primitives::Address>()?;
            let consensus_public_key = NarwhalPublicKey::decode_base64(&validator.consensus_public_key)?;
            let (base_port, num_workers) = self.parse_worker_port_range(&validator.worker_port_range)?;
            
            let identity = crate::working_validator_registry::ValidatorIdentity {
                evm_address,
                consensus_public_key: consensus_public_key.clone(),
                metadata: crate::working_validator_registry::ValidatorMetadata {
                    name: Some(validator.name.clone()),
                    description: Some("Committee member".to_string()),
                    contact: None,
                },
            };
            
            registry.register_validator(identity)?;
            stakes.insert(evm_address, validator.stake);
            
            let worker_config = narwhal::types::WorkerConfiguration {
                num_workers,
                base_port,
                base_address: "127.0.0.1".to_string(),
                worker_ports: None,
            };
            worker_configs.insert(evm_address, worker_config);
            network_addresses.insert(evm_address, (validator.network_address.clone(), consensus_public_key));
        }
        
        info!("✅ Created committee with {} validators", committee_config.validators.len());
        
        Ok(registry.create_committee_with_per_validator_workers(
            committee_config.epoch,
            &stakes,
            &worker_configs,
            &network_addresses,
        )?)
    }
    
    /// Get peer addresses from committee config
    fn get_peer_addresses(&self) -> Result<Vec<std::net::SocketAddr>> {
        let committee_file = self.config.committee_config_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Committee config file is required"))?;
        
        use std::fs;
        use crate::types::CommitteeConfig;
        
        let committee_content = fs::read_to_string(committee_file)?;
        let committee_config: CommitteeConfig = serde_json::from_str(&committee_content)?;
        
        let my_address = format!("127.0.0.1:{}", self.config.consensus_port);
        
        let peer_addresses: Vec<std::net::SocketAddr> = committee_config.validators
            .iter()
            .filter(|v| v.network_address != my_address)
            .map(|v| v.network_address.parse())
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(peer_addresses)
    }
    
    /// Parse worker port range
    fn parse_worker_port_range(&self, port_range: &str) -> Result<(u16, u32)> {
        let parts: Vec<&str> = port_range.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid worker port range format"));
        }
        
        let start_port = parts[0].parse::<u16>()?;
        let end_port = parts[1].parse::<u16>()?;
        
        if end_port <= start_port {
            return Err(anyhow::anyhow!("Invalid port range"));
        }
        
        let num_workers = (end_port - start_port + 1) as u32;
        Ok((start_port, num_workers))
    }
}