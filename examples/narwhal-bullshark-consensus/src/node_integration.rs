//! Integration with Reth node for Narwhal+Bullshark consensus
//!
//! This module provides the actual integration that connects to a running
//! Reth node and submits blocks from the BFT consensus.

use crate::{
    types::ConsensusConfig,
    complete_integration::{NarwhalRethBridge, ServiceConfig, RethIntegrationConfig},
    validator_keys::ValidatorKeyPair,
    working_validator_registry::ValidatorRegistry,
    reth_block_executor::RethBlockExecutor,
    canonical_state_fix::CanonicalStateUpdater,
    consensus_storage::MdbxConsensusStorage,
    reth_database_ops::RethDatabaseOps,
};
use reth_provider::BlockReaderIdExt;
use reth_transaction_pool::TransactionPool;
use tracing::info;
use anyhow::Result;

/// Integration that connects Narwhal+Bullshark consensus to a Reth node
pub struct NodeIntegration<Provider, Pool, EvmConfig> {
    /// Chain spec
    chain_spec: std::sync::Arc<reth_chainspec::ChainSpec>,
    /// Provider for reading blockchain data
    provider: Provider,
    /// Transaction pool
    pool: Pool,
    /// EVM configuration
    evm_config: EvmConfig,
    /// Engine API handle
    engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
    /// Validator key
    validator_key: ValidatorKeyPair,
    /// Consensus configuration
    config: ConsensusConfig,
}

impl<Provider, Pool, EvmConfig> NodeIntegration<Provider, Pool, EvmConfig>
where
    Provider: reth_provider::StateProviderFactory + reth_provider::DatabaseProviderFactory + BlockReaderIdExt + Clone + Send + Sync + 'static + std::fmt::Debug,
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    EvmConfig: reth_evm::ConfigureEvm + Clone + Send + Sync + 'static,
{
    /// Create a new node integration
    pub fn new(
        chain_spec: std::sync::Arc<reth_chainspec::ChainSpec>,
        provider: Provider,
        pool: Pool,
        evm_config: EvmConfig,
        engine_handle: reth_node_api::BeaconConsensusEngineHandle<reth_ethereum_engine_primitives::EthEngineTypes>,
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

    /// Run the integration using the complete working implementation
    pub async fn run(self) -> Result<()> {
        info!("Starting Narwhal+Bullshark node integration with complete implementation");
        
        // Use the complete working implementation from pre-move version
        info!("Using COMPLETE Narwhal+Bullshark consensus (restored from pre-move)");
        
        // Create committee using the working validator registry
        let committee = self.create_committee()?;
        
        // Get peer addresses from committee config
        let peer_addresses = self.get_peer_addresses()?;
        info!("Committee has {} members, {} peer addresses provided", committee.authorities.len(), peer_addresses.len());
        
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
        
        // Create consensus storage with real MDBX database operations
        let storage = Some({
            let mut storage = MdbxConsensusStorage::new();
            
            // Create concrete database operations implementation using the provider
            let db_ops = Box::new(RethDatabaseOps::new(std::sync::Arc::new(self.provider.clone())));
            storage.set_db_ops(db_ops);
            
            info!("✅ REAL: Injected database operations into consensus storage");
            info!("✅ REAL: Connected consensus storage to Reth database");
            
            std::sync::Arc::new(storage)
        });
        
        // Create the complete Narwhal-Reth bridge
        let mut bridge = NarwhalRethBridge::new_with_network_config(
            service_config,
            storage,
            Some(network_config),
        )?;
        
        // Set up block executor
        let block_executor = std::sync::Arc::new(RethBlockExecutor::new());
        bridge.set_block_executor(block_executor);
        
        // Start the bridge
        bridge.start().await?;
        
        // Main loop: get blocks from consensus and submit to engine
        loop {
            match bridge.get_next_block().await {
                Ok(Some(block)) => {
                    info!("Received block #{} from consensus", block.number);
                    
                    // Submit to engine API
                    if let Err(e) = self.submit_block_to_engine(block).await {
                        tracing::error!("Failed to submit block to engine: {}", e);
                    }
                }
                Ok(None) => {
                    info!("Consensus stopped producing blocks");
                    break;
                }
                Err(e) => {
                    tracing::error!("Error getting block from consensus: {}", e);
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Create committee from committee config file (required)
    fn create_committee(&self) -> Result<narwhal::types::Committee> {
        let committee_file = self.config.committee_config_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Committee config file is required. Use --validator.committee-config <file>"))?;
        
        self.create_committee_from_file(committee_file)
    }
    
    /// Create committee from shared committee configuration file
    fn create_committee_from_file(&self, committee_file: &str) -> Result<narwhal::types::Committee> {
        use std::fs;
        use std::collections::HashMap;
        use narwhal::types::PublicKey as NarwhalPublicKey;
        use fastcrypto::traits::EncodeDecodeBase64;
        use crate::types::{CommitteeConfig, CommitteeValidator};
        
        info!("Loading committee from shared configuration file: {}", committee_file);
        
        // Read and parse committee config file
        let committee_content = fs::read_to_string(committee_file)
            .map_err(|e| anyhow::anyhow!("Failed to read committee config file '{}': {}", committee_file, e))?;
        
        let committee_config: CommitteeConfig = serde_json::from_str(&committee_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse committee config file: {}", e))?;
        
        let mut registry = ValidatorRegistry::new();
        let mut stakes = HashMap::new();
        let mut worker_configs = HashMap::new();
        let mut network_addresses = HashMap::new();
        
        // Process each validator from the committee config
        for validator in &committee_config.validators {
            // Parse EVM address
            let evm_address = validator.evm_address.parse::<alloy_primitives::Address>()
                .map_err(|e| anyhow::anyhow!("Invalid EVM address '{}': {}", validator.evm_address, e))?;
            
            // Parse consensus public key
            let consensus_public_key = NarwhalPublicKey::decode_base64(&validator.consensus_public_key)
                .map_err(|e| anyhow::anyhow!("Failed to decode consensus public key for {}: {}", validator.name, e))?;
            
            // Parse worker port range
            let (base_port, num_workers) = self.parse_worker_port_range(&validator.worker_port_range)?;
            
            info!("Registered validator: EVM {} <-> Consensus \"{}\"", evm_address, consensus_public_key);
            
            // Create validator identity
            let identity = crate::working_validator_registry::ValidatorIdentity {
                evm_address,
                consensus_public_key: consensus_public_key.clone(),
                metadata: crate::working_validator_registry::ValidatorMetadata {
                    name: Some(validator.name.clone()),
                    description: Some("Committee member".to_string()),
                    contact: None,
                },
            };
            
            // Register validator
            registry.register_validator(identity)?;
            stakes.insert(evm_address, validator.stake);
            
            // Create worker configuration
            let worker_config = narwhal::types::WorkerConfiguration {
                num_workers,
                base_port,
                base_address: "127.0.0.1".to_string(),
                worker_ports: None,
            };
            worker_configs.insert(evm_address, worker_config);
            network_addresses.insert(evm_address, (validator.network_address.clone(), consensus_public_key));
            
            info!("Configured validator {} with worker ports {}-{}", evm_address, base_port, base_port + num_workers as u16 - 1);
        }
        
        if stakes.is_empty() {
            return Err(anyhow::anyhow!("No validators found in committee config file"));
        }
        
        info!("✅ Created committee with {} validators", committee_config.validators.len());
        
        // Create committee using the working registry
        let committee = registry.create_committee_with_per_validator_workers(
            committee_config.epoch,
            &stakes,
            &worker_configs,
            &network_addresses,
        )?;
        
        Ok(committee)
    }
    
    /// Get peer addresses from committee config (excludes this node's address)
    pub fn get_peer_addresses(&self) -> Result<Vec<std::net::SocketAddr>> {
        let committee_file = self.config.committee_config_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Committee config file is required"))?;
        
        use std::fs;
        use crate::types::CommitteeConfig;
        
        // Read and parse committee config file
        let committee_content = fs::read_to_string(committee_file)
            .map_err(|e| anyhow::anyhow!("Failed to read committee config file: {}", e))?;
        
        let committee_config: CommitteeConfig = serde_json::from_str(&committee_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse committee config file: {}", e))?;
        
        let mut peer_addresses = Vec::new();
        let our_network_addr = self.config.network_addr;
        
        for validator in &committee_config.validators {
            let validator_addr: std::net::SocketAddr = validator.network_address.parse()
                .map_err(|e| anyhow::anyhow!("Invalid network address '{}': {}", validator.network_address, e))?;
            
            // Only add addresses that are not ours
            if validator_addr != our_network_addr {
                peer_addresses.push(validator_addr);
            }
        }
        
        Ok(peer_addresses)
    }
    
    /// Submit block to engine API
    async fn submit_block_to_engine(&self, block: reth_primitives::SealedBlock) -> Result<()> {
        use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
        use reth_ethereum_engine_primitives::EthPayloadTypes;
        use reth_payload_primitives::PayloadTypes;
        use reth_node_api::EngineApiMessageVersion;
        
        // Convert to payload
        let payload = EthPayloadTypes::block_to_payload(block.clone());
        
        // Submit new payload
        let status = self.engine_handle.new_payload(payload).await?;
        
        match status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} accepted as VALID", block.number);
                
                // Update fork choice
                let forkchoice = ForkchoiceState {
                    head_block_hash: block.hash(),
                    safe_block_hash: block.hash(),
                    finalized_block_hash: block.hash(),
                };
                
                let fc_response = self.engine_handle
                    .fork_choice_updated(forkchoice, None, EngineApiMessageVersion::default())
                    .await?;
                    
                match fc_response.payload_status.status {
                    PayloadStatusEnum::Valid => {
                        info!("Block #{} is now canonical", block.number);
                    }
                    _ => {
                        tracing::error!("Fork choice update failed for block #{}", block.number);
                        return Err(anyhow::anyhow!("Fork choice update failed"));
                    }
                }
            }
            PayloadStatusEnum::Invalid { .. } => {
                tracing::error!("Block #{} rejected as INVALID", block.number);
                return Err(anyhow::anyhow!("Block rejected by engine"));
            }
            _ => {
                tracing::error!("Unexpected status for block #{}", block.number);
                return Err(anyhow::anyhow!("Unexpected block status"));
            }
        }
        
        Ok(())
    }
    
    /// Parse worker port range from string format "19000:19003"
    fn parse_worker_port_range(&self, port_range: &str) -> Result<(u16, u32)> {
        let parts: Vec<&str> = port_range.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid worker port range format '{}'. Expected 'start:end'", port_range));
        }
        
        let start_port = parts[0].parse::<u16>()
            .map_err(|e| anyhow::anyhow!("Invalid start port '{}': {}", parts[0], e))?;
        let end_port = parts[1].parse::<u16>()
            .map_err(|e| anyhow::anyhow!("Invalid end port '{}': {}", parts[1], e))?;
        
        if end_port <= start_port {
            return Err(anyhow::anyhow!("End port {} must be greater than start port {}", end_port, start_port));
        }
        
        let num_workers = (end_port - start_port + 1) as u32;
        Ok((start_port, num_workers))
    }

}