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
        
        // Create consensus storage with optimized database operations to reduce lock contention
        let storage = Some({
            let mut storage = MdbxConsensusStorage::new();
            
            // Use optimized database operations with shorter transaction lifetimes
            use crate::simple_consensus_db::OptimizedConsensusDb;
            
            let optimized_db = OptimizedConsensusDb::new(std::sync::Arc::new(self.provider.clone()));
            let db_ops = Box::new(optimized_db);
            storage.set_db_ops(db_ops);
            
            info!("✅ REAL: Using optimized consensus database operations with shorter transaction lifetimes");
            info!("✅ REAL: This should reduce lock contention with engine API operations");
            
            std::sync::Arc::new(storage)
        });
        
        // Create the complete Narwhal-Reth bridge
        let mut bridge = NarwhalRethBridge::new_with_network_config(
            service_config,
            storage,
            Some(network_config),
        )?;
        
        // Set the chain spec for proper base fee calculations
        bridge.set_chain_spec(self.chain_spec.clone());
        
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
                    if let Err(e) = self.submit_block_to_engine(block.clone(), &bridge).await {
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
    async fn submit_block_to_engine(&self, block: reth_primitives::SealedBlock, bridge: &NarwhalRethBridge) -> Result<()> {
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
                        
                        // Update consensus service chain state with full block info for proper parent caching
                        bridge.update_chain_state_with_block_info(
                            block.number, 
                            block.hash(),
                            block.gas_limit,
                            block.gas_used,
                            block.base_fee_per_gas.unwrap_or(875_000_000),
                            block.timestamp
                        ).await;
                        info!("Updated consensus chain state to block {} hash {} timestamp {}", block.number, block.hash(), block.timestamp);
                    }
                    _ => {
                        tracing::error!("Fork choice update failed for block #{}", block.number);
                        return Err(anyhow::anyhow!("Fork choice update failed"));
                    }
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                tracing::warn!("Block #{} validation error: {}", block.number, validation_error);
                
                // Log specific errors for debugging but don't retry
                if validation_error.contains("mismatched block state root") {
                    tracing::warn!("State root mismatch - consensus needs to sync with engine state");
                } else if validation_error.contains("base fee mismatch") {
                    tracing::warn!("Base fee mismatch - parent block state may be out of sync");
                }
                
                tracing::error!("Block #{} rejected as INVALID - moving on", block.number);
                
                // Don't retry - just return error and let consensus continue
                // The next block from consensus should have the correct parent
                return Err(anyhow::anyhow!("Block rejected by engine: {}", validation_error));
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
    
    /* REMOVED: Retry logic - keeping it simple, no retries
    /// Extract the correct state root from engine validation error message
    fn extract_correct_state_root(&self, error_msg: &str) -> Option<alloy_primitives::B256> {
        tracing::info!("Attempting to extract state root from error: {}", error_msg);
        
        // Parse error message like: "mismatched block state root: got 0x0bd0570f3da85d151b62002747dc26d1219e04ea55e430efa58da58c00196149, expected 0x0000000000000000000000000000000000000000000000000000000000000000"
        if let Some(start) = error_msg.find("got ") {
            let start = start + 4; // Skip "got "
            if let Some(end) = error_msg[start..].find(',') {
                let state_root_str = &error_msg[start..start + end];
                tracing::info!("Found state root string: {}", state_root_str);
                
                if let Ok(state_root) = state_root_str.parse::<alloy_primitives::B256>() {
                    tracing::info!("Successfully parsed state root: {}", state_root);
                    return Some(state_root);
                } else {
                    tracing::warn!("Failed to parse state root from string: {}", state_root_str);
                }
            } else {
                tracing::warn!("Could not find comma after 'got' in error message");
            }
        } else {
            tracing::warn!("Could not find 'got' in error message");
        }
        None
    }
    
    /// Extract the correct base fee from engine validation error message
    fn extract_correct_base_fee(&self, error_msg: &str) -> Option<u64> {
        tracing::info!("Attempting to extract base fee from error: {}", error_msg);
        
        // Parse error message like: "block base fee mismatch: got 26, expected 29"
        if let Some(start) = error_msg.find("expected ") {
            let start = start + 9; // Skip "expected "
            let remaining = &error_msg[start..];
            
            // Find the end of the number (could be end of string or next non-digit)
            let end = remaining.find(|c: char| !c.is_numeric()).unwrap_or(remaining.len());
            let base_fee_str = &remaining[..end];
            
            tracing::info!("Found base fee string: {}", base_fee_str);
            
            if let Ok(base_fee) = base_fee_str.parse::<u64>() {
                tracing::info!("Successfully parsed base fee: {}", base_fee);
                return Some(base_fee);
            } else {
                tracing::warn!("Failed to parse base fee from string: {}", base_fee_str);
            }
        } else {
            tracing::warn!("Could not find 'expected' in error message");
        }
        None
    }
    
    /// Submit block to engine API (final attempt, no retry)
    async fn submit_block_to_engine_final(&self, block: reth_primitives::SealedBlock, bridge: &NarwhalRethBridge) -> Result<()> {
        use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
        use reth_ethereum_engine_primitives::EthPayloadTypes;
        use reth_payload_primitives::PayloadTypes;
        use reth_node_api::EngineApiMessageVersion;
        
        tracing::info!(
            "Final submission of block #{} (hash: {}) to engine API",
            block.number,
            block.hash()
        );

        // Convert to payload
        let payload = EthPayloadTypes::block_to_payload(block.clone());

        // Submit new payload
        let status = self.engine_handle.new_payload(payload).await?;

        match status.status {
            PayloadStatusEnum::Valid => {
                tracing::info!("Block #{} accepted as VALID on retry", block.number);

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
                        tracing::info!("Block #{} is now canonical", block.number);
                        
                        // Update consensus service chain state after successful retry
                        bridge.update_chain_state_with_block_info(
                            block.number, 
                            block.hash(),
                            block.gas_limit,
                            block.gas_used,
                            block.base_fee_per_gas.unwrap_or(875_000_000)
                        ).await;
                        tracing::info!("Updated consensus chain state to block {} hash {} with parent info (after retry)", block.number, block.hash());
                    }
                    _ => {
                        tracing::error!("Fork choice update failed for block #{}", block.number);
                        return Err(anyhow::anyhow!("Fork choice update failed"));
                    }
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                // Check if it's a state root mismatch on retry
                if validation_error.contains("mismatched block state root") {
                    tracing::info!("Detected state root mismatch on retry, attempting final correction...");
                    
                    // Try to extract the correct state root from the error
                    if let Some(correct_state_root) = self.extract_correct_state_root(&validation_error) {
                        tracing::info!("Final retry of block #{} with correct state root: {}", block.number, correct_state_root);
                        
                        // Rebuild the block with the correct state root
                        let mut new_header = block.header().clone();
                        new_header.state_root = correct_state_root;
                        
                        let new_block = reth_primitives::Block::new(new_header, block.body().clone());
                        let new_sealed_block = reth_primitives::SealedBlock::seal_slow(new_block);
                        
                        // Final retry with correct state root
                        return self.submit_block_to_engine_final_final(new_sealed_block, bridge).await;
                    }
                }
                
                tracing::error!(
                    "Block #{} rejected as INVALID on retry: {}",
                    block.number,
                    validation_error
                );
                return Err(anyhow::anyhow!(
                    "Block rejected on retry: {}",
                    validation_error
                ));
            }
            _ => {
                tracing::error!("Unexpected status for block #{} on retry", block.number);
                return Err(anyhow::anyhow!("Unexpected block status on retry"));
            }
        }

        Ok(())
    }
    
    /// Submit block to engine API (truly final attempt, no more retries)
    async fn submit_block_to_engine_final_final(&self, block: reth_primitives::SealedBlock, bridge: &NarwhalRethBridge) -> Result<()> {
        use alloy_rpc_types::engine::{ForkchoiceState, PayloadStatusEnum};
        use reth_ethereum_engine_primitives::EthPayloadTypes;
        use reth_payload_primitives::PayloadTypes;
        use reth_node_api::EngineApiMessageVersion;
        
        tracing::info!(
            "FINAL submission of block #{} (hash: {}) to engine API with corrected state root",
            block.number,
            block.hash()
        );

        // Convert to payload
        let payload = EthPayloadTypes::block_to_payload(block.clone());

        // Submit new payload
        let status = self.engine_handle.new_payload(payload).await?;

        match status.status {
            PayloadStatusEnum::Valid => {
                tracing::info!("Block #{} FINALLY accepted as VALID", block.number);

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
                        tracing::info!("Block #{} is now canonical after all corrections", block.number);
                        
                        // Update consensus service chain state after successful retry
                        bridge.update_chain_state_with_block_info(
                            block.number,
                            block.hash(),
                            block.gas_limit,
                            block.gas_used,
                            block.base_fee_per_gas.unwrap_or(875_000_000)
                        ).await;
                        tracing::info!("Updated consensus chain state to block {} hash {}", block.number, block.hash());
                    }
                    _ => {
                        tracing::error!("Fork choice update failed for block #{}", block.number);
                        return Err(anyhow::anyhow!("Fork choice update failed"));
                    }
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                tracing::error!(
                    "Block #{} STILL rejected as INVALID after all corrections: {}",
                    block.number,
                    validation_error
                );
                return Err(anyhow::anyhow!(
                    "Block rejected after all corrections: {}",
                    validation_error
                ));
            }
            _ => {
                tracing::error!("Unexpected status for block #{} on final retry", block.number);
                return Err(anyhow::anyhow!("Unexpected block status on final retry"));
            }
        }

        Ok(())
    }

*/
}
