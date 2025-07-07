//! Example of how to properly integrate with Reth's engine API
//! This would replace the current direct database writes

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_engine::{
    ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, 
    PayloadAttributes, PayloadStatus, PayloadStatusEnum
};
use reth_rpc_api::EngineApiClient;
use reth_primitives::SealedBlock;

/// Example integration that uses engine API instead of direct DB writes
pub struct EngineApiConsensusIntegration {
    /// The engine API client (normally exposed via RPC)
    engine_api: Box<dyn EngineApiClient>,
    
    /// Latest finalized block hash
    finalized_hash: B256,
    
    /// Our validator address for rewards
    fee_recipient: Address,
}

impl EngineApiConsensusIntegration {
    /// Process a block from consensus through the engine API
    pub async fn process_consensus_block(
        &mut self,
        block: SealedBlock,
    ) -> anyhow::Result<()> {
        // Step 1: Convert our SealedBlock to an ExecutionPayload
        let payload = self.block_to_payload(block.clone())?;
        
        // Step 2: Submit the payload for execution via newPayload
        info!("Submitting block #{} to engine API", block.number);
        let payload_status = self.engine_api
            .new_payload_v3(payload, vec![], block.parent_beacon_block_root.unwrap_or_default())
            .await?;
        
        // Step 3: Check if the payload was valid
        match &payload_status.status {
            PayloadStatusEnum::Valid => {
                info!("Block #{} validated by engine", block.number);
                
                // Step 4: Update fork choice to make this block canonical
                let forkchoice_state = ForkchoiceState {
                    head_block_hash: payload_status.latest_valid_hash.unwrap_or(block.hash()),
                    safe_block_hash: payload_status.latest_valid_hash.unwrap_or(block.hash()),
                    finalized_block_hash: self.finalized_hash,
                };
                
                let fc_response = self.engine_api
                    .forkchoice_updated_v3(forkchoice_state, None)
                    .await?;
                
                if fc_response.payload_status.status == PayloadStatusEnum::Valid {
                    info!("Block #{} is now canonical!", block.number);
                    
                    // Update finalized hash periodically (e.g., every 32 blocks)
                    if block.number % 32 == 0 {
                        self.finalized_hash = block.hash();
                    }
                } else {
                    error!("Fork choice update failed: {:?}", fc_response);
                }
            }
            PayloadStatusEnum::Invalid => {
                error!("Block #{} rejected as invalid: {:?}", 
                    block.number, payload_status.validation_error);
            }
            PayloadStatusEnum::Syncing => {
                warn!("Engine is syncing, block #{} queued", block.number);
            }
            PayloadStatusEnum::Accepted => {
                info!("Block #{} accepted, waiting for execution", block.number);
            }
        }
        
        Ok(())
    }
    
    /// Convert a SealedBlock to an ExecutionPayload for the engine API
    fn block_to_payload(&self, block: SealedBlock) -> anyhow::Result<ExecutionPayloadV3> {
        Ok(ExecutionPayloadV3 {
            // Header fields
            parent_hash: block.parent_hash,
            fee_recipient: self.fee_recipient,
            state_root: block.state_root,
            receipts_root: block.receipts_root,
            logs_bloom: block.logs_bloom,
            prev_randao: B256::random(), // Would come from consensus
            block_number: block.number.into(),
            gas_limit: block.gas_limit.into(),
            gas_used: block.gas_used.into(),
            timestamp: block.timestamp.into(),
            extra_data: Bytes::from("Narwhal+Bullshark"),
            base_fee_per_gas: block.base_fee_per_gas.unwrap_or_default().into(),
            block_hash: block.hash(),
            
            // Transaction data
            transactions: block.body.transactions
                .iter()
                .map(|tx| tx.encoded_2718())
                .collect(),
            
            // Post-merge fields
            withdrawals: vec![], // No withdrawals in our consensus
            
            // Cancun fields  
            blob_gas_used: block.blob_gas_used.unwrap_or_default().into(),
            excess_blob_gas: block.excess_blob_gas.unwrap_or_default().into(),
        })
    }
    
    /// Example of building a new block (for validators)
    pub async fn build_new_block(
        &self,
        parent_hash: B256,
        timestamp: u64,
        transactions: Vec<Bytes>,
    ) -> anyhow::Result<SealedBlock> {
        // Step 1: Update fork choice to parent
        let forkchoice_state = ForkchoiceState {
            head_block_hash: parent_hash,
            safe_block_hash: parent_hash, 
            finalized_block_hash: self.finalized_hash,
        };
        
        // Step 2: Request a new payload with our transactions
        let payload_attrs = PayloadAttributes {
            timestamp: timestamp.into(),
            prev_randao: B256::random(),
            suggested_fee_recipient: self.fee_recipient,
            withdrawals: Some(vec![]),
            parent_beacon_block_root: Some(B256::ZERO),
        };
        
        let fc_response = self.engine_api
            .forkchoice_updated_v3(forkchoice_state, Some(payload_attrs))
            .await?;
        
        if let Some(payload_id) = fc_response.payload_id {
            // Step 3: Get the built payload
            let payload = self.engine_api
                .get_payload_v3(payload_id)
                .await?;
            
            // Convert back to SealedBlock
            // ... conversion logic ...
            
            todo!("Convert ExecutionPayload back to SealedBlock")
        } else {
            anyhow::bail!("No payload ID returned")
        }
    }
}

/// Alternative: Use local engine handle instead of RPC
pub mod local_engine {
    use reth_engine_tree::EngineTree;
    use reth_provider::BlockchainProvider;
    
    /// Direct integration with local engine tree (no RPC)
    pub struct LocalEngineIntegration<N: NodeTypesWithDB> {
        /// Direct handle to engine tree
        engine: EngineTree<N>,
        
        /// Provider for reading state
        provider: BlockchainProvider<N>,
    }
    
    impl<N: NodeTypesWithDB> LocalEngineIntegration<N> {
        pub async fn process_block(&mut self, block: SealedBlock) -> anyhow::Result<()> {
            // Direct engine tree integration would go here
            // This bypasses RPC and directly updates the canonical state
            
            // 1. engine.insert_block(block)
            // 2. engine.make_canonical(block.hash())
            // 3. Canonical state automatically updated!
            
            todo!("Implement direct engine tree integration")
        }
    }
}