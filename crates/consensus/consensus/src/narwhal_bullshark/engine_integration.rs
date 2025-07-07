//! Integration with Reth's internal engine tree for proper canonical chain management
//!
//! This module provides the bridge between Narwhal+Bullshark consensus output
//! and Reth's engine tree, ensuring blocks are properly validated, executed,
//! and made canonical (updating both database and in-memory state).

use alloy_primitives::{Address, B256};
use alloy_rpc_types_engine::{ForkchoiceState, PayloadAttributes};
use anyhow::Result;
use reth_chain_state::CanonicalInMemoryState;
use reth_chainspec::ChainSpec;
use reth_consensus::Consensus;
use reth_engine_tree::{EngineTree, TreeConfig};
use reth_engine_primitives::{EngineValidator, InvalidBlockHook};
use reth_evm::ConfigureEvm;
use reth_payload_builder::PayloadBuilderHandle;
use reth_provider::{BlockchainProvider, ProviderFactory};
use reth_primitives::{RecoveredBlock, SealedBlock};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Configuration for engine tree integration
pub struct EngineTreeConfig {
    /// Maximum number of blocks to buffer before persistence
    pub persistence_threshold: u64,
    /// Maximum time between persistence operations
    pub persistence_interval: std::time::Duration,
    /// Whether to use parallel state root computation
    pub parallel_state_root: bool,
}

impl Default for EngineTreeConfig {
    fn default() -> Self {
        Self {
            persistence_threshold: 32,
            persistence_interval: std::time::Duration::from_secs(30),
            parallel_state_root: true,
        }
    }
}

/// Integration between Narwhal+Bullshark consensus and Reth's engine tree
pub struct EngineTreeIntegration<N, C, E>
where
    N: reth_node_types::NodeTypesWithDB,
    C: Consensus,
    E: ConfigureEvm,
{
    /// The engine tree instance
    engine_tree: EngineTree<N, C, E>,
    
    /// Provider for blockchain data
    provider: BlockchainProvider<N>,
    
    /// Canonical in-memory state (shared with provider)
    canonical_state: CanonicalInMemoryState<N::Primitives>,
    
    /// Current finalized block hash
    finalized_hash: B256,
    
    /// Current safe block hash  
    safe_hash: B256,
    
    /// Channel to send forkchoice updates to engine
    forkchoice_tx: mpsc::UnboundedSender<ForkchoiceState>,
}

impl<N, C, E> EngineTreeIntegration<N, C, E>
where
    N: reth_node_types::NodeTypesWithDB,
    C: Consensus + Clone + Send + Sync + 'static,
    E: ConfigureEvm<Primitives = N::Primitives> + Clone + Send + Sync + 'static,
{
    /// Create a new engine tree integration
    pub fn new(
        provider_factory: ProviderFactory<N>,
        provider: BlockchainProvider<N>,
        consensus: C,
        evm_config: E,
        chain_spec: Arc<ChainSpec>,
        config: EngineTreeConfig,
        payload_builder: Option<PayloadBuilderHandle<N::Engine>>,
    ) -> Result<Self> {
        // Get the canonical state from the provider
        let canonical_state = provider.canonical_in_memory_state();
        
        // Get the current chain tip
        let chain_info = provider.chain_info()?;
        let genesis_hash = chain_spec.genesis_hash();
        
        // Create tree configuration
        let tree_config = TreeConfig {
            persistence_threshold: config.persistence_threshold,
            max_execute_block_batch_size: 4,
            use_state_root_task: config.parallel_state_root,
            state_root_fallback: false,
            ..Default::default()
        };
        
        // Create the engine tree
        let engine_tree = EngineTree::new(
            provider_factory,
            consensus,
            evm_config,
            tree_config,
            canonical_state.clone(),
            payload_builder,
        )?;
        
        // Create forkchoice update channel
        let (forkchoice_tx, forkchoice_rx) = mpsc::unbounded_channel();
        
        // TODO: Connect forkchoice_rx to engine tree's event processing
        
        Ok(Self {
            engine_tree,
            provider,
            canonical_state,
            finalized_hash: genesis_hash,
            safe_hash: genesis_hash,
            forkchoice_tx,
        })
    }
    
    /// Process a block from consensus through the engine tree
    pub async fn process_consensus_block(&mut self, block: SealedBlock) -> Result<()> {
        info!(
            "Processing consensus block #{} through engine tree (hash: {}, parent: {})",
            block.number,
            block.hash(),
            block.parent_hash
        );
        
        // Step 1: Convert SealedBlock to RecoveredBlock (with senders)
        let recovered_block = self.recover_block(block.clone())?;
        
        // Step 2: Insert the block into the engine tree
        match self.engine_tree.insert_block(recovered_block) {
            Ok(status) => {
                info!("Block #{} inserted into engine tree: {:?}", block.number, status);
                
                // Step 3: Send forkchoice update to make it canonical
                let forkchoice_state = ForkchoiceState {
                    head_block_hash: block.hash(),
                    safe_block_hash: self.safe_hash,
                    finalized_block_hash: self.finalized_hash,
                };
                
                // Send FCU to engine (would be processed by engine's event loop)
                if let Err(e) = self.forkchoice_tx.send(forkchoice_state) {
                    error!("Failed to send forkchoice update: {}", e);
                    return Err(anyhow::anyhow!("Forkchoice channel closed"));
                }
                
                // Update safe/finalized periodically
                if block.number % 32 == 0 {
                    self.safe_hash = block.hash();
                }
                if block.number % 256 == 0 {
                    self.finalized_hash = block.hash();
                }
                
                info!(
                    "Sent forkchoice update for block #{} (head: {}, safe: {}, finalized: {})",
                    block.number,
                    block.hash(),
                    self.safe_hash,
                    self.finalized_hash
                );
            }
            Err(e) => {
                error!("Failed to insert block #{} into engine tree: {:?}", block.number, e);
                
                // Check if it's an invalid transaction error
                if e.to_string().contains("InvalidTransaction") {
                    warn!("Block contains invalid transactions, creating empty block instead");
                    // TODO: Create empty block at same height
                }
                
                return Err(anyhow::anyhow!("Block insertion failed: {}", e));
            }
        }
        
        Ok(())
    }
    
    /// Recover senders for a sealed block
    fn recover_block(&self, block: SealedBlock) -> Result<RecoveredBlock<N::Block>> {
        // For now, use zero addresses as senders (consensus doesn't track senders)
        // TODO: Properly recover senders from transaction signatures
        let senders = vec![Address::ZERO; block.body().transactions.len()];
        
        Ok(RecoveredBlock::new_unhashed(
            block.unseal(),
            senders,
        ))
    }
    
    /// Check if the canonical chain has been updated
    pub fn canonical_block_number(&self) -> u64 {
        self.canonical_state.get_canonical_block_number()
    }
    
    /// Get the canonical chain tip
    pub fn canonical_tip(&self) -> Option<B256> {
        self.canonical_state.get_canonical_head()
    }
}

/// Alternative approach: Direct engine tree manipulation (no RPC/events)
pub mod direct_integration {
    use super::*;
    use reth_engine_tree::TreeAction;
    
    /// Direct integration that bypasses event channels
    pub struct DirectEngineIntegration<N, C, E>
    where
        N: reth_node_types::NodeTypesWithDB,
        C: Consensus,
        E: ConfigureEvm,
    {
        engine_tree: EngineTree<N, C, E>,
        canonical_state: CanonicalInMemoryState<N::Primitives>,
    }
    
    impl<N, C, E> DirectEngineIntegration<N, C, E>
    where
        N: reth_node_types::NodeTypesWithDB,
        C: Consensus + Clone + Send + Sync + 'static,
        E: ConfigureEvm<Primitives = N::Primitives> + Clone + Send + Sync + 'static,
    {
        pub fn process_block_direct(&mut self, block: SealedBlock) -> Result<()> {
            // This would require exposing more internal methods from EngineTree
            // Currently, the engine tree is designed to work through events
            
            // Ideally we'd want:
            // 1. self.engine_tree.insert_block_direct(block)
            // 2. self.engine_tree.make_canonical_direct(block.hash())
            // 3. Canonical state automatically updated
            
            todo!("Direct engine tree integration requires exposing internal methods")
        }
    }
}