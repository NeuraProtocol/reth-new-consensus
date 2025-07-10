//! Narwhal + Bullshark consensus engine for Reth
//! This is the consensus engine implementation that integrates with Reth's consensus trait

use crate::types::FinalizedBatch;
use reth_consensus::{Consensus, ConsensusError, HeaderValidator, FullConsensus};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock, SealedBlock};
use reth_execution_types::BlockExecutionResult;
use tokio::sync::mpsc::UnboundedReceiver;

/// Narwhal + Bullshark consensus implementation for Reth
#[derive(Debug, Clone)]
pub struct NarwhalBullsharkConsensus {
    /// Configuration parameters (can be added later)
    _config: (),
}

impl NarwhalBullsharkConsensus {
    /// Create a new Narwhal + Bullshark consensus validator
    pub fn new() -> Self {
        Self { _config: () }
    }
}

impl Default for NarwhalBullsharkConsensus {
    fn default() -> Self {
        Self::new()
    }
}

// Implement the HeaderValidator trait (required by Consensus)
impl<H> HeaderValidator<H> for NarwhalBullsharkConsensus
where
    H: Clone,
{
    fn validate_header(&self, _header: &reth_primitives_traits::SealedHeader<H>) -> Result<(), ConsensusError> {
        // Narwhal + Bullshark consensus doesn't validate individual headers
        // Validation happens at the batch/consensus level
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &reth_primitives_traits::SealedHeader<H>,
        _parent: &reth_primitives_traits::SealedHeader<H>,
    ) -> Result<(), ConsensusError> {
        // Narwhal + Bullshark consensus doesn't validate headers against parents
        // The consensus protocol handles ordering and validity
        Ok(())
    }
}

// Implement the Consensus trait for block validation
impl<B> Consensus<B> for NarwhalBullsharkConsensus
where
    B: reth_primitives_traits::Block,
{
    type Error = ConsensusError;

    fn validate_body_against_header(
        &self,
        _body: &B::Body,
        _header: &reth_primitives_traits::SealedHeader<B::Header>,
    ) -> Result<(), Self::Error> {
        // Body validation is handled by the Narwhal + Bullshark consensus process
        // Blocks coming from consensus are already validated
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock<B>) -> Result<(), Self::Error> {
        // Pre-execution validation is handled by the consensus protocol
        // All blocks from Narwhal + Bullshark are pre-validated
        Ok(())
    }
}

// Implement the FullConsensus trait for execution outcome validation
impl<N> FullConsensus<N> for NarwhalBullsharkConsensus
where
    N: NodePrimitives,
{
    fn validate_block_post_execution(
        &self,
        _block: &RecoveredBlock<N::Block>,
        _result: &BlockExecutionResult<N::Receipt>,
    ) -> Result<(), ConsensusError> {
        // Post-execution validation
        // In Narwhal + Bullshark, we trust the consensus process
        // But we could add additional validation here if needed
        Ok(())
    }
}

/// Stream for receiving finalized batches from Narwhal + Bullshark consensus
pub struct FinalizationStream {
    receiver: UnboundedReceiver<FinalizedBatch>,
}

impl std::fmt::Debug for FinalizationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinalizationStream")
            .finish_non_exhaustive()
    }
}

impl FinalizationStream {
    /// Create a new finalization stream
    pub fn new(receiver: UnboundedReceiver<FinalizedBatch>) -> Self {
        Self { receiver }
    }
    
    /// Get the next finalized batch
    pub async fn next_batch(&mut self) -> Option<FinalizedBatch> {
        self.receiver.recv().await
    }
}