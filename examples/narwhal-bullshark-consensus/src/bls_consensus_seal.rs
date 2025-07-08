//! BLS signature implementation for consensus seals
//! 
//! This module provides BLS signature aggregation for creating consensus seals
//! that prove a quorum of validators agreed on a block.

use alloy_primitives::B256;
use fastcrypto::{
    bls12381::{BLS12381AggregateSignature, BLS12381Signature, BLS12381PublicKey, BLS12381KeyPair},
    traits::{AggregateAuthenticator, ToFromBytes, KeyPair as _, Signer},
    Verifier,
};
use crate::types::FinalizedBatch;
use tracing::{info, debug};
use anyhow::Result;
use std::collections::BTreeMap;

/// BLS consensus seal that contains aggregated signature from validators
#[derive(Debug, Clone)]
pub struct BlsConsensusSeal {
    /// Aggregated BLS signature from validators
    pub aggregate_signature: Vec<u8>,
    /// Bitmap indicating which validators signed
    pub signers_bitmap: Vec<u8>,
    /// Total stake that signed
    pub total_stake_signed: u64,
}

impl BlsConsensusSeal {
    /// Create consensus seal from validator signatures
    pub fn create_from_signatures(
        batch: &FinalizedBatch,
        signatures: &BTreeMap<BLS12381PublicKey, (BLS12381Signature, u64)>, // (signature, stake)
        total_stake: u64,
    ) -> Result<Self> {
        debug!(
            "Creating BLS consensus seal for batch {} with {} signatures",
            batch.certificate_digest,
            signatures.len()
        );

        // Calculate signing message (batch digest)
        let message = Self::compute_signing_message(batch);
        
        // Aggregate signatures
        let mut sig_vec = Vec::new();
        let mut bitmap = vec![0u8; (signatures.len() + 7) / 8]; // Bit vector for signers
        let mut total_stake_signed = 0u64;
        let mut validator_index = 0;
        
        for (_pubkey, (signature, stake)) in signatures {
            sig_vec.push(signature.clone());
            
            // Set bit in bitmap
            let byte_index = validator_index / 8;
            let bit_index = validator_index % 8;
            bitmap[byte_index] |= 1 << bit_index;
            
            total_stake_signed += stake;
            validator_index += 1;
        }
        
        // Create aggregate signature
        let aggregate = BLS12381AggregateSignature::aggregate(sig_vec)?;
        let aggregate_bytes = aggregate.as_bytes().to_vec();
        
        info!(
            "Created BLS consensus seal with {}/{} stake signed",
            total_stake_signed, total_stake
        );
        
        Ok(Self {
            aggregate_signature: aggregate_bytes,
            signers_bitmap: bitmap,
            total_stake_signed,
        })
    }
    
    /// Compute the message that validators sign for a batch
    pub fn compute_signing_message(batch: &FinalizedBatch) -> Vec<u8> {
        // Create deterministic message from batch data
        let mut message = Vec::new();
        message.extend_from_slice(b"NEURA_CONSENSUS_SEAL_V1");
        message.extend_from_slice(&batch.block_number.to_le_bytes());
        message.extend_from_slice(batch.certificate_digest.as_ref());
        message.extend_from_slice(&batch.round.to_le_bytes());
        message.extend_from_slice(&batch.timestamp.to_le_bytes());
        message
    }
    
    /// Verify consensus seal against committee
    pub fn verify(
        &self,
        batch: &FinalizedBatch,
        validators: &[(BLS12381PublicKey, u64)], // (pubkey, stake)
        quorum_threshold: u64,
    ) -> Result<bool> {
        // Reconstruct aggregate signature
        let aggregate = BLS12381AggregateSignature::from_bytes(&self.aggregate_signature)?;
        
        // Collect public keys that signed
        let mut signing_pubkeys = Vec::new();
        let mut total_stake = 0u64;
        
        for (index, (pubkey, stake)) in validators.iter().enumerate() {
            let byte_index = index / 8;
            let bit_index = index % 8;
            
            if byte_index < self.signers_bitmap.len() {
                if (self.signers_bitmap[byte_index] & (1 << bit_index)) != 0 {
                    signing_pubkeys.push(pubkey.clone());
                    total_stake += stake;
                }
            }
        }
        
        // Check stake threshold
        if total_stake < quorum_threshold {
            debug!(
                "Insufficient stake for consensus seal: {} < {}",
                total_stake, quorum_threshold
            );
            return Ok(false);
        }
        
        // Verify aggregate signature
        let message = Self::compute_signing_message(batch);
        match aggregate.verify(&signing_pubkeys, &message) {
            Ok(_) => {
                debug!(
                    "BLS consensus seal verification: true (stake: {}/{})",
                    total_stake, quorum_threshold
                );
                Ok(true)
            }
            Err(e) => {
                debug!(
                    "BLS consensus seal verification: false (stake: {}/{}) - error: {}",
                    total_stake, quorum_threshold, e
                );
                Ok(false)
            }
        }
    }
    
    /// Encode seal to bytes for block extra data
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        
        // Version byte
        encoded.push(1u8);
        
        // Signature length and data
        encoded.extend_from_slice(&(self.aggregate_signature.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&self.aggregate_signature);
        
        // Bitmap length and data
        encoded.extend_from_slice(&(self.signers_bitmap.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&self.signers_bitmap);
        
        // Total stake signed
        encoded.extend_from_slice(&self.total_stake_signed.to_le_bytes());
        
        encoded
    }
    
    /// Decode seal from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow::anyhow!("Empty consensus seal data"));
        }
        
        let mut cursor = 0;
        
        // Version byte
        let version = data[cursor];
        if version != 1 {
            return Err(anyhow::anyhow!("Unsupported consensus seal version: {}", version));
        }
        cursor += 1;
        
        // Signature
        let sig_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;
        let aggregate_signature = data[cursor..cursor + sig_len].to_vec();
        cursor += sig_len;
        
        // Bitmap
        let bitmap_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;
        let signers_bitmap = data[cursor..cursor + bitmap_len].to_vec();
        cursor += bitmap_len;
        
        // Total stake
        let total_stake_signed = u64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
        
        Ok(Self {
            aggregate_signature,
            signers_bitmap,
            total_stake_signed,
        })
    }
}

/// Helper to sign a batch with a BLS key
pub fn sign_batch(
    batch: &FinalizedBatch,
    keypair: &BLS12381KeyPair,
) -> Result<BLS12381Signature> {
    let message = BlsConsensusSeal::compute_signing_message(batch);
    let signature = keypair.sign(&message);
    Ok(signature)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_consensus_seal_create_verify() {
        // Create test batch
        let batch = FinalizedBatch {
            round: 1,
            block_number: 100,
            transactions: vec![],
            certificate_digest: B256::random(),
            proposer: Default::default(),
            timestamp: 1234567890,
        };
        
        // Create test validators
        let mut validators = Vec::new();
        let mut signatures = BTreeMap::new();
        
        for i in 0..3 {
            let keypair = BLS12381KeyPair::generate(&mut rand::thread_rng());
            let stake = 100u64;
            
            // Sign batch
            let signature = sign_batch(&batch, &keypair).unwrap();
            
            validators.push((keypair.public().clone(), stake));
            signatures.insert(keypair.public().clone(), (signature, stake));
        }
        
        // Create seal
        let seal = BlsConsensusSeal::create_from_signatures(&batch, &signatures, 300).unwrap();
        
        // Verify seal
        let verified = seal.verify(&batch, &validators, 200).unwrap(); // 2/3 quorum
        assert!(verified);
        
        // Test encoding/decoding
        let encoded = seal.encode();
        let decoded = BlsConsensusSeal::decode(&encoded).unwrap();
        assert_eq!(seal.aggregate_signature, decoded.aggregate_signature);
        assert_eq!(seal.signers_bitmap, decoded.signers_bitmap);
        assert_eq!(seal.total_stake_signed, decoded.total_stake_signed);
    }
}