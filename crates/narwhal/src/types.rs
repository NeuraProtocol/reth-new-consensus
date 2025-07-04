//! Core types for Narwhal DAG consensus

use crate::{DagError, DagResult, Round, BatchDigest};
use crate::WorkerId;
// Blake2 functionality is provided by fastcrypto
use fastcrypto::{
    traits::{AggregateAuthenticator, EncodeDecodeBase64, Signer},
    Hash, Digest, Verifier,
};
use blake2::{digest::Update, VarBlake2b};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use base64;
use std::{
    collections::{BTreeSet, HashMap, BTreeMap},
    fmt,
};
use derive_builder::Builder;
use indexmap::IndexMap;
use alloy_primitives::B256;

/// The epoch number
pub type Epoch = u64;

/// A public key for consensus
pub type PublicKey = fastcrypto::bls12381::BLS12381PublicKey;

/// A signature
pub type Signature = fastcrypto::bls12381::BLS12381Signature;

/// An aggregate signature
pub type AggregateSignature = fastcrypto::bls12381::BLS12381AggregateSignature;

/// Validator stake
pub type Stake = u64;

/// Information about an authority in the committee
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Authority {
    /// The voting power of this authority
    pub stake: Stake,
    /// The network address of the primary
    pub primary_address: String,
    /// Network key of the primary
    pub network_key: PublicKey,
    /// Worker configuration
    pub workers: WorkerConfiguration,
}

/// Worker configuration for an authority
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct WorkerConfiguration {
    /// Number of workers
    pub num_workers: u32,
    /// Base port for worker addresses (workers use sequential ports)
    pub base_port: u16,
    /// Base address for workers (e.g., "127.0.0.1")
    pub base_address: String,
    /// Optional explicit worker ports (overrides base_port if specified)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_ports: Option<Vec<u16>>,
}

impl WorkerConfiguration {
    /// Get the address for a specific worker
    pub fn get_worker_address(&self, worker_id: WorkerId) -> Option<String> {
        if worker_id >= self.num_workers {
            return None;
        }
        
        let port = if let Some(ports) = &self.worker_ports {
            // Use explicit ports if specified
            ports.get(worker_id as usize).copied()?
        } else {
            // Fall back to sequential ports from base_port
            self.base_port + worker_id as u16
        };
        
        Some(format!("{}:{}", self.base_address, port))
    }
    
    /// Get all worker addresses
    pub fn get_all_worker_addresses(&self) -> HashMap<WorkerId, String> {
        (0..self.num_workers)
            .map(|id| (id, self.get_worker_address(id).unwrap()))
            .collect()
    }
}

/// Committee information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Committee {
    /// Current epoch
    pub epoch: Epoch,
    /// Mapping of authority to its information
    pub authorities: HashMap<PublicKey, Authority>,
    /// Total stake
    pub total_stake: Stake,
}

impl Committee {
    /// Create a committee with proper worker configuration
    pub fn new_for_test(
        epoch: Epoch, 
        authorities: HashMap<PublicKey, Stake>,
        num_workers_per_authority: u32,
        base_port: u16,
    ) -> Self {
        let mut port_offset = 0u16;
        let authorities = authorities.into_iter().enumerate().map(|(idx, (key, stake))| {
            // Each authority gets a unique primary port
            let primary_port = 8000 + idx as u16;
            
            // Workers for this authority start at base_port + offset
            let worker_base_port = base_port + port_offset;
            port_offset += num_workers_per_authority as u16;
            
            let authority = Authority {
                stake,
                primary_address: format!("127.0.0.1:{}", primary_port),
                network_key: key.clone(),
                workers: WorkerConfiguration {
                    num_workers: num_workers_per_authority,
                    base_port: worker_base_port,
                    base_address: "127.0.0.1".to_string(),
                    worker_ports: None,
                },
            };
            (key, authority)
        }).collect();
        
        Self::new(epoch, authorities)
    }
    
    /// Create a simple committee for backward compatibility
    pub fn new_simple(epoch: Epoch, authorities: HashMap<PublicKey, Stake>) -> Self {
        // Default to 4 workers per authority, starting at port 9000
        Self::new_for_test(epoch, authorities, 4, 9000)
    }
    
    /// Create a new committee
    pub fn new(epoch: Epoch, authorities: HashMap<PublicKey, Authority>) -> Self {
        let total_stake = authorities.values().map(|a| a.stake).sum();
        Self {
            epoch,
            authorities,
            total_stake,
        }
    }
    
    /// Get the stake of an authority
    pub fn stake(&self, authority: &PublicKey) -> Stake {
        self.authorities.get(authority).map(|a| a.stake).unwrap_or(0)
    }
    
    /// Get authority information
    pub fn authority(&self, name: &PublicKey) -> Option<&Authority> {
        self.authorities.get(name)
    }
    
    /// Get the quorum threshold (2/3 + 1)
    pub fn quorum_threshold(&self) -> Stake {
        (self.total_stake * 2) / 3 + 1
    }
    
    /// Get the validity threshold (1/3 + 1)
    pub fn validity_threshold(&self) -> Stake {
        self.total_stake / 3 + 1
    }
    
    /// Get the total stake of all authorities
    pub fn total_stake(&self) -> Stake {
        self.total_stake
    }
    
    /// Select the leader for a given round
    pub fn leader(&self, round: Round) -> &PublicKey {
        // Sort keys by their base64 encoding for consistent ordering
        let mut sorted_keys: Vec<_> = self.authorities.keys().collect();
        sorted_keys.sort_by_key(|k| k.encode_base64());
        
        let leader_index = (round as usize) % sorted_keys.len();
        sorted_keys[leader_index]
    }
    
    /// Get all authority keys in consistent order
    pub fn keys(&self) -> Vec<&PublicKey> {
        let mut keys: Vec<_> = self.authorities.keys().collect();
        keys.sort_by_key(|k| k.encode_base64());
        keys
    }
}

/// A header digest
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HeaderDigest([u8; 32]);

impl From<HeaderDigest> for Digest {
    fn from(hd: HeaderDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let encoded = base64::encode(self.0);
        let display_str = encoded.get(0..16).unwrap_or(&encoded);
        write!(f, "{}", display_str)
    }
}

impl HeaderDigest {
    /// Convert to a 32-byte array
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl From<HeaderDigest> for B256 {
    fn from(hd: HeaderDigest) -> Self {
        B256::from(hd.0)
    }
}

/// A certificate digest
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CertificateDigest([u8; 32]);

impl From<CertificateDigest> for Digest {
    fn from(cd: CertificateDigest) -> Self {
        Digest::new(cd.0)
    }
}

impl CertificateDigest {
    /// Create a new certificate digest from bytes
    pub fn new(digest: [u8; 32]) -> CertificateDigest {
        CertificateDigest(digest)
    }
    
    /// Get the raw bytes of this digest
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    
    /// Convert to a 32-byte array
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

// Implementation for conversion to B256 (alloy_primitives FixedBytes<32>)
impl From<CertificateDigest> for alloy_primitives::FixedBytes<32> {
    fn from(digest: CertificateDigest) -> Self {
        alloy_primitives::FixedBytes::from(digest.0)
    }
}

impl fmt::Debug for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let encoded = base64::encode(self.0);
        let display_str = encoded.get(0..16).unwrap_or(&encoded);
        write!(f, "{}", display_str)
    }
}

/// A header in the DAG
#[derive(Builder, Clone, Default, Deserialize, Serialize)]
#[builder(pattern = "owned", build_fn(skip))]
pub struct Header {
    /// The author of this header
    pub author: PublicKey,
    /// The round number
    pub round: Round,
    /// The epoch
    pub epoch: Epoch,
    /// Payload batches and their worker assignments
    #[serde(with = "indexmap::map::serde_seq")]
    pub payload: IndexMap<BatchDigest, WorkerId>,
    /// Parent certificates this header builds on
    pub parents: BTreeSet<CertificateDigest>,
    /// The header digest
    pub id: HeaderDigest,
    /// Author's signature
    pub signature: Signature,
}

impl HeaderBuilder {
    /// Build a header with the given signer using real cryptography
    pub fn build<F>(self, signer: &F) -> Result<Header, fastcrypto::traits::Error>
    where
        F: Signer<Signature>,
    {
        let h = Header {
            author: self.author.unwrap(),
            round: self.round.unwrap(),
            epoch: self.epoch.unwrap(),
            payload: self.payload.unwrap(),
            parents: self.parents.unwrap(),
            id: HeaderDigest::default(),
            signature: Signature::default(),
        };

        let id = h.digest();
        let id_digest: Digest = id.into();
        let signature = signer.try_sign(id_digest.as_ref())?;

        Ok(Header {
            id,
            signature,
            ..h
        })
    }
}

impl Hash for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let hasher_update = |hasher: &mut blake2::VarBlake2b| {
            hasher.update(&self.author);
            hasher.update(self.round.to_le_bytes());
            hasher.update(self.epoch.to_le_bytes());
            for (x, y) in self.payload.iter() {
                hasher.update(Digest::from(*x));
                hasher.update(y.to_le_bytes());
            }
            for x in self.parents.iter() {
                hasher.update(Digest::from(*x))
            }
        };
        HeaderDigest(fastcrypto::blake2b_256(hasher_update))
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}({}, E{}, {}B)",
            self.id,
            self.round,
            self.author.encode_base64(),
            self.epoch,
            self.payload.keys().map(|x| Digest::from(*x).size()).sum::<usize>(),
        )
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

/// A vote on a header
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vote {
    /// Header being voted on
    pub id: HeaderDigest,
    /// Round of the header
    pub round: Round,
    /// Epoch of the header
    pub epoch: Epoch,
    /// Original author of the header
    pub origin: PublicKey,
    /// Voter's public key
    pub author: PublicKey,
    /// Voter's signature
    pub signature: Signature,
}

impl Vote {
    // Note: Vote creation requires proper signing. Use either:
    // - new_with_signer() for synchronous signing (e.g., in tests)
    // - new_with_signature_service() for async signing (production code)

    /// Create a new vote with signer using real cryptography
    pub fn new_with_signer<S>(header: &Header, author: &PublicKey, signer: &S) -> Self
    where
        S: Signer<Signature>,
    {
        let vote = Self {
            id: header.id,
            round: header.round,
            epoch: header.epoch,
            origin: header.author.clone(),
            author: author.clone(),
            signature: Signature::default(),
        };

        let vote_digest: Digest = vote.digest().into();
        let signature = signer.try_sign(vote_digest.as_ref()).unwrap_or_default();

        Self { signature, ..vote }
    }
    
    /// Create a new vote with signature service using real cryptography
    pub async fn new_with_signature_service(
        header: &Header, 
        author: &PublicKey, 
        signature_service: &mut fastcrypto::SignatureService<Signature>
    ) -> Self {
        let vote = Self {
            id: header.id,
            round: header.round,
            epoch: header.epoch,
            origin: header.author.clone(),
            author: author.clone(),
            signature: Signature::default(),
        };

        let vote_digest: Digest = vote.digest().into();
        let signature = signature_service.request_signature(vote_digest).await;

        Self { signature, ..vote }
    }
    
    /// Verify this vote against the committee
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the vote is from the correct epoch
        if self.epoch != committee.epoch {
            return Err(DagError::InvalidEpoch {
                expected: committee.epoch,
                received: self.epoch,
            });
        }

        // Ensure the authority has voting rights
        if committee.stake(&self.author) == 0 {
            return Err(DagError::UnknownAuthority(self.author.encode_base64()));
        }

        // Check the signature
        let vote_digest: Digest = self.digest().into();
        self.author.verify(vote_digest.as_ref(), &self.signature)
            .map_err(DagError::InvalidSignature)
    }
}

/// A vote digest
#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct VoteDigest([u8; 32]);

impl From<VoteDigest> for Digest {
    fn from(vd: VoteDigest) -> Self {
        Digest::new(vd.0)
    }
}

impl fmt::Debug for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let encoded = base64::encode(self.0);
        let display_str = encoded.get(0..16).unwrap_or(&encoded);
        write!(f, "{}", display_str)
    }
}

impl Hash for Vote {
    type TypedDigest = VoteDigest;

    fn digest(&self) -> VoteDigest {
        let hasher_update = |hasher: &mut blake2::VarBlake2b| {
            hasher.update(Digest::from(self.id));
            hasher.update(self.round.to_le_bytes());
            hasher.update(self.epoch.to_le_bytes());
            hasher.update(&self.origin);
        };

        VoteDigest(fastcrypto::blake2b_256(hasher_update))
    }
}

/// A certificate containing a header and votes
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    /// The header being certified
    pub header: Header,
    /// Aggregated signature from voters
    aggregated_signature: AggregateSignature,
    /// Bitmap of signing authorities
    #[serde_as(as = "NarwhalBitmap")]
    signed_authorities: roaring::RoaringBitmap,
}

impl Certificate {
    /// Create genesis certificates for a committee
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| Self {
                header: Header {
                    author: name.clone(),
                    epoch: committee.epoch,
                    ..Header::default()
                },
                ..Self::default()
            })
            .collect()
    }
    
    /// Create a new certificate from a header and votes
    pub fn new(
        committee: &Committee,
        header: Header,
        votes: Vec<(PublicKey, Signature)>,
    ) -> DagResult<Certificate> {
        let mut votes = votes;
        votes.sort_by_key(|(pk, _)| pk.clone());
        let mut votes: std::collections::VecDeque<_> = votes.into_iter().collect();

        let mut weight = 0;
        let keys = committee.keys();
        let mut sigs = Vec::new();

        let filtered_votes = keys
            .iter()
            .enumerate()
            .filter(|(_, &pk)| {
                if !votes.is_empty() && pk == &votes.front().unwrap().0 {
                    sigs.push(votes.pop_front().unwrap());
                    weight += committee.stake(pk);
                    // If there are repeats, also remove them
                    while !votes.is_empty() && votes.front().unwrap() == sigs.last().unwrap() {
                        votes.pop_front().unwrap();
                    }
                    return true;
                }
                false
            })
            .map(|(index, _)| index as u32);

        let signed_authorities = roaring::RoaringBitmap::from_sorted_iter(filtered_votes)
            .map_err(|_| DagError::InvalidBitmap("Failed to convert votes into bitmap".to_string()))?;

        // Ensure that all authorities in the set of votes are known
        if !votes.is_empty() {
            return Err(DagError::UnknownAuthority(votes.front().unwrap().0.encode_base64()));
        }

        // Ensure that the authorities have enough weight
        if weight < committee.quorum_threshold() {
            return Err(DagError::CertificateRequiresQuorum);
        }

        let aggregated_signature = if sigs.is_empty() {
            AggregateSignature::default()
        } else {
            AggregateSignature::aggregate(sigs.into_iter().map(|(_, sig)| sig).collect())
                .map_err(DagError::InvalidSignature)?
        };

        Ok(Certificate {
            header,
            aggregated_signature,
            signed_authorities,
        })
    }
    
    /// Get the round of this certificate
    pub fn round(&self) -> Round {
        self.header.round
    }
    
    /// Get the epoch of this certificate
    pub fn epoch(&self) -> Epoch {
        self.header.epoch
    }
    
    /// Get the origin (author) of this certificate
    pub fn origin(&self) -> PublicKey {
        self.header.author.clone()
    }
    
    /// Verify this certificate against the committee
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Verify the embedded header first
        let header_digest: Digest = self.header.id.into();
        self.header.author.verify(header_digest.as_ref(), &self.header.signature)
            .map_err(DagError::InvalidSignature)?;
        
        // Verify certificate has valid quorum
        let committee_keys: Vec<&PublicKey> = committee.authorities.keys().collect();
        let mut total_stake = 0u64;
        
        for (index, &authority) in committee_keys.iter().enumerate() {
            if self.signed_authorities.contains(index as u32) {
                total_stake += committee.stake(authority);
            }
        }
        
        if total_stake < committee.quorum_threshold() {
            return Err(DagError::CertificateRequiresQuorum);
        }

        Ok(())
    }
    
    /// Get the aggregated signature
    pub fn aggregated_signature(&self) -> &AggregateSignature {
        &self.aggregated_signature
    }
    
    /// Get signers' public keys and their signatures  
    pub fn signers(&self, committee: &Committee) -> Vec<(PublicKey, Signature)> {
        let committee_keys: Vec<&PublicKey> = committee.authorities.keys().collect();
        let mut signers = Vec::new();
        
        for (index, &authority) in committee_keys.iter().enumerate() {
            if self.signed_authorities.contains(index as u32) {
                // BLS aggregate signatures cannot be decomposed back to individual signatures
                // This is a cryptographic property of BLS signatures
                // Return the authority with a default signature as individual signatures are not recoverable
                signers.push((authority.clone(), Signature::default()));
            }
        }
        
        signers
    }
}

impl Hash for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(fastcrypto::blake2b_256(|hasher: &mut blake2::VarBlake2b| {
            hasher.update(Digest::from(self.header.digest()));
        }))
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({}, E{}, {})",
            self.digest(),
            self.round(),
            self.origin().encode_base64(),
            self.epoch(),
            self.header.payload.len()
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

/// Bitmap serialization helper
#[derive(Debug)]
pub struct NarwhalBitmap;

impl serde_with::SerializeAs<roaring::RoaringBitmap> for NarwhalBitmap {
    fn serialize_as<S>(
        source: &roaring::RoaringBitmap,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = vec![];
        source
            .serialize_into(&mut bytes)
            .map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> serde_with::DeserializeAs<'de, roaring::RoaringBitmap> for NarwhalBitmap {
    fn deserialize_as<D>(deserializer: D) -> Result<roaring::RoaringBitmap, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
        roaring::RoaringBitmap::deserialize_from(&bytes[..])
            .map_err(serde::de::Error::custom)
    }
} 