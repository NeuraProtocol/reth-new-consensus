//! Core types for Narwhal DAG consensus

use crate::{DagError, DagResult, Round, WorkerId, BatchDigest};
use blake2::{digest::Update, VarBlake2b};
use fastcrypto::{
    traits::{AggregateAuthenticator, EncodeDecodeBase64, Signer},
    Hash, Digest, Verifier,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use base64;
use std::{
    collections::{BTreeSet, HashMap},
    fmt,
};
use derive_builder::Builder;
use indexmap::IndexMap;

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

/// Committee information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Committee {
    /// Current epoch
    pub epoch: Epoch,
    /// Mapping of authority to stake
    pub authorities: HashMap<PublicKey, Stake>,
    /// Total stake
    pub total_stake: Stake,
}

impl Committee {
    /// Create a new committee
    pub fn new(epoch: Epoch, authorities: HashMap<PublicKey, Stake>) -> Self {
        let total_stake = authorities.values().sum();
        Self {
            epoch,
            authorities,
            total_stake,
        }
    }
    
    /// Get the stake of an authority
    pub fn stake(&self, authority: &PublicKey) -> Stake {
        self.authorities.get(authority).copied().unwrap_or(0)
    }
    
    /// Get the quorum threshold (2/3 + 1)
    pub fn quorum_threshold(&self) -> Stake {
        (self.total_stake * 2) / 3 + 1
    }
    
    /// Get the validity threshold (1/3 + 1)
    pub fn validity_threshold(&self) -> Stake {
        self.total_stake / 3 + 1
    }
    
    /// Select the leader for a given round
    pub fn leader(&self, round: Round) -> &PublicKey {
        let leader_index = (round as usize) % self.authorities.len();
        self.authorities.keys().nth(leader_index).expect("Committee is not empty")
    }
    
    /// Get all authority keys
    pub fn keys(&self) -> Vec<&PublicKey> {
        self.authorities.keys().collect()
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

/// A certificate digest
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CertificateDigest([u8; 32]);

impl From<CertificateDigest> for Digest {
    fn from(cd: CertificateDigest) -> Self {
        Digest::new(cd.0)
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
    /// Build a header with the given signer (signing currently stubbed out)
    pub fn build<F>(self, _signer: &F) -> Result<Header, fastcrypto::traits::Error>
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

        Ok(Header {
            id: h.digest(),
            signature: Signature::default(), // TODO: Fix signing with correct fastcrypto API
            ..h
        })
    }
}

impl Hash for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let hasher_update = |hasher: &mut VarBlake2b| {
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
    /// Create a new vote (signature will be added later when signing is properly implemented)
    pub fn new(header: &Header, author: &PublicKey) -> Self {
        Self {
            id: header.id,
            round: header.round,
            epoch: header.epoch,
            origin: header.author.clone(),
            author: author.clone(),
            signature: Signature::default(), // TODO: Fix signing with correct fastcrypto API
        }
    }

    /// Create a new vote with signer (signing currently stubbed out)
    pub fn new_with_signer<S>(header: &Header, author: &PublicKey, _signer: &S) -> Self
    where
        S: Signer<Signature>,
    {
        // For now, just create a basic vote without signature due to trait conflicts
        // TODO: Implement proper signing once trait conflicts are resolved
        Self::new(header, author)
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
        let hasher_update = |hasher: &mut VarBlake2b| {
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
}

impl Hash for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(fastcrypto::blake2b_256(|hasher| {
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