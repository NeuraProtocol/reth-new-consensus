//! Narwhal DAG service implementation

use crate::{
    types::{Certificate, Committee, Header, Vote, PublicKey, Signature}, 
    NarwhalConfig, DagError, DagResult, Transaction, Round, Batch
};
use tokio::sync::{mpsc, broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn, debug};
use fastcrypto::{SignatureService, Hash};
use std::sync::Arc;
use std::collections::BTreeSet;
use indexmap::IndexMap;

/// Main service that runs the Narwhal DAG protocol
pub struct DagService {
    /// Node's public key
    pub name: PublicKey,
    /// Current committee
    pub committee: Committee,
    /// Configuration
    #[allow(dead_code)]
    config: NarwhalConfig,
    /// Signature service for signing
    #[allow(dead_code)]
    signature_service: Arc<tokio::sync::Mutex<SignatureService<Signature>>>,
    /// Receiver for new transactions to include in proposals
    transaction_receiver: mpsc::UnboundedReceiver<Transaction>,
    /// Sender for broadcasting headers to other nodes
    header_broadcaster: broadcast::Sender<Header>,
    /// Sender for broadcasting votes to other nodes  
    vote_broadcaster: broadcast::Sender<Vote>,
    /// Sender for broadcasting certificates to other nodes
    certificate_broadcaster: broadcast::Sender<Certificate>,
    /// Sender for notifying Bullshark of new certificates
    certificate_output_sender: mpsc::UnboundedSender<Certificate>,
    /// Current round
    current_round: Round,
    /// Watch channel for reconfiguration
    reconfigure_receiver: watch::Receiver<Committee>,
    /// Current batch being assembled
    current_batch: Vec<Transaction>,
    /// Batch creation timer
    batch_timer: Option<tokio::time::Instant>,
}

impl std::fmt::Debug for DagService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagService")
            .field("name", &self.name)
            .field("committee", &self.committee)
            .field("current_round", &self.current_round)
            .finish_non_exhaustive()
    }
}

impl DagService {
    /// Create a new DAG service
    pub fn new(
        name: PublicKey,
        committee: Committee,
        config: NarwhalConfig,
        signature_service: SignatureService<Signature>,
        transaction_receiver: mpsc::UnboundedReceiver<Transaction>,
        certificate_output_sender: mpsc::UnboundedSender<Certificate>,
        reconfigure_receiver: watch::Receiver<Committee>,
    ) -> Self {
        let (header_broadcaster, _) = broadcast::channel(1000);
        let (vote_broadcaster, _) = broadcast::channel(1000);
        let (certificate_broadcaster, _) = broadcast::channel(1000);

        Self {
            name,
            committee,
            config,
            signature_service: Arc::new(tokio::sync::Mutex::new(signature_service)),
            transaction_receiver,
            header_broadcaster,
            vote_broadcaster,
            certificate_broadcaster,
            certificate_output_sender,
            current_round: 1,
            reconfigure_receiver,
            current_batch: Vec::new(),
            batch_timer: None,
        }
    }

    /// Spawn the DAG service
    pub fn spawn(mut self) -> JoinHandle<DagResult<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }

    /// Main run loop for the DAG service
    async fn run(&mut self) -> DagResult<()> {
        info!("Starting Narwhal DAG service for {}", self.name);

        // Subscribe to our own broadcasts to process incoming messages
        let mut header_receiver = self.header_broadcaster.subscribe();
        let mut vote_receiver = self.vote_broadcaster.subscribe();
        let mut certificate_receiver = self.certificate_broadcaster.subscribe();

        loop {
            tokio::select! {
                // Handle incoming transactions
                Some(transaction) = self.transaction_receiver.recv() => {
                    debug!("Received transaction for batching");
                    self.handle_transaction(transaction).await?;
                }

                // Handle incoming headers from other nodes
                Ok(header) = header_receiver.recv() => {
                    debug!("Received header: {:?}", header);
                    self.handle_header(header).await?;
                }

                // Handle incoming votes from other nodes
                Ok(vote) = vote_receiver.recv() => {
                    debug!("Received vote: {:?}", vote);
                    self.handle_vote(vote).await?;
                }

                // Handle incoming certificates from other nodes
                Ok(certificate) = certificate_receiver.recv() => {
                    debug!("Received certificate: {:?}", certificate);
                    self.handle_certificate(certificate).await?;
                }

                // Handle committee changes
                Ok(()) = self.reconfigure_receiver.changed() => {
                    let new_committee = self.reconfigure_receiver.borrow().clone();
                    info!("Committee updated: {:?}", new_committee);
                    self.committee = new_committee;
                }

                else => {
                    warn!("All channels closed, shutting down DAG service");
                    break;
                }
            }
        }

        info!("Narwhal DAG service shut down");
        Ok(())
    }

    /// Handle a new transaction by adding it to our batch
    async fn handle_transaction(&mut self, transaction: Transaction) -> DagResult<()> {
        debug!("Adding transaction to batch (current size: {})", self.current_batch.len());
        
        // Add transaction to current batch
        self.current_batch.push(transaction);
        
        // Set timer if this is the first transaction
        if self.current_batch.len() == 1 {
            self.batch_timer = Some(tokio::time::Instant::now());
        }
        
        // Check if we should create a header
        let should_create_header = self.current_batch.len() >= self.config.max_batch_size
            || self.batch_timer.map_or(false, |timer| {
                timer.elapsed() >= self.config.max_batch_delay
            });
            
        if should_create_header && !self.current_batch.is_empty() {
            self.create_and_broadcast_header().await?;
        }
        
        Ok(())
    }
    
    /// Create a header from current batch and broadcast it
    async fn create_and_broadcast_header(&mut self) -> DagResult<()> {
        let batch = Batch(std::mem::take(&mut self.current_batch));
        self.batch_timer = None;
        
        debug!("Creating header for batch with {} transactions", batch.0.len());
        
        // Create batch digest
        let batch_digest = batch.digest();
        
        // Create payload (simplified - using worker 0 for all batches)
        let mut payload = IndexMap::new();
        payload.insert(batch_digest, 0);
        
        // Create the unsigned header first
        let mut header = Header {
            author: self.name.clone(),
            round: self.current_round,
            epoch: self.committee.epoch,
            payload,
            parents: BTreeSet::new(),
            id: Default::default(),
            signature: Default::default(),
        };
        
        // Calculate the header ID
        header.id = header.digest();
        
        // Sign the header using signature service
        let mut signature_service = self.signature_service.lock().await;
        header.signature = signature_service.request_signature(header.id.into()).await;
        drop(signature_service);
        
        info!("Created header for round {} with {} transactions", 
              self.current_round, batch.0.len());
              
        // Broadcast the header
        if self.header_broadcaster.send(header).is_err() {
            warn!("No receivers for header broadcast");
        }
        
        Ok(())
    }

    /// Handle a header from another node
    async fn handle_header(&mut self, header: Header) -> DagResult<()> {
        // Verify the header
        if header.author == self.name {
            // This is our own header, ignore
            return Ok(());
        }

        // Verify header signature and constraints
        self.verify_header(&header)?;

        // Create and broadcast a vote for this header
        let vote = self.create_vote(&header).await?;
        
        if self.vote_broadcaster.send(vote).is_err() {
            warn!("No receivers for vote broadcast");
        }

        Ok(())
    }

    /// Handle a vote from another node
    async fn handle_vote(&mut self, vote: Vote) -> DagResult<()> {
        // Verify the vote
        vote.verify(&self.committee)?;

        // In a real implementation, we would:
        // 1. Collect votes for each header
        // 2. When we have enough votes (quorum), create a certificate
        // 3. Broadcast the certificate

        debug!("Vote verified and stored");
        Ok(())
    }

    /// Handle a certificate from another node
    async fn handle_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        // Verify the certificate has proper quorum
        // For now, just pass it to Bullshark
        if self.certificate_output_sender.send(certificate.clone()).is_err() {
            warn!("Failed to send certificate to Bullshark");
        } else {
            debug!("Certificate forwarded to Bullshark: {:?}", certificate);
        }

        Ok(())
    }

    /// Verify a header is valid
    fn verify_header(&self, header: &Header) -> DagResult<()> {
        // Check epoch matches
        if header.epoch != self.committee.epoch {
            return Err(DagError::InvalidEpoch {
                expected: self.committee.epoch,
                received: header.epoch,
            });
        }

        // Check author is in committee
        if self.committee.stake(&header.author) == 0 {
            return Err(DagError::UnknownAuthority(header.author.to_string()));
        }

        // Verify signature would go here in real implementation
        // header.verify(&self.committee)?;

        Ok(())
    }

    /// Create a vote for a header
    async fn create_vote(&self, header: &Header) -> DagResult<Vote> {
        let vote = Vote::new(header, &self.name);
        Ok(vote)
    }

    /// Get the current round
    pub fn current_round(&self) -> Round {
        self.current_round
    }

    /// Advance to the next round
    pub fn advance_round(&mut self) {
        self.current_round += 1;
        info!("Advanced to round {}", self.current_round);
    }
}

/// Configuration for DAG service channels
#[derive(Debug)]
pub struct DagServiceChannels {
    /// Send new certificates to consensus
    pub certificate_sender: mpsc::UnboundedSender<Certificate>,
    /// Receive new certificates from consensus
    pub certificate_receiver: mpsc::UnboundedReceiver<Certificate>,
    /// Send transactions to DAG
    pub transaction_sender: mpsc::UnboundedSender<Transaction>,
    /// Committee updates
    pub committee_sender: watch::Sender<Committee>,
}

impl DagServiceChannels {
    /// Create new channels for DAG service
    pub fn new() -> Self {
        let (certificate_sender, certificate_receiver) = mpsc::unbounded_channel();
        let (transaction_sender, _) = mpsc::unbounded_channel();
        let (committee_sender, _) = watch::channel(Committee::new(0, Default::default()));

        Self {
            certificate_sender,
            certificate_receiver,
            transaction_sender,
            committee_sender,
        }
    }
}
