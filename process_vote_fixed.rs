    /// Process vote (adapted from reference implementation)  
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing vote from {} for header {}", vote.author, vote.id);
        
        // Following reference: votes have already been validated by sanitize_vote
        // to ensure they are for our current header
        
        // Store vote in persistent storage
        self.storage.store_vote(vote.id, vote.clone()).await?;
        
        // Add vote to aggregator
        self.votes_aggregator.add_vote(vote, &self.committee)?;
        
        // Check if we can form a certificate
        if let Some(certificate) = self.votes_aggregator.try_form_certificate(&self.committee)? {
            let header_id = certificate.header.id;
            info!("Created certificate for header {} with {} votes", 
                  header_id, self.votes_aggregator.vote_count());
            
            // Send certificate for network broadcast
            if let Some(ref sender) = self.network_sender {
                if sender.send(DagMessage::Certificate(certificate.clone())).is_err() {
                    warn!("Failed to send certificate for broadcast - channel closed");
                } else {
                    debug!("Sent certificate for network broadcast");
                }
            }
            
            // Process the certificate
            self.process_certificate(certificate).await?;
            
            // Reset aggregator after forming certificate (ready for next header)
            self.votes_aggregator = VotesAggregator::new();
        }

        Ok(())
    }