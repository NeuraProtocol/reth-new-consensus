//! Utility functions for Bullshark consensus

use crate::dag::{Dag, BullsharkDag};
use narwhal::{
    Round,
    types::{Certificate, CertificateDigest, Committee},
};
use fastcrypto::Hash;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::debug;

/// Order leaders according to Bullshark protocol
/// Returns leaders in reverse chronological order (newest first)
pub fn order_leaders<F>(
    committee: &Committee,
    leader_certificate: &Certificate,
    dag: &BullsharkDag,
    leader_fn: F,
) -> Vec<Certificate>
where
    F: for<'a> Fn(&'a Committee, Round, &'a Dag) -> Option<&'a (CertificateDigest, Certificate)>,
{
    use tracing::info;
    
    info!("CONSENSUS RULE: Starting leader ordering from {} at round {}", 
          leader_certificate.origin(), leader_certificate.round());
    
    let mut sequence = Vec::new();
    let mut current_leader = leader_certificate.clone();
    let mut visited = HashSet::new();

    loop {
        let leader_digest = current_leader.digest();
        
        // Avoid infinite loops
        if visited.contains(&leader_digest) {
            info!("CONSENSUS RULE: Already visited leader {}, stopping to avoid loop", leader_digest);
            break;
        }
        visited.insert(leader_digest);

        sequence.push(current_leader.clone());
        info!("CONSENSUS RULE: Added leader {} from round {} to commit sequence (position {})", 
               current_leader.origin(), current_leader.round(), sequence.len());

        // Find the previous leader this one references
        let current_round = current_leader.round();
        if current_round < 2 {
            info!("CONSENSUS RULE: Round {} is too low for previous leaders, stopping", current_round);
            break;
        }

        // Look for the previous leader (two rounds back for Bullshark)
        let prev_leader_round = current_round - 2;
        info!("CONSENSUS RULE: Looking for previous leader at round {} (current - 2)", prev_leader_round);
        
        // Find if current leader references the previous leader
        let prev_leader = find_referenced_leader(
            &current_leader,
            prev_leader_round,
            committee,
            dag,
            &leader_fn,
        );

        match prev_leader {
            Some(leader) => {
                info!("CONSENSUS RULE: Found previous leader {} at round {}, continuing chain", 
                      leader.origin(), prev_leader_round);
                current_leader = leader;
            }
            None => {
                info!("CONSENSUS RULE: No valid previous leader found at round {}, stopping chain", 
                      prev_leader_round);
                break;
            }
        }
    }

    info!("CONSENSUS RULE: Leader sequence complete with {} leaders to commit", sequence.len());
    sequence
}

/// Find a leader that is referenced by the current certificate
fn find_referenced_leader<F>(
    certificate: &Certificate,
    target_round: Round,
    committee: &Committee,
    dag: &BullsharkDag,
    _leader_fn: &F,
) -> Option<Certificate>
where
    F: for<'a> Fn(&'a Committee, Round, &'a Dag) -> Option<&'a (CertificateDigest, Certificate)>,
{
    use tracing::{info, warn};
    
    info!("CONSENSUS RULE: Searching for leader at round {} referenced by certificate from {}", 
          target_round, certificate.origin());
    
    // Get the leader for the target round
    if let Some(leader_cert) = dag.get_leader_certificate(target_round, committee) {
        let leader_digest = leader_cert.digest();
        info!("CONSENSUS RULE: Found leader {} at round {}, checking if referenced", 
              leader_cert.origin(), target_round);
        
        // Check if the current certificate (or its ancestors) reference this leader
        if certificate_references_leader(certificate, &leader_digest, dag) {
            info!("CONSENSUS RULE: Leader {} IS referenced by certificate chain - COMMIT", 
                  leader_cert.origin());
            return Some(leader_cert.clone());
        } else {
            warn!("CONSENSUS RULE: Leader {} is NOT referenced by certificate chain - SKIP", 
                  leader_cert.origin());
        }
    } else {
        warn!("CONSENSUS RULE: No leader certificate found at round {}", target_round);
    }
    
    None
}

/// Check if a certificate references a specific leader (directly or transitively)
fn certificate_references_leader(
    certificate: &Certificate,
    leader_digest: &CertificateDigest,
    dag: &BullsharkDag,
) -> bool {
    use tracing::info;
    
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut traversal_depth = 0;
    
    info!("CONSENSUS RULE: Checking if certificate references leader {} via parent traversal", leader_digest);
    
    // Start with the certificate's parents
    for parent_digest in &certificate.header.parents {
        queue.push_back((*parent_digest, 1));
    }
    info!("CONSENSUS RULE: Starting with {} parent certificates", certificate.header.parents.len());

    while let Some((current_digest, depth)) = queue.pop_front() {
        if visited.contains(&current_digest) {
            continue;
        }
        visited.insert(current_digest);
        traversal_depth = traversal_depth.max(depth);

        // Check if this is the leader we're looking for
        if current_digest == *leader_digest {
            info!("CONSENSUS RULE: Found leader reference at depth {} after visiting {} certificates", 
                  depth, visited.len());
            return true;
        }

        // Add this certificate's parents to the queue
        if let Some(cert) = dag.get_certificate(&current_digest) {
            for parent_digest in &cert.header.parents {
                queue.push_back((*parent_digest, depth + 1));
            }
        }
    }

    info!("CONSENSUS RULE: Leader NOT found after traversing {} certificates (max depth: {})", 
          visited.len(), traversal_depth);
    false
}

/// Order the DAG sub-graph referenced by a leader certificate
/// Returns certificates in topological order for execution
pub fn order_dag(gc_depth: Round, leader: &Certificate, dag: &BullsharkDag) -> Vec<Certificate> {
    order_dag_with_limit(gc_depth, leader, dag, 500)
}

/// Order the DAG sub-graph with a configurable limit
pub fn order_dag_with_limit(gc_depth: Round, leader: &Certificate, dag: &BullsharkDag, max_certificates: usize) -> Vec<Certificate> {
    let mut sequence = Vec::new();
    let mut visited = HashSet::new();
    
    // Perform topological sort starting from the leader
    topological_sort_from_certificate(leader, dag, &mut visited, &mut sequence, gc_depth);
    
    // When catching up after being offline, the DAG can contain thousands of certificates
    // Limit the returned sequence to prevent overwhelming the system
    if sequence.len() > max_certificates {
        debug!("DAG traversal found {} certificates, limiting to {} most recent", 
               sequence.len(), max_certificates);
        // Return the most recent certificates (end of the sequence)
        sequence = sequence.split_off(sequence.len() - max_certificates);
    }
    
    sequence
}

/// Perform topological sort starting from a certificate
fn topological_sort_from_certificate(
    certificate: &Certificate,
    dag: &BullsharkDag,
    visited: &mut HashSet<CertificateDigest>,
    sequence: &mut Vec<Certificate>,
    gc_depth: Round,
) {
    let cert_digest = certificate.digest();
    
    if visited.contains(&cert_digest) {
        return;
    }
    
    visited.insert(cert_digest);
    
    // First, recursively process all parents
    for parent_digest in &certificate.header.parents {
        if let Some(parent_cert) = dag.get_certificate(parent_digest) {
            // Only process if within GC depth
            if parent_cert.round() + gc_depth >= certificate.round() {
                topological_sort_from_certificate(parent_cert, dag, visited, sequence, gc_depth);
            }
        }
    }
    
    // Then add this certificate to the sequence
    sequence.push(certificate.clone());
    debug!("Added certificate {} to execution sequence", cert_digest);
}

/// Calculate the causal order of certificates for a given round
pub fn calculate_causal_order(
    certificates: &[Certificate],
    dag: &BullsharkDag,
) -> Vec<Certificate> {
    let mut ordered = Vec::new();
    let mut processed = HashSet::new();
    
    // Sort by certificate digest for deterministic ordering
    let mut sorted_certs = certificates.to_vec();
    sorted_certs.sort_by_key(|cert| cert.digest());
    
    for certificate in sorted_certs {
        if !processed.contains(&certificate.digest()) {
            let mut causal_sequence = Vec::new();
            let mut visited = HashSet::new();
            
            collect_causal_dependencies(
                &certificate,
                dag,
                &mut visited,
                &mut causal_sequence,
            );
            
            // Add all dependencies that haven't been processed yet
            for cert in causal_sequence {
                let digest = cert.digest();
                if !processed.contains(&digest) {
                    ordered.push(cert);
                    processed.insert(digest);
                }
            }
        }
    }
    
    ordered
}

/// Collect all causal dependencies of a certificate
fn collect_causal_dependencies(
    certificate: &Certificate,
    dag: &BullsharkDag,
    visited: &mut HashSet<CertificateDigest>,
    sequence: &mut Vec<Certificate>,
) {
    let cert_digest = certificate.digest();
    
    if visited.contains(&cert_digest) {
        return;
    }
    
    visited.insert(cert_digest);
    
    // First collect dependencies
    for parent_digest in &certificate.header.parents {
        if let Some(parent_cert) = dag.get_certificate(parent_digest) {
            collect_causal_dependencies(parent_cert, dag, visited, sequence);
        }
    }
    
    // Then add this certificate
    sequence.push(certificate.clone());
}

/// Check if a certificate is ready for processing
/// (all its dependencies are available)
pub fn is_certificate_ready(
    certificate: &Certificate,
    dag: &BullsharkDag,
) -> bool {
    for parent_digest in &certificate.header.parents {
        if dag.get_certificate(parent_digest).is_none() {
            return false;
        }
    }
    true
}

/// Get the immediate dependencies of a certificate
pub fn get_dependencies(certificate: &Certificate) -> Vec<CertificateDigest> {
    certificate.header.parents.iter().copied().collect()
}

/// Calculate the depth of a certificate in the DAG
pub fn calculate_certificate_depth(
    certificate: &Certificate,
    dag: &BullsharkDag,
    cache: &mut HashMap<CertificateDigest, usize>,
) -> usize {
    let cert_digest = certificate.digest();
    
    if let Some(&depth) = cache.get(&cert_digest) {
        return depth;
    }
    
    let mut max_parent_depth = 0;
    for parent_digest in &certificate.header.parents {
        if let Some(parent_cert) = dag.get_certificate(parent_digest) {
            let parent_depth = calculate_certificate_depth(parent_cert, dag, cache);
            max_parent_depth = max_parent_depth.max(parent_depth);
        }
    }
    
    let depth = max_parent_depth + 1;
    cache.insert(cert_digest, depth);
    depth
} 