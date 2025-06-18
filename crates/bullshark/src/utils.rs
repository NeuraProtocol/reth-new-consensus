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
    let mut sequence = Vec::new();
    let mut current_leader = leader_certificate.clone();
    let mut visited = HashSet::new();

    loop {
        let leader_digest = current_leader.digest();
        
        // Avoid infinite loops
        if visited.contains(&leader_digest) {
            break;
        }
        visited.insert(leader_digest);

        sequence.push(current_leader.clone());
        debug!("Added leader {} from round {} to sequence", 
               current_leader.origin(), current_leader.round());

        // Find the previous leader this one references
        let current_round = current_leader.round();
        if current_round < 2 {
            // No more leaders to process
            break;
        }

        // Look for the previous leader (two rounds back for Bullshark)
        let prev_leader_round = current_round - 2;
        
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
                current_leader = leader;
            }
            None => {
                // No valid previous leader found
                break;
            }
        }
    }

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
    // Get the leader for the target round
    if let Some(leader_cert) = dag.get_leader_certificate(target_round, committee) {
        let leader_digest = leader_cert.digest();
        
        // Check if the current certificate (or its ancestors) reference this leader
        if certificate_references_leader(certificate, &leader_digest, dag) {
            return Some(leader_cert.clone());
        }
    }
    
    None
}

/// Check if a certificate references a specific leader (directly or transitively)
fn certificate_references_leader(
    certificate: &Certificate,
    leader_digest: &CertificateDigest,
    dag: &BullsharkDag,
) -> bool {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    
    // Start with the certificate's parents
    for parent_digest in &certificate.header.parents {
        queue.push_back(*parent_digest);
    }

    while let Some(current_digest) = queue.pop_front() {
        if visited.contains(&current_digest) {
            continue;
        }
        visited.insert(current_digest);

        // Check if this is the leader we're looking for
        if current_digest == *leader_digest {
            return true;
        }

        // Add this certificate's parents to the queue
        if let Some(cert) = dag.get_certificate(&current_digest) {
            for parent_digest in &cert.header.parents {
                queue.push_back(*parent_digest);
            }
        }
    }

    false
}

/// Order the DAG sub-graph referenced by a leader certificate
/// Returns certificates in topological order for execution
pub fn order_dag(gc_depth: Round, leader: &Certificate, dag: &BullsharkDag) -> Vec<Certificate> {
    let mut sequence = Vec::new();
    let mut visited = HashSet::new();
    
    // Perform topological sort starting from the leader
    topological_sort_from_certificate(leader, dag, &mut visited, &mut sequence, gc_depth);
    
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