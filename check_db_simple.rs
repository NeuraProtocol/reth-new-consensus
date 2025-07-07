use std::process::Command;

fn main() {
    println!("Checking Reth database for blocks...\n");
    
    // Check eth_blockNumber on all nodes
    for port in [8545, 8546, 8547, 8548] {
        println!("Node on port {} - eth_blockNumber:", port);
        let output = Command::new("curl")
            .arg("-s")
            .arg("-X")
            .arg("POST")
            .arg(format!("http://localhost:{}", port))
            .arg("-H")
            .arg("Content-Type: application/json")
            .arg("-d")
            .arg(r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#)
            .output()
            .expect("Failed to execute curl");
        
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }
    
    println!("\nChecking logs for block execution...");
    
    // Check node logs for successful execution messages
    let output = Command::new("grep")
        .arg("-i")
        .arg("successfully executed and persisted")
        .arg("/home/peastew/.neura/node1/node.log")
        .output()
        .expect("Failed to execute grep");
    
    let log_lines = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = log_lines.lines().collect();
    
    if lines.is_empty() {
        println!("No blocks found in logs");
    } else {
        println!("Found {} block execution messages:", lines.len());
        // Show last 5
        for line in lines.iter().rev().take(5) {
            println!("{}", line);
        }
    }
}