use reth_node_ethereum::EthereumNode;
use reth_chainspec::ChainSpecBuilder;
use reth_provider::{
    providers::ReadOnlyConfig, BlockReader, HeaderProvider,
};

fn main() -> eyre::Result<()> {
    // Use the Neura node1 datadir
    let datadir = "/home/peastew/.neura/node1";
    
    println!("Opening database at: {}", datadir);
    
    // Build Neura chainspec (chainID 266)
    let spec = ChainSpecBuilder::default()
        .chain(266.into())
        .genesis_file(std::path::Path::new("/srv/tank/src/reth-new-consensus/crates/chainspec/res/genesis/neura-mainnet.json").into())
        .build();
        
    // Open the database in read-only mode
    let factory = EthereumNode::provider_factory_builder()
        .open_read_only(spec.into(), ReadOnlyConfig::from_datadir(datadir))?;
    
    let provider = factory.provider()?;
    
    // Check the latest block number
    println!("\n=== Database Status ===");
    match provider.last_block_number() {
        Ok(num) => println!("Last block number in DB: {}", num),
        Err(e) => println!("Error getting last block number: {}", e),
    }
    
    // Try to read blocks 0-10
    println!("\n=== Checking blocks 0-10 ===");
    for i in 0..=10 {
        match provider.block_by_number(i) {
            Ok(Some(block)) => {
                println!("Block {} exists - hash: {}, parent: {}, txs: {}", 
                    i, 
                    block.header.hash_slow(),
                    block.header.parent_hash,
                    block.body.transactions.len()
                );
            }
            Ok(None) => println!("Block {} not found", i),
            Err(e) => println!("Error reading block {}: {}", i, e),
        }
    }
    
    // Check blocks around what we saw in logs (70-85)
    println!("\n=== Checking blocks 70-85 ===");
    for i in 70..=85 {
        match provider.block_by_number(i) {
            Ok(Some(block)) => {
                println!("Block {} exists - hash: {}, parent: {}, txs: {}", 
                    i, 
                    block.header.hash_slow(),
                    block.header.parent_hash,
                    block.body.transactions.len()
                );
            }
            Ok(None) => println!("Block {} not found", i),
            Err(e) => println!("Error reading block {}: {}", i, e),
        }
    }
    
    // Check headers table separately
    println!("\n=== Checking headers ===");
    match provider.sealed_headers_range(0..10) {
        Ok(headers) => println!("Found {} headers in range 0-10", headers.len()),
        Err(e) => println!("Error reading headers: {}", e),
    }
    
    Ok(())
}