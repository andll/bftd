mod test_cluster;

use crate::test_cluster::TestCluster;
use anyhow::bail;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::{fs, process};

#[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
enum Args {
    /// Name of the person to greet
    // #[arg(short, long)]
    NewChain(NewChainArgs),
}

#[derive(Parser, Debug)]
struct NewChainArgs {
    name: String,
    peer_addresses: Vec<SocketAddr>,
    #[arg(long, short = 'b')]
    bind: Option<String>,
}

fn main() {
    let args = Args::parse();
    if let Err(err) = handle_args(args) {
        println!("error: {err}");
        process::exit(1);
    }
}

fn handle_args(args: Args) -> anyhow::Result<()> {
    match args {
        Args::NewChain(new_chain) => handle_new_chain(new_chain),
    }
}
fn handle_new_chain(new_chain: NewChainArgs) -> anyhow::Result<()> {
    let clusters_path = PathBuf::from("clusters");
    fs::create_dir_all(&clusters_path)?;
    let path = clusters_path.join(&new_chain.name);
    if let Err(err) = fs::create_dir(&path) {
        bail!("Failed to create cluster directory at {path:?}: {err}");
    }
    println!("Generating new chain");
    println!("Name: {}", new_chain.name);
    println!("Peers: {:?}", new_chain.peer_addresses);
    if new_chain.peer_addresses.len() < 4 {
        bail!("Chain must have at least 4 peers");
    }
    let test_cluster =
        TestCluster::generate(&new_chain.name, new_chain.peer_addresses, new_chain.bind);
    println!("Storing test cluster into {path:?}");
    test_cluster.store_into(&path)?;
    Ok(())
}
