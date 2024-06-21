mod test_cluster;

use std::net::SocketAddr;
use std::process;
use anyhow::bail;
use clap::Parser;
use crate::test_cluster::TestCluster;

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
        Args::NewChain(new_chain) => {
            handle_new_chain(new_chain)
        }
    }
}
fn handle_new_chain(new_chain: NewChainArgs) -> anyhow::Result<()> {
    println!("Generating new chain");
    println!("Name: {}", new_chain.name);
    println!("Peers: {:?}", new_chain.peer_addresses);
    if new_chain.peer_addresses.len() < 4 {
        bail!("Chain must have at least 4 peers");
    }
    let _test_cluster = TestCluster::generate(&new_chain.name, new_chain.peer_addresses);
    Ok(())
}
