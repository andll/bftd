mod node;
mod prometheus;
mod test_cluster;

use crate::test_cluster::TestCluster;
use anyhow::bail;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::{fs, process};

#[derive(Parser, Debug)]
enum Args {
    NewChain(NewChainArgs),
    LocalCluster(LocalClusterArgs),
}

#[derive(Parser, Debug)]
struct NewChainArgs {
    name: String,
    peer_addresses: Vec<SocketAddr>,
    #[arg(long, short = 'b')]
    bind: Option<String>,
    #[arg(long, short = 'p')]
    prometheus_bind: Option<SocketAddr>,
}

#[derive(Parser, Debug)]
struct LocalClusterArgs {
    name: String,
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
        Args::NewChain(args) => handle_new_chain(args),
        Args::LocalCluster(args) => handle_local_cluster(args),
    }
}
fn handle_new_chain(args: NewChainArgs) -> anyhow::Result<()> {
    let clusters_path = PathBuf::from("clusters");
    fs::create_dir_all(&clusters_path)?;
    let path = clusters_path.join(&args.name);
    if let Err(err) = fs::create_dir(&path) {
        bail!("Failed to create cluster directory at {path:?}: {err}");
    }
    println!("Generating new chain");
    println!("Name: {}", args.name);
    println!("Peers: {:?}", args.peer_addresses);
    if args.peer_addresses.len() < 4 {
        bail!("Chain must have at least 4 peers");
    }
    let test_cluster = TestCluster::generate(
        &args.name,
        args.peer_addresses,
        args.bind,
        args.prometheus_bind,
    );
    println!("Storing test cluster into {path:?}");
    test_cluster.store_into(&path)?;
    Ok(())
}

fn handle_local_cluster(args: LocalClusterArgs) -> anyhow::Result<()> {
    let clusters_path = PathBuf::from("clusters");
    let path = clusters_path.join(&args.name);

    Ok(())
}
