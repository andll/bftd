pub mod config;
mod load_gen;
pub mod mempool;
mod node;
mod prometheus;
pub mod server;
mod test_cluster;

use crate::test_cluster::{start_node, TestCluster};
use anyhow::bail;
use clap::Parser;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::{fs, process, thread};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
enum Args {
    NewChain(NewChainArgs),
    LocalCluster(LocalClusterArgs),
    Run(RunArgs),
}

#[derive(Parser, Debug)]
struct NewChainArgs {
    name: String,
    peer_addresses: Vec<String>,
    #[arg(long)]
    no_check_peer_address: bool,
    #[arg(long, short = 'b')]
    bind: Option<String>,
    #[arg(long, short = 'p')]
    prometheus_bind: Option<SocketAddr>,
    #[arg(long, short = 's')]
    http_server_bind: Option<SocketAddr>,
    #[arg(long, short = 't', requires = "prometheus_bind")]
    prometheus_template: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct LocalClusterArgs {
    name: String,
}

#[derive(Parser, Debug)]
struct RunArgs {
    dir: PathBuf,
    #[arg(long, short = 'l')]
    load_gen: Option<String>,
}

fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_names(true)
        .init();
    if let Err(err) = handle_args(args) {
        println!("error: {err}");
        process::exit(1);
    }
}

fn handle_args(args: Args) -> anyhow::Result<()> {
    match args {
        Args::NewChain(args) => handle_new_chain(args),
        Args::LocalCluster(args) => handle_local_cluster(args),
        Args::Run(args) => handle_run(args),
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
    if args.no_check_peer_address {
        println!(
            "--no-check-peer-address specified - not checking if peer addresses can be resolved"
        );
    } else {
        let errs: Vec<_> = args
            .peer_addresses
            .iter()
            .filter_map(|a| a.to_socket_addrs().err().map(|e| (a, e)))
            .collect();
        if !errs.is_empty() {
            println!("Failed to resolve some of the peer addresses(use --no-check-peer-address to skip this check):");
            for (a, e) in errs {
                println!("{a}: {e}")
            }
            process::exit(1);
        }
    }
    let test_cluster = TestCluster::generate(
        &args.name,
        args.peer_addresses,
        args.bind,
        args.prometheus_bind,
        args.http_server_bind,
    );
    println!("Storing test cluster into {path:?}");
    test_cluster.store_into(&path, args.prometheus_template)?;
    Ok(())
}

fn handle_local_cluster(args: LocalClusterArgs) -> anyhow::Result<()> {
    let clusters_path = PathBuf::from("clusters");
    let path = clusters_path.join(&args.name);
    let _handles = TestCluster::start_test_cluster(path)?;
    thread::park();
    Ok(())
}

fn handle_run(args: RunArgs) -> anyhow::Result<()> {
    let _handle = start_node(args.dir, args.load_gen)?;
    thread::park();
    Ok(())
}
