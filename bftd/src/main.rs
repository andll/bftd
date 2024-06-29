pub mod config;
mod load_gen;
pub mod mempool;
mod node;
mod prometheus;
pub mod server;
#[cfg(test)]
mod smoke_tests;
mod test_cluster;

use crate::node::NodeHandle;
use crate::test_cluster::{start_node, TestCluster};
use anyhow::bail;
use bftd_core::protocol_config::ProtocolConfigBuilder;
use clap::Parser;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
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
    #[arg(
        long,
        help = "Do not check if peer addresses can be resolved locally when generating genesis"
    )]
    no_check_peer_address: bool,
    #[arg(
        long,
        short = 'b',
        help = "Protocol bind address. If not set, corresponding address from peer_addresses is used"
    )]
    bind: Option<String>,
    #[arg(
        long,
        short = 'p',
        help = "Bind address for prometheus server. Prometheus server is not started if this is not set"
    )]
    prometheus_bind: Option<SocketAddr>,
    #[arg(
        long,
        short = 's',
        help = "Bind address for bftd server. Bftd server is not started if this is not set."
    )]
    http_server_bind: Option<SocketAddr>,
    #[arg(
        long,
        short = 't',
        requires = "prometheus_bind",
        help = "Path to template file for prometheus configuration. If not set prometheus configuration won't be generated."
    )]
    prometheus_template: Option<PathBuf>,
    #[arg(long, short = 'l', help = "Leader timeout")]
    leader_timeout_ms: Option<u64>,
    #[arg(long, short = 'e', num_args = 0..=1, default_missing_value = Some(""), help ="Empty commit timeout allows to slow down empty blocks generation if there is nothing to propose or commit. Specify this argument without value to generate empty commit timeout automatically based on leader timeout. If argument is not specified empty commit timeout feature is not used and blocks are generated as fast as possible.")]
    empty_commit_timeout_ms: Option<String>,
}

#[derive(Parser, Debug)]
struct LocalClusterArgs {
    name: String,
    #[arg(long, short = 'd')]
    duration: Option<u64>,
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
    let mut protocol_config = ProtocolConfigBuilder::default();
    if let Some(leader_timeout_ms) = args.leader_timeout_ms {
        protocol_config.with_leader_timeout(Duration::from_millis(leader_timeout_ms));
        println!("Using commit timeout {leader_timeout_ms} ms")
    }
    if let Some(empty_commit_timeout_ms) = args.empty_commit_timeout_ms {
        if empty_commit_timeout_ms.is_empty() {
            protocol_config.with_recommended_empty_commit_timeout();
        } else {
            let empty_commit_timeout_ms = empty_commit_timeout_ms.parse().unwrap();
            protocol_config
                .with_empty_commit_timeout(Duration::from_millis(empty_commit_timeout_ms));
        }
        println!(
            "Using empty commit timeout {} ms",
            protocol_config.empty_commit_timeout().as_millis()
        );
    }
    let test_cluster = TestCluster::generate(
        &args.name,
        args.peer_addresses,
        args.bind,
        args.prometheus_bind,
        args.http_server_bind,
        protocol_config.build(),
    );
    println!("Storing test cluster into {path:?}");
    test_cluster.store_into(&path, args.prometheus_template)?;
    Ok(())
}

fn handle_local_cluster(args: LocalClusterArgs) -> anyhow::Result<()> {
    let clusters_path = PathBuf::from("clusters");
    let path = clusters_path.join(&args.name);
    let handles = TestCluster::start_test_cluster(path)?;
    if let Some(duration) = args.duration {
        thread::sleep(Duration::from_secs(duration));
    } else {
        thread::park();
    }
    NodeHandle::stop_all(handles);
    Ok(())
}

fn handle_run(args: RunArgs) -> anyhow::Result<()> {
    let _handle = start_node(args.dir, args.load_gen)?;
    thread::park();
    Ok(())
}
