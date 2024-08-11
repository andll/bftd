use anyhow::bail;
use bftd_core::block::ValidatorIndex;
use bftd_core::consensus::LeaderElection;
use bftd_core::protocol_config::ProtocolConfigBuilder;
use bftd_server::node::NodeHandle;
use bftd_server::test_cluster::{start_node, TestCluster};
use clap::Parser;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
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
        short = 's',
        help = "Pre-defined set of configs to use. Currently only support 'low-volume'"
    )]
    preset: Option<String>,
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
        help = "Bind address for bftd-server server. Bftd server is not started if this(or http_server_base_port) is not set.",
        conflicts_with = "http_server_base_port"
    )]
    http_server_bind: Option<SocketAddr>,
    #[arg(
        long,
        help = "Base port for binding bftd-server server. Bftd server is not started if this(or http_server_bind) is not set.",
        conflicts_with = "http_server_bind"
    )]
    http_server_base_port: Option<u16>,
    #[arg(
        long,
        help = "Ip address for binding bftd-server server when http_server_base_port is set.",
        default_value = "127.0.0.1",
        requires = "http_server_base_port"
    )]
    http_server_address: IpAddr,
    #[arg(
        long,
        short = 't',
        requires = "prometheus_bind",
        help = "Path to template file for prometheus configuration. If not set prometheus configuration won't be generated."
    )]
    prometheus_template: Option<PathBuf>,
    #[arg(long, short = 'l', help = "Leader timeout", conflicts_with = "preset")]
    leader_timeout_ms: Option<u64>,
    #[arg(long,
        short = 'e',
        num_args = 0..=1,
        default_missing_value = Some(""),
        help ="Empty commit timeout allows to slow down empty blocks generation if there is nothing to propose or commit. Specify this argument without value to generate empty commit timeout automatically based on leader timeout. If argument is not specified empty commit timeout feature is not used and blocks are generated as fast as possible.",
        conflicts_with = "preset"
    )]
    empty_commit_timeout_ms: Option<String>,
    #[arg(
        long,
        help = "Enables or disables critical_block_check protocol config. Enabled by default",
        conflicts_with = "preset"
    )]
    critical_block_check: Option<bool>,
    #[arg(
        long,
        help = "Load gen parameters in form of transaction_size[::tps_limit]"
    )]
    load_gen: Option<String>,
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
    if let Some(preset) = args.preset {
        if &preset == "low-volume" {
            protocol_config.with_leader_timeout(Duration::from_secs(1));
            protocol_config.with_recommended_empty_commit_timeout();
            protocol_config.with_critical_block_check(false);
            protocol_config.with_leader_election(LeaderElection::MultiLeader(1));
        } else {
            bail!("Unknown preset {preset}. Currently only supported preset is low-volume");
        }
    }
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
    if let Some(critical_block_check) = args.critical_block_check {
        println!(
            "Critical block check manually {}",
            if critical_block_check {
                "enabled"
            } else {
                "disabled"
            }
        );
        protocol_config.with_critical_block_check(critical_block_check);
    }
    let http_server_bind = |vi: ValidatorIndex| {
        if let Some(bind) = args.http_server_bind {
            Some(bind)
        } else if let Some(base_port) = args.http_server_base_port {
            let address = args.http_server_address;
            Some(SocketAddr::new(address, base_port + (vi.0 as u16)))
        } else {
            None
        }
    };

    let test_cluster = TestCluster::generate(
        &args.name,
        args.peer_addresses,
        args.bind,
        args.prometheus_bind,
        http_server_bind,
        protocol_config.build(),
        args.load_gen,
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
    let _handle = start_node(args.dir)?;
    thread::park();
    Ok(())
}
