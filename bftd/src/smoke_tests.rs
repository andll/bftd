use crate::node::NodeHandle;
use crate::test_cluster::TestCluster;
use bftd_core::consensus::Commit;
use bftd_core::store::CommitStore;
use futures::future::join_all;
use std::collections::HashSet;
use std::env;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;

#[test]
fn smoke_test() {
    const BASE_PORT: usize = 11_000;
    enable_test_logging_smoke_test();
    const NUM_PEERS: usize = 10;
    const WAIT_COMMIT: u64 = 128;
    let dir = tempdir::TempDir::new("smoke_test").unwrap();
    let dir = dir.path().to_path_buf();
    let handles = start_smoke_test_cluster(dir, NUM_PEERS, BASE_PORT);
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    wait_check_commits(&runtime, &handles, WAIT_COMMIT);
    NodeHandle::stop_all(handles);
}

#[test]
fn smoke_test_one_down() {
    const BASE_PORT: usize = 11_100;
    enable_test_logging_smoke_test();
    const NUM_PEERS: usize = 10;
    const WAIT_COMMIT_J: u64 = 32; // "J" is a 10th node
    const WAIT_COMMIT_OTHERS: u64 = 64;
    let dir = tempdir::TempDir::new("smoke_test").unwrap();
    let dir = dir.path().to_path_buf();
    let mut handles = start_smoke_test_cluster(dir, NUM_PEERS, BASE_PORT);
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let last = handles.pop().unwrap();
    runtime.block_on(timeout(wait_for_commit(
        last.commit_receiver().clone(),
        last.store().clone(),
        WAIT_COMMIT_J,
    )));
    last.stop();
    let max_commit = max_commit(&handles);
    wait_check_commits(&runtime, &handles, max_commit + WAIT_COMMIT_OTHERS);
    NodeHandle::stop_all(handles);
}

#[test]
fn smoke_test_catch_up_when_one_node_down() {
    const BASE_PORT: usize = 11_200;
    enable_test_logging_smoke_test();
    const NUM_PEERS: usize = 10;
    const WAIT_COMMIT_I: u64 = 32; // "I" is a 9th node
    const WAIT_COMMIT_OTHERS: u64 = 64;
    const WAIT_COMMIT_AFTER_STARTING_J: u64 = 64;
    let dir = tempdir::TempDir::new("smoke_test").unwrap();
    let dir = dir.path().to_path_buf();
    // Start all nodes except J
    let mut handles =
        start_smoke_test_cluster_partially(dir.clone(), NUM_PEERS, NUM_PEERS - 1, BASE_PORT);
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let node_i = handles.pop().unwrap();
    runtime.block_on(timeout(wait_for_commit(
        node_i.commit_receiver().clone(),
        node_i.store().clone(),
        WAIT_COMMIT_I,
    )));
    // Stop node I
    node_i.stop();
    let max_commit = max_commit(&handles);
    // wait commits while both I and J are down
    wait_check_commits(&runtime, &handles, max_commit + WAIT_COMMIT_OTHERS);
    // Start J
    let more_handles =
        TestCluster::start_test_cluster_partially(dir, Some(vec![NUM_PEERS - 1])).unwrap();
    handles.extend(more_handles);
    let max_commit = crate::smoke_tests::max_commit(&handles);
    // Wait for commits between all validators and "J"
    // "J" will catch up because validators will start waiting for its proposal on leader rounds
    // "J" will get blocks for "I" via block fetcher from other nodes(since "I" is down)
    wait_check_commits(
        &runtime,
        &handles,
        max_commit + WAIT_COMMIT_AFTER_STARTING_J,
    );
    println!("All done");
    NodeHandle::stop_all(handles);
}

async fn timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), f)
        .await
        .expect("Timed out")
}

/// Wait for commit on each node and confirm all commits are the same
fn wait_check_commits(runtime: &Runtime, handles: &[NodeHandle], commit_index: u64) {
    let waiters: Vec<_> = handles
        .iter()
        .map(|h| wait_for_commit(h.commit_receiver().clone(), h.store().clone(), commit_index))
        .collect();
    let commits = runtime.block_on(timeout(join_all(waiters)));
    let mut commit_set = HashSet::new();
    commit_set.extend(&commits);
    if commit_set.len() != 1 {
        panic!("Commit sequence diverged between validators: {commits:?}")
    }
}

fn start_smoke_test_cluster(dir: PathBuf, num_peers: usize, base_port: usize) -> Vec<NodeHandle> {
    let peers = (0..num_peers)
        .map(|p| format!("127.0.0.1:{}", base_port + p))
        .collect();
    let cluster = TestCluster::generate("test", peers, None, None, None);
    cluster.store_into(&dir, None).unwrap();
    TestCluster::start_test_cluster(dir).unwrap()
}

fn start_smoke_test_cluster_partially(
    dir: PathBuf,
    num_peers: usize,
    start_up_to: usize,
    base_port: usize,
) -> Vec<NodeHandle> {
    let peers = (0..num_peers)
        .map(|p| format!("127.0.0.1:{}", base_port + p))
        .collect();
    let cluster = TestCluster::generate("test", peers, None, None, None);
    cluster.store_into(&dir, None).unwrap();
    let to_start = (0..start_up_to).collect();
    TestCluster::start_test_cluster_partially(dir, Some(to_start)).unwrap()
}

fn max_commit(handles: &[NodeHandle]) -> u64 {
    handles
        .iter()
        .map(|h| h.commit_receiver().borrow().unwrap_or_default())
        .max()
        .unwrap()
}

async fn wait_for_commit(
    mut r: watch::Receiver<Option<u64>>,
    store: impl CommitStore,
    index: u64,
) -> Commit {
    assert!(index > 0);
    loop {
        let last_commit = *r.borrow_and_update();
        if last_commit.unwrap_or_default() >= index {
            break;
        }
        r.changed().await.unwrap();
    }
    store.get_commit(index).unwrap()
}

pub fn enable_test_logging_smoke_test() {
    use tracing_subscriber::EnvFilter;
    let v =  env::var("RUST_LOG").unwrap_or("bftd_core::rpc=debug,bftd_core::syncer=debug,bftd_core::fetcher=debug".to_string());
    let env_filter = EnvFilter::builder().parse(v).unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_thread_names(true)
        .with_test_writer()
        .try_init()
        .ok();
}
