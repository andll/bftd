use bftd_core::genesis::Genesis;
use bftd_server::node::Node;
use std::env::args;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

// Run new-chain command first
// You can then run example as the following:
// * cargo run --example embedded -- clusters/<chain_name>/genesis clusters/<chain_name>/<node_number>
// Note - you need to run at least 3 different nodes (if total cluster is 4 nodes)
//  for protocol to work to produce a commit
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_names(true)
        .init();

    let mut args = args();
    args.next();
    let genesis = args
        .next()
        .expect("Usage: cargo run --example embedded -- <genesis_path> <config_path>");
    let wd = args
        .next()
        .expect("Usage: cargo run --example embedded -- <genesis_path> <config_path>");
    let wd = PathBuf::from_str(&wd).unwrap();
    let genesis = Arc::new(Genesis::load(&genesis).unwrap());
    let node = Node::load(&wd, genesis).unwrap();
    let handle = node.start().unwrap();
    let runtime = handle.runtime().clone();
    runtime.block_on(async {
        handle.send_transaction(vec![1, 2, 3]).await;
        println!("Transaction was sent, waiting for it to be committed");
        let mut commit_reader = handle.read_commits_from(0);
        loop {
            let commit = commit_reader
                .recv_commit()
                .await
                .expect("Server has stopped before we got expected commit");
            for (block, transactions) in commit.blocks.iter() {
                for transaction in transactions.iter_slices() {
                    if transaction == &[1, 2, 3] {
                        println!(
                            "Our transaction is found in block {} committed by commit {}!",
                            block.reference(),
                            hex::encode(&commit.info.commit_hash()[..4])
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        print!("Stopping now...");
                        return;
                    }
                }
            }
        }
    });
    handle.stop();
}
