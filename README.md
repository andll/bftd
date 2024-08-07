[![Discord][discord-badge]][discord-url]

[discord-badge]: https://img.shields.io/discord/1269357435194314957?style=flat
[discord-url]: https://discord.gg/SvPZWfvu

# What is bftd?

bftd is an implementation of proof-of-stake byzantine fault-tolerant consensus. bftd implements [Adelie](https://arxiv.org/abs/2408.02000) consensus protocol (which is built on top of Mysticeti consensus) — a cutting-edge DAG-based consensus protocol designed for low latency and high throughput.

Adelie protocol couples consensus with transaction dissemination, which helps to reduce overall latency and simplify usage.

You can check out [Glossary](#Glossary) below if some terms are unfamiliar.

# Performance
`bftd` is likely the most performant BFT consensus engine out there as of today.

On a small cluster of 12 nodes of an inexpensive AWS instance type c7gn.xlarge, distributed across 4 regions globally (us-west, us-east, eu-west and ap-northeast) **bftd can deliver 450K TPS with a 630 millisecond finality**. (With a transaction size of 512 bytes)

Under lower load conditions, bftd commit latency across 4 regions goes down to **440ms**.

# What are the main features of bftd consensus?

* **Proof of stake**. Each validator(node) is assigned certain stake in the system. The safety properties of the system are based on the assumption that at least 2/3 of the total stake follow the protocol.
* **Byzantine fault-tolerance**. Protocol maintains its properties as long as only 1/3 validators in the network are malicious. 
* **Deterministic finality**. Protocol gives explicit (deterministic) signal when transaction is final. Some consensus protocols (for example bitcoin PoW) only provide probabilistic finality. 
* **Low finality and high throughput**. bftd employs DAG-based consensus protocol based on uncertified DAG approach, allowing it to reach low latency combined with very high throughput. 
* **Coupling between consensus and transaction dissemination**. When using bftd, a client only needs to submit transaction to a single validator. The transaction will be included in the chain even if this validator does not get a chance to be a leader. With other consensus protocols, for example, HotStuff picking the right validator can be essential to minimizing client latency.

# State of the project

This is still in early development, so bugs are possible and likely.
That said, you can already run bftd locally or on the cluster of real
machines and integrate with itm by either using HTTP API, or by embedding it in your application.

The code also has not gone through an audit at this time.

# Running bftd locally

You can run bftd cluster locally.
Set up the cluster:
```
cargo run --package bftd -- new-chain my-awesome-chain 127.0.0.1:{8080..8083} --leader-timeout-ms 1000 --empty-commit-timeout-ms --http-server-base-port 9080
```
Run all nodes locally:
```
RUST_LOG=bftd_core::syncer=info cargo run --package bftd -- local-cluster my-awesome-chain
```
When the cluster has started, you can tail commits via http interface:
```
curl 'http://127.0.0.1:9080/tail?from=0&skip_empty_commits=true&pretty=true'
```
And send a transaction:
```
curl -XPOST http://127.0.0.1:9080/send -d 'ABCDEFG'
```

# Running bftd on the real cluster 

This section gives guidance on how to setup bftd on a real cluster.

The process of setting up bftd consists of two steps:

* **Step #1**: Generating genesis file and configs for your chain (new-chain command). This step will generate configuration files for each node locally, it is useful for running a testnet, but since all the keys are generated on a single machine, a different process should be used for production networks.  
* **Step #2**: Deploying configuration files to the machines and running bftd.

You need to know IP addresses of validators ahead of time. 
To bootstrap your testnet you can run the following command locally:
```
cargo run --package bftd -- new-chain awesome-testnet {IP-addresses-of-machines}:9200 --prometheus-bind 127.0.0.1:9091 --bind 0.0.0.0 --prometheus-template bftd-server/resources/prometheus.yml
```

This will create a directory `clusters/awesome-testnet` with configuration files for each validator.
**Note**: This command generates private keys for all validators locally, so it can only be used for test networks.

After genesis is done, you should copy configuration files to each machine.

After that, clone the repo and build bftd on each validator:
```
git clone <this repo>
cd bftd
cargo build --release
```

You can then run bftd on each machine:
```
bftd/target/release/bftd run <configuration-directory>
```

We provide few templates that can be used to simplify deployment of bftd in `bftd-server/resources` directory:

* `setup.sh` can be used to install rust toolchain and other dependencies on Ubuntu linux
* `bftd.service` is an example template for the Systemd service
* `prometheus.yml` is a template for the configuration file. The `new-chain` command will actually use this template to generate individual `prometheus.yml` files for each validator that you can use. 
* `dashboard.json` is a Grafana dashboard with the most important bftd metrics.

# Embedding bftd

bftd can be embedded into another blockchain as a library(rust crate).

We provide two packages:
* `bftd-core` provides consensus engine. It has much fewer dependencies, but you would need to provide entry-points for users yourself.
* `bftd-server` provides additional wrappers on top of `bftd-core`. It also contains a simple HTTP server that can be used to send transactions and tail commits. 

We are going to provide instructions on how integrate with the `bftd-server` crate:

First, you need to generate configs as described above.

After that, you can start the `Node` instance with the given config:

```rust
fn main() -> anyhow::Result<()> {
    let genesis = Arc::new(Genesis::load("genesis_path").unwrap());
    let node = Node::load("node_directory_path", genesis).unwrap();
    let handle = node.start().unwrap();
    let runtime = handle.runtime().clone();
    runtime.block_on(async {
        handle.send_transaction(vec![1, 2, 3]).await;
        println!("Transaction was sent, waiting for it to be committed");
        // Replay log from the last seen commit
        // You can use FullCommit::index to get index of the last commit
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
                        return;
                    }
                }
            }
        }
    });
    handle.stop();
}
```

You can run a working example from `examples` directory (after running new-chain command):
```
cargo run --example embedded -- clusters/<chain_name>/genesis clusters/<chain_name>/<node_number>
```

Note, that you need to run at least 3 different nodes(if the total cluster is 4 nodes) to produce a commit

# Glossary 

## What is consensus?

Consensus algorithm allows a cluster of distributed machines to agree on certain state of the system.

Most of the time, rather than agreeing on the whole state,
systems typically have implicit agreement on the initial state of the system,
and consensus then replicates an ordered list of transactions that change that state.

“Traditional” [consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) protocols ([Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)), [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)), etc)
can replicate the state in a fault-tolerant manner in presence of hardware failures - 
networking delays, reordering of networking messages, machines failures, etc. 
Traditional consensus is typically used by databases such as [Google Spanner](https://en.wikipedia.org/wiki/Spanner_(database)) to concisely 
replicate data between different machines in case some of them fail.

## What does Byzantine Fault Tolerant mean?

[Byzantine](https://en.wikipedia.org/wiki/Byzantine_fault) Fault Tolerant (BFT) consensus protocols in addition
to handling hardware failures can also handle malicious nodes that intentionally try to subvert the protocol.

This is typically not needed for the databases, since all machines are owned by the same company. 
BFT consensus is widely used in blockchain, since the individual blockchain nodes(validators) are owned by 
different people, some of them might use a malicious version of software that tries to subvert the protocol. 

BFT consensus protocols can consistently replicate data for even in the presence of such malicious nodes.

## What kind of BFT protocols are out there?

Early blockchains such as bitcoin used Proof-of-Work consensus,
which is fairly simple but requires a lot of computational resources to maintain its safety properties.

**Proof-of-Work** consensus also has long non-deterministic commit(finality) times,
low throughput and is typically associated with high transaction cost. 

The **Proof-of-Stake** consensus on the contrary can achieve a very low latency,
high throughput and requires significantly less computational resources
that typically result in much lower transaction fees for the blockchain.

## What is DAG-based consensus protocol?

Within the **Proof-of-Stake** family,
some BFT consensus protocols such as [HotStuff](https://arxiv.org/abs/1803.05069) 
are structured similarly to the traditional consensus protocols
where leader proposes the list of transactions and multiphase message exchange is then used to commit the change.

The newer family of Proof-of-Stake BFT consensus protocols are so called **DAG-based BFT consensus** protocols.
DAG-based consensus protocols couple consensus with the data-dissemination, allowing everyone, 
not just current leader to share transactions in the process of reaching the consensus.

This combination of data dissemination and consensus is shown to allow very high throughput.
The narwhal/bullshark, one of the earlier DAG-based consensus protocols, can achieve 100K TPS, but trade high TPS for 
a significantly increased latency.

The newer protocols, such as Adelie protocol implemented in bftd however, combine extremely high throughput with the 
low latency of traditional  consensus protocols. 


# Where does the protocol name (Adelie) come from?

Many DAG-based BFT consensus protocols are named after marine animals: 
narhwal and mysticeti are marine mammals, bullshark and hammerhead are marine fish.

We continue the tradition by naming our protocol after a marine bird - Adélie penguin.

# License

This project is licensed under the Apache2 license. See [LICENSE](LICENSE).
