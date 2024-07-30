What is BFTd?

bftd is an implementation proof-of-stake byzantine fault-tolerant consensus. bftd implements Oxa consensus protocol (which is built on top of Mysticeti consensus) — a cutting-edge DAG-based consensus protocol designed for low latency and high throughput.

Oxa protocol couples consensus with transaction dissemination which helps to reduce overall latency and simplify usage.v

# Performance
`bftd` is likely the most performant BFT consensus engine out there.

A small cluster of 12 nodes of an inexpensive AWS instance type c7gn.xlarge, distributed across 4 regions globally (us-west, us-east, eu-west and ap-northeast) **bftd can deliver 450K TPS with a 630 millisecond finality**. (With a transaction size of 512 bytes)

Under the lower load, bftd commit latency across 4 regions can go down to **440ms**.

# What are the main features of bftd consensus?

* **Proof of stake**. Each validator(node) is assigned certain stake in the system. The safety properties of the system are based on the assumption that at least 2/3 of total stake follow the protocol.
* **Byzantine fault-tolerance**. Protocol maintains its properties as long as less than 1/3 validators in the network are malicious. 
* **Deterministic finality**. Protocol gives explicit (deterministic) signal when transaction is final. Some consensus protocols (for example bitcoin PoW) only provide probabilistic finality. 
* **Low finality and high throughput**. Bftd employs DAG-based consensus protocol with uncertified DAG, allowing it to demonstrate low latency with very high throughput. 
* **Coupling between consensus and transaction dissemination**. When using bftd, a client only needs to submit transaction to a single validator. The transaction will be included in the chain even if this validator does not get a chance to be a leader. With other consensus protocols, for example, HotStuff picking the right validator can be essential to minimizing client latency.
 
# Running bftd locally

You can run bftd cluster locally.
Set up the cluster:
```
cargo run --package bftd -- new-chain my-awesome-chain 127.0.0.1:{8080..8084} --leader-timeout-ms 2000 --empty-commit-timeout-ms --http-server-base-port 9080
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

This section gives some guidance on how to setup bftd in a real cluster.

The process of setting up bftd consists of two steps:

- Generating genesis file and configs for your chain (new-chain command). This step will generate configuration files for each node locally, it is useful for running a testnet, but since all the keys are generated on a single machine, a different process should be used for production networks.  
- Deploying configuration files to the machines and running bftd.

You need to know IP addresses of validators ahead of time. 
To bootstrap your testnet you can run the following command locally:
```
cargo run --package bftd -- new-chain awesome-testnet {IP-addresses-of-machines}:9200 --prometheus-bind 127.0.0.1:9091 --bind 0.0.0.0 --prometheus-template bftd/resources/prometheus.yml
```

This will create a directory `clusters/awesome-testnet` with configuration files for each validator.
**Note**: This command generates private keys for all validators locally, so it can only be used for test networks.

After genesis is done, you should copy configuration files to each machine.

After that, checkout and build bftd on each validator:
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

