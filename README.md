What is BFTd?

bftd is an implementation proof-of-stake byzantine fault-tolerant consensus. bftd implements mysticeti consensus protocol — a cutting-edge DAG-based consensus protocol designed for low latency and high throughput.
bftd protocol couples consensus with transaction dissemination which helps to reduce latency and simplify usage.

# Performance
bftd is likely the most performant byzantine consensus engine out there.

With a small cluster of 12 nodes of an inexpensive AWS instance type c7gn.xlarge, distributed across 4 regions globally (us-west, us-east, eu-west and ap-northeast) **bftd can deliver 360K TPS with a 600 millisecond finality**. (With a transaction size of 512 bytes)  

# What are the main features of bftd consensus?

* **Byzantine fault-tolerance**. Protocol maintains its properties as long as less than 1/3 validators in the network are malicious. 
* **Proof of stake**. 
* **Deterministic finality**. Protocol gives explicit (deterministic) signal when transaction is final. Some consensus protocols (for example bitcoin PoW) only provide probabilistic finality. 
* **Low finality**. Bftd efficiently implements mysticeti protocol, providing low overhead and allowing to achieve subsecond latency with hundreds of thousands transactions per second.
* **Coupling between consensus and transaction dissemination**. In bftd protocol use only needs to submit transaction to single validator - it will be automatically included in the chain even if validator does not get a chance to be a leader. With other consensus protocols, for example, HotStuff picking the right validator can be essential to minimizing client latency.
 
# Running bftd locally

You can run bftd cluster locally.
Setup the cluster:
```
cargo run --package bftd -- new-chain my-awesome-chain 127.0.0.1:{8080..8084} --leader-timeout-ms 2000 --empty-commit-timeout-ms --http_server_base_port 9080
```
Run all nodes locally:
```
RUST_LOG=bftd_core::syncer=info cargo run --package bftd -- local-cluster my-awesome-chain
```
When the cluster has started, you can tail commits via http interface:
```
curl http://127.0.0.1:9080/tail?from=0&skip_empty_commits=true&pretty=true
```
And send a transaction:
```
curl -XPOST http://127.0.0.1:9080/send -d 'ABCDEFG'
```

# Embedding BFTd

bftd can be embedded into another blockchain as a library.
