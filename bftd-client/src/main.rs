use crate::client::BftdClient;
use anyhow::bail;
use bftd_types::http_types::Metadata;
use bftd_types::types::{ChannelElementKey, ChannelId};
use clap::{Parser, Subcommand};
use futures_util::future::{select_all, try_join_all};
use futures_util::stream::StreamExt;
use futures_util::FutureExt;
use rand::rngs::ThreadRng;
use rand::{Rng, RngCore};
use reqwest::Url;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, io, mem};
use tokio::time;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod client;

#[derive(Parser, Debug)]
struct Args {
    #[arg(
        long,
        help = "Specific bftd url to test against. Conflicts with --network"
    )]
    url: Option<String>,
    #[arg(
        long,
        help = "Well known network name, currently supported: devnet. Conflicts with --url",
        conflicts_with = "url"
    )]
    network: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Submit(SubmitArgs),
    Tail(TailArgs),
    LoadGen(LoadGenArgs),
}

#[derive(Parser, Debug)]
struct TailArgs {
    #[arg(long)]
    from: ChannelElementKey,
    #[arg(long, default_value = "false")]
    json: bool,
    #[arg(long, value_delimiter = ',')]
    metadata: Vec<Metadata>,
}

#[derive(Parser, Debug)]
struct SubmitArgs {
    #[arg(long, value_delimiter = ',')]
    channels: Vec<ChannelId>,
    #[arg(long)]
    data: Option<String>,
    #[arg(long, default_value = "false", conflicts_with = "data")]
    stream: bool,
}
#[derive(Parser, Debug, Clone)]
struct LoadGenArgs {
    #[arg(long)]
    channel: Option<ChannelId>,
    #[arg(long, default_value = "1000")]
    rate: usize,
    #[arg(long, default_value = "512")]
    size: usize,
    #[arg(long, default_value = "8")]
    instances: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let url = match (args.url, args.network) {
        (Some(url), None) => {
            println!("Using URL {url}");
            url.parse()?
        }
        (None, Some(network)) => {
            if &network != "devnet" {
                bail!("Unknown network {network}");
            }
            let network_seed_url = Url::parse("http://a.dev.bftd.io:9080")?;
            println!("Running discovery for network {network}");
            let network_info = BftdClient::discover(&network_seed_url).await?;
            println!(
                "{} peers discovered, preferred {} [{} ms latency]",
                network_info.peers_count, network_info.preferred_peer, network_info.latency_ms
            );
            println!("Chain id: {}", network_info.chain_id);

            network_info.preferred_peer
        }
        (None, None) => {
            bail!("Either --url or --network must be specified")
        }
        _ => unreachable!(),
    };
    let command = args.command;
    let num_clients = match env::var("BFTD_NUM_CLIENTS") {
        Ok(c) => c.parse().unwrap(),
        Err(_) => {
            if let Command::LoadGen(l) = &command {
                (l.instances * l.rate + 19) / 20
            } else {
                1
            }
        }
    };
    println!("Using {num_clients} connections");
    let main = BftdClientMain {
        client: BftdClient::new(url, num_clients),
    };
    match command {
        Command::Submit(s) => main.handle_submit(s).await,
        Command::Tail(t) => main.handle_tail(t).await,
        Command::LoadGen(l) => main.handle_load_gen(l).await,
    }
}

struct BftdClientMain {
    client: BftdClient,
}

const TIME_LEN: usize = 16;
const TAG_LEN: usize = 8;
const SEC_TO_MS: u128 = 1000;

impl BftdClientMain {
    async fn handle_submit(self, args: SubmitArgs) -> anyhow::Result<()> {
        if args.stream {
            let mut buf = String::new();
            while io::stdin().read_line(&mut buf)? > 0 {
                self.client
                    .submit(
                        args.channels.clone(),
                        mem::take(&mut buf).into_bytes().into(),
                    )
                    .await?;
            }
            Ok(())
        } else {
            let data = match args.data {
                Some(d) => d.into_bytes(),
                None => {
                    let mut buf = Vec::with_capacity(1024);
                    io::stdin().read_to_end(&mut buf)?;
                    buf
                }
            };
            self.client.submit(args.channels, data.into()).await
        }
    }

    async fn handle_tail(self, args: TailArgs) -> anyhow::Result<()> {
        let mut request = self.client.tail(args.from, args.metadata)?;
        while let Some(element) = request.next().await {
            let element = element?;
            if args.json {
                println!("{}", serde_json::to_string(&element).unwrap());
            } else {
                println!("id {}", element.key);
                if let Some(timestamp_ns) = element.metadata.timestamp_ns {
                    println!("timestamp_ns {}", timestamp_ns);
                }
                if !element.metadata.block.is_empty() {
                    println!("block {}", element.metadata.block);
                }
                if !element.metadata.commit.is_empty() {
                    println!("commit {}", element.metadata.commit);
                }
                match std::str::from_utf8(&element.data) {
                    Ok(s) => println!("(str) {s}"),
                    Err(_) => println!("(bin) {}", hex::encode(&element.data)),
                }
                println!("=====");
            }
        }
        Ok(())
    }

    async fn handle_load_gen(self, args: LoadGenArgs) -> anyhow::Result<()> {
        if args.size < TAG_LEN + TIME_LEN {
            panic!(
                "Minimal transaction size for load test is {}",
                TAG_LEN + TIME_LEN
            );
        }
        let latency = (0..args.instances).map(|_| Default::default()).collect();
        let stat = LoadGenStat {
            latency,
            ..Default::default()
        };
        let stat = Arc::new(stat);
        let mut futures = Vec::with_capacity(args.instances);
        for i in 0..args.instances {
            futures.push(
                Self::load_gen_instance(self.client.clone(), args.clone(), stat.clone(), i).boxed(),
            );
        }
        futures.push(Self::reporter(args, stat).boxed());
        let (r, _, _) = select_all(futures).await;
        r
    }

    async fn load_gen_instance(
        client: BftdClient,
        args: LoadGenArgs,
        stat: Arc<LoadGenStat>,
        index: usize,
    ) -> anyhow::Result<()> {
        let tag: [u8; TAG_LEN] = ThreadRng::default().gen();
        let start = Instant::now();
        let channel = if let Some(channel) = args.channel {
            channel
        } else {
            let channel = ChannelId(ThreadRng::default().gen());
            // println!("Using randomly generated channel {}", channel);
            channel
        };
        let generator = tokio::spawn(Self::generator(
            tag,
            start,
            client.clone(),
            channel,
            args,
            stat.clone(),
        ));
        let reporter = tokio::spawn(Self::tailer(
            tag,
            start,
            client,
            channel,
            stat.clone(),
            index,
        ));
        let (r, _, _) = select_all(vec![generator, reporter]).await;
        r.unwrap()
    }

    async fn reporter(args: LoadGenArgs, stat: Arc<LoadGenStat>) -> anyhow::Result<()> {
        const REPORT_RATE: usize = 5;
        println!("  *   sent recv  Bytes   avg lat(ms)");
        time::sleep(Duration::from_secs(REPORT_RATE as u64)).await;
        let mut sent = stat.sent.load(Ordering::Relaxed);
        let mut received = stat.received.load(Ordering::Relaxed);
        let mut i = 0;
        loop {
            i += 1;
            time::sleep(Duration::from_secs(REPORT_RATE as u64)).await;
            let this_sent = stat.sent.load(Ordering::Relaxed);
            let this_received = stat.received.load(Ordering::Relaxed);
            let s = (this_sent - sent) / REPORT_RATE;
            let r = (this_received - received) / REPORT_RATE;
            let l: u64 = stat.latency.iter().map(|l| l.load(Ordering::Relaxed)).sum();
            let l = l / stat.latency.len() as u64;
            let rb = r * args.size / REPORT_RATE;
            let (rb, rbs) = format_bytes(rb);
            println!("[{i:3}] {s:4} {r:4} {rb:4} {rbs}/s {l:4}");
            sent = this_sent;
            received = this_received;
        }
    }

    async fn tailer(
        tag: [u8; TAG_LEN],
        start: Instant,
        client: BftdClient,
        channel: ChannelId,
        stat: Arc<LoadGenStat>,
        index: usize,
    ) -> anyhow::Result<()> {
        let mut tot_delay = 0;
        let mut count = 0u128;
        let mut tick = start.elapsed().as_millis();
        let mut stream = client.tail(
            ChannelElementKey::channel_start(channel),
            Default::default(),
        )?;
        let to = Duration::from_secs(20);
        while let Some(e) = timeout(to, stream.next())
            .await
            .map_err(|_| anyhow::format_err!("Stream timeout"))?
        {
            let e = e?;
            let got_tag = &e.data[..TAG_LEN];
            if &tag[..] != got_tag {
                bail!("Tag on the channel does not match expected - multiple load gens on the channel?");
            }
            let ts = e.data[TAG_LEN..TAG_LEN + TIME_LEN].try_into().unwrap();
            let ts = u128::from_ne_bytes(ts);
            let elapsed = start.elapsed();
            let delay = elapsed.as_nanos() - ts;
            tot_delay += delay / (1000 * 1000);
            count += 1;
            let elapsed_ms = elapsed.as_millis();
            if elapsed_ms - tick > 5 * SEC_TO_MS {
                stat.received.fetch_add(count as usize, Ordering::Relaxed);
                let latency = (tot_delay / count) as u64;
                stat.latency[index].store(latency, Ordering::Relaxed);
                tot_delay = 0;
                count = 0;
                tick = elapsed_ms;
            }
        }
        bail!("Stream ended unexpectedly");
    }

    async fn generator(
        tag: [u8; TAG_LEN],
        start: Instant,
        client: BftdClient,
        channel: ChannelId,
        args: LoadGenArgs,
        stat: Arc<LoadGenStat>,
    ) -> anyhow::Result<()> {
        loop {
            let started = Instant::now();
            let mut futures = Vec::with_capacity(args.rate);
            for _ in 0..args.rate {
                let mut buf = vec![0u8; args.size];
                buf[..TAG_LEN].copy_from_slice(&tag);
                buf[TAG_LEN..TAG_LEN + 16]
                    .copy_from_slice(&start.elapsed().as_nanos().to_ne_bytes());
                ThreadRng::default().fill_bytes(&mut buf[TAG_LEN + TIME_LEN..]);
                let fut = client.submit(vec![channel], buf.into());
                futures.push(fut);
            }
            let futures = timeout(Duration::from_secs(10), try_join_all(futures));
            let _: Vec<()> = futures
                .await
                .map_err(|_| anyhow::format_err!("Send timeout"))??;
            stat.sent.fetch_add(args.rate, Ordering::Relaxed);
            time::sleep_until((started + Duration::from_secs(1)).into()).await;
        }
    }
}

#[derive(Default)]
struct LoadGenStat {
    sent: AtomicUsize,
    received: AtomicUsize,
    latency: Vec<AtomicU64>,
}

fn format_bytes(bytes: usize) -> (usize, &'static str) {
    if bytes > 1024 * 1024 * 1024 {
        (bytes / (1024 * 1024 * 1024), "GB")
    } else if bytes > 1024 * 1024 {
        (bytes / (1024 * 1024), "MB")
    } else if bytes > 1024 {
        (bytes / 1024, "KB")
    } else {
        (bytes, " B")
    }
}
