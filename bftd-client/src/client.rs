use anyhow::Context;
use bftd_types::http_types::{
    ChannelElementWithMetadata, Metadata, NetworkInfo, SubmitQuery, TailQuery,
};
use bftd_types::types::{ChannelElementKey, ChannelId};
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{stream, Stream, TryStreamExt};
use rand::prelude::SliceRandom;
use rand::rngs::ThreadRng;
use reqwest::{Client, ClientBuilder, Method, Url};
use reqwest_eventsource::{Event, EventSource};
use std::future;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct BftdClient {
    url: Url,
    clients: Vec<Client>,
}

impl BftdClient {
    pub fn new(url: Url, num_clients: usize) -> Self {
        let mut clients = Vec::with_capacity(num_clients);
        for _ in 0..num_clients {
            clients.push(Self::make_client());
        }
        Self { url, clients }
    }

    pub async fn discover(seed_url: &Url) -> anyhow::Result<DiscoveryInfo> {
        let client = Self::make_client();
        let info = client
            .get(seed_url.join("/info")?)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let info: NetworkInfo = serde_json::from_slice(&info)?;
        let mut urls: Vec<_> = info
            .committee
            .iter()
            .map(|s| Url::parse(s))
            .collect::<Result<Vec<_>, _>>()?;
        let probes: Vec<_> = urls.iter().map(|u| Self::probe(&client, u)).collect();
        let probes = join_all(probes).await;
        let (min_idx, min) = probes
            .into_iter()
            .enumerate()
            .min_by_key(|(_, l)| *l.as_ref().unwrap_or(&u64::MAX))
            .unwrap();
        let latency_ms = min.with_context(|| "All probes have failed")?;
        let peers_count = urls.len();
        let peer = urls.remove(min_idx);
        Ok(DiscoveryInfo {
            chain_id: info.chain_id,
            peers_count,
            preferred_peer: peer,
            latency_ms,
        })
    }

    async fn probe(client: &Client, url: &Url) -> anyhow::Result<u64> {
        let url = url.join("/ping")?;
        // Warm up / connect
        Self::ping(&client, &url).await?;
        const ATTEMPTS: u64 = 5;
        let mut total = 0u64;
        for _ in 0..ATTEMPTS {
            total += Self::ping(&client, &url).await?;
        }
        Ok(total / ATTEMPTS)
    }

    async fn ping(client: &Client, url: &Url) -> anyhow::Result<u64> {
        let now = Instant::now();
        client
            .get(url.join("/ping")?)
            .timeout(Duration::from_secs(1))
            .send()
            .await?
            .error_for_status()?;
        Ok(now.elapsed().as_millis() as u64)
    }

    fn make_client() -> Client {
        ClientBuilder::new()
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .http2_prior_knowledge()
            .build()
            .unwrap()
    }

    pub async fn submit(&self, channels: Vec<ChannelId>, data: Bytes) -> anyhow::Result<()> {
        let query = SubmitQuery { channels };
        self.client()
            .request(Method::POST, self.url.join("submit")?)
            .query(&query)
            .body(data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub fn tail(
        &self,
        from: ChannelElementKey,
        metadata: Vec<Metadata>,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<ChannelElementWithMetadata>>> {
        let query = TailQuery { from, metadata };
        let request = self
            .client()
            .request(Method::GET, self.url.join("tail")?)
            .query(&query);
        let event_source = EventSource::new(request)?;
        Ok(event_source
            .map_err(Into::into)
            .try_filter_map(|e| future::ready(Self::map_event(e)))
            .try_flatten())
    }

    fn client(&self) -> &Client {
        self.clients.choose(&mut ThreadRng::default()).unwrap()
    }

    fn map_event(
        event: Event,
    ) -> anyhow::Result<Option<impl Stream<Item = anyhow::Result<ChannelElementWithMetadata>>>>
    {
        match event {
            Event::Open => Ok(None),
            Event::Message(message) => {
                let items = serde_json::from_str::<Vec<_>>(&message.data)?;
                Ok(Some(stream::iter(items.into_iter().map(Ok))))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DiscoveryInfo {
    pub chain_id: String,
    pub peers_count: usize,
    pub preferred_peer: Url,
    pub latency_ms: u64,
}
