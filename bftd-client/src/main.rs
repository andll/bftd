use clap::Parser;
use futures_util::stream::StreamExt;
use reqwest::{Client, Method, Url};
use reqwest_eventsource::{Event, EventSource};
use serde::Serialize;
use serde_with::serde_as;
use serde_with::serde_derive::Deserialize;
use std::{env, process};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
enum Command {
    Submit(SubmitArgs),
    Tail(TailArgs),
}

#[derive(Parser, Debug)]
struct TailArgs {
    #[arg(long)]
    from: String,
}

#[derive(Parser, Debug)]
struct SubmitArgs {
    #[arg(long)]
    channels: String,
    #[arg(long)]
    data: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let command = Command::parse();
    let Ok(url) = env::var("BFTD_URL") else {
        println!("BFTD_URL env var is not set");
        process::exit(1);
    };
    let main = BftdClientMain {
        url: url.parse()?,
        client: Client::new(),
    };
    match command {
        Command::Submit(s) => main.handle_submit(s).await,
        Command::Tail(t) => main.handle_tail(t).await,
    }
}

struct BftdClientMain {
    url: Url,
    client: Client,
}

impl BftdClientMain {
    async fn handle_submit(self, args: SubmitArgs) -> anyhow::Result<()> {
        let query = SubmitQuery {
            channels: args.channels,
        };
        let response = self
            .client
            .request(Method::POST, self.url.join("submit").unwrap())
            .query(&query)
            .body(args.data)
            .send()
            .await?;
        response.error_for_status()?;
        Ok(())
    }

    async fn handle_tail(self, args: TailArgs) -> anyhow::Result<()> {
        let query = TailQuery { from: args.from };
        let request = self
            .client
            .request(Method::GET, self.url.join("tail").unwrap())
            .query(&query);
        let mut event_source = EventSource::new(request)?;
        while let Some(event) = event_source.next().await {
            let event = event?;
            match event {
                Event::Open => {}
                Event::Message(message) => {
                    println!("id {}", message.id);
                    let event: AsString = serde_json::from_str(&message.data)?;
                    match String::from_utf8(event.0) {
                        Ok(s) => println!("{s}"),
                        Err(_) => println!("{}", message.data),
                    }

                    println!("=====");
                }
            }
        }
        Ok(())
    }
}
#[serde_as]
#[derive(Deserialize)]
struct AsString(#[serde_as(as = "serde_with::Bytes")] Vec<u8>);

#[derive(Serialize)]
struct SubmitQuery {
    channels: String,
}

#[derive(Serialize)]
struct TailQuery {
    from: String,
}
