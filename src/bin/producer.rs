#![allow(unused_imports)]
use clap::{ArgGroup, Parser};
use hyper::{Body, Client, Request, Uri};
use hyper_tls::HttpsConnector;
use log::{debug, error, info, warn};
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

// FIXME: Requires local port-forwarding `kubectl port-forward service/pulsar-proxy 6650:6650 -n pipeline`

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("destination")
        .required(true)
        .args(&["topic", "http_endpoint"]),
))]
struct Args {
    /// Number of messages to publish
    #[arg(short, long, default_value_t = 1)]
    count: usize,

    /// Size of each message in kilobytes
    #[arg(short, long, default_value_t = 1)]
    size: usize,

    /// Topic to publish messages to
    #[arg(short, long)]
    topic: Option<String>,

    /// HTTP endpoint to publish messages to
    #[arg(long)]
    http_endpoint: Option<String>,

    /// Authorization token for HTTP requests
    #[arg(long)]
    auth_token: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl SerializeMessage for TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

fn generate_large_data(num: usize, size_kb: usize) -> String {
    let base_string = format!("This is a test message {} ", num);
    let target_size = size_kb * 1024;
    let repetitions = target_size / base_string.len();
    base_string.repeat(repetitions)
}

type HttpClient = Client<HttpsConnector<hyper::client::HttpConnector>>;

fn create_http_client() -> HttpClient {
    let https = HttpsConnector::new();
    Client::builder().build(https)
}

async fn send_to_http(
    client: &HttpClient,
    endpoint: &str,
    data: &TestData,
    auth_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let uri = Uri::from_str(endpoint)?;
    let body = serde_json::to_string(data)?;
    let mut req = Request::post(uri).header("content-type", "application/json");

    if let Some(token) = auth_token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }

    let req = req.body(Body::from(body))?;

    let resp = client.request(req).await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP request failed with status: {}", resp.status()).into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let use_http = args.http_endpoint.is_some();
    let mut pulsar_producer = None;
    let http_client = create_http_client();

    if !use_http {
        if let Some(topic) = &args.topic {
            let addr = "pulsar://localhost:6650";
            let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
            pulsar_producer = Some(
                pulsar
                    .producer()
                    .with_topic(topic)
                    .with_name("my producer")
                    .with_options(producer::ProducerOptions {
                        schema: Some(proto::Schema {
                            r#type: proto::schema::Type::String as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .build()
                    .await?,
            );
        }
    }

    for i in 0..args.count {
        let data = TestData {
            data: generate_large_data(i, args.size),
        };

        if use_http {
            send_to_http(
                &http_client,
                args.http_endpoint.as_ref().unwrap(),
                &data,
                args.auth_token.as_deref(),
            )
            .await?;
        } else {
            if let Some(producer) = &mut pulsar_producer {
                producer.send(data).await?;
            }
        }

        info!(
            "Sent message {} of {} (size: {}KB) to {}",
            i + 1,
            args.count,
            args.size,
            if use_http {
                "HTTP endpoint"
            } else {
                "Pulsar topic"
            }
        );
    }

    info!("Finished sending {} messages", args.count);
    Ok(())
}
