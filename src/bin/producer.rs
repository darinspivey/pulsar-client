#![allow(unused_imports)]
use clap::Parser;
use log::{debug, error, info, warn};
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};
use serde::{Deserialize, Serialize};

// FIXME: Requires local port-forwarding `kubectl port-forward service/pulsar-proxy 6650:6650 -n pipeline`

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of messages to publish
    #[arg(short, long, default_value_t = 1)]
    count: usize,

    /// Size of each message in kilobytes
    #[arg(short, long, default_value_t = 1)]
    size: usize,
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

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();
    let args = Args::parse();

    let addr = "pulsar://localhost:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic("your_tenant/your_namespace/your_topic")
        .with_name("my producer")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .build()
        .await?;

    // let large_data = generate_large_data(args.size);

    for i in 0..args.count {
        producer
            .send(TestData {
                data: generate_large_data(i, args.size),
            })
            .await?;

        info!(
            "Sent message {} of {} (size: {}KB)",
            i + 1,
            args.count,
            args.size
        );
    }

    info!("Finished sending {} messages", args.count);
    Ok(())
}
