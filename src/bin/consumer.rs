#![allow(unused_imports)]
use futures::TryStreamExt;
use log;
use pulsar::{
    consumer::{ConsumerOptions, InitialPosition},
    message::{proto::command_subscribe::SubType, Payload},
    Consumer, DeserializeMessage, Pulsar, TokioExecutor,
};
use serde::{Deserialize, Serialize};

// FIXME: Requires local port-forwarding `kubectl port-forward service/pulsar-proxy 6650:6650 -n pipeline`

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl DeserializeMessage for TestData {
    type Output = Result<TestData, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let addr = "pulsar://localhost:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic("mezmo/pipeline/pipeline.v1.darin")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Shared)
        .with_subscription("test_subscription")
        .with_options(ConsumerOptions {
            initial_position: InitialPosition::Earliest,
            ..Default::default()
        })
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                break;
            }
        };

        log::debug!("received message: {:?}", data.data);
        counter += 1;
        log::info!("got {} messages", counter);
    }

    Ok(())
}
