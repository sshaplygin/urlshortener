use tokio::sync::mpsc;
use tracing::{Level, span};
use ydb::{TopicWriter, TopicWriterMessageBuilder};

use crate::entity;

pub async fn create(mut receiver: mpsc::Receiver<entity::VisitInfo>, producer: TopicWriter) {
    let worker_span = span!(Level::DEBUG, "visit_writer_worker");
    let _guard = worker_span.enter();
    let mut writer = producer;

    while let Some(visit) = receiver.recv().await {
        tracing::debug!("worker received a visit to write");

        let payload = match serde_json::to_vec(&visit) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::error!("serialize visit info: {}", err);
                continue;
            }
        };

        let message = match TopicWriterMessageBuilder::default()
            .data(payload)
            .build()
        {
            Ok(msg) => msg,
            Err(err) => {
                tracing::error!("build message: {}", err);
                continue;
            }
        };

        if let Err(err) = writer.write(message).await {
            tracing::error!("write message to ydb topic: {}", err);
        }
    }
    if let Err(err) = writer.stop().await {
        tracing::error!("stop writer: {}", err);
    }

    tracing::info!("writer channel closed, shutting down worker.");
}
