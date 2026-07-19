//! Durable visit writes.
//!
//! Visits are written to the topic from inside the request that produced them.
//! This service runs in a serverless container, where "a CPU resource is
//! allocated whenever a container instance is processing at least one request"
//! and a suspended instance's "running processes remain in RAM but are not
//! processed by the CPU". Work handed to a background task after the response
//! is returned may therefore never run at all: the task is frozen mid-flight
//! and discarded when the instance is eventually terminated. Anything that must
//! happen has to happen while the request is still open.

use tokio::sync::Mutex;
use ydb::{TopicWriter, TopicWriterMessageBuilder};

use crate::entity;

#[derive(Debug)]
pub enum VisitWriteError {
    Serialize(serde_json::Error),
    BuildMessage(String),
    Write(ydb::YdbError),
}

impl std::fmt::Display for VisitWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VisitWriteError::Serialize(err) => write!(f, "serialize visit info: {err}"),
            VisitWriteError::BuildMessage(err) => write!(f, "build message: {err}"),
            VisitWriteError::Write(err) => write!(f, "write message to ydb topic: {err}"),
        }
    }
}

impl std::error::Error for VisitWriteError {}

/// Writes visits to the YDB topic and waits for the server to acknowledge them.
pub struct VisitWriter {
    writer: Mutex<TopicWriter>,
}

impl VisitWriter {
    pub fn new(writer: TopicWriter) -> Self {
        VisitWriter {
            writer: Mutex::new(writer),
        }
    }

    /// Writes one visit, returning only once the topic has acknowledged it.
    ///
    /// Note on acknowledgement: `write_with_ack_future` resolves to the SDK's
    /// `MessageWriteStatus`, which distinguishes `Written` from
    /// `Skipped(AlreadyWritten)`. That type lives in a `pub(crate)` module and
    /// cannot be named — let alone matched — from outside the `ydb` crate, so a
    /// server-side deduplication skip is indistinguishable from a real write
    /// here. Skips are therefore prevented rather than detected, by giving each
    /// process a unique producer id (see `instance_producer_id` in main.rs).
    pub async fn write(&self, visit: &entity::VisitInfo) -> Result<(), VisitWriteError> {
        let payload = serde_json::to_vec(visit).map_err(VisitWriteError::Serialize)?;

        let message = TopicWriterMessageBuilder::default()
            .data(payload)
            .build()
            .map_err(|err| VisitWriteError::BuildMessage(err.to_string()))?;

        // The lock is held only long enough to hand the message to the writer's
        // internal queue; the acknowledgement is awaited after releasing it, so
        // concurrent requests pipeline instead of queueing behind one round-trip.
        let ack = {
            let mut writer = self.writer.lock().await;
            writer
                .write_with_ack_future(message)
                .await
                .map_err(VisitWriteError::Write)?
        };

        ack.await.map_err(VisitWriteError::Write)?;

        Ok(())
    }
}

// No `stop`/`flush` wrapper here on purpose: every write is acknowledged before
// the request that produced it returns, so the writer never holds unflushed
// state. That is the property that makes this safe on a platform which can
// suspend or terminate the process at any point between requests.
