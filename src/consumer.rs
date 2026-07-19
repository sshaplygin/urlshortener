use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::sync::Mutex;
use ua_parser::Extractor;
use ydb::{TableClient, YdbError};

use crate::db;
use crate::entity::VisitInfo;

/// Consecutive transaction failures tolerated before the reader is discarded
/// and reopened. `TopicReader` latches its first error permanently, so retrying
/// the same reader forever would stall the pipeline silently.
const FAILURES_BEFORE_READER_RESET: u32 = 3;
/// Backoff bounds applied after a failed transaction, so an outage cannot turn
/// this loop into a busy spin.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

// `instrument` rather than a manual span guard: an entered guard held across an
// await point leaks the span into whatever task the executor resumes next.
#[tracing::instrument(name = "visit_collect_worker", skip_all)]
pub async fn create(
    table_path: String,
    ua_extractor: Extractor<'static>,
    topic_table_client: TableClient,
    table_client: TableClient,
    topic_db: ydb::Client,
    consumer_name: String,
    topic_path: String,
) {
    let topic_tc = &topic_table_client;
    let tc_ref = &table_client;
    let table_path_ref = &table_path;
    let ua_extractor_ref = &ua_extractor;

    let mut backoff = INITIAL_BACKOFF;

    // Outer loop owns the reader's lifetime. `TopicReader` latches its first
    // error permanently, so a single transient failure would otherwise stop the
    // pipeline forever while the process stays up and looks healthy. After
    // repeated failures we discard the reader and open a fresh one.
    loop {
        let reader = match topic_db
            .topic_client()
            .create_reader(consumer_name.clone(), topic_path.clone())
            .await
        {
            Ok(reader) => reader,
            Err(err) => {
                let delay = backoff_with_jitter(backoff);
                tracing::error!("open topic reader, retrying in {:?}: {}", delay, err);
                tokio::time::sleep(delay).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        tracing::info!("topic reader opened for {}", topic_path);
        backoff = INITIAL_BACKOFF;

        let reader_mutex = Arc::new(Mutex::new(reader));
        let mut consecutive_failures: u32 = 0;

        while consecutive_failures < FAILURES_BEFORE_READER_RESET {
            let result = topic_tc
                .retry_transaction(|mut t| {
                    let reader_mutex = reader_mutex.clone();

                    async move {
                        let mut reader_guard = reader_mutex.lock().await;

                        // Read without a timeout. Wrapping this in `tokio::time::timeout`
                        // cancels the future mid-read: the server may already have cut a
                        // batch and advanced its cursor, and dropping the future then
                        // discards those messages. This worker is always-on, so blocking
                        // until a batch arrives is exactly what we want.
                        let batch = match reader_guard.pop_batch_in_tx(&mut t).await {
                            Ok(batch) => batch,
                            Err(err) => {
                                tracing::error!("reading batch: {err}");
                                return Err(ydb::YdbOrCustomerError::YDB(err));
                            }
                        };

                        tracing::debug!("read batch with {} messages", batch.messages.len());

                        let mut visits = Vec::<db::VisitData>::new();
                        for mut message in batch.messages {
                            let raw_data: Result<Option<Vec<u8>>, YdbError> =
                                message.read_and_take().await;

                            let json_bytes = match raw_data? {
                                Some(data) => data,
                                None => {
                                    tracing::warn!("empty message data, skipping");
                                    continue;
                                }
                            };

                            let visit_info: VisitInfo = match serde_json::from_slice(&json_bytes) {
                                Ok(visit_info) => visit_info,
                                Err(err) => {
                                    // Malformed payloads can never succeed on retry,
                                    // so drop them rather than blocking the batch.
                                    let s = String::from_utf8_lossy(&json_bytes);
                                    tracing::error!(
                                        "deserialize visit info: {} message: {}",
                                        err,
                                        s
                                    );
                                    continue;
                                }
                            };

                            let mut ua_info = None;
                            let mut referer = None;
                            let mut ip = None;

                            if let Some(info) = visit_info.request_info {
                                referer = info.referer;
                                ip = info.ip;
                                ua_info = parse_ua_info(ua_extractor_ref, info.user_agent);
                            }

                            visits.push(db::VisitData {
                                code: visit_info.link_info.code,
                                url: visit_info.link_info.url,

                                ua_info,
                                referer,
                                ip,

                                utm_campaign: visit_info.link_info.utm_campaign,
                                utm_content: visit_info.link_info.utm_content,
                                utm_source: visit_info.link_info.utm_source,

                                event_timestamp: visit_info.created_at,
                            });
                        }

                        if !visits.is_empty() {
                            tracing::debug!("visits batch: {:?}", &visits);

                            // Propagate the failure instead of committing: committing
                            // here would advance the topic offset and discard these
                            // visits permanently.
                            if let Err(err) =
                                db::add_visit(table_path_ref.clone(), tc_ref, visits).await
                            {
                                tracing::error!("add visits: {}", err);
                                return Err(ydb::YdbOrCustomerError::YDB(err));
                            }

                            tracing::debug!("visits added successfully");
                        }

                        t.commit().await?;

                        tracing::debug!("consumer batch committed successfully");

                        Ok(true)
                    }
                })
                .await;

            match result {
                Ok(_) => {
                    consecutive_failures = 0;
                    backoff = INITIAL_BACKOFF;
                }
                Err(err) => {
                    consecutive_failures += 1;
                    let delay = backoff_with_jitter(backoff);
                    tracing::error!(
                        "transaction failed ({}/{}), retrying in {:?}: {err}",
                        consecutive_failures,
                        FAILURES_BEFORE_READER_RESET,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }

        tracing::warn!(
            "reopening topic reader after {} consecutive failures",
            consecutive_failures
        );
    }
}

/// Adds up to 25% random jitter so concurrent workers do not retry in lockstep.
fn backoff_with_jitter(backoff: Duration) -> Duration {
    let max_jitter_ms = (backoff.as_millis() as u64) / 4;
    if max_jitter_ms == 0 {
        return backoff;
    }

    // Scoped so the non-Send RNG is dropped before any await point.
    let jitter_ms = {
        let mut rng = rand::rng();
        rng.random_range(0..=max_jitter_ms)
    };

    backoff + Duration::from_millis(jitter_ms)
}

fn parse_ua_info(extractor: &Extractor, user_agent: Option<String>) -> Option<db::UaInfo> {
    let ua_str = user_agent?;
    let ext = extractor.extract(&ua_str);

    let mut res = db::UaInfo {
        user_agent: Some(ua_str.clone()),
        browser: None,
        browser_version: None,
        os: None,
        os_version: None,
        device: None,
    };

    if let Some(user_agent) = ext.0 {
        let user_agent = user_agent.into_owned();
        res.browser_version = Some(format!(
            "{}.{}.{}",
            user_agent.major.as_deref().unwrap_or("N/A"),
            user_agent.minor.as_deref().unwrap_or("N/A"),
            user_agent.patch.as_deref().unwrap_or("N/A"),
        ));
        res.browser = Some(user_agent.family);
    }

    if let Some(os) = ext.1 {
        let os = os.into_owned();
        res.os_version = Some(format!(
            "{}.{}.{}",
            os.major.as_deref().unwrap_or("N/A"),
            os.minor.as_deref().unwrap_or("N/A"),
            os.patch.as_deref().unwrap_or("N/A"),
        ));
        res.os = Some(os.os);
    }

    if let Some(device) = ext.2 {
        let d = device.into_owned();
        res.device = Some(format!(
            "{} {} {}",
            d.brand.as_deref().unwrap_or("N/A"),
            d.device,
            d.model.as_deref().unwrap_or("N/A"),
        ));
    }

    Some(res)
}
