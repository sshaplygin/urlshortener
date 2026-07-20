use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::sync::Mutex;
use ua_parser::Extractor;
use ydb::{QueryClient, TableClient, YdbError};

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
    topic_query_client: QueryClient,
    table_client: TableClient,
    topic_db: ydb::Client,
    consumer_name: String,
    topic_path: String,
) {
    let topic_qc = &topic_query_client;
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
            let result = topic_qc
                .retry_tx(async |t| {
                    let mut reader_guard = reader_mutex.lock().await;

                    // Read without a timeout. Wrapping this in `tokio::time::timeout`
                    // cancels the future mid-read: the server may already have cut a
                    // batch and advanced its cursor, and dropping the future then
                    // discards those messages. This worker is always-on, so blocking
                    // until a batch arrives is exactly what we want.
                    let batch = match reader_guard.pop_batch_in_tx(t).await {
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
                                tracing::error!("deserialize visit info: {} message: {}", err, s);
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

                        // Return the failure instead of `Ok`: returning `Ok` commits,
                        // which would advance the topic offset and discard these
                        // visits permanently.
                        if let Err(err) =
                            db::add_visit(table_path_ref.clone(), tc_ref, visits).await
                        {
                            tracing::error!("add visits: {}", err);
                            return Err(ydb::YdbOrCustomerError::YDB(err));
                        }

                        tracing::debug!("visits added successfully");
                    }

                    // Returning `Ok` commits the transaction, advancing the topic
                    // offset past this batch. Every path that must not lose messages
                    // returns `Err` above, which rolls back instead.
                    Ok(true)
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

#[cfg(test)]
mod tests {
    // Tests assert by panicking; a failed `expect` here is the failure report.
    #![allow(clippy::expect_used)]

    use chrono::Utc;
    use ydb::{
        ClientBuilder, ConsumerBuilder, CreateTopicOptionsBuilder, StaticDiscovery,
        TopicWriterOptions,
    };

    use super::*;
    use crate::entity::{RequestInfo, VisitInfo};
    use crate::producer::VisitWriter;

    const TEST_CONNECTION_STRING: &str = "grpc://localhost:2136?database=/local";
    const TEST_ENDPOINT: &str = "grpc://localhost:2136";
    const VISITS_TABLE_PATH: &str = "/local/visits";

    fn extractor() -> Extractor<'static> {
        let regexes_bytes: &[u8] = include_bytes!("../regexes.yaml");
        let regexes: ua_parser::Regexes =
            serde_yaml::from_slice(regexes_bytes).expect("parse ua regexes");

        Extractor::try_from(regexes).expect("build ua extractor")
    }

    async fn client() -> ydb::Client {
        let client = ClientBuilder::new_from_connection_string(TEST_CONNECTION_STRING)
            .expect("parse connection string")
            .with_credentials(ydb::AnonymousCredentials::new())
            // The container advertises its own hostname through discovery,
            // which the host cannot resolve; pinning skips discovery.
            .with_discovery(StaticDiscovery::new_from_str(TEST_ENDPOINT).expect("static discovery"))
            .client()
            .expect("build client");

        client
            .wait()
            .await
            .expect("connect to local ydb (is `docker compose up -d ydb` running?)");

        client
    }

    /// Drives one full pass of the real drain loop: a visit is written to a
    /// topic by the producer, then read back and persisted by `create`.
    ///
    /// This is the only coverage of the migrated `retry_tx` callback, where
    /// commit became implicit — the SDK now commits when the callback returns
    /// `Ok`, so a mistake here silently advances the topic offset and drops
    /// visits rather than failing loudly.
    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn drains_a_visit_from_the_topic_into_the_visits_table() {
        // The `db` tests own the shared schema; reuse it so both suites agree
        // on the `visits` columns.
        let client = crate::db::tests::schema().await;

        let topic_name = "consumer-drain-test";
        let topic_path = format!("/local/topics/{topic_name}");
        let consumer_name = "urlshortener-consumer-test".to_string();
        let code = format!("drain-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));

        let mut topic_client = client.topic_client();
        // Ignore the error: the topic usually does not exist yet.
        let _ = topic_client.drop_topic(topic_path.clone()).await;
        topic_client
            .create_topic(
                topic_path.clone(),
                CreateTopicOptionsBuilder::default()
                    .consumers(vec![
                        ConsumerBuilder::default()
                            .name(consumer_name.clone())
                            .build()
                            .expect("build consumer"),
                    ])
                    .build()
                    .expect("build topic options"),
            )
            .await
            .expect("create topic");

        // Publish one visit through the real producer.
        let writer = topic_client
            .create_writer_with_params(
                TopicWriterOptions::builder()
                    .topic_path(topic_path.clone())
                    .producer_id("consumer-drain-test-producer".to_string())
                    .build(),
            )
            .await
            .expect("create topic writer");

        let visit = VisitInfo {
            request_info: Some(RequestInfo {
                user_agent: Some(
                    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/128.0".to_string(),
                ),
                referer: Some("https://news.test/".to_string()),
                ip: Some("203.0.113.9".parse().expect("valid ip")),
            }),
            link_info: crate::db::LinkInfo {
                code: code.clone(),
                url: Some("https://example.test/drained".to_string()),
                utm_source: Some("telegram".to_string()),
                utm_campaign: Some("launch".to_string()),
                utm_content: Some("banner".to_string()),
            },
            created_at: Utc::now(),
        };

        VisitWriter::new(writer)
            .write(&visit)
            .await
            .expect("publish visit to topic");

        // Run the real drain loop until it has persisted the visit. `create`
        // never returns, so race it against a deadline rather than spawning it
        // — its `retry_tx` future is not `Send` and cannot be sent to a task.
        // `ydb::Client` is not `Clone`, and the drain takes ownership of one to
        // open its reader, so give it a connection of its own.
        let consumer_db = self::client().await;

        let drain = create(
            VISITS_TABLE_PATH.to_string(),
            extractor(),
            client.query_client(),
            client.table_client(),
            consumer_db,
            consumer_name,
            topic_path.clone(),
        );

        let mut qc = client.query_client();
        let persisted = tokio::select! {
            _ = drain => unreachable!("the drain loop should not return"),
            found = wait_for_visit(&mut qc, &code) => found,
        };

        assert!(
            persisted,
            "the consumer should have written the visit to `visits`"
        );

        // The UA string must have been parsed on the way through, not stored raw.
        let mut row = qc
            .query_row(
                "
                    DECLARE $code AS Utf8;
                    SELECT browser, referer FROM visits WHERE code = $code;
                ",
            )
            .params(ydb::ydb_params!("$code" => code))
            .await
            .expect("read back the drained visit");

        let browser: Option<String> = row
            .remove_field_by_name("browser")
            .expect("browser column")
            .try_into()
            .expect("browser is Utf8");
        let referer: Option<String> = row
            .remove_field_by_name("referer")
            .expect("referer column")
            .try_into()
            .expect("referer is Utf8");

        assert_eq!(browser.as_deref(), Some("Firefox"));
        assert_eq!(referer.as_deref(), Some("https://news.test/"));

        let _ = topic_client.drop_topic(topic_path).await;
    }

    /// Polls until the visit shows up, or gives up after a bounded wait.
    async fn wait_for_visit(qc: &mut ydb::QueryClient, code: &str) -> bool {
        for _ in 0..100 {
            let found = qc
                .query_row(
                    "
                        DECLARE $code AS Utf8;
                        SELECT code FROM visits WHERE code = $code;
                    ",
                )
                .params(ydb::ydb_params!("$code" => code.to_string()))
                .optional()
                .await
                .expect("poll visits");

            if found.is_some() {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        false
    }
}
