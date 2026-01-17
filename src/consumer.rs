use std::{sync::Arc, time::Duration};
use ua_parser::Extractor;

use crate::entity::VisitInfo;
use tokio::sync::Mutex;
use tracing::{Level, span};
use ydb::{TableClient, TopicReader, YdbError};

use crate::db;

pub async fn create(
    table_path: String,
    ua_extractor: Extractor<'static>,
    topic_table_client: TableClient,
    table_client: TableClient,
    reader: TopicReader,
) {
    let worker_span = span!(Level::DEBUG, "visit_collect_worker");
    let _guard = worker_span.enter();

    let reader_mutex = Arc::new(Mutex::new(reader));

    let topic_tc = &topic_table_client;
    let tc_ref = &table_client;
    let table_path_ref = &table_path;
    let ua_extractor_ref = &ua_extractor;

    loop {
        let result = topic_tc
            .retry_transaction(|mut t| {
                let reader_mutex = reader_mutex.clone();

                async move {
                    let mut reader_guard = reader_mutex.lock().await;

                    let batch_result = tokio::time::timeout(
                        Duration::from_secs(3),
                        reader_guard.pop_batch_in_tx(&mut t),
                    )
                    .await;

                    match batch_result {
                        Ok(Ok(batch)) => {
                            tracing::debug!("read batch with {} messages", batch.messages.len());

                            let mut visits = Vec::<db::VisitData>::new();
                            for mut message in batch.messages {
                                let raw_data: Result<Option<Vec<u8>>, YdbError> =
                                    message.read_and_take().await;

                                let json_bytes =
                                    raw_data.unwrap().ok_or("empty message data").unwrap();
                                let visit_info: VisitInfo =
                                    match serde_json::from_slice(&json_bytes) {
                                        Ok(visit_info) => visit_info,
                                        Err(err) => {
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
                                };

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

                                match db::add_visit(table_path_ref.clone(), tc_ref, visits).await {
                                    Err(err) => {
                                        tracing::error!("add visits: {}", err);
                                    }
                                    _ => {
                                        tracing::debug!("visits added successfully");
                                    }
                                };
                            }

                            t.commit().await?;

                            tracing::debug!("consumer batch committed successfully");

                            Ok(true)
                        }
                        Ok(Err(err)) => {
                            tracing::error!("reading batch: {err}");
                            Err(ydb::YdbOrCustomerError::YDB(err))
                        }
                        Err(_timeout_err) => {
                            tracing::debug!("timeout reading batch - no more messages available");
                            Ok(false)
                        }
                    }
                }
            })
            .await;

        match result {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!("all messages have been read and stored");
            }
            Err(err) => {
                tracing::error!("transaction failed: {err}");
            }
        }
    }
}

fn parse_ua_info(extractor: &Extractor, user_agent: Option<String>) -> Option<db::UaInfo> {
    let ua_str = match user_agent {
        Some(ua_str) => ua_str,
        _ => return None,
    };

    let ua = &ua_str.clone();
    let ext = extractor.extract(ua);

    let mut res = db::UaInfo {
        user_agent: Some(ua_str),
        browser: None,
        browser_version: None,
        os: None,
        os_version: None,
        device: None,
    };

    if let Some(user_agent) = ext.0 {
        let user_agent = user_agent.into_owned();
        res.browser = Some(user_agent.family);
        res.browser_version = Some(format!(
            "{}.{}.{}",
            user_agent.major.unwrap_or("N/A".to_string()),
            user_agent.minor.unwrap_or("N/A".to_string()),
            user_agent.patch.unwrap_or("N/A".to_string()),
        ));
    }

    if let Some(os) = ext.1 {
        let os = os.into_owned();
        res.os = Some(os.os);
        res.os_version = Some(format!(
            "{}.{}.{}",
            os.major.unwrap_or("N/A".to_string()),
            os.minor.unwrap_or("N/A".to_string()),
            os.patch.unwrap_or("N/A".to_string()),
        ));
    };

    if let Some(device) = ext.2 {
        let d = device.into_owned();
        res.device = Some(format!(
            "{} {} {}",
            d.brand.unwrap_or("N/A".to_string()),
            d.device,
            d.model.unwrap_or("N/A".to_string()),
        ));
    };

    Some(res)
}
