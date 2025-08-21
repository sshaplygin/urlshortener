use axum::{
    Router,
    extract::{FromRequestParts, Path, State},
    http::StatusCode,
    http::request::Parts,
    response::{IntoResponse, Json, Redirect},
    routing::{get, post},
};
use axum_client_ip::XRealIp as ClientIp;
use axum_extra::{TypedHeader, headers::Referer, headers::UserAgent};
use chrono::DateTime;
use dotenv::dotenv;
use hashers::fnv::fnv1a32;
use serde::{Deserialize, Serialize};
use std::{
    env,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{Mutex, mpsc};
use tracing::{Level, span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use ydb::{
    TableClient, TopicReader, TopicWriter, TopicWriterMessageBuilder, TopicWriterOptionsBuilder,
    YdbError,
};

use crate::config::Environment;

mod config;
mod db;

#[derive(Clone)]
struct AppState {
    urls_client: TableClient,
    visit_sender: mpsc::Sender<VisitInfo>,
    config: config::Config,
}

#[derive(Serialize)]
struct ShortenResponse {
    code: String,
    short_url: String,
}

#[derive(Deserialize)]
struct ShortnerRequest {
    url: String,
    utm_source: Option<String>,
    utm_campaign: Option<String>,
    utm_content: Option<String>,
    description: Option<String>,
}

async fn shorten(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ShortnerRequest>,
) -> Result<Json<ShortenResponse>, StatusCode> {
    let code = generate_code(&body.url, 6);

    let data = db::CreateData {
        url: body.url.clone(),
        code: code.clone(),
        utm_source: body.utm_source,
        utm_campaign: body.utm_campaign,
        utm_content: body.utm_content,
        description: body.description,
    };

    match db::insert(&state.urls_client, data).await {
        Ok(()) => Ok(Json(ShortenResponse {
            code: code.clone(),
            short_url: state.config.short_url(&code),
        })),
        Err(err) => {
            tracing::error!("shorten: get return err: {}", err);

            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    user_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    referer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<IpAddr>,
}

impl RequestInfo {
    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.referer.is_none() && self.ip.is_none()
    }
}

impl<S> FromRequestParts<S> for RequestInfo
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let user_agent = TypedHeader::<UserAgent>::from_request_parts(parts, _state)
            .await
            .map(|TypedHeader(ua)| ua.to_string())
            .ok();

        let referer = TypedHeader::<Referer>::from_request_parts(parts, _state)
            .await
            .map(|TypedHeader(r)| r.to_string())
            .ok();

        let ip = parts.extensions.get::<ClientIp>().map(|ClientIp(ip)| *ip);

        Ok(RequestInfo {
            user_agent,
            referer,
            ip,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct VisitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    request_info: Option<RequestInfo>,
    link_info: db::LinkInfo,
    created_at: DateTime<chrono::Utc>,
}

impl VisitInfo {
    fn is_empty(&self) -> bool {
        self.request_info.is_none()
    }
}

async fn redirect(
    request_info: RequestInfo,
    Path(code): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let handler_span = span!(Level::DEBUG, "redirect_handler");
    let _guard = handler_span.enter();

    if !request_info.is_empty() {
        tracing::debug!("user request info: {:?}", request_info);
    }

    let link_info = match db::get(&state.urls_client, code.clone()).await {
        Ok(link_info) => link_info,
        Err(YdbError::NoRows) => db::LinkInfo {
            code,
            url: None,
            utm_source: None,
            utm_campaign: None,
            utm_content: None,
        },
        Err(err) => {
            tracing::error!("get origin url: {}", err);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let visit = VisitInfo {
        request_info: if request_info.is_empty() {
            None
        } else {
            Some(request_info)
        },
        link_info: link_info.clone(),
        created_at: chrono::Utc::now(),
    };

    if !visit.is_empty() {
        if let Err(err) = state.visit_sender.send(visit).await {
            tracing::error!("send visit info: {}", err);
        }
    }

    if let Some(url) = link_info.url {
        return Ok(Redirect::temporary(&url));
    }

    // TODO: return static page for feedback
    Err(StatusCode::NO_CONTENT)
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let app_env = env::var("APP_ENV").unwrap_or_else(|_| "development".into());
    let env: Environment = app_env.parse().unwrap_or(Environment::Development);

    let tracing_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // axum logs rejections from built-in extractors with the `axum::rejection`
            // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
            format!(
                "{}=debug,tower_http=debug,axum::rejection=trace",
                env!("CARGO_CRATE_NAME")
            )
            .into()
        });

    match env {
        Environment::Production => {
            tracing_subscriber::registry()
                .with(tracing_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(tracing_filter)
                .with(tracing_subscriber::fmt::layer().with_line_number(true))
                .init();
        }
    }

    let db = match tokio::time::timeout(
        Duration::from_secs(5),
        db::init_db("YDB_URLS_CONNECTION_STRING".to_string(), env.clone()),
    )
    .await
    {
        Ok(Ok(db)) => db,
        Ok(Err(err)) => {
            tracing::error!("init ydb: {}", err);
            return;
        }
        Err(err) => {
            tracing::error!("connect to ydb by timeout: {}", err);
            return;
        }
    };

    let urls_client = db.table_client();

    db::init_urls_tables(&urls_client).await.unwrap();

    let mut topic_client = db.topic_client();
    let producer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path("/topics/visits".to_string())
                .producer_id("producer".to_string())
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let config = config::Config::new()
        .with_domain(&env::var("DOMAIN").expect("DOMAIN must be set"))
        .with_env(env.clone());

    let (tx, rx) = mpsc::channel::<VisitInfo>(1024);

    tokio::spawn(visit_writer_worker(rx, producer));

    let consumer = topic_client
        .create_reader(
            "urlshortener-consumer".to_string(),
            "/topics/visits".to_string(),
        )
        .await
        .unwrap();

    let db = match tokio::time::timeout(
        Duration::from_secs(5),
        db::init_db("YDB_VISITS_CONNECTION_STRING".to_string(), env),
    )
    .await
    {
        Ok(Ok(db)) => db,
        Ok(Err(err)) => {
            tracing::error!("init ydb: {}", err);
            return;
        }
        Err(err) => {
            tracing::error!("connect to ydb by timeout: {}", err);
            return;
        }
    };

    let visits_client = db.table_client();

    db::init_visits_tables(&visits_client).await.unwrap();

    let regexes_bytes: &[u8] = include_bytes!("../regexes.yaml");
    let regexes: ua_parser::Regexes = serde_yaml::from_slice(regexes_bytes).unwrap();
    let extractor = ua_parser::Extractor::try_from(regexes).unwrap();

    let ua_info = extractor.extract("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");

    println!("ua_info: {:?}", ua_info);

    tokio::spawn(visit_collect_worker(
        urls_client.clone(),
        visits_client,
        consumer,
    ));

    let state = Arc::new(AppState {
        urls_client,
        visit_sender: tx,
        config,
    });

    let app = Router::new()
        .route("/api/shorten", post(shorten))
        .route("/cc/{code}", get(redirect))
        .with_state(state);

    let port = env::var("PORT")
        .expect("PORT must be set")
        .parse::<u16>()
        .expect("PORT must be a valid u16 number");
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::debug!("Listeging on {}", addr);
    let listner = TcpListener::bind(addr.to_string()).await.unwrap();

    axum::serve(listner, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn visit_collect_worker(
    topic_table_client: TableClient,
    table_client: TableClient,
    reader: TopicReader,
) {
    let worker_span = span!(Level::DEBUG, "visit_collect_worker");
    let _guard = worker_span.enter();

    let reader_mutex = Arc::new(Mutex::new(reader));

    let topic_tc = &topic_table_client;
    let tc_ref = &table_client;

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
                                    ua_info = get_ua_info(info.user_agent);
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

                                db::add_visit(tc_ref, visits).await.unwrap();

                                tracing::debug!("visits added successfully");
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
                            tracing::info!("timeout reading batch - no more messages available");
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

fn get_ua_info(user_agent: Option<String>) -> Option<db::UaInfo> {
    // TODO: Implement actual UA parsing using the `ua-parser` crate
    // For now, returning a dummy UaInfo with N/A values
    let ua_string = user_agent.unwrap_or_else(|| "N/A".to_string());

    Some(db::UaInfo {
        user_agent: ua_string,
        browser: "N/A".to_string(),
        browser_version: "N/A".to_string(),
        os: "N/A".to_string(),
        os_version: "N/A".to_string(),
        device: "N/A".to_string(),
    })

    // Example of how to use ua-parser (needs to be initialized and passed in)

    // if let Some(ua) = client.user_agent {
    //     let family = ua.family;
    //     let major = ua.major.unwrap_or_else(|| "N/A".to_string());
    //     let minor = ua.minor.unwrap_or_else(|| "N/A".to_string());
    //     let patch = ua.patch.unwrap_or_else(|| "N/A".to_string());

    //     println!("\n--- Browser ---");
    //     println!("Family: {}", family);
    //     println!("Version: {}.{}.{}", major, minor, patch);
    // }

    // if let Some(os) = client.os {
    //     let family = os.family;
    //     let major = os.major.unwrap_or_else(|| "N/A".to_string());

    //     println!("\n--- Operating System ---");
    //     println!("Family: {}", family);
    //     println!("Major Version: {}", major);
    // }

    // if let Some(device) = client.device {
    //     let family = device.family;

    //     println!("\n--- Device ---");
    //     println!("Family: {}", family);
    // }
}

async fn visit_writer_worker(mut receiver: mpsc::Receiver<VisitInfo>, producer: TopicWriter) {
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

        let message = TopicWriterMessageBuilder::default()
            .data(payload)
            .build()
            .unwrap();

        if let Err(err) = writer.write(message).await {
            tracing::error!("write message to ydb topic: {}", err);
        }
    }
    writer.stop().await.unwrap();

    tracing::info!("writer channel closed, shutting down worker.");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn generate_code(input: &str, length: usize) -> String {
    let hash = fnv1a32(input.as_bytes());
    let encoded = base_62::encode(&hash.to_be_bytes());
    encoded[..length.min(encoded.len())].to_string()
}
