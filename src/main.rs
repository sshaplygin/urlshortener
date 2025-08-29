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
use ua_parser::Extractor;
use url::Url;
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
// TODO: add ttl with milleconds field and expired_at timestamp with tz
struct ShortnerRequest {
    url: String,
    utm_source: Option<String>,
    utm_campaign: Option<String>,
    utm_content: Option<String>,
    description: Option<String>,
}

impl ShortnerRequest {
    fn build_url_with_utm(&self) -> Result<String, url::ParseError> {
        let mut url = Url::parse(&self.url)?;
        {
            let mut query_pairs = url.query_pairs_mut();

            if let Some(source) = &self.utm_source {
                query_pairs.append_pair("utm_source", source);
            }
            if let Some(campaign) = &self.utm_campaign {
                query_pairs.append_pair("utm_campaign", campaign);
            }
            if let Some(content) = &self.utm_content {
                query_pairs.append_pair("utm_content", content);
            }
        }

        // 4. Преобразуем обратно в строку и возвращаем.
        Ok(url.to_string())
    }
}

async fn shorten(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ShortnerRequest>,
) -> Result<Json<ShortenResponse>, StatusCode> {
    let handler_span = span!(Level::DEBUG, "shorten");
    let _guard = handler_span.enter();

    let u = match body.build_url_with_utm() {
        Ok(u) => u,
        Err(err) => {
            tracing::error!("build url with utm: {}", err);

            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let code = generate_code(u.as_str(), 6);

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

    // NOTICE: uncomment if you need run migration
    // db::init_urls_tables(&urls_client).await.unwrap();

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
        .with_origin(&env::var("ORIGIN").expect("ORIGIN must be set"))
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

    // NOTICE: uncomment if you need run migration
    // db::init_visits_tables(&visits_client).await.unwrap();

    let regexes_bytes: &[u8] = include_bytes!("../regexes.yaml");
    let regexes: ua_parser::Regexes = serde_yaml::from_slice(regexes_bytes).unwrap();
    let extractor = ua_parser::Extractor::try_from(regexes).unwrap();

    tokio::spawn(visit_collect_worker(
        env::var("VISITS_TABLE_PATH").expect("VISITS_TABLE_PATH must be set"),
        extractor,
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
                                    ua_info = get_ua_info(ua_extractor_ref, info.user_agent);
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

fn get_ua_info(extractor: &Extractor, user_agent: Option<String>) -> Option<db::UaInfo> {
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
