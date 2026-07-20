#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
// The consumer's `retry_tx` callback is a large async closure whose nested
// future layout exceeds the default limit when the compiler computes it.
#![recursion_limit = "256"]

mod code;
mod config;
mod consumer;
mod db;
mod entity;
mod producer;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Redirect, Response},
    routing::{get, post},
};
use dotenvy::dotenv;
use http::Request;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::signal;
use tower_governor::{
    GovernorLayer,
    errors::GovernorError,
    governor::GovernorConfigBuilder,
    key_extractor::{KeyExtractor, PeerIpKeyExtractor, SmartIpKeyExtractor},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use ydb::{QueryClient, TopicWriterOptions};

use crate::code::CodeAllocator;
use crate::config::{AppRole, Config, Environment};
use crate::db::InsertOutcome;

const VISITS_TOPIC_PATH: &str = "/topics/visits";
const CONSUMER_NAME: &str = "urlshortener-consumer";
const DB_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Builds a producer id unique to this process.
///
/// YDB deduplicates topic messages on `(producer_id, seq_no)`, and each writer
/// resumes its sequence from the server's `last_seq_no` at init. A shared,
/// constant producer id therefore breaks under scale-out: every concurrent
/// instance reads the same starting sequence and emits the same numbers, so the
/// server accepts one instance's messages and silently skips the rest as
/// duplicates. The skip is invisible to us — the SDK's `MessageWriteStatus`
/// type is `pub(crate)` and cannot be inspected — so uniqueness is the only
/// available defence.
fn instance_producer_id() -> String {
    let id: u128 = rand::rng().random();

    format!("urlshortener-{id:032x}")
}

/// Schemes accepted as redirect targets. Anything else (`javascript:`, `data:`,
/// `file:`, custom app schemes) is rejected so the service cannot be used to
/// launder hostile URLs behind a trusted domain.
const ALLOWED_SCHEMES: [&str; 2] = ["http", "https"];

#[derive(OpenApi)]
#[openapi(
    paths(shorten, redirect, health),
    components(schemas(ShortenResponse, ShortnerRequest, ErrorResponse, HealthResponse))
)]
struct ApiDoc;

struct AppState {
    urls_client: QueryClient,
    code_allocator: CodeAllocator,
    visit_writer: producer::VisitWriter,
    config: Config,
}

#[derive(Serialize, ToSchema)]
struct ShortenResponse {
    code: String,
    short_url: String,
}

#[derive(Serialize, ToSchema)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize, ToSchema)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Deserialize, ToSchema)]
// TODO: add ttl with milliseconds field and expired_at timestamp with tz
struct ShortnerRequest {
    url: String,
    utm_source: Option<String>,
    utm_campaign: Option<String>,
    utm_content: Option<String>,
    description: Option<String>,
}

/// Why a submitted URL was refused. Each variant maps to a client-safe message;
/// none of them expose internal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UrlValidationError {
    Malformed,
    UnsupportedScheme,
    MissingHost,
    EmbeddedCredentials,
}

impl UrlValidationError {
    fn message(self) -> &'static str {
        match self {
            UrlValidationError::Malformed => "url is not a valid absolute URL",
            UrlValidationError::UnsupportedScheme => "url scheme must be http or https",
            UrlValidationError::MissingHost => "url must include a host",
            UrlValidationError::EmbeddedCredentials => "url must not embed credentials",
        }
    }
}

/// Error returned to API clients. Internal failures deliberately carry a fixed
/// message so database and topology details never leak into responses.
enum ApiError {
    BadRequest(&'static str),
    NotFound(&'static str),
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
        };

        (
            status,
            Json(ErrorResponse {
                error: message.to_string(),
            }),
        )
            .into_response()
    }
}

impl From<UrlValidationError> for ApiError {
    fn from(err: UrlValidationError) -> Self {
        ApiError::BadRequest(err.message())
    }
}

impl ShortnerRequest {
    /// Validates the submitted URL and returns the final redirect target with
    /// UTM parameters applied.
    fn build_target_url(&self) -> Result<Url, UrlValidationError> {
        let mut url = Url::parse(&self.url).map_err(|_| UrlValidationError::Malformed)?;

        if !ALLOWED_SCHEMES.contains(&url.scheme()) {
            return Err(UrlValidationError::UnsupportedScheme);
        }

        if !url.has_host() {
            return Err(UrlValidationError::MissingHost);
        }

        // `https://trusted.example@evil.test` renders as the trusted host in
        // many clients, so refuse userinfo outright.
        if !url.username().is_empty() || url.password().is_some() {
            return Err(UrlValidationError::EmbeddedCredentials);
        }

        let utm_params = [
            ("utm_source", self.utm_source.as_deref()),
            ("utm_campaign", self.utm_campaign.as_deref()),
            ("utm_content", self.utm_content.as_deref()),
        ];

        // Only touch the query when there is something to add: entering
        // `query_pairs_mut` unconditionally appends a bare `?` to URLs that had
        // no query string.
        if utm_params.iter().any(|(_, value)| value.is_some()) {
            let mut query_pairs = url.query_pairs_mut();

            for (key, value) in utm_params {
                if let Some(value) = value {
                    query_pairs.append_pair(key, value);
                }
            }
        }

        Ok(url)
    }
}

#[utoipa::path(
    post,
    path = "/api/shorten",
    request_body = ShortnerRequest,
    responses(
        (status = StatusCode::OK, description = "Short link created", body = ShortenResponse),
        (status = StatusCode::BAD_REQUEST, description = "Invalid target URL", body = ErrorResponse),
        (status = StatusCode::TOO_MANY_REQUESTS, description = "Rate limit exceeded"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Error", body = ErrorResponse)
    )
)]
#[tracing::instrument(skip_all)]
async fn shorten(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ShortnerRequest>,
) -> Result<Json<ShortenResponse>, ApiError> {
    let target = body.build_target_url().inspect_err(|err| {
        tracing::debug!("reject shorten request: {}", err.message());
    })?;
    let target = target.to_string();

    // One shot, no retry loop: the allocator mints codes from a counter through
    // a bijection, so the code it returns has provably never been issued.
    let code = state.code_allocator.next_code().await.map_err(|err| {
        tracing::error!("allocate short code: {}", err);
        ApiError::Internal
    })?;

    let data = db::CreateData {
        url: target,
        code: code.clone(),
        utm_source: body.utm_source.clone(),
        utm_campaign: body.utm_campaign.clone(),
        utm_content: body.utm_content.clone(),
        description: body.description.clone(),
    };

    // Cloned per request: the query builders need `&mut`, and the state behind
    // the `Arc` is shared. A clone is a handle onto the same pool, not a new
    // connection.
    match db::insert(&mut state.urls_client.clone(), data).await {
        Ok(InsertOutcome::Inserted) => Ok(Json(ShortenResponse {
            short_url: state.config.short_url(&code),
            code,
        })),
        // Unreachable while the counter and CODE_SECRET are intact. Reaching it
        // means one of them changed underneath us, so fail loudly rather than
        // overwrite a link that belongs to somebody else.
        Ok(InsertOutcome::Conflict) => {
            tracing::error!(
                "short code {} already exists; the code counter or CODE_SECRET has changed",
                code
            );

            Err(ApiError::Internal)
        }
        Err(err) => {
            tracing::error!("insert short link: {}", err);
            Err(ApiError::Internal)
        }
    }
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub struct RedirectPath {
    pub code: String,
}

#[utoipa::path(
    get,
    path = "/cc/{code}",
    responses(
        (status = StatusCode::TEMPORARY_REDIRECT, description = "Found short code and make tmp redirect"),
        (status = StatusCode::NOT_FOUND, description = "Unknown short code", body = ErrorResponse),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Error", body = ErrorResponse)
    ),
    params(
        ("code" = String, Path, description = "Unique short code", example = "TiJpa4")
    )
)]
#[tracing::instrument(skip_all, fields(code = %params.code))]
async fn redirect(
    request_info: entity::RequestInfo,
    Path(params): Path<RedirectPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    if !request_info.is_empty() {
        tracing::debug!("user request info: {:?}", request_info);
    }

    let link_info = match db::get(&mut state.urls_client.clone(), params.code.clone()).await {
        Ok(Some(link_info)) => link_info,
        Ok(None) => db::LinkInfo {
            code: params.code.clone(),
            url: None,
            utm_source: None,
            utm_campaign: None,
            utm_content: None,
        },
        Err(err) => {
            tracing::error!("get origin url: {}", err);
            return Err(ApiError::Internal);
        }
    };

    record_visit(&state, &link_info, request_info).await;

    match link_info.url {
        Some(url) => Ok(Redirect::temporary(&url).into_response()),
        // TODO: return static page for feedback
        None => Err(ApiError::NotFound("unknown short code")),
    }
}

/// Queues a visit for the analytics pipeline.
///
/// Every redirect attempt is recorded, including misses and requests carrying no
/// client metadata; gating on the presence of metadata silently under-counts
/// traffic. Uses `try_send` so a stalled analytics pipeline degrades reporting
/// rather than blocking user-facing redirects.
async fn record_visit(
    state: &AppState,
    link_info: &db::LinkInfo,
    request_info: entity::RequestInfo,
) {
    let visit = entity::VisitInfo {
        request_info: if request_info.is_empty() {
            None
        } else {
            Some(request_info)
        },
        link_info: link_info.clone(),
        created_at: chrono::Utc::now(),
    };

    // Awaited rather than handed to a background task: this process is frozen
    // between requests, so anything deferred past the response may never run.
    // A failure is logged and swallowed — degraded analytics must not turn into
    // a failed redirect.
    if let Err(err) = state.visit_writer.write(&visit).await {
        tracing::error!("record visit: {}", err);
    }
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = StatusCode::OK, description = "Service is live", body = HealthResponse))
)]
async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

/// Rate-limit key source, kept in sync with [`Config::client_ip_source`].
///
/// Picking the wrong one is a real failure either way: trusting forwarded
/// headers on a directly exposed server lets a client spoof its way past the
/// limit, while using the peer IP behind a proxy buckets every user together
/// and rate-limits the whole service as one client.
#[derive(Clone)]
enum IpKeyExtractor {
    PeerIp(PeerIpKeyExtractor),
    Forwarded(SmartIpKeyExtractor),
}

impl KeyExtractor for IpKeyExtractor {
    type Key = IpAddr;

    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        match self {
            IpKeyExtractor::PeerIp(extractor) => extractor.extract(req),
            IpKeyExtractor::Forwarded(extractor) => extractor.extract(req),
        }
    }
}

impl IpKeyExtractor {
    fn from_config(config: &Config) -> Self {
        match config.client_ip_source {
            axum_client_ip::ClientIpSource::ConnectInfo => {
                IpKeyExtractor::PeerIp(PeerIpKeyExtractor)
            }
            _ => IpKeyExtractor::Forwarded(SmartIpKeyExtractor),
        }
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    // Configuration is resolved before tracing exists, so these failures go to
    // stderr rather than the structured logger.
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("configuration error: {err:#}");
            std::process::exit(1);
        }
    };

    init_tracing(config.env);

    if let Err(err) = run(config).await {
        tracing::error!("fatal error: {err:#}");
        std::process::exit(1);
    }
}

async fn run(config: Config) -> Result<()> {
    tracing::info!("starting {} in {} mode", config.role, config.env);

    match config.role {
        AppRole::Server => run_server(config).await,
        AppRole::Consumer => run_consumer(config).await,
        AppRole::All => run_all(config).await,
    }
}

/// The HTTP service. Deployed as a serverless container.
async fn run_server(config: Config) -> Result<()> {
    let urls_db = connect_db(
        &config.urls_connection_string,
        config.ydb_credentials,
        "urls",
    )
    .await?;

    let urls_client = urls_db.query_client();

    if config.run_migrations {
        let visits_db = connect_db(
            &config.visits_connection_string,
            config.ydb_credentials,
            "visits",
        )
        .await?;
        run_migrations(&mut urls_client.clone(), &mut visits_db.query_client()).await?;
    }

    let app = build_server(&config, &urls_db, urls_client).await?;

    serve(app, &config).await
}

/// Both roles in one process.
///
/// Only valid on a host with continuously allocated CPU — a VM, Kubernetes, or
/// plain Docker. On a serverless container the in-process consumer would be
/// suspended between requests and make no progress, which is the failure this
/// role split exists to avoid.
async fn run_all(config: Config) -> Result<()> {
    tracing::warn!(
        "APP_ROLE=all runs the consumer in-process: use it only where CPU is \
         continuously allocated (VM, Kubernetes, docker compose). On a \
         serverless container run APP_ROLE=server and deploy the consumer \
         separately, or visits will not be drained."
    );

    let urls_db = connect_db(
        &config.urls_connection_string,
        config.ydb_credentials,
        "urls",
    )
    .await?;
    let visits_db = connect_db(
        &config.visits_connection_string,
        config.ydb_credentials,
        "visits",
    )
    .await?;

    let urls_client = urls_db.query_client();
    // The visits table is written with a bulk upsert, which is a table-service
    // RPC rather than YQL, so this role needs both client types.
    let visits_client = visits_db.table_client();

    if config.run_migrations {
        run_migrations(&mut urls_client.clone(), &mut visits_db.query_client()).await?;
    }

    let app = build_server(&config, &urls_db, urls_client.clone()).await?;
    let extractor = load_ua_extractor()?;

    // Whichever side finishes first ends the process: a consumer that has given
    // up should not leave a server running that silently stops being drained,
    // and a stopped server should not leave the consumer orphaned.
    tokio::select! {
        result = serve(app, &config) => result,
        _ = consumer::create(
            config.visits_table_path.clone(),
            extractor,
            urls_client,
            visits_client,
            urls_db,
            CONSUMER_NAME.to_string(),
            VISITS_TOPIC_PATH.to_string(),
        ) => {
            tracing::error!("consumer stopped unexpectedly");
            Ok(())
        }
    }
}

/// Builds the HTTP router shared by the `server` and `all` roles.
async fn build_server(
    config: &Config,
    urls_db: &ydb::Client,
    urls_client: QueryClient,
) -> Result<Router> {
    let visit_writer = build_visit_writer(urls_db).await?;

    let code_allocator = CodeAllocator::new(
        urls_client.clone(),
        &config.code_secret,
        config.code_length,
        config.code_block_size,
    )
    .map_err(|err| anyhow!("init code allocator: {err}"))?;

    let state = Arc::new(AppState {
        urls_client,
        code_allocator,
        visit_writer,
        config: config.clone(),
    });

    build_router(state, config)
}

/// The topic drain. Must run somewhere with continuously allocated CPU: a
/// serverless instance is suspended between requests, so this loop would only
/// advance during unrelated HTTP traffic and would be killed mid-batch whenever
/// the instance is terminated.
async fn run_consumer(config: Config) -> Result<()> {
    let urls_db = connect_db(
        &config.urls_connection_string,
        config.ydb_credentials,
        "urls",
    )
    .await?;
    let visits_db = connect_db(
        &config.visits_connection_string,
        config.ydb_credentials,
        "visits",
    )
    .await?;

    let urls_client = urls_db.query_client();
    let visits_client = visits_db.table_client();

    if config.run_migrations {
        run_migrations(&mut urls_client.clone(), &mut visits_db.query_client()).await?;
    }

    let extractor = load_ua_extractor()?;

    tracing::info!(
        "draining {} into {}",
        VISITS_TOPIC_PATH,
        config.visits_table_path
    );

    // Run in the foreground: this process exists only to drain the topic, so a
    // detached task would leave nothing holding the runtime open. The consumer
    // opens (and reopens) its own reader, so a latched reader error cannot
    // permanently stall it.
    tokio::select! {
        _ = consumer::create(
            config.visits_table_path.clone(),
            extractor,
            urls_client,
            visits_client,
            urls_db,
            CONSUMER_NAME.to_string(),
            VISITS_TOPIC_PATH.to_string(),
        ) => {}
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, stopping consumer");
        }
    }

    Ok(())
}

async fn build_visit_writer(urls_db: &ydb::Client) -> Result<producer::VisitWriter> {
    let produce_params = TopicWriterOptions::builder()
        .topic_path(VISITS_TOPIC_PATH.to_string())
        .producer_id(instance_producer_id())
        .build();

    let writer = urls_db
        .topic_client()
        .create_writer_with_params(produce_params)
        .await
        .map_err(|err| anyhow!("init topic writer: {err}"))?;

    Ok(producer::VisitWriter::new(writer))
}

fn init_tracing(env: Environment) {
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

    if env.is_production() {
        tracing_subscriber::registry()
            .with(tracing_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_filter)
            .with(tracing_subscriber::fmt::layer().with_line_number(true))
            .init();
    }
}

async fn connect_db(
    connection_string: &str,
    credentials: config::YdbCredentials,
    label: &str,
) -> Result<ydb::Client> {
    tokio::time::timeout(
        DB_CONNECT_TIMEOUT,
        db::init_db(connection_string, credentials),
    )
    .await
    .map_err(|err| anyhow!("connect to ydb {label} timed out: {err}"))?
    .map_err(|err| anyhow!("init {label} ydb: {err}"))
}

async fn run_migrations(
    urls_client: &mut QueryClient,
    visits_client: &mut QueryClient,
) -> Result<()> {
    tracing::info!("RUN_MIGRATIONS is set, applying schema");

    db::init_urls_tables(urls_client)
        .await
        .map_err(|err| anyhow!("create urls table: {err}"))?;
    db::init_code_counter_table(urls_client)
        .await
        .map_err(|err| anyhow!("create code_counter table: {err}"))?;
    db::init_visits_tables(visits_client)
        .await
        .map_err(|err| anyhow!("create visits table: {err}"))?;

    tracing::info!("migrations applied");

    Ok(())
}

fn load_ua_extractor() -> Result<ua_parser::Extractor<'static>> {
    let regexes_bytes: &[u8] = include_bytes!("../regexes.yaml");

    let regexes: ua_parser::Regexes =
        serde_yaml::from_slice(regexes_bytes).map_err(|err| anyhow!("parse ua regexes: {err}"))?;

    ua_parser::Extractor::try_from(regexes).map_err(|err| anyhow!("create ua extractor: {err}"))
}

fn build_router(state: Arc<AppState>, config: &Config) -> Result<Router> {
    let governor_config = GovernorConfigBuilder::default()
        .key_extractor(IpKeyExtractor::from_config(config))
        .per_second(config.rate_limit_refill_seconds)
        .burst_size(config.rate_limit_burst)
        .finish()
        .ok_or_else(|| anyhow!("invalid rate limit configuration"))?;

    // Rate limiting covers link creation only; redirects are the read path and
    // are expected to be hot.
    let shorten_router = Router::new()
        .route("/api/shorten", post(shorten))
        .layer(GovernorLayer::new(Arc::new(governor_config)));

    let mut app = Router::new()
        .merge(shorten_router)
        .route("/cc/{code}", get(redirect))
        .route("/health", get(health))
        .with_state(state);

    if config.enable_swagger {
        tracing::info!("Use swagger on /swagger-ui");
        app = app
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));
    } else {
        tracing::info!("swagger UI disabled");
    }

    // Tells the `ClientIp` extractor and the rate limiter where to read the
    // caller's address from.
    Ok(app.layer(config.client_ip_source.clone().into_extension()))
}

async fn serve(app: Router, config: &Config) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow!("bind address {addr}: {err}"))?;

    tracing::info!("Listening on {}", addr);

    // `into_make_service_with_connect_info` is required for peer-IP based
    // client-IP resolution and rate limiting.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .map_err(|err| anyhow!("server error: {err}"))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = signal::ctrl_c().await {
            tracing::error!("failed to install Ctrl+C handler: {}", err);
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!("failed to install signal handler: {}", err);
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(url: &str) -> ShortnerRequest {
        ShortnerRequest {
            url: url.to_string(),
            utm_source: None,
            utm_campaign: None,
            utm_content: None,
            description: None,
        }
    }

    #[test]
    fn accepts_http_and_https() {
        assert!(request("http://example.com/a").build_target_url().is_ok());
        assert!(request("https://example.com/a").build_target_url().is_ok());
    }

    #[test]
    fn rejects_non_http_schemes() {
        for url in [
            "javascript:alert(1)",
            "data:text/html;base64,PHNjcmlwdD4=",
            "file:///etc/passwd",
            "ftp://example.com/x",
        ] {
            assert_eq!(
                request(url).build_target_url().err(),
                Some(UrlValidationError::UnsupportedScheme),
                "expected {url} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_malformed_and_relative_urls() {
        for url in ["not a url", "/relative/path", ""] {
            assert_eq!(
                request(url).build_target_url().err(),
                Some(UrlValidationError::Malformed),
                "expected {url} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_embedded_credentials() {
        // Renders as the trusted host in many clients while resolving elsewhere.
        assert_eq!(
            request("https://trusted.example@evil.test/")
                .build_target_url()
                .err(),
            Some(UrlValidationError::EmbeddedCredentials)
        );
        assert_eq!(
            request("https://user:pass@evil.test/")
                .build_target_url()
                .err(),
            Some(UrlValidationError::EmbeddedCredentials)
        );
    }

    #[test]
    fn appends_utm_parameters_to_redirect_target() {
        let req = ShortnerRequest {
            url: "https://example.com/item".to_string(),
            utm_source: Some("google".to_string()),
            utm_campaign: Some("spring".to_string()),
            utm_content: Some("banner".to_string()),
            description: None,
        };

        let target = req.build_target_url().map(|u| u.to_string());

        assert_eq!(
            target.as_deref(),
            Ok("https://example.com/item?utm_source=google&utm_campaign=spring&utm_content=banner")
        );
    }

    #[test]
    fn preserves_existing_query_parameters() {
        let req = ShortnerRequest {
            url: "https://example.com/item?id=7".to_string(),
            utm_source: Some("telegram".to_string()),
            utm_campaign: None,
            utm_content: None,
            description: None,
        };

        assert_eq!(
            req.build_target_url().map(|u| u.to_string()).as_deref(),
            Ok("https://example.com/item?id=7&utm_source=telegram")
        );
    }

    #[test]
    fn omits_utm_parameters_when_absent() {
        assert_eq!(
            request("https://example.com/item")
                .build_target_url()
                .map(|u| u.to_string())
                .as_deref(),
            Ok("https://example.com/item")
        );
    }

    #[test]
    fn escapes_utm_values() {
        let req = ShortnerRequest {
            url: "https://example.com/".to_string(),
            utm_source: Some("a b&c=d".to_string()),
            utm_campaign: None,
            utm_content: None,
            description: None,
        };

        let target = req.build_target_url().map(|u| u.to_string());
        assert_eq!(
            target.as_deref(),
            Ok("https://example.com/?utm_source=a+b%26c%3Dd")
        );
    }
}
