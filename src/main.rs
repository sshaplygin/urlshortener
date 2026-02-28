use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Redirect},
    routing::{get, post},
};
use dotenv::dotenv;
use hashers::fnv::fnv1a32;
use serde::{Deserialize, Serialize};
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{Level, span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use url::Url;
use ydb::{TableClient, TopicWriterOptionsBuilder, YdbError};

use crate::config::Environment;

mod config;
mod consumer;
mod db;
mod entity;
mod producer;

#[deny(clippy::unwrap_used)]
#[deny(clippy::expect_used)]
#[derive(OpenApi)]
#[openapi(
    paths(shorten, redirect),
    components(schemas(ShortenResponse, ShortnerRequest))
)]
struct ApiDoc;

#[derive(Clone)]
struct AppState {
    urls_client: TableClient,
    visit_sender: mpsc::Sender<entity::VisitInfo>,
    config: config::Config,
}

#[derive(Serialize, ToSchema)]
struct ShortenResponse {
    code: String,
    short_url: String,
}

#[derive(Deserialize, ToSchema)]
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

        Ok(url.to_string())
    }
}

#[utoipa::path(
    post,
    path = "/api/shorten",
    request_body = ShortnerRequest,
    responses(
        (status = StatusCode::OK, description = "Short link created", body = ShortenResponse),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Error")
    )
)]
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

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub struct RedirectPath {
    pub code: String,
}

#[utoipa::path(
    get,
    path = "/cc/{code}",
    params(RedirectPath),
    responses(
        (status = StatusCode::NO_CONTENT, description = "Not found request short code"),
        (status = StatusCode::TEMPORARY_REDIRECT, description = "Found short code and make tmp redirect"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Error")
    ),
    params(
        ("code" = String, Path, description = "Unique short code", example = "TiJpa4")
    )
)]
async fn redirect(
    request_info: entity::RequestInfo,
    Path(params): Path<RedirectPath>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let handler_span = span!(Level::DEBUG, "redirect_handler");
    let _guard = handler_span.enter();

    if !request_info.is_empty() {
        tracing::debug!("user request info: {:?}", request_info);
    }

    let link_info = match db::get(&state.urls_client, params.code.clone()).await {
        Ok(link_info) => link_info,
        Err(YdbError::NoRows) => db::LinkInfo {
            code: params.code,
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

    let visit = entity::VisitInfo {
        request_info: if request_info.is_empty() {
            None
        } else {
            Some(request_info)
        },
        link_info: link_info.clone(),
        created_at: chrono::Utc::now(),
    };

    if !visit.is_empty()
        && let Err(err) = state.visit_sender.send(visit).await
    {
        tracing::error!("send visit info: {}", err);
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
            tracing::error!("init urls ydb: {}", err);
            return;
        }
        Err(err) => {
            tracing::error!("connect to ydb urls by timeout: {}", err);
            return;
        }
    };

    let urls_client = db.table_client();

    // NOTICE: uncomment if you need run migration
    // db::init_urls_tables(&urls_client).await.unwrap();

    let mut topic_client = db.topic_client();
    let producer_res = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path("/topics/visits".to_string())
                .producer_id("producer".to_string())
                .build()
                .unwrap(),
        )
        .await;

    let producer = match producer_res {
        Ok(p) => p,
        Err(err) => {
            tracing::error!("init producer: {}", err);
            return;
        }
    };

    let config = config::Config::new()
        .with_origin(&env::var("ORIGIN").expect("ORIGIN must be set"))
        .with_env(env.clone());

    let (tx, rx) = mpsc::channel::<entity::VisitInfo>(1024);

    tokio::spawn(producer::create(rx, producer));

    let consumer_res = topic_client
        .create_reader(
            "urlshortener-consumer".to_string(),
            "/topics/visits".to_string(),
        )
        .await;

    let consumer = match consumer_res {
        Ok(c) => c,
        Err(err) => {
            tracing::error!("init consumer: {}", err);
            return;
        }
    };

    let db = match tokio::time::timeout(
        Duration::from_secs(5),
        db::init_db("YDB_VISITS_CONNECTION_STRING".to_string(), env),
    )
    .await
    {
        Ok(Ok(db)) => db,
        Ok(Err(err)) => {
            tracing::error!("init vists ydb: {}", err);
            return;
        }
        Err(err) => {
            tracing::error!("connect to ydb visits by timeout: {}", err);
            return;
        }
    };

    let visits_client = db.table_client();

    // NOTICE: uncomment if you need run migration
    // db::init_visits_tables(&visits_client).await.unwrap();

    let regexes_bytes: &[u8] = include_bytes!("../regexes.yaml");
    let regexes: ua_parser::Regexes = serde_yaml::from_slice(regexes_bytes).unwrap();
    let extractor = ua_parser::Extractor::try_from(regexes).unwrap();

    let topic_table_client = urls_client.clone();
    tokio::spawn(consumer::create(
        env::var("VISITS_TABLE_PATH").expect("VISITS_TABLE_PATH must be set"),
        extractor,
        topic_table_client,
        visits_client,
        consumer,
    ));

    let origin = config.inner().origin.clone();

    let state = Arc::new(AppState {
        urls_client,
        visit_sender: tx,
        config,
    });

    let swagger_ui = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi());

    let app = Router::new()
        .route("/api/shorten", post(shorten))
        .route("/cc/{code}", get(redirect))
        .with_state(state)
        .merge(swagger_ui);

    let port = env::var("PORT")
        .expect("PORT must be set")
        .parse::<u16>()
        .expect("PORT must be a valid u16 number");
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!("Listeging on {}", port);
    tracing::info!("Use swagger on {}/swagger-ui", origin);

    let listner = TcpListener::bind(addr.to_string()).await.unwrap();

    axum::serve(listner, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
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
