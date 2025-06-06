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
use std::env;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use ydb::{TableClient, YdbError};

mod config;
mod db;

#[derive(Clone)]
struct AppState {
    db: TableClient,
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
    utm_source: Option<String>, // от куда пришел пользователь: google, telegram, github
    utm_campaign: Option<String>, // какие-то кампании
    utm_content: Option<String>, // с какого конкретного места: номер поста, readme.md, linkedin/id
}

async fn shorten(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ShortnerRequest>,
) -> Result<Json<ShortenResponse>, StatusCode> {
    let code = generate_code(&body.url, 6);

    let reply = match db::insert(&state.db, body.url, code.clone()).await {
        Ok(()) => Ok(Json(ShortenResponse {
            code: code.clone(),
            short_url: format!(
                "{}://{}/{}",
                state.config.inner().scheme,
                state.config.inner().host,
                code.clone()
            ),
        })),
        Err(err) => {
            tracing::error!("shorten: get return err: {}", err);

            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    };

    reply
}

async fn redirect(
    Path(code): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let reply = match db::get(&state.db, code).await {
        Ok(src) => {
            let localion = format!(
                "Location: {}://{}/{}",
                state.config.inner().scheme,
                state.config.inner().host,
                src,
            );

            Ok(Redirect::temporary(&localion))
        }
        Err(YdbError::NoRows) => Err(StatusCode::NO_CONTENT),
        Err(err) => {
            tracing::error!("redirect: get return err: {}", err);

            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    };

    reply
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let app_env = env::var("APP_ENV").unwrap_or_else(|_| "development".into());

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

    if app_env == "production" {
        tracing_subscriber::registry()
            .with(tracing_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_filter)
            .with(tracing_subscriber::fmt::layer().with_line_number(true))
            .init();
    };

    let db = match tokio::time::timeout(Duration::from_secs(3), db::init_db(app_env.as_str())).await
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

    let table_client = db.table_client();

    db::init_tables(&table_client).await.unwrap();

    let config = config::Config::new()
        .with_host(&env::var("HOST").expect("HOST must be set"))
        .with_app_env(app_env);

    let state = Arc::new(AppState {
        db: table_client,
        config: config,
    });

    let app = Router::new()
        .route("/shorten", post(shorten))
        .route("/t/{code}", get(redirect))
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
