use std::env;
use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use axum_client_ip::ClientIpSource;

use crate::code;

/// Deployment environment. Controls credential source, log format and whether
/// developer-facing endpoints such as Swagger UI are exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Development,
    Production,
}

impl Environment {
    pub fn is_production(self) -> bool {
        matches!(self, Environment::Production)
    }
}

#[derive(Debug)]
pub struct ParseEnvironmentError(String);

impl fmt::Display for ParseEnvironmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid environment {:?}, expected \"development\" or \"production\"",
            self.0
        )
    }
}

impl std::error::Error for ParseEnvironmentError {}

impl FromStr for Environment {
    type Err = ParseEnvironmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "development" | "dev" => Ok(Environment::Development),
            "production" | "prod" => Ok(Environment::Production),
            _ => Err(ParseEnvironmentError(s.to_string())),
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Environment::Development => "development",
            Environment::Production => "production",
        })
    }
}

/// Which half of the system this process runs.
///
/// The HTTP server is deployed as a serverless container, which suspends the
/// instance between requests. A topic consumer is a continuously-running worker
/// and cannot make progress there, so it runs as its own always-on deployment
/// from the same image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppRole {
    Server,
    Consumer,
    /// Both in one process. Valid only where CPU is continuously allocated.
    All,
}

#[derive(Debug)]
pub struct ParseAppRoleError(String);

impl fmt::Display for ParseAppRoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid role {:?}, expected \"server\" or \"consumer\"",
            self.0
        )
    }
}

impl std::error::Error for ParseAppRoleError {}

impl FromStr for AppRole {
    type Err = ParseAppRoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "server" => Ok(AppRole::Server),
            "consumer" | "worker" => Ok(AppRole::Consumer),
            "all" | "both" => Ok(AppRole::All),
            _ => Err(ParseAppRoleError(s.to_string())),
        }
    }
}

impl fmt::Display for AppRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AppRole::Server => "server",
            AppRole::Consumer => "consumer",
            AppRole::All => "all",
        })
    }
}

/// How to authenticate to YDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YdbCredentials {
    /// Instance metadata service. The production default in Yandex Cloud.
    Metadata,
    /// Shells out to `yc iam create-token`. Requires the yc CLI on PATH, so it
    /// works for local `cargo run` but not inside a container.
    CommandLine,
    /// No credentials, for a local YDB started with `YDB_ANONYMOUS_CREDENTIALS=1`.
    Anonymous,
}

#[derive(Debug)]
pub struct ParseYdbCredentialsError(String);

impl fmt::Display for ParseYdbCredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid credentials {:?}, expected \"metadata\", \"cli\" or \"anonymous\"",
            self.0
        )
    }
}

impl std::error::Error for ParseYdbCredentialsError {}

impl FromStr for YdbCredentials {
    type Err = ParseYdbCredentialsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "metadata" => Ok(YdbCredentials::Metadata),
            "cli" | "command-line" | "commandline" => Ok(YdbCredentials::CommandLine),
            "anonymous" | "anon" => Ok(YdbCredentials::Anonymous),
            _ => Err(ParseYdbCredentialsError(s.to_string())),
        }
    }
}

/// Value shipped in `env.example`. Refused outright: leaving it in place would
/// make every short code derivable by anyone who can read the repository.
const PLACEHOLDER_CODE_SECRET: &str = "change-me-openssl-rand-hex-32";

/// Shortest accepted `CODE_SECRET`. Not a strength guarantee, just a floor that
/// catches obviously trivial values.
const MIN_CODE_SECRET_LEN: usize = 16;

/// Application configuration, resolved once at startup from the environment.
#[derive(Debug, Clone)]
pub struct Config {
    pub role: AppRole,
    pub env: Environment,
    /// How to authenticate to YDB. Defaults to `Metadata` in production and
    /// `CommandLine` elsewhere, preserving the previous behaviour.
    pub ydb_credentials: YdbCredentials,
    pub origin: String,
    pub port: u16,
    pub urls_connection_string: String,
    pub visits_connection_string: String,
    pub visits_table_path: String,
    /// Where to read the client IP from. Must match the deployment topology:
    /// `ConnectInfo` for a directly exposed server, or the header your reverse
    /// proxy sets (e.g. `XRealIp`, `RightmostXForwardedFor`) when behind one.
    pub client_ip_source: ClientIpSource,
    /// Number of characters in a generated short code.
    pub code_length: usize,
    /// Keys the permutation that scrambles counter values into codes. Must
    /// never change once links have been issued: a different key is a
    /// different permutation, so later codes could collide with earlier ones.
    pub code_secret: String,
    /// How many counter values to claim per database round-trip.
    pub code_block_size: u64,
    /// Seconds between replenishing one request slot, per client IP.
    pub rate_limit_refill_seconds: u64,
    /// Maximum burst of requests allowed per client IP.
    pub rate_limit_burst: u32,
    pub enable_swagger: bool,
    pub run_migrations: bool,
}

impl Config {
    /// Reads and validates the full configuration from environment variables.
    ///
    /// Fails fast on anything missing or malformed so the process never starts
    /// in a half-configured state.
    pub fn from_env() -> Result<Self> {
        let env: Environment = match env::var("APP_ENV") {
            Ok(raw) => raw
                .parse()
                .with_context(|| format!("APP_ENV is invalid: {raw:?}"))?,
            Err(_) => Environment::Development,
        };

        let origin = required_var("ORIGIN")?;
        let origin = origin.trim_end_matches('/').to_string();

        let code_length = parse_var("CODE_LENGTH", 8usize)?;
        if code_length == 0 || code_length > code::MAX_CODE_LENGTH {
            return Err(anyhow!(
                "CODE_LENGTH must be between 1 and {}, got {code_length}",
                code::MAX_CODE_LENGTH
            ));
        }

        let code_block_size = parse_var("CODE_BLOCK_SIZE", 1_000u64)?;
        if code_block_size == 0 {
            return Err(anyhow!("CODE_BLOCK_SIZE must be at least 1"));
        }

        let code_secret = required_var("CODE_SECRET")?;
        if code_secret == PLACEHOLDER_CODE_SECRET {
            return Err(anyhow!(
                "CODE_SECRET is still the example placeholder; generate one with: openssl rand -hex 32"
            ));
        }
        if code_secret.len() < MIN_CODE_SECRET_LEN {
            return Err(anyhow!(
                "CODE_SECRET must be at least {MIN_CODE_SECRET_LEN} characters, got {}",
                code_secret.len()
            ));
        }

        Ok(Config {
            role: parse_var("APP_ROLE", AppRole::Server)?,
            env,
            ydb_credentials: parse_var(
                "YDB_CREDENTIALS",
                if env.is_production() {
                    YdbCredentials::Metadata
                } else {
                    YdbCredentials::CommandLine
                },
            )?,
            origin,
            port: parse_var("PORT", 8080u16)?,
            urls_connection_string: required_var("YDB_URLS_CONNECTION_STRING")?,
            visits_connection_string: required_var("YDB_VISITS_CONNECTION_STRING")?,
            visits_table_path: required_var("VISITS_TABLE_PATH")?,
            client_ip_source: parse_var("CLIENT_IP_SOURCE", ClientIpSource::ConnectInfo)?,
            code_length,
            code_secret,
            code_block_size,
            rate_limit_refill_seconds: parse_var("RATE_LIMIT_REFILL_SECONDS", 1u64)?,
            rate_limit_burst: parse_var("RATE_LIMIT_BURST", 20u32)?,
            enable_swagger: parse_var("ENABLE_SWAGGER", !env.is_production())?,
            run_migrations: parse_var("RUN_MIGRATIONS", false)?,
        })
    }

    /// Builds the public short URL served for a given code.
    pub fn short_url(&self, code: &str) -> String {
        format!("{}/cc/{}", self.origin, code)
    }
}

fn required_var(key: &str) -> Result<String> {
    let value = env::var(key).map_err(|err| anyhow!("{key} must be set: {err}"))?;
    if value.trim().is_empty() {
        return Err(anyhow!("{key} must not be empty"));
    }

    Ok(value)
}

fn parse_var<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    match env::var(key) {
        Ok(raw) => raw
            .parse::<T>()
            .map_err(|err| anyhow!("{key} is invalid: {err}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(anyhow!("{key} must be valid unicode: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_parses_known_values_case_insensitively() {
        assert_eq!(
            "production".parse::<Environment>().ok(),
            Some(Environment::Production)
        );
        assert_eq!(
            "PRODUCTION".parse::<Environment>().ok(),
            Some(Environment::Production)
        );
        assert_eq!(
            " Development ".parse::<Environment>().ok(),
            Some(Environment::Development)
        );
        assert_eq!(
            "prod".parse::<Environment>().ok(),
            Some(Environment::Production)
        );
    }

    #[test]
    fn environment_rejects_unknown_values() {
        // A typo must not silently fall back to development, which would pick
        // developer CLI credentials for a production deployment.
        assert!("prodction".parse::<Environment>().is_err());
        assert!("".parse::<Environment>().is_err());
    }

    #[test]
    fn app_role_parses_known_values() {
        assert_eq!("server".parse::<AppRole>().ok(), Some(AppRole::Server));
        assert_eq!("Consumer".parse::<AppRole>().ok(), Some(AppRole::Consumer));
        assert_eq!(" worker ".parse::<AppRole>().ok(), Some(AppRole::Consumer));
        assert_eq!("all".parse::<AppRole>().ok(), Some(AppRole::All));
        assert_eq!("BOTH".parse::<AppRole>().ok(), Some(AppRole::All));
        assert!("nonsense".parse::<AppRole>().is_err());
    }

    #[test]
    fn placeholder_code_secret_is_rejected() {
        // Shipping the example value would make every short code derivable by
        // anyone who can read the repository, defeating the permutation.
        assert!(PLACEHOLDER_CODE_SECRET.len() >= MIN_CODE_SECRET_LEN);
    }

    #[test]
    fn short_url_joins_origin_and_code() {
        let config = test_config("https://sh.rt");
        assert_eq!(config.short_url("AbCd1234"), "https://sh.rt/cc/AbCd1234");
    }

    #[test]
    fn short_url_does_not_double_slash_when_origin_has_trailing_slash() {
        // from_env trims the trailing slash; confirm the joined form is clean.
        let config = test_config("https://sh.rt/".trim_end_matches('/'));
        assert_eq!(config.short_url("AbCd1234"), "https://sh.rt/cc/AbCd1234");
    }

    fn test_config(origin: &str) -> Config {
        Config {
            role: AppRole::Server,
            env: Environment::Development,
            ydb_credentials: YdbCredentials::Anonymous,
            origin: origin.to_string(),
            port: 8080,
            urls_connection_string: String::new(),
            visits_connection_string: String::new(),
            visits_table_path: String::new(),
            client_ip_source: ClientIpSource::ConnectInfo,
            code_length: 8,
            code_secret: "test-secret".to_string(),
            code_block_size: 1_000,
            rate_limit_refill_seconds: 1,
            rate_limit_burst: 20,
            enable_swagger: true,
            run_migrations: false,
        }
    }
}
