use std::net::IpAddr;
use std::time::SystemTime;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tracing::trace;
use ydb::{
    AnonymousCredentials, ClientBuilder, CommandLineCredentials, MetadataUrlCredentials, Query,
    TableClient, Value, YdbError, YdbOrCustomerError, YdbResult, ydb_params, ydb_struct,
};
use ydb_grpc::generated::ydb::status_ids::StatusCode;

use crate::config::YdbCredentials;

pub async fn init_db(
    connection_string: &str,
    credentials: YdbCredentials,
) -> ydb::YdbResult<ydb::Client> {
    let mut client_builder: ClientBuilder =
        ydb::ClientBuilder::new_from_connection_string(connection_string)?;

    client_builder = match credentials {
        YdbCredentials::Metadata => client_builder.with_credentials(MetadataUrlCredentials::new()),
        // Shells out to the yc CLI, so this only works where that binary
        // exists — not inside the container image.
        YdbCredentials::CommandLine => client_builder
            .with_credentials(CommandLineCredentials::from_cmd("yc iam create-token")?),
        YdbCredentials::Anonymous => client_builder.with_credentials(AnonymousCredentials::new()),
    };

    let client = client_builder.client()?;

    client.wait().await?;

    trace!("init_db wait finished");

    Ok(client)
}

pub async fn init_urls_tables(table_client: &TableClient) -> ydb::YdbResult<()> {
    let create_urls = String::from(
        "
        CREATE TABLE urls (
            src Utf8 NOT NULL,
            code Utf8 NOT NULL,
            utm_source Utf8,
            utm_campaign Utf8,
            utm_content Utf8,
            description Utf8,
            created_at Timestamp NOT NULL,

            PRIMARY KEY(code)
        );
    ",
    );

    table_client.retry_execute_scheme_query(create_urls).await?;

    Ok(())
}

/// Backing store for the short-code counter. A single row, updated in a
/// serializable transaction, is what makes generated codes collision-free.
pub async fn init_code_counter_table(table_client: &TableClient) -> ydb::YdbResult<()> {
    let create_counter = String::from(
        "
        CREATE TABLE code_counter (
            name Utf8 NOT NULL,
            next_id Uint64 NOT NULL,

            PRIMARY KEY(name)
        );
    ",
    );

    table_client
        .retry_execute_scheme_query(create_counter)
        .await?;

    Ok(())
}

/// Name of the counter row. A single row is enough: callers claim ids in
/// blocks, so this is touched once per `block_size` links rather than per link.
const CODE_COUNTER_NAME: &str = "short_code";

/// Claims `block_size` consecutive counter values and returns the first.
///
/// Runs in a serializable transaction, so two instances racing here conflict
/// and one retries — neither can be handed a range the other already owns.
/// Values left unused when a process exits are simply skipped; codes have no
/// reason to be contiguous.
pub async fn reserve_code_block(
    table_client: &TableClient,
    block_size: u64,
) -> ydb::YdbResult<u64> {
    let start = table_client
        .retry_transaction(|tx| async move {
            let mut tx = tx;

            let read = Query::from(
                "
                    DECLARE $name as Utf8;

                    SELECT
                        next_id
                    FROM
                        code_counter
                    WHERE
                        name = $name;
                ",
            )
            .with_params(ydb_params!("$name" => CODE_COUNTER_NAME.to_string()));

            // A missing row means the counter has never been used. Starting at
            // zero is safe only because this runs inside the same serializable
            // transaction as the write below.
            let current: u64 = match tx.query(read).await?.into_only_row() {
                Ok(mut row) => row.remove_field_by_name("next_id")?.try_into()?,
                Err(YdbError::NoRows) => 0,
                Err(err) => return Err(err.into()),
            };

            let write = Query::from(
                "
                    DECLARE $name as Utf8;
                    DECLARE $next_id as Uint64;

                    UPSERT INTO code_counter (name, next_id) VALUES ($name, $next_id);
                ",
            )
            .with_params(ydb_params!(
                "$name" => CODE_COUNTER_NAME.to_string(),
                "$next_id" => current.saturating_add(block_size),
            ));

            tx.query(write).await?;
            tx.commit().await?;

            Ok(current)
        })
        .await
        .map_err(|err| err.to_ydb_error())?;

    trace!("reserved code block starting at {}", start);

    Ok(start)
}

pub async fn init_visits_tables(table_client: &TableClient) -> ydb::YdbResult<()> {
    let create_visits = String::from(
        "
        CREATE TABLE visits (
            code Utf8 NOT NULL,
            src Utf8,
            user_agent Utf8,
            browser Utf8,
            browser_version Utf8,
            os Utf8,
            os_version Utf8,
            device Utf8,
            referer Utf8,
            ip Utf8,
            utm_source Utf8,
            utm_campaign Utf8,
            utm_content Utf8,
            event_date Date NOT NULL,
            event_timestamp Timestamp NOT NULL,

            PRIMARY KEY(event_timestamp, code)
        )
        PARTITION BY HASH(event_timestamp)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
        );
        ",
    );

    table_client
        .retry_execute_scheme_query(create_visits)
        .await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkInfo {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utm_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utm_campaign: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utm_content: Option<String>,
}

/// Looks up a short code. Returns `Ok(None)` when the code is unknown.
pub async fn get(table_client: &TableClient, code: String) -> YdbResult<Option<LinkInfo>> {
    let table_client = table_client.clone_with_transaction_options(
        ydb::TransactionOptions::new()
            .with_autocommit(true)
            .with_mode(ydb::Mode::OnlineReadonly),
    );

    let res: Option<LinkInfo> = table_client
        .retry_transaction(|tx| async {
            let mut tx = tx;

            let query = Query::from(
                "
                    DECLARE $code as Utf8;

                    SELECT
                        src,
                        utm_source,
                        utm_campaign,
                        utm_content
                    FROM
                        urls
                    WHERE
                        code = $code;
                ",
            )
            .with_params(ydb_params!("$code"=>code.clone()));

            let mut row = match tx.query(query).await?.into_only_row() {
                Ok(row) => row,
                Err(YdbError::NoRows) => return Ok(None),
                Err(err) => return Err(err.into()),
            };

            let url: String = row.remove_field_by_name("src")?.try_into()?;
            let utm_source: Option<String> = row.remove_field_by_name("utm_source")?.try_into()?;
            let utm_campaign: Option<String> =
                row.remove_field_by_name("utm_campaign")?.try_into()?;
            let utm_content: Option<String> =
                row.remove_field_by_name("utm_content")?.try_into()?;

            Ok(Some(LinkInfo {
                url: Some(url),
                code: code.clone(),
                utm_source,
                utm_campaign,
                utm_content,
            }))
        })
        .await?;

    Ok(res)
}

pub struct CreateData {
    pub code: String,
    /// Final redirect target, including any UTM parameters.
    pub url: String,
    /// Traffic origin: google, telegram, github.
    pub utm_source: Option<String>,
    /// Promotional campaign name.
    pub utm_campaign: Option<String>,
    /// Specific placement the traffic came from.
    pub utm_content: Option<String>,
    /// Free-form note about the link.
    pub description: Option<String>,
}

/// Result of attempting to claim a short code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    /// The code is already taken. Codes come from a counter run through a
    /// bijection, so this cannot happen in normal operation — it means the
    /// counter row was reset or `CODE_SECRET` was rotated. Kept as a hard
    /// backstop: silently reusing the row would hand the caller a link
    /// pointing at somebody else's URL.
    Conflict,
}

pub async fn insert(table_client: &TableClient, data: CreateData) -> ydb::YdbResult<InsertOutcome> {
    let res = table_client
        .retry_transaction(|tx| async {
            let mut tx = tx;

            let query = ydb::Query::from(
                "
                    DECLARE $src as Utf8;
                    DECLARE $code as Utf8;
                    DECLARE $utm_source as Optional<Utf8>;
                    DECLARE $utm_campaign as Optional<Utf8>;
                    DECLARE $utm_content as Optional<Utf8>;
                    DECLARE $description as Optional<Utf8>;

                    INSERT INTO urls (
                        src,
                        code,
                        utm_source,
                        utm_campaign,
                        utm_content,
                        description,
                        created_at
                    )
                    VALUES (
                        $src,
                        $code,
                        $utm_source,
                        $utm_campaign,
                        $utm_content,
                        $description,
                        CurrentUtcTimestamp()
                    );
                ",
            )
            .with_params(ydb_params!(
                "$src"=>data.url.clone(),
                "$code" => data.code.clone(),
                "$utm_source" => data.utm_source.clone(),
                "$utm_campaign" => data.utm_campaign.clone(),
                "$utm_content" => data.utm_content.clone(),
                "$description" => data.description.clone(),
            ));

            tx.query(query).await?;
            tx.commit().await?;

            Ok(())
        })
        .await;

    match res {
        Ok(()) => Ok(InsertOutcome::Inserted),
        Err(YdbOrCustomerError::YDB(YdbError::YdbStatusError(status_err))) => {
            // YDB reports a primary-key clash on INSERT as PRECONDITION_FAILED.
            // Surface it as a conflict so the caller can pick another code.
            if let Ok(status_code) = status_err.operation_status()
                && status_code == StatusCode::PreconditionFailed
            {
                return Ok(InsertOutcome::Conflict);
            }

            Err(YdbError::YdbStatusError(status_err))
        }
        Err(err) => Err(err.to_ydb_error()),
    }
}

#[derive(Debug)]
pub struct VisitData {
    pub code: String,
    pub url: Option<String>,

    pub ua_info: Option<UaInfo>,
    pub referer: Option<String>,
    pub ip: Option<IpAddr>,

    pub utm_source: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,

    pub event_timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct UaInfo {
    pub user_agent: Option<String>,
    pub browser: Option<String>,
    pub browser_version: Option<String>,
    pub os: Option<String>,
    pub os_version: Option<String>,
    pub device: Option<String>,
}

pub async fn add_visit(
    table_path: String,
    table_client: &TableClient,
    visits: Vec<VisitData>,
) -> ydb::YdbResult<()> {
    let rows: Vec<Value> = visits
        .iter()
        .map(|visit| {
            let ip_str = visit.ip.map(|ip| ip.to_string());

            let start_of_day_datetime = visit
                .event_timestamp
                .date_naive()
                .and_time(chrono::NaiveTime::MIN);
            let start_of_day_utc: DateTime<Utc> = Utc.from_utc_datetime(&start_of_day_datetime);

            ydb_struct!(
                "code" => visit.code.clone(),
                "src" => visit.url.clone(),
                "user_agent" => visit.ua_info.as_ref().and_then(|ua| ua.user_agent.clone()),
                "browser" => visit.ua_info.as_ref().and_then(|ua| ua.browser.clone()),
                "browser_version" => visit.ua_info.as_ref().and_then(|ua| ua.browser_version.clone()),
                "os" => visit.ua_info.as_ref().and_then(|ua| ua.os.clone()),
                "os_version" => visit.ua_info.as_ref().and_then(|ua| ua.os_version.clone()),
                "device" => visit.ua_info.as_ref().and_then(|ua| ua.device.clone()),
                "referer" => visit.referer.clone(),
                "ip" => ip_str,
                "utm_source" => visit.utm_source.clone(),
                "utm_campaign" => visit.utm_campaign.clone(),
                "utm_content" => visit.utm_content.clone(),
                // chrono's SystemTime conversion keeps sub-second precision and
                // handles pre-epoch instants without wrapping.
                "event_date" => Value::Date(SystemTime::from(start_of_day_utc)),
                "event_timestamp" => Value::Timestamp(SystemTime::from(visit.event_timestamp)),
            )
        })
        .collect();

    table_client
        .retry_execute_bulk_upsert(table_path, rows)
        .await
}
