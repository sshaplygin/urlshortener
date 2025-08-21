use std::{net::IpAddr, time::SystemTime};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::time::{Duration, UNIX_EPOCH};
use ydb::{
    ClientBuilder, CommandLineCredentials, MetadataUrlCredentials, Query, TableClient, Value,
    YdbError, YdbOrCustomerError, YdbResult, ydb_params, ydb_struct,
};
use ydb_grpc::generated::ydb::status_ids::StatusCode;

use crate::config::Environment;

pub async fn init_db(connection_key: String, env: Environment) -> ydb::YdbResult<ydb::Client> {
    let conn_string = std::env::var(connection_key).expect("connection string must be set");

    let mut client_builder: ClientBuilder =
        ydb::ClientBuilder::new_from_connection_string(conn_string)?;

    client_builder = match env {
        Environment::Production => client_builder.with_credentials(MetadataUrlCredentials::new()),
        _ => client_builder
            .with_credentials(CommandLineCredentials::from_cmd("yc iam create-token")?),
    };

    let client = client_builder.client()?;

    client.wait().await?;

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

pub async fn init_visits_tables(table_client: &TableClient) -> ydb::YdbResult<()> {
    let create_vists = String::from(
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
            referer String,
            ip String,
            utm_source Utf8,
            utm_campaign Utf8,
            utm_content Utf8,
            event_date Date NOT NULL,
            event_timestamp Timestamp NOT NULL,

            PRIMARY KEY(code, event_timestamp, event_date)
        )
        PARTITION BY HASH(event_date)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
        );
        ",
    );

    table_client
        .retry_execute_scheme_query(create_vists)
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

pub async fn get(table_client: &TableClient, code: String) -> YdbResult<LinkInfo> {
    let table_client = table_client.clone_with_transaction_options(
        ydb::TransactionOptions::new()
            .with_autocommit(true)
            .with_mode(ydb::Mode::OnlineReadonly),
    );

    let res: LinkInfo = table_client
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

            let mut row = tx.query(query).await?.into_only_row()?;

            let url: String = row.remove_field_by_name("src")?.try_into()?;
            let utm_source: Option<String> = row.remove_field_by_name("utm_source")?.try_into()?;
            let utm_campaign: Option<String> =
                row.remove_field_by_name("utm_campaign")?.try_into()?;
            let utm_content: Option<String> =
                row.remove_field_by_name("utm_content")?.try_into()?;

            Ok(LinkInfo {
                url: Some(url),
                code: code.clone(),
                utm_source,
                utm_campaign,
                utm_content,
            })
        })
        .await?;

    Ok(res)
}

pub struct CreateData {
    pub code: String,
    pub url: String,
    pub utm_source: Option<String>, // от куда пришел пользователь: google, telegram, github
    pub utm_campaign: Option<String>, // промо кампании
    pub utm_content: Option<String>, // с какого конкретного места
    pub description: Option<String>, // дополнительная информация
}

pub async fn insert(table_client: &TableClient, data: CreateData) -> ydb::YdbResult<()> {
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
        Err(YdbOrCustomerError::YDB(YdbError::YdbStatusError(status_err))) => {
            if let Ok(status_code) = status_err.operation_status() {
                if status_code == StatusCode::PreconditionFailed {
                    return Ok(());
                }
            }

            Err(YdbError::YdbStatusError(status_err))
        }
        Err(err) => Err(err.to_ydb_error()),
        _ => Ok(()),
    }
}

pub struct VisitData {
    pub code: String,
    pub url: Option<String>,

    pub ua_info: Option<UaInfo>,
    pub referer: Option<String>,
    pub ip: Option<IpAddr>,

    pub utm_source: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,

    pub event_timestamp: DateTime<chrono::Utc>,
}

pub struct UaInfo {
    pub user_agent: String,
    pub browser: String,
    pub browser_version: String,
    pub os: String,
    pub os_version: String,
    pub device: String,
}

pub async fn add_visit(table_client: &TableClient, visits: Vec<VisitData>) -> ydb::YdbResult<()> {
    let rows = visits
        .iter()
        .map(|visit| {
            let ip_str = visit.ip.map(|ip| ip.to_string());

            let start_of_day_datetime = visit
                .event_timestamp
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let start_of_day_utc: DateTime<Utc> = Utc.from_utc_datetime(&start_of_day_datetime);

            ydb_struct!(
                "code" => visit.code.clone(),
                "src" => visit.url.clone(),
                "user_agent" => visit.ua_info.as_ref().map(|ua| ua.user_agent.clone()),
                "browser" => visit.ua_info.as_ref().map(|ua| ua.browser.clone()),
                "browser_version" => visit.ua_info.as_ref().map(|ua| ua.browser_version.clone()),
                "os" => visit.ua_info.as_ref().map(|ua| ua.os.clone()),
                "os_version" => visit.ua_info.as_ref().map(|ua| ua.os_version.clone()),
                "device" => visit.ua_info.as_ref().map(|ua| ua.device.clone()),
                "referer" => visit.referer.clone(),
                "ip" => ip_str,
                "utm_source" => visit.utm_source.clone(),
                "utm_campaign" => visit.utm_campaign.clone(),
                "utm_content" => visit.utm_content.clone(),
                "event_date" => datetime_to_system_time(start_of_day_utc),
                "event_timestamp" => datetime_to_system_time(visit.event_timestamp),
            )
        })
        .collect();

    let my_optional_value: Option<String> = Some("hello".to_string());
    let ydb_value: Value = my_optional_value.into();

    let example = Value::struct_from_fields(vec![
        ("code".to_string(), "test".into()),
        ("src".to_string(), ydb_value.clone()),
        ("user_agent".to_string(), ydb_value.clone()),
        ("browser".to_string(), ydb_value.clone()),
        ("browser_version".to_string(), ydb_value.clone()),
        ("os".to_string(), ydb_value.clone()),
        ("os_version".to_string(), ydb_value.clone()),
        ("device".to_string(), ydb_value.clone()),
        ("referer".to_string(), ydb_value.clone()),
        ("ip".to_string(), ydb_value.clone()),
        ("utm_source".to_string(), ydb_value.clone()),
        ("utm_campaign".to_string(), ydb_value.clone()),
        ("utm_content".to_string(), ydb_value),
        ("event_date".to_string(), Value::Date(SystemTime::now())),
        (
            "event_timestamp".to_string(),
            Value::Timestamp(SystemTime::now()),
        ),
    ]);

    let rows = Value::list_from(example, rows)?;
    let res = table_client
        .retry_execute_bulk_upsert(
            "/ru-central1/b1gjv06vpc5tb4h6eijk/etnbjkch1br9j9e4se4n/visits".to_string(),
            rows,
        )
        .await;

    match res {
        Err(err) => Err(err),
        _ => Ok(()),
    }
}

fn datetime_to_system_time(datetime: DateTime<chrono::Utc>) -> std::time::SystemTime {
    let secs_since_epoch = datetime.timestamp();
    UNIX_EPOCH + Duration::from_secs(secs_since_epoch as u64)
}
