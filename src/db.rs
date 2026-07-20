use std::net::IpAddr;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, TimeZone, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::trace;
use ydb::{
    AnonymousCredentials, ClientBuilder, CommandLineCredentials, MetadataUrlCredentials,
    QueryClient, TableClient, TxMode, Value, YdbError, YdbResult, ydb_params, ydb_struct,
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

pub async fn init_urls_tables(query_client: &mut QueryClient) -> ydb::YdbResult<()> {
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

    // DDL text goes through the query service in an implicit (non-transactional)
    // transaction; it is rejected inside an interactive one.
    query_client.exec(create_urls).await?;

    Ok(())
}

/// Backing store for the short-code counter. A single row, updated in a
/// serializable transaction, is what makes generated codes collision-free.
pub async fn init_code_counter_table(query_client: &mut QueryClient) -> ydb::YdbResult<()> {
    let create_counter = String::from(
        "
        CREATE TABLE code_counter (
            name Utf8 NOT NULL,
            next_id Uint64 NOT NULL,

            PRIMARY KEY(name)
        );
    ",
    );

    query_client.exec(create_counter).await?;

    Ok(())
}

/// Name of the counter row. A single row is enough: callers claim ids in
/// blocks, so this is touched once per `block_size` links rather than per link.
const CODE_COUNTER_NAME: &str = "short_code";

/// Attempts allowed when contention aborts the counter claim. Concurrent
/// claimers serialize onto one row, so a busy service sees these routinely.
const RESERVE_MAX_ATTEMPTS: u32 = 8;
/// First backoff between contended attempts; doubles each time.
const RESERVE_INITIAL_BACKOFF: Duration = Duration::from_millis(5);

/// Claims `block_size` consecutive counter values and returns the first.
///
/// Runs in a serializable transaction, so two instances racing here conflict
/// and one retries — neither can be handed a range the other already owns.
/// Values left unused when a process exits are simply skipped; codes have no
/// reason to be contiguous.
pub async fn reserve_code_block(
    query_client: &mut QueryClient,
    block_size: u64,
) -> ydb::YdbResult<u64> {
    let mut backoff = RESERVE_INITIAL_BACKOFF;

    for attempt in 1..=RESERVE_MAX_ATTEMPTS {
        match reserve_once(query_client, block_size).await {
            Ok(start) => {
                trace!("reserved code block starting at {}", start);
                return Ok(start);
            }
            // A serialization conflict means another claimer committed first.
            // Re-running re-reads the counter, so the retry claims a fresh
            // block rather than the contested one. The one-shot call does not
            // retry these itself — only the SDK's interactive `retry_tx` does,
            // and its future is not `Send`, so it cannot be used here.
            Err(err) if attempt < RESERVE_MAX_ATTEMPTS && is_serialization_conflict(&err) => {
                trace!("counter contended on attempt {attempt}, retrying in {backoff:?}");

                // Scoped so the non-Send RNG is dropped before the await.
                let jittered = {
                    let mut rng = rand::rng();
                    backoff
                        + Duration::from_micros(rng.random_range(0..=backoff.as_micros() as u64))
                };

                tokio::time::sleep(jittered).await;
                backoff *= 2;
            }
            Err(err) => return Err(err),
        }
    }

    // The loop always returns on its final attempt.
    unreachable!("reserve_code_block exhausted its attempts without returning")
}

/// True when YDB aborted the statement because another transaction touched the
/// same row first, which is safe to re-run.
fn is_serialization_conflict(err: &YdbError) -> bool {
    let YdbError::YdbStatusError(status_err) = err else {
        return false;
    };

    matches!(status_err.operation_status(), Ok(StatusCode::Aborted))
}

/// One attempt at the counter claim.
///
/// Read and write happen in a single multi-statement query rather than an
/// interactive transaction: the SDK's `retry_tx` future is boxed without a
/// `Send` bound and so cannot be awaited inside an axum handler, while a
/// one-shot call can. The statements still execute as one serializable
/// transaction, which is what stops two instances claiming the same range.
///
/// Marked idempotent so an ambiguous failure is retried. A retry landing after
/// the original committed re-reads and claims a *different* block, abandoning
/// the first — blocks may be skipped, never reused, the same guarantee as a
/// process exiting mid-block.
async fn reserve_once(query_client: &mut QueryClient, block_size: u64) -> ydb::YdbResult<u64> {
    let mut row = query_client
        .query_row(
            "
                DECLARE $name AS Utf8;
                DECLARE $block_size AS Uint64;

                -- Bound as its own named expression: COALESCE does not accept
                -- an inline subquery.
                $current_opt = (SELECT next_id FROM code_counter WHERE name = $name);
                $current = COALESCE($current_opt, 0ul);

                UPSERT INTO code_counter (name, next_id)
                VALUES ($name, $current + $block_size);

                SELECT $current AS start;
            ",
        )
        .params(ydb_params!(
            "$name" => CODE_COUNTER_NAME.to_string(),
            "$block_size" => block_size,
        ))
        .with_tx_mode(TxMode::SerializableReadWrite)
        .idempotent(true)
        .await?;

    let start: u64 = row.remove_field_by_name("start")?.try_into()?;

    Ok(start)
}

pub async fn init_visits_tables(query_client: &mut QueryClient) -> ydb::YdbResult<()> {
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

    query_client.exec(create_visits).await?;

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
pub async fn get(query_client: &mut QueryClient, code: String) -> YdbResult<Option<LinkInfo>> {
    // A one-shot call rather than an interactive transaction: `OnlineReadOnly`
    // is only accepted outside one. The read is a single statement, so it needs
    // no transaction of its own — the SDK still retries it internally.
    let row = query_client
        .query_row(
            "
                DECLARE $code AS Utf8;

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
        .params(ydb_params!("$code" => code.clone()))
        .with_tx_mode(TxMode::OnlineReadOnly)
        // An unknown code is a miss, not an error.
        .optional()
        .await?;

    let Some(mut row) = row else {
        return Ok(None);
    };

    let url: String = row.remove_field_by_name("src")?.try_into()?;
    let utm_source: Option<String> = row.remove_field_by_name("utm_source")?.try_into()?;
    let utm_campaign: Option<String> = row.remove_field_by_name("utm_campaign")?.try_into()?;
    let utm_content: Option<String> = row.remove_field_by_name("utm_content")?.try_into()?;

    Ok(Some(LinkInfo {
        url: Some(url),
        code,
        utm_source,
        utm_campaign,
        utm_content,
    }))
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

pub async fn insert(
    query_client: &mut QueryClient,
    data: CreateData,
) -> ydb::YdbResult<InsertOutcome> {
    // A single statement, so it runs as a one-shot call. Left non-idempotent on
    // purpose: a retried INSERT that already landed would come back as a
    // spurious conflict.
    let res = query_client
        .exec(
            "
                DECLARE $src AS Utf8;
                DECLARE $code AS Utf8;
                DECLARE $utm_source AS Optional<Utf8>;
                DECLARE $utm_campaign AS Optional<Utf8>;
                DECLARE $utm_content AS Optional<Utf8>;
                DECLARE $description AS Optional<Utf8>;

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
        .params(ydb_params!(
            "$src" => data.url.clone(),
            "$code" => data.code.clone(),
            "$utm_source" => data.utm_source.clone(),
            "$utm_campaign" => data.utm_campaign.clone(),
            "$utm_content" => data.utm_content.clone(),
            "$description" => data.description.clone(),
        ))
        .await;

    match res {
        Ok(()) => Ok(InsertOutcome::Inserted),
        Err(YdbError::YdbStatusError(status_err)) => {
            // YDB reports a primary-key clash on INSERT as PRECONDITION_FAILED.
            // Surface it as a conflict so the caller can pick another code.
            if let Ok(status_code) = status_err.operation_status()
                && status_code == StatusCode::PreconditionFailed
            {
                return Ok(InsertOutcome::Conflict);
            }

            Err(YdbError::YdbStatusError(status_err))
        }
        Err(err) => Err(err),
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

    table_client.bulk_upsert(table_path, rows).await
}

/// Integration tests against a real YDB.
///
/// These are `#[ignore]`d so `cargo test` stays hermetic; run them with
/// `docker compose up -d ydb && cargo test -- --ignored`. They exist because
/// every query in this module is only checked by the server: a rejected YQL
/// statement, a renamed column, or a changed error code compiles perfectly and
/// fails at runtime.
#[cfg(test)]
pub(crate) mod tests {
    // Tests assert by panicking; a failed `expect` here is the failure report.
    #![allow(clippy::expect_used)]

    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::TimeZone;
    use tokio::sync::OnceCell;
    use ydb::StaticDiscovery;

    use super::*;

    const TEST_CONNECTION_STRING: &str = "grpc://localhost:2136?database=/local";
    const TEST_ENDPOINT: &str = "grpc://localhost:2136";
    const VISITS_TABLE_PATH: &str = "/local/visits";

    /// Schema is created once per test binary; tests then isolate themselves by
    /// using distinct codes rather than distinct tables, since table names are
    /// baked into the queries under test.
    static SCHEMA: OnceCell<()> = OnceCell::const_new();

    /// Keeps generated test codes unique across concurrently running tests.
    static NEXT_CODE: AtomicU64 = AtomicU64::new(0);

    fn unique_code(prefix: &str) -> String {
        format!("{prefix}-{}", NEXT_CODE.fetch_add(1, Ordering::Relaxed))
    }

    async fn client() -> ydb::Client {
        let client = ClientBuilder::new_from_connection_string(TEST_CONNECTION_STRING)
            .expect("parse connection string")
            .with_credentials(AnonymousCredentials::new())
            // The container advertises its own hostname (`ydb`) through
            // discovery, which does not resolve from the host running the
            // tests. Pinning the endpoint skips discovery entirely. Production
            // goes through `init_db`, which keeps discovery on.
            .with_discovery(StaticDiscovery::new_from_str(TEST_ENDPOINT).expect("static discovery"))
            .client()
            .expect("build client");

        client
            .wait()
            .await
            .expect("connect to local ydb (is `docker compose up -d ydb` running?)");

        client
    }

    /// Drops and recreates every table, exercising the real migration path.
    ///
    /// Shared with the `consumer` tests so both suites agree on the schema.
    pub(crate) async fn schema() -> ydb::Client {
        let client = client().await;

        SCHEMA
            .get_or_init(|| async {
                let mut qc = client.query_client();

                for table in ["urls", "code_counter", "visits"] {
                    qc.exec(format!("DROP TABLE IF EXISTS {table};"))
                        .await
                        .expect("drop table");
                }

                init_urls_tables(&mut qc).await.expect("create urls");
                init_code_counter_table(&mut qc)
                    .await
                    .expect("create code_counter");
                create_visits_table(&mut qc).await;
            })
            .await;

        client
    }

    /// Creates `visits`, falling back to a row-store table when the server has
    /// no column-store support.
    ///
    /// `ydbplatform/local-ydb` rejects `STORE = COLUMN` with
    /// `OLAP schema operations are not supported`. The fallback keeps identical
    /// columns and types, so the bulk upsert — the row shape, the column names,
    /// the `Date`/`Timestamp` conversions — is still exercised end to end.
    /// What it does *not* cover is the storage-engine and partitioning clause in
    /// `init_visits_tables`; that only runs against a real column-store YDB.
    async fn create_visits_table(qc: &mut QueryClient) {
        match init_visits_tables(qc).await {
            Ok(()) => return,
            Err(err) if is_olap_unsupported(&err) => {
                eprintln!(
                    "note: server has no column-store support, creating `visits` as a \
                     row-store table; the STORE = COLUMN clause is not covered here"
                );
            }
            Err(err) => panic!("create visits: {err}"),
        }

        qc.exec(
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
            );
            ",
        )
        .await
        .expect("create row-store visits");
    }

    fn is_olap_unsupported(err: &YdbError) -> bool {
        err.to_string().contains("OLAP schema operations")
    }

    fn create_data(code: &str) -> CreateData {
        CreateData {
            code: code.to_string(),
            url: "https://example.test/target".to_string(),
            utm_source: Some("telegram".to_string()),
            utm_campaign: Some("launch".to_string()),
            utm_content: Some("banner".to_string()),
            description: Some("a test link".to_string()),
        }
    }

    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn migrations_create_every_table() {
        // `schema()` runs the three real `CREATE TABLE` statements; reaching
        // here at all means the YQL was accepted, including the column-store
        // options on `visits`.
        let client = schema().await;
        let mut qc = client.query_client();

        for table in ["urls", "code_counter", "visits"] {
            qc.query_result_set(format!("SELECT * FROM {table} LIMIT 0;"))
                .await
                .unwrap_or_else(|err| panic!("table {table} is not queryable: {err}"));
        }
    }

    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn insert_then_get_round_trips_every_field() {
        let client = schema().await;
        let mut qc = client.query_client();
        let code = unique_code("roundtrip");

        let outcome = insert(&mut qc, create_data(&code)).await.expect("insert");
        assert_eq!(outcome, InsertOutcome::Inserted);

        let found = get(&mut qc, code.clone())
            .await
            .expect("get")
            .expect("row should exist");

        assert_eq!(found.code, code);
        assert_eq!(found.url.as_deref(), Some("https://example.test/target"));
        assert_eq!(found.utm_source.as_deref(), Some("telegram"));
        assert_eq!(found.utm_campaign.as_deref(), Some("launch"));
        assert_eq!(found.utm_content.as_deref(), Some("banner"));
    }

    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn get_unknown_code_is_a_miss_not_an_error() {
        let client = schema().await;
        let mut qc = client.query_client();

        let found = get(&mut qc, unique_code("absent")).await.expect("get");

        assert!(found.is_none(), "unknown code must return None");
    }

    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn nullable_utm_fields_round_trip_as_none() {
        let client = schema().await;
        let mut qc = client.query_client();
        let code = unique_code("bare");

        let data = CreateData {
            code: code.clone(),
            url: "https://example.test/bare".to_string(),
            utm_source: None,
            utm_campaign: None,
            utm_content: None,
            description: None,
        };
        insert(&mut qc, data).await.expect("insert");

        let found = get(&mut qc, code).await.expect("get").expect("row exists");

        assert_eq!(found.utm_source, None);
        assert_eq!(found.utm_campaign, None);
        assert_eq!(found.utm_content, None);
    }

    /// The backstop in `InsertOutcome::Conflict`. It depends on YDB reporting a
    /// primary-key clash as PRECONDITION_FAILED — an assumption about the
    /// server's error code that nothing else verifies. If this breaks, a
    /// duplicate code surfaces as a 500 instead of a conflict, or worse is
    /// mistaken for success.
    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn reinserting_a_code_reports_conflict_rather_than_overwriting() {
        let client = schema().await;
        let mut qc = client.query_client();
        let code = unique_code("conflict");

        let first = insert(&mut qc, create_data(&code)).await.expect("insert");
        assert_eq!(first, InsertOutcome::Inserted);

        let mut clashing = create_data(&code);
        clashing.url = "https://attacker.test/stolen".to_string();

        let second = insert(&mut qc, clashing)
            .await
            .expect("a key clash must be an Ok(Conflict), not an Err");
        assert_eq!(
            second,
            InsertOutcome::Conflict,
            "duplicate code must be reported as a conflict"
        );

        // The original row must survive untouched.
        let found = get(&mut qc, code).await.expect("get").expect("row exists");
        assert_eq!(
            found.url.as_deref(),
            Some("https://example.test/target"),
            "conflicting insert must not overwrite the existing link"
        );
    }

    /// Covers the counter's cold start, its advance, and the property the whole
    /// short-code scheme rests on: two concurrent callers never receive
    /// overlapping ranges. Written as one test because the counter is a single
    /// shared row, so parallel tests over it would interfere.
    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn counter_hands_out_disjoint_blocks() {
        let client = schema().await;
        let mut qc = client.query_client();

        // Cold start: no counter row yet.
        let first = reserve_code_block(&mut qc, 10).await.expect("first block");
        assert_eq!(first, 0, "an unused counter must start at zero");

        let second = reserve_code_block(&mut qc, 10).await.expect("second block");
        assert_eq!(second, 10, "the counter must advance by the block size");

        // Concurrent reservations. `reserve_code_block` must be `Send` for this
        // to compile at all, which is itself part of what is being checked.
        const BLOCK: u64 = 5;
        const TASKS: usize = 16;

        let mut handles = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let mut task_qc = client.query_client();
            handles.push(tokio::spawn(async move {
                reserve_code_block(&mut task_qc, BLOCK)
                    .await
                    .expect("concurrent block")
            }));
        }

        let mut starts = Vec::with_capacity(TASKS);
        for handle in handles {
            starts.push(handle.await.expect("task panicked"));
        }
        starts.sort_unstable();

        // Blocks may be skipped (a retry abandons its predecessor), but no two
        // may overlap — that would hand the same code out twice.
        for pair in starts.windows(2) {
            assert!(
                pair[1] - pair[0] >= BLOCK,
                "blocks {} and {} overlap: {starts:?}",
                pair[0],
                pair[1]
            );
        }
    }

    /// Exercises the 15-column `ydb_struct!` against the real column-store
    /// schema. A field renamed or typed wrongly here is invisible until the
    /// server rejects the upsert.
    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn add_visit_bulk_upserts_every_column() {
        let client = schema().await;
        let table_client = client.table_client();
        let mut qc = client.query_client();
        let code = unique_code("visit");

        let event_timestamp = Utc
            .with_ymd_and_hms(2026, 7, 20, 12, 30, 45)
            .single()
            .expect("valid timestamp");

        let visit = VisitData {
            code: code.clone(),
            url: Some("https://example.test/target".to_string()),
            ua_info: Some(UaInfo {
                user_agent: Some("Mozilla/5.0".to_string()),
                browser: Some("Firefox".to_string()),
                browser_version: Some("128.0.0".to_string()),
                os: Some("Linux".to_string()),
                os_version: Some("6.1.0".to_string()),
                device: Some("Other N/A N/A".to_string()),
            }),
            referer: Some("https://news.test/".to_string()),
            ip: Some("203.0.113.7".parse().expect("valid ip")),
            utm_source: Some("telegram".to_string()),
            utm_campaign: Some("launch".to_string()),
            utm_content: Some("banner".to_string()),
            event_timestamp,
        };

        add_visit(VISITS_TABLE_PATH.to_string(), &table_client, vec![visit])
            .await
            .expect("bulk upsert");

        let mut row = qc
            .query_row(
                "
                    DECLARE $code AS Utf8;

                    SELECT browser, ip, event_date
                    FROM visits
                    WHERE code = $code;
                ",
            )
            .params(ydb_params!("$code" => code))
            .await
            .expect("read back the visit");

        let browser: Option<String> = row
            .remove_field_by_name("browser")
            .expect("browser column")
            .try_into()
            .expect("browser is Utf8");
        let ip: Option<String> = row
            .remove_field_by_name("ip")
            .expect("ip column")
            .try_into()
            .expect("ip is Utf8");

        assert_eq!(browser.as_deref(), Some("Firefox"));
        assert_eq!(ip.as_deref(), Some("203.0.113.7"));

        // `event_date` is derived by truncating the timestamp to its UTC day.
        let event_date = row.remove_field_by_name("event_date").expect("event_date");
        assert!(
            matches!(event_date, Value::Date(_) | Value::Optional(_)),
            "event_date should be a Date, got {event_date:?}"
        );
    }

    /// An empty batch must not reach the server as a malformed upsert.
    #[tokio::test]
    #[ignore = "requires a local YDB: docker compose up -d ydb"]
    async fn add_visit_tolerates_an_empty_batch() {
        let client = schema().await;
        let table_client = client.table_client();

        add_visit(VISITS_TABLE_PATH.to_string(), &table_client, Vec::new())
            .await
            .expect("empty batch should be a no-op");
    }
}
