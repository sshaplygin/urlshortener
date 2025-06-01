use ydb::{
    CommandLineCredentials, Query, TableClient, YdbError, YdbOrCustomerError, YdbResult, ydb_params,
};

use ydb_grpc::generated::ydb::status_ids::StatusCode;

pub async fn init_db() -> ydb::YdbResult<ydb::Client> {
    let conn_string =
        std::env::var("YDB_CONNECTION_STRING").expect("YDB_CONNECTION_STRING must be set");

    let client = ydb::ClientBuilder::new_from_connection_string(conn_string)?
        .with_credentials(CommandLineCredentials::from_cmd("yc iam create-token")?)
        .client()?;

    client.wait().await?;

    Ok(client)
}

pub async fn init_tables(table_client: &TableClient) -> ydb::YdbResult<()> {
    let create_url_table = String::from(
        "
        CREATE TABLE urls (
            src Utf8 NOT NULL,
            code Utf8 NOT NULL,
            created_at Timestamp NOT NULL,

            PRIMARY KEY(code)
        );
    ",
    );
    table_client
        .retry_execute_scheme_query(create_url_table)
        .await?;

    let create_vists = String::from(
        "
        CREATE TABLE visits (
            code Utf8 NOT NULL,
            referer String,
            ip String,
            timestamp Timestamp,

            PRIMARY KEY(code, timestamp)
        );
        ",
    );

    table_client
        .retry_execute_scheme_query(create_vists)
        .await?;

    Ok(())
}

pub async fn get(table_client: &TableClient, code: String) -> YdbResult<String> {
    let table_client = table_client.clone_with_transaction_options(
        ydb::TransactionOptions::new()
            .with_autocommit(true)
            .with_mode(ydb::Mode::OnlineReadonly),
    );

    let src: Option<String> = table_client
        .retry_transaction(|tx| async {
            let mut tx = tx;
            print!("{}", code);
            let src: Option<String> = tx
                .query(
                    Query::from(
                        "
                        DECLARE $code as Utf8;

                        SELECT 
                            src
                        FROM 
                            urls
                        WHERE
                            code = $code;
                    ",
                    )
                    .with_params(ydb_params!("$code"=>code.clone())),
                )
                .await?
                .into_only_row()?
                .remove_field_by_name("src")?
                .try_into()?;

            Ok(src)
        })
        .await?;

    if let Some(src) = src {
        Ok(src)
    } else {
        Err(YdbError::NoRows)
    }
}

pub async fn insert(table_client: &TableClient, src: String, code: String) -> ydb::YdbResult<()> {
    let res = table_client
        .retry_transaction(|tx| async {
            let mut tx = tx;

            tx.query(
                ydb::Query::from(
                    "
                        DECLARE $src as Utf8;
                        DECLARE $code as Utf8;

                        INSERT INTO urls (src, code, created_at)
                        VALUES ($src, $code, CurrentUtcTimestamp())
                    ",
                )
                .with_params(ydb_params!("$src"=>src.clone(), "$code" => code.clone())),
            )
            .await?;

            tx.commit().await?;
            Ok(())
        })
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(YdbOrCustomerError::YDB(YdbError::YdbStatusError(status_err))) => {
            if let Some(status_code) = status_err.operation_status().ok() {
                if status_code == StatusCode::PreconditionFailed {
                    return Ok(());
                }
            }

            Err(YdbError::YdbStatusError(status_err))
        }
        Err(err) => Err(err.to_ydb_error()),
    }
}
