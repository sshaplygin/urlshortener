use ydb::{Query, TableClient, YdbError, YdbResult, ydb_params};

pub async fn init_db() -> ydb::YdbResult<ydb::Client> {
    let conn_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136?database=/local".to_string());

    let client = ydb::ClientBuilder::new_from_connection_string(conn_string)?.client()?;

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
        Err(YdbError::Convert("".into()))
    }
}

pub async fn insert(table_client: &TableClient, src: String, code: String) -> ydb::YdbResult<()> {
    table_client
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
        .await?;

    Ok(())
}
