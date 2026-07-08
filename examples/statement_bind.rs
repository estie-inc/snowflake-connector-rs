use std::env;

use chrono::{FixedOffset, NaiveDate};

use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, ExternalBrowserConfig, Session, SessionConfig, Statement,
    bind::{Binary, BindType, Integer, RawBind, TimestampTz},
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let session = build_session().await?;

    session
        .query(
            "
            CREATE TEMPORARY TABLE statement_bind_demo (
                id NUMBER,
                text_val STRING,
                flag BOOLEAN,
                nullable_text STRING,
                ts_tz TIMESTAMP_TZ,
                payload BINARY,
                variant_col VARIANT,
                big_num NUMBER(38, 0)
            )
            ",
        )
        .await?;

    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(21, 30, 45)
        .unwrap()
        .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
        .unwrap();

    // Use `INSERT ... SELECT ..., PARSE_JSON(?), ...` instead of `INSERT ... VALUES (..., PARSE_JSON(?), ...)`:
    // Snowflake's VALUES clause rejects bind variables wrapped in VARIANT-producing expressions.
    session
        .query(
            Statement::new(
                "
                INSERT INTO statement_bind_demo (
                    id,
                    text_val,
                    flag,
                    nullable_text,
                    ts_tz,
                    payload,
                    variant_col,
                    big_num
                ) SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?), ?
                ",
            )
            .bind(1_i64)
            .bind("hello")
            .bind(true)
            .bind(None::<&str>)
            .bind(TimestampTz::try_from(ts)?)
            .bind(Binary::new(b"hello".as_slice()))
            .bind(r#"{"k":"v"}"#)
            .bind(Integer::try_from(9_999_999_999_999_999_i128)?),
        )
        .await?;

    let positional = session
        .query_as(
            "
            SELECT
                id, text_val, flag, nullable_text
            FROM
                statement_bind_demo
            ",
        )
        .await?
        .collect::<Vec<(i64, String, bool, Option<String>)>>()
        .await?;
    println!("positional bind row: {positional:?}");

    let named = session
        .query_as(
            Statement::new(
                "
                SELECT
                    id, text_val
                FROM
                    statement_bind_demo
                WHERE
                    id = :id OR id = :1
                ",
            )
            .bind_named("id", 1_i64)
            .bind_named("1", 999_i64),
        )
        .await?
        .collect::<Vec<(i64, String)>>()
        .await?;
    println!("named bind rows: {named:?}");

    session
        .query(
            Statement::new(
                "
                UPDATE
                    statement_bind_demo
                SET
                    variant_col = PARSE_JSON(?)
                WHERE
                    id = ?
                ",
            )
            .bind(RawBind::new(BindType::Text, r#"{"k":"v2"}"#))
            .bind(1_i64),
        )
        .await?;

    let rendered = session
        .query_as(
            "
            SELECT
                TO_CHAR(ts_tz, 'YYYY-MM-DD HH24:MI:SS TZHTZM'),
                payload,
                TO_JSON(variant_col),
                big_num
            FROM
                statement_bind_demo
            WHERE
                id = 1
            ",
        )
        .await?
        .collect::<Vec<(String, String, String, i128)>>()
        .await?;
    println!("rendered bind values: {rendered:?}");

    Ok(())
}

async fn build_session() -> std::result::Result<Session, Box<dyn std::error::Error>> {
    let username = env::var("SNOWFLAKE_USERNAME")?;
    let account = env::var("SNOWFLAKE_ACCOUNT")?;
    let role = env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = env::var("SNOWFLAKE_DATABASE").ok();
    let schema = env::var("SNOWFLAKE_SCHEMA").ok();

    let mut session_config = SessionConfig::new();
    if let Some(value) = warehouse {
        session_config = session_config.with_warehouse(value);
    }
    if let Some(value) = database {
        session_config = session_config.with_database(value);
    }
    if let Some(value) = schema {
        session_config = session_config.with_schema(value);
    }
    if let Some(value) = role {
        session_config = session_config.with_role(value);
    }

    let client = Client::new(
        ClientConfig::new(
            &username,
            &account,
            AuthConfig::external_browser(ExternalBrowserConfig::default()),
        )
        .with_session(session_config),
    )?;
    Ok(client.create_session().await?)
}
