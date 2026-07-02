use std::env;

use url::Url;

use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, EndpointConfig, ExternalBrowserConfig, FromRow, QueryConfig,
    Result, Session, SessionConfig,
};

#[derive(Debug, PartialEq, FromRow)]
struct DefaultLabels {
    id: i64,
    user_name: String,
    note: Option<String>,
}

#[derive(Debug, PartialEq, FromRow)]
struct ExactLabels {
    #[snowflake(rename = "display name")]
    display_name: String,
}

#[derive(Debug, PartialEq, FromRow)]
struct TupleByPosition(i64, String);

#[derive(Debug, PartialEq, FromRow)]
#[snowflake(positional)]
struct NamedByPosition {
    id: i64,
    label: String,
}

#[derive(Debug, PartialEq, FromRow)]
#[snowflake(rename_all = "none")]
struct LowercaseLabels {
    name: String,
    value: String,
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_main())?;
    Ok(())
}

async fn async_main() -> Result<()> {
    let client = connect_with_configs(session_config(), QueryConfig::default())?;
    let session = client.create_session().await?;

    run_default_mapping_example(&session).await?;
    run_rename_all_none_example(&session).await?;
    run_exact_rename_example(&session).await?;
    run_position_examples(&session).await?;
    run_streaming_example(&session).await?;

    Ok(())
}

async fn run_default_mapping_example(session: &Session) -> Result<()> {
    // Default field-name mapping uses compile-time SCREAMING_SNAKE_CASE labels.
    let rows = session
        .query_as(r#"SELECT 1 AS id, 'alice' AS user_name, NULL::STRING AS note"#)
        .await?
        .collect::<Vec<DefaultLabels>>()
        .await?;
    assert_eq!(
        rows,
        vec![DefaultLabels {
            id: 1,
            user_name: "alice".to_string(),
            note: None,
        }]
    );
    println!("default mapping: {rows:?}");
    Ok(())
}

async fn run_rename_all_none_example(session: &Session) -> Result<()> {
    // Lowercase quoted labels can opt out of the default renaming rule.
    let rows = session
        .query_as(r#"SELECT 'TIMEZONE' AS "name", 'Asia/Tokyo' AS "value""#)
        .await?
        .collect::<Vec<LowercaseLabels>>()
        .await?;
    assert_eq!(
        rows,
        vec![LowercaseLabels {
            name: "TIMEZONE".to_string(),
            value: "Asia/Tokyo".to_string(),
        }]
    );
    println!("rename_all = none: {rows:?}");
    Ok(())
}

async fn run_exact_rename_example(session: &Session) -> Result<()> {
    // `rename` targets the exact raw label, including spaces.
    let rows = session
        .query_as(r#"SELECT 'Alice Example' AS "display name""#)
        .await?
        .collect::<Vec<ExactLabels>>()
        .await?;
    assert_eq!(
        rows,
        vec![ExactLabels {
            display_name: "Alice Example".to_string(),
        }]
    );
    println!("rename: {rows:?}");
    Ok(())
}

async fn run_position_examples(session: &Session) -> Result<()> {
    // Tuple structs decode by position automatically; named structs opt in with
    // container-level `#[snowflake(positional)]`.
    let tuple_rows = session
        .query_as(r#"SELECT 7, 'tuple row'"#)
        .await?
        .collect::<Vec<TupleByPosition>>()
        .await?;
    assert_eq!(
        tuple_rows,
        vec![TupleByPosition(7, "tuple row".to_string())]
    );

    let named_rows = session
        .query_as(r#"SELECT 9, 'named row'"#)
        .await?
        .collect::<Vec<NamedByPosition>>()
        .await?;
    assert_eq!(
        named_rows,
        vec![NamedByPosition {
            id: 9,
            label: "named row".to_string(),
        }]
    );
    println!("positional: tuple={tuple_rows:?}, named={named_rows:?}");
    Ok(())
}

async fn run_streaming_example(session: &Session) -> Result<()> {
    // Typed results can be streamed table by table instead of collected eagerly.
    let mut result = session
        .query_as::<DefaultLabels, _>(
            r#"SELECT 1 AS id, 'alice' AS user_name, NULL::STRING AS note
               UNION ALL
               SELECT 2 AS id, 'bob' AS user_name, 'ready' AS note
               ORDER BY id"#,
        )
        .await?;

    while let Some(table) = result.next_table().await? {
        for row in table.rows() {
            let row = row?;
            println!("streamed row: {row:?}");
        }
    }

    Ok(())
}

fn connect_with_configs(
    session_config: SessionConfig,
    query_config: QueryConfig,
) -> Result<Client> {
    let username = env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME");
    let account = env::var("SNOWFLAKE_ACCOUNT").expect("set SNOWFLAKE_ACCOUNT");
    let host = env::var("SNOWFLAKE_HOST").ok();
    let port = env::var("SNOWFLAKE_PORT")
        .ok()
        .and_then(|var| var.parse().ok());
    let protocol = env::var("SNOWFLAKE_PROTOCOL").ok();

    let mut client_config = ClientConfig::new(
        &username,
        &account,
        AuthConfig::external_browser(ExternalBrowserConfig::default()),
    )
    .with_session(session_config)
    .with_query(query_config);

    if let Some(host) = host {
        let scheme = protocol.unwrap_or_else(|| "https".to_string());
        let mut url = Url::parse(&format!("{scheme}://{host}"))
            .map_err(|e| snowflake_connector_rs::Error::other(e.to_string()))?;
        if let Some(port) = port {
            url.set_port(Some(port))
                .map_err(|_| snowflake_connector_rs::Error::other("invalid base url port"))?;
        }
        client_config = client_config.with_endpoint(EndpointConfig::custom_base_url(url));
    }

    Client::new(client_config)
}

fn session_config() -> SessionConfig {
    let role = env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = env::var("SNOWFLAKE_DATABASE").ok();
    let schema = env::var("SNOWFLAKE_SCHEMA").ok();

    let mut session_config = SessionConfig::default();
    if let Some(warehouse) = warehouse {
        session_config = session_config.with_warehouse(warehouse);
    }
    if let Some(database) = database {
        session_config = session_config.with_database(database);
    }
    if let Some(schema) = schema {
        session_config = session_config.with_schema(schema);
    }
    if let Some(role) = role {
        session_config = session_config.with_role(role);
    }

    session_config
}
