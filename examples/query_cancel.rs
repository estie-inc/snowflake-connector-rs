use std::{env, time::Duration};

use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, ExternalBrowserConfig, QueryCancelStatus, QueryOptions,
    Session, SessionConfig,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let session = build_session().await?;

    let query = session.query_handle_with_options(
        "CALL SYSTEM$WAIT(120)",
        QueryOptions::new().with_query_cancel_request_timeout(Duration::from_secs(10)),
    )?;

    let canceller = query.canceller();
    let execution = tokio::spawn(query.execute());

    tokio::time::sleep(Duration::from_secs(1)).await;

    let status = canceller.cancel().await?;
    if status == QueryCancelStatus::NotSubmitted {
        println!("cancelled before the query was submitted");
    } else {
        println!("cancel status: {status:?}");
    }

    let execution_result = execution.await?;
    match execution_result {
        Ok(_cursor) => println!("query completed before the cancel took effect"),
        Err(error) if error.is_cancelled() => println!("query was cancelled: {error}"),
        Err(error) => return Err(error.into()),
    }

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
