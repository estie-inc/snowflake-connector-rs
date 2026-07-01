use std::{env, num::NonZeroU16};

use snowflake_connector_rs::{
    BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthConfig, SnowflakeClient,
    SnowflakeClientConfig, SnowflakeSessionConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = env::var("SNOWFLAKE_USERNAME")?;
    let account = env::var("SNOWFLAKE_ACCOUNT")?;
    let role = env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").ok();

    let redirect_port = NonZeroU16::new(3037).unwrap();
    let external_browser =
        ExternalBrowserConfig::without_callback_listener(BrowserLaunchMode::Manual, redirect_port);

    let mut session_config = SnowflakeSessionConfig::default();
    if let Some(warehouse) = warehouse {
        session_config = session_config.with_warehouse(warehouse);
    }
    if let Some(role) = role {
        session_config = session_config.with_role(role);
    }

    let client = SnowflakeClient::new(
        SnowflakeClientConfig::new(
            &username,
            &account,
            SnowflakeAuthConfig::external_browser(external_browser),
        )
        .with_session(session_config),
    )?;

    let session = client.create_session().await?;
    let rows = session
        .query_as("SELECT CURRENT_VERSION()")
        .await?
        .collect::<Vec<(String,)>>()
        .await?;
    let version = &rows[0].0;
    println!("Connected. Snowflake version={version}");

    Ok(())
}
