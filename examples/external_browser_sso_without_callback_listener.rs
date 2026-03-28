use std::num::NonZeroU16;

use snowflake_connector_rs::{
    BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod, SnowflakeClient,
    SnowflakeClientConfig, SnowflakeSessionConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = std::env::var("SNOWFLAKE_USERNAME")?;
    let account = std::env::var("SNOWFLAKE_ACCOUNT")?;
    let role = std::env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok();

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
            SnowflakeAuthMethod::ExternalBrowser(external_browser),
        )
        .with_session(session_config),
    )?;

    let session = client.create_session().await?;
    let rows = session.query("SELECT CURRENT_VERSION()").await?;
    assert_eq!(rows.len(), 1);
    println!(
        "Connected. Snowflake version={}",
        rows[0].get::<String>("CURRENT_VERSION()")?
    );

    Ok(())
}
