use std::num::NonZeroU16;

use snowflake_connector_rs::{
    BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod, SnowflakeClient,
    SnowflakeClientConfig,
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

    let client = SnowflakeClient::new(
        &username,
        SnowflakeAuthMethod::ExternalBrowser(external_browser),
        SnowflakeClientConfig {
            account,
            warehouse,
            database: None,
            schema: None,
            role,
            timeout: Some(std::time::Duration::from_secs(90)),
        },
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
