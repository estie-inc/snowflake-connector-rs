use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

pub fn connect() -> Result<SnowflakeClient> {
    let username = std::env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME for testing");
    let password = std::env::var("SNOWFLAKE_PASSWORD").expect("set SNOWFLAKE_PASSWORD for testing");
    let account = std::env::var("SNOWFLAKE_ACCOUNT").expect("set SNOWFLAKE_ACCOUNT for testing");

    let role = std::env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = std::env::var("SNOWFLAKE_DATABASE").ok();
    let schema = std::env::var("SNOWFLAKE_SCHEMA").ok();
    let host = std::env::var("SNOWFLAKE_HOST").ok();
    let port = std::env::var("SNOWFLAKE_PORT")
        .ok()
        .and_then(|var| var.parse().ok());
    let protocol = std::env::var("SNOWFLAKE_PROTOCOL").ok();

    let client = SnowflakeClient::new(
        &username,
        SnowflakeAuthMethod::Password(password),
        SnowflakeClientConfig {
            account,
            warehouse,
            database,
            schema,
            role,
            timeout: None,
        },
    )?;

    let client = if let Some(ref host) = host {
        client.with_address(host, port, protocol)?
    } else {
        client
    };

    Ok(client)
}
