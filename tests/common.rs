use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

pub fn connect() -> Result<SnowflakeClient> {
    let username = std::env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME for testing");
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
        auth_method(),
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

#[cfg(not(feature = "external-browser-sso"))]
fn auth_method() -> SnowflakeAuthMethod {
    let private_key =
        std::env::var("SNOWFLAKE_PRIVATE_KEY").expect("set SNOWFLAKE_PRIVATE_KEY for testing");
    let private_key_password = std::env::var("SNOWFLAKE_PRIVATE_KEY_PASSWORD")
        .expect("set SNOWFLAKE_PRIVATE_KEY_PASSWORD for testing");

    SnowflakeAuthMethod::KeyPair {
        encrypted_pem: private_key,
        password: private_key_password.into_bytes(),
    }
}

#[cfg(feature = "external-browser-sso")]
fn auth_method() -> SnowflakeAuthMethod {
    use snowflake_connector_rs::ExternalBrowserConfig;

    SnowflakeAuthMethod::ExternalBrowser(ExternalBrowserConfig::default())
}
