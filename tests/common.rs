use snowflake_connector_rs::{
    Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeEndpointConfig,
    SnowflakeSessionConfig,
};
use url::Url;

#[allow(dead_code)]
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

    let mut session_config = SnowflakeSessionConfig::default();
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

    let mut client_config =
        SnowflakeClientConfig::new(&username, &account, auth_method()).with_session(session_config);

    if let Some(host) = host {
        let scheme = protocol.unwrap_or_else(|| "https".to_string());
        let mut url = Url::parse(&format!("{scheme}://{host}"))
            .map_err(|e| snowflake_connector_rs::Error::Url(e.to_string()))?;
        if let Some(port) = port {
            url.set_port(Some(port))
                .map_err(|_| snowflake_connector_rs::Error::Url("invalid base url port".into()))?;
        }
        client_config = client_config.with_endpoint(SnowflakeEndpointConfig::custom_base_url(url));
    }

    SnowflakeClient::new(client_config)
}

#[cfg(not(feature = "external-browser-sso"))]
pub(crate) fn auth_method() -> SnowflakeAuthMethod {
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
pub(crate) fn auth_method() -> SnowflakeAuthMethod {
    use snowflake_connector_rs::ExternalBrowserConfig;

    SnowflakeAuthMethod::ExternalBrowser(ExternalBrowserConfig::default())
}
