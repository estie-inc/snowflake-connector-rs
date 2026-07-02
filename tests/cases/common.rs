use std::env;

use url::Url;

use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, EndpointConfig, QueryConfig, Result, SessionConfig,
};

pub fn connect() -> Result<Client> {
    connect_with_configs(session_config(), QueryConfig::default())
}

pub fn connect_with_session(session_config: SessionConfig) -> Result<Client> {
    connect_with_configs(session_config, QueryConfig::default())
}

pub fn connect_with_query(query_config: QueryConfig) -> Result<Client> {
    connect_with_configs(session_config(), query_config)
}

fn connect_with_configs(
    session_config: SessionConfig,
    query_config: QueryConfig,
) -> Result<Client> {
    let username = env::var("SNOWFLAKE_USERNAME").expect("set SNOWFLAKE_USERNAME for testing");
    let account = env::var("SNOWFLAKE_ACCOUNT").expect("set SNOWFLAKE_ACCOUNT for testing");
    let host = env::var("SNOWFLAKE_HOST").ok();
    let port = env::var("SNOWFLAKE_PORT")
        .ok()
        .and_then(|var| var.parse().ok());
    let protocol = env::var("SNOWFLAKE_PROTOCOL").ok();

    let mut client_config = ClientConfig::new(&username, &account, auth_method())
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

pub fn session_config() -> SessionConfig {
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

#[cfg(all(not(feature = "external-browser-sso"), feature = "key-pair-auth"))]
fn auth_method() -> AuthConfig {
    use snowflake_connector_rs::KeyPairConfig;

    let private_key =
        env::var("SNOWFLAKE_PRIVATE_KEY").expect("set SNOWFLAKE_PRIVATE_KEY for testing");
    let private_key_password = env::var("SNOWFLAKE_PRIVATE_KEY_PASSWORD")
        .expect("set SNOWFLAKE_PRIVATE_KEY_PASSWORD for testing");

    AuthConfig::key_pair(KeyPairConfig::from_encrypted_pem(
        private_key,
        private_key_password.into_bytes(),
    ))
}

#[cfg(feature = "external-browser-sso")]
fn auth_method() -> AuthConfig {
    use snowflake_connector_rs::ExternalBrowserConfig;

    AuthConfig::external_browser(ExternalBrowserConfig::default())
}
