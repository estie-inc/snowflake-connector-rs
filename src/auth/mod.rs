mod key_pair;

use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};

use crate::{Error, Result, SnowflakeAuthMethod, SnowflakeClientConfig, SnowflakeConnectionConfig};

use self::key_pair::generate_jwt_from_key_pair;

fn get_base_url(
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
) -> String {
    if let Some(connection_config) = connection_config {
        let host = &connection_config.host;
        let port = connection_config
            .port
            .map(|p| format!(":{p}"))
            .unwrap_or_else(|| "".to_string());
        let protocol = connection_config
            .protocol
            .clone()
            .unwrap_or_else(|| "https".to_string());

        format!("{protocol}://{host}{port}")
    } else {
        format!("https://{}.snowflakecomputing.com", config.account)
    }
}

/// Login to Snowflake and return a session token.
pub(super) async fn login(
    http: &Client,
    username: &str,
    auth: &SnowflakeAuthMethod,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
) -> Result<String> {
    let base_url = get_base_url(config, connection_config);
    let url = format!("{base_url}/session/v1/login-request");

    let mut queries = vec![];
    if let Some(warehouse) = &config.warehouse {
        queries.push(("warehouse", warehouse));
    }
    if let Some(database) = &config.database {
        queries.push(("databaseName", database));
    }
    if let Some(schema) = &config.schema {
        queries.push(("schemaName", schema));
    }
    if let Some(role) = &config.role {
        queries.push(("roleName", role));
    }

    let login_data = login_request_data(username, auth, config)?;
    let response = http
        .post(url)
        .query(&queries)
        .json(&json!({
            "data": login_data
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(body));
    }

    let response: Response = serde_json::from_str(&body).map_err(|_| Error::Communication(body))?;
    if !response.success {
        return Err(Error::Communication(response.message.unwrap_or_default()));
    }

    Ok(response.data.token)
}

fn login_request_data(
    username: &str,
    auth: &SnowflakeAuthMethod,
    config: &SnowflakeClientConfig,
) -> Result<Value> {
    match auth {
        SnowflakeAuthMethod::Password(password) => Ok(json!({
            "LOGIN_NAME": username,
            "PASSWORD": password,
            "ACCOUNT_NAME": config.account
        })),
        SnowflakeAuthMethod::KeyPair {
            encrypted_pem,
            password,
        } => {
            let jwt = generate_jwt_from_key_pair(
                encrypted_pem,
                password,
                username,
                &config.account,
                Utc::now().timestamp(),
            )?;
            Ok(json!({
                "LOGIN_NAME": username,
                "ACCOUNT_NAME": config.account,
                "TOKEN": jwt,
                "AUTHENTICATOR": "SNOWFLAKE_JWT"
            }))
        }
        SnowflakeAuthMethod::Oauth { token } => Ok(json!({
            "AUTHENTICATOR": "OAUTH",
            "TOKEN": token
        })),
    }
}

#[derive(serde::Deserialize)]
struct LoginResponse {
    token: String,
}

#[derive(serde:: Deserialize)]
struct Response {
    data: LoginResponse,
    message: Option<String>,
    success: bool,
}
