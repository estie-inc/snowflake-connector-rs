mod key_pair;

use chrono::Utc;
use pkcs8::DecodePrivateKey;
use reqwest::Client;
use rsa::RsaPrivateKey;
use serde_json::{json, Value};

use crate::{Error, Result, SnowflakeAuthMethod, SnowflakeClientConfig};

use self::key_pair::generate_jwt_from_key_pair;

/// Login to Snowflake and return a session token.
pub(super) async fn login(
    http: &Client,
    username: &str,
    auth: &SnowflakeAuthMethod,
    config: &SnowflakeClientConfig,
) -> Result<String> {
    let url = format!(
        "https://{account}.snowflakecomputing.com/session/v1/login-request",
        account = config.account
    );

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
            let private_key = match password.len() {
                0 => RsaPrivateKey::from_pkcs8_pem(&encrypted_pem)?,
                _ => RsaPrivateKey::from_pkcs8_encrypted_pem(encrypted_pem, password)?,
            };

            let jwt = generate_jwt_from_key_pair(
                private_key,
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
