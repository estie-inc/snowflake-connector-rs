use reqwest::Client;

use crate::{types::SnowflakeResponse, Error, Result, SnowflakeClientConfig};

/// Login to Snowflake and return a session token.
pub(super) async fn login(
    http: &Client,
    username: &str,
    password: &str,
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

    let response = http
        .post(url)
        .query(&queries)
        .json(&LoginRequest {
            data: LoginRequestData {
                login_name: username.to_string(),
                password: password.to_string(),
                account_name: config.account.clone(),
            },
        })
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(body));
    }

    let response: SnowflakeResponse<LoginResponse> =
        serde_json::from_str(&body).map_err(|_| Error::Communication(body))?;
    if !response.success {
        return Err(Error::Communication(response.message.unwrap_or_default()));
    }

    Ok(response.data.token)
}

#[derive(serde::Serialize)]
struct LoginRequest {
    data: LoginRequestData,
}
#[derive(serde::Serialize)]
struct LoginRequestData {
    #[serde(rename = "LOGIN_NAME")]
    login_name: String,
    #[serde(rename = "PASSWORD")]
    password: String,
    #[serde(rename = "ACCOUNT_NAME")]
    account_name: String,
}
#[derive(serde::Deserialize)]
struct LoginResponse {
    token: String,
}
