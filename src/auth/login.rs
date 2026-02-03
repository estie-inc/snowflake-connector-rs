use chrono::Utc;
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Error, Result, SnowflakeAuthMethod, SnowflakeClientConfig, SnowflakeConnectionConfig};

#[cfg(feature = "external-browser-sso")]
use super::external_browser::run_external_browser_flow;
use super::key_pair::generate_jwt_from_key_pair;

pub(crate) fn get_base_url(
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
) -> Result<Url> {
    if let Some(connection_config) = connection_config {
        let host = &connection_config.host;
        let protocol = connection_config
            .protocol
            .clone()
            .unwrap_or_else(|| "https".to_string());
        let mut url = Url::parse(&format!("{protocol}://{host}"))?;
        if let Some(port) = connection_config.port {
            url.set_port(Some(port))
                .map_err(|_| Error::Url("invalid base url port".to_string()))?;
        }
        Ok(url)
    } else {
        Ok(Url::parse(&format!(
            "https://{}.snowflakecomputing.com",
            config.account
        ))?)
    }
}

fn base_login_request_data(username: &str, config: &SnowflakeClientConfig) -> Value {
    json!({
        "ACCOUNT_NAME": config.account,
        "LOGIN_NAME": username,
    })
}

/// Login to Snowflake and return a session token.
pub(crate) async fn login(
    http: &Client,
    username: &str,
    auth: &SnowflakeAuthMethod,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
) -> Result<String> {
    let base_url = get_base_url(config, connection_config)?;
    let url = base_url.join("session/v1/login-request")?;

    let mut queries: Vec<(&str, &str)> = vec![];
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
    #[cfg(feature = "external-browser-sso")]
    let is_externalbrowser = matches!(auth, SnowflakeAuthMethod::ExternalBrowser);
    #[cfg(not(feature = "external-browser-sso"))]
    let is_externalbrowser = false;
    let request_id = if is_externalbrowser {
        Some(Uuid::new_v4().to_string())
    } else {
        None
    };
    if let Some(id) = request_id.as_deref() {
        queries.push(("request_id", id));
    }

    let login_data = match auth {
        SnowflakeAuthMethod::Password(_)
        | SnowflakeAuthMethod::KeyPair { .. }
        | SnowflakeAuthMethod::Oauth { .. } => login_request_data(username, auth, config)?,
        #[cfg(feature = "external-browser-sso")]
        SnowflakeAuthMethod::ExternalBrowser => {
            let result =
                run_external_browser_flow(http, username, config, connection_config).await?;

            let mut data = base_login_request_data(username, config);
            if let Some(obj) = data.as_object_mut() {
                obj.insert("AUTHENTICATOR".to_string(), json!("EXTERNALBROWSER"));
                obj.insert("TOKEN".to_string(), json!(result.token));
                if let Some(proof_key) = result.proof_key {
                    obj.insert("PROOF_KEY".to_string(), json!(proof_key));
                }
            }
            data
        }
    };

    let mut request = http.post(url).query(&queries).json(&json!({
        "data": login_data
    }));
    if is_externalbrowser {
        let login_timeout = config
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(120));
        request = request
            .header(ACCEPT, "application/snowflake")
            .header(
                USER_AGENT,
                format!(
                    "{}/{}",
                    crate::auth::client::client_app_id(),
                    crate::auth::client::client_app_version()
                ),
            )
            .timeout(login_timeout);
    }

    let resp = request.send().await?;

    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(body));
    }

    let parsed: Response = serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;
    if !parsed.success {
        return Err(Error::Communication(parsed.message.unwrap_or_default()));
    }

    let data = parsed
        .data
        .ok_or_else(|| Error::Communication("missing login-response data".to_string()))?;

    Ok(data.token)
}

fn login_request_data(
    username: &str,
    auth: &SnowflakeAuthMethod,
    config: &SnowflakeClientConfig,
) -> Result<Value> {
    match auth {
        SnowflakeAuthMethod::Password(password) => {
            let mut data = base_login_request_data(username, config);
            if let Some(obj) = data.as_object_mut() {
                obj.insert("PASSWORD".to_string(), json!(password));
            }
            Ok(data)
        }
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
            let mut data = base_login_request_data(username, config);
            if let Some(obj) = data.as_object_mut() {
                obj.insert("TOKEN".to_string(), json!(jwt));
                obj.insert("AUTHENTICATOR".to_string(), json!("SNOWFLAKE_JWT"));
            }
            Ok(data)
        }
        SnowflakeAuthMethod::Oauth { token } => Ok(json!({
            "AUTHENTICATOR": "OAUTH",
            "TOKEN": token
        })),
        #[cfg(feature = "external-browser-sso")]
        SnowflakeAuthMethod::ExternalBrowser => Err(Error::UnsupportedFormat(
            "external browser flow should be handled upstream".into(),
        )),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponseData {
    token: String,
}

#[derive(serde::Deserialize)]
struct Response {
    data: Option<LoginResponseData>,
    message: Option<String>,
    success: bool,
}
