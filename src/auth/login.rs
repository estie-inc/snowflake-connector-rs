use chrono::Utc;
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Error, Result, SnowflakeAuthMethod, SnowflakeClientConfig};

use super::client::AUTH_REQUEST_TIMEOUT;
#[cfg(feature = "external-browser-sso")]
use super::external_browser::run_external_browser_flow;
use super::key_pair::generate_jwt_from_key_pair;

fn base_login_request_data(username: &str, account: &str) -> Value {
    json!({
        "ACCOUNT_NAME": account,
        "LOGIN_NAME": username,
    })
}

/// Login to Snowflake and return a session token.
pub(crate) async fn login(
    http: &Client,
    config: &SnowflakeClientConfig,
    base_url: &Url,
) -> Result<String> {
    let username = config.username();
    let account = config.account();
    let auth = config.auth();
    let session = config.session();

    let url = base_url.join("session/v1/login-request")?;

    let mut queries: Vec<(&str, &str)> = vec![];
    if let Some(warehouse) = session.warehouse() {
        queries.push(("warehouse", warehouse));
    }
    if let Some(database) = session.database() {
        queries.push(("databaseName", database));
    }
    if let Some(schema) = session.schema() {
        queries.push(("schemaName", schema));
    }
    if let Some(role) = session.role() {
        queries.push(("roleName", role));
    }
    #[cfg(feature = "external-browser-sso")]
    let is_externalbrowser = matches!(auth, SnowflakeAuthMethod::ExternalBrowser(_));
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

    let mut login_data = match auth {
        SnowflakeAuthMethod::Password(_)
        | SnowflakeAuthMethod::KeyPair { .. }
        | SnowflakeAuthMethod::KeyPairUnencrypted { .. }
        | SnowflakeAuthMethod::Oauth { .. } => login_request_data(username, account, auth)?,
        #[cfg(feature = "external-browser-sso")]
        SnowflakeAuthMethod::ExternalBrowser(external_browser_config) => {
            let result = run_external_browser_flow(
                http,
                username,
                account,
                base_url,
                external_browser_config,
            )
            .await?;

            let mut data = base_login_request_data(username, account);
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

    let session_parameters = session.session_parameters();
    if !session_parameters.is_empty()
        && let Some(obj) = login_data.as_object_mut()
    {
        obj.insert(
            "SESSION_PARAMETERS".to_string(),
            session_parameters
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect(),
        );
    }

    let mut request = http.post(url).query(&queries).json(&json!({
        "data": login_data
    }));
    request = request.timeout(AUTH_REQUEST_TIMEOUT);
    if is_externalbrowser {
        request = request.header(ACCEPT, "application/snowflake").header(
            USER_AGENT,
            format!(
                "{}/{}",
                crate::auth::client::client_app_id(),
                crate::auth::client::client_app_version()
            ),
        );
    }

    let resp = request.send().await?;

    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(format!("HTTP {status}: {body}")));
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

fn login_request_data(username: &str, account: &str, auth: &SnowflakeAuthMethod) -> Result<Value> {
    match auth {
        SnowflakeAuthMethod::Password(password) => {
            let mut data = base_login_request_data(username, account);
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
                Some(password),
                username,
                account,
                Utc::now().timestamp(),
            )?;
            let mut data = base_login_request_data(username, account);
            if let Some(obj) = data.as_object_mut() {
                obj.insert("TOKEN".to_string(), json!(jwt));
                obj.insert("AUTHENTICATOR".to_string(), json!("SNOWFLAKE_JWT"));
            }
            Ok(data)
        }
        SnowflakeAuthMethod::KeyPairUnencrypted { pem } => {
            let jwt = generate_jwt_from_key_pair(
                pem,
                Option::<Vec<u8>>::None,
                username,
                account,
                Utc::now().timestamp(),
            )?;
            let mut data = base_login_request_data(username, account);
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
        SnowflakeAuthMethod::ExternalBrowser(_) => Err(Error::UnsupportedFormat(
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
