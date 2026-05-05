use chrono::Utc;
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    Result, SnowflakeAuthMethod, SnowflakeClientConfig,
    error::{AuthError, ConfigError, NetworkError, ProtocolError},
};

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

    let url = base_url
        .join("session/v1/login-request")
        .map_err(|e| ConfigError::invalid_url(e.to_string()))?;

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

    let resp = request.send().await.map_err(NetworkError::request)?;

    let status = resp.status();
    let body = resp.text().await.map_err(NetworkError::request)?;
    if !status.is_success() {
        return Err(NetworkError::http_status(status.as_u16(), body.as_bytes()).into());
    }

    let token = parse_login_response(&body)?;

    Ok(token)
}

fn parse_login_response(body: &str) -> Result<String> {
    let parsed: Response =
        serde_json::from_str(body).map_err(|e| ProtocolError::json_parse(e, body))?;
    if !parsed.success {
        return Err(AuthError::login_rejected(parsed.message).into());
    }

    let data = parsed
        .data
        .ok_or_else(|| ProtocolError::missing_field("data"))?;

    data.token
        .ok_or_else(|| ProtocolError::missing_field("data.token").into())
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
        SnowflakeAuthMethod::ExternalBrowser(_) => unreachable!("handled in caller"),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponseData {
    token: Option<String>,
}

#[derive(serde::Deserialize)]
struct Response {
    data: Option<LoginResponseData>,
    message: Option<String>,
    success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn parse_login_response_missing_data_is_protocol_error() {
        let err = parse_login_response(r#"{"success":true}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.snowflake_message(), None);
    }

    #[test]
    fn parse_login_response_missing_token_is_protocol_error() {
        let err = parse_login_response(r#"{"success":true,"data":{}}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.token"
        );
    }

    #[test]
    fn parse_login_response_rejected_keeps_auth_classification() {
        let err =
            parse_login_response(r#"{"success":false,"message":"bad credentials"}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), Some("bad credentials"));
    }

    #[test]
    fn parse_login_response_rejected_without_message_preserves_absence() {
        let err = parse_login_response(r#"{"success":false}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), None);
        assert_eq!(err.to_string(), "authentication rejected");
    }
}
