use std::env;
use std::io::{self, Write};
use std::time::Duration;

use reqwest::Client;
use reqwest::Url;
use serde::Deserialize;
use serde_json::json;
use tokio::time;

use crate::external_browser_launcher::{BrowserLauncher, LaunchOutcome, SystemCommandRunner};
use crate::external_browser_listener::{CallbackPayload, ListenerConfig, spawn_listener};

use super::login::get_base_url;
use crate::{Error, Result, SnowflakeClientConfig, SnowflakeConnectionConfig};

#[cfg(unix)]
mod manual_input_unix;

pub struct ExternalBrowserResult {
    pub token: String,
    pub proof_key: Option<String>,
}

pub async fn run_external_browser_flow(
    http: &Client,
    username: &str,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
) -> Result<ExternalBrowserResult> {
    let listener_config = listener_config_from_env()?;
    let listener = spawn_listener(listener_config)
        .await
        .map_err(|e| Error::Communication(e.to_string()))?;
    let redirect_port = listener.addr.port();

    let auth = match request_authenticator(http, username, config, connection_config, redirect_port)
        .await
    {
        Ok(data) => data,
        Err(err) => {
            let _ = listener.shutdown.send(());
            let _ = listener.handle.await;
            return Err(err);
        }
    };

    let launcher = BrowserLauncher::new();
    let timeout = config.timeout.unwrap_or_else(|| Duration::from_secs(60));
    let payload_result: Result<CallbackPayload> = match launcher.open(&auth.sso_url) {
        Ok(LaunchOutcome::Opened) => wait_for_token(listener.payloads, timeout).await,
        Ok(LaunchOutcome::ManualOpen { url }) => manual_token_flow(&url).await,
        Err(err) => Err(Error::Communication(format!(
            "failed to open browser: {err}"
        ))),
    };

    let _ = listener.shutdown.send(());
    let _ = listener.handle.await;

    let payload = payload_result?;
    Ok(ExternalBrowserResult {
        token: payload.token,
        proof_key: auth.proof_key,
    })
}

async fn request_authenticator(
    http: &Client,
    username: &str,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
    redirect_port: u16,
) -> Result<AuthenticatorData> {
    let base_url = get_base_url(config, connection_config)?;
    let url = base_url.join("session/authenticator-request")?;

    let body = authenticator_request_body(username, config, redirect_port);

    let resp = http.post(url).json(&body).send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(text));
    }

    let parsed: AuthenticatorResponse =
        serde_json::from_str(&text).map_err(|e| Error::Json(e, text.clone()))?;
    if !parsed.success {
        return Err(Error::Communication(parsed.message.unwrap_or_default()));
    }

    let data = parsed
        .data
        .ok_or_else(|| Error::Communication("missing authenticator-response data".to_string()))?;
    Ok(data)
}

async fn wait_for_token(
    rx: tokio::sync::watch::Receiver<Option<CallbackPayload>>,
    timeout: Duration,
) -> Result<CallbackPayload> {
    let mut rx = rx.clone();
    if let Some(payload) = rx.borrow().clone() {
        return Ok(payload);
    }

    let fut = async move {
        loop {
            if rx.changed().await.is_err() {
                return Err(Error::Communication(
                    "listener stopped before receiving token".into(),
                ));
            }
            if let Some(payload) = rx.borrow().clone() {
                return Ok(payload);
            }
        }
    };

    match time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_) => Err(Error::TimedOut),
    }
}

#[derive(Debug, Deserialize)]
struct AuthenticatorData {
    #[serde(rename = "ssoUrl")]
    sso_url: String,
    #[serde(rename = "proofKey")]
    proof_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthenticatorResponse {
    data: Option<AuthenticatorData>,
    message: Option<String>,
    success: bool,
}

fn authenticator_request_body(
    username: &str,
    config: &SnowflakeClientConfig,
    redirect_port: u16,
) -> serde_json::Value {
    json!({
        "data": {
            "ACCOUNT_NAME": config.account,
            "LOGIN_NAME": username,
            "CLIENT_ENVIRONMENT": super::client::client_environment(config.timeout),
            "AUTHENTICATOR": "EXTERNALBROWSER",
            "BROWSER_MODE_REDIRECT_PORT": redirect_port.to_string(),
        }
    })
}

fn listener_config_from_env() -> Result<ListenerConfig> {
    let host = env::var("SF_AUTH_SOCKET_ADDR").unwrap_or_else(|_| "localhost".to_string());
    // Normalize "localhost" to "127.0.0.1" to ensure IPv4 binding.
    // This avoids issues where `localhost` resolves to `::1` (IPv6) first,
    // causing the listener to bind to IPv6 while the browser redirects to IPv4.
    let host = if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let port = match env::var("SF_AUTH_SOCKET_PORT") {
        Ok(val) => val.parse().map_err(|_| {
            Error::Communication("SF_AUTH_SOCKET_PORT must be a valid u16".to_string())
        })?,
        Err(_) => 0,
    };

    Ok(ListenerConfig {
        application: Some(super::client::client_app_id().to_string()),
        host,
        port,
        protocol: "http".to_string(),
    })
}

async fn manual_token_flow(sso_url: &str) -> Result<CallbackPayload> {
    eprintln!(
        "{}",
        BrowserLauncher::<SystemCommandRunner>::manual_open_message(sso_url)
    );
    tokio::task::spawn_blocking(manual_token_flow_blocking)
        .await
        .map_err(|e| Error::Communication(format!("manual input task failed: {e}")))?
}

fn manual_token_flow_blocking() -> Result<CallbackPayload> {
    eprintln!(
        "After completing authentication, paste the URL you were redirected to (not logged)."
    );

    eprint!("Redirected URL: ");
    let _ = io::stderr().flush();
    let input = read_redirected_url_line()?;

    eprintln!("Received redirected URL input. Continuing authentication...");

    payload_from_redirect_input(input.trim())
}

fn read_redirected_url_line() -> Result<String> {
    #[cfg(unix)]
    if let Some(result) = manual_input_unix::try_read_redirected_url_line_noncanonical() {
        return result;
    }

    // TODO(windows): investigate whether long redirected URLs can be truncated in
    // canonical console input mode and add a Windows-specific raw/non-canonical fallback.

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::Communication(format!("failed to read input: {e}")))?;

    validate_redirected_url_input(input)
}

fn validate_redirected_url_input(input: String) -> Result<String> {
    if input.trim().is_empty() {
        return Err(Error::Communication(
            "No redirected URL was provided".to_string(),
        ));
    }
    Ok(input)
}

fn extract_payload_from_url(url: &str) -> Option<CallbackPayload> {
    let parsed = Url::parse(url).ok()?;
    let mut token: Option<String> = None;
    let mut consent: Option<bool> = None;
    for (k, v) in parsed.query_pairs() {
        if k.eq_ignore_ascii_case("token") {
            if !v.is_empty() {
                token = Some(v.into_owned());
            }
        } else if k.eq_ignore_ascii_case("consent") {
            let val = v.trim();
            if val.eq_ignore_ascii_case("true") {
                consent = Some(true);
            } else if val.eq_ignore_ascii_case("false") {
                consent = Some(false);
            }
        }
    }

    Some(CallbackPayload {
        token: token?,
        consent,
    })
}

fn payload_from_redirect_input(input: &str) -> Result<CallbackPayload> {
    extract_payload_from_url(input).ok_or_else(|| {
        Error::Communication("Unable to extract token from redirected URL".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::payload_from_redirect_input;

    #[test]
    fn payload_from_redirect_input_extracts_token() {
        let url = "https://example.test/callback?token=abc123&other=1";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, None);
    }

    #[test]
    fn payload_from_redirect_input_extracts_consent() {
        let url = "https://example.test/callback?token=abc123&consent=false";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn payload_from_redirect_input_fails_without_token() {
        let url = "https://example.test/callback?missing=true";
        let err = payload_from_redirect_input(url).unwrap_err();
        assert!(format!("{err}").contains("Unable to extract token"));
    }
}
