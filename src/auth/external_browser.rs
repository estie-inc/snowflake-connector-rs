use std::io::{self, Write};
use std::time::Duration;

use reqwest::Client;
use reqwest::Url;
use serde::Deserialize;
use serde_json::json;
use tokio::time;

use super::login::get_base_url;
use crate::external_browser_launcher::{BrowserLauncher, LaunchOutcome, SystemCommandRunner};
use crate::external_browser_listener::{
    CallbackPayload, ListenerConfig, RunningListener, spawn_listener,
};
use crate::external_browser_payload::parse_token_and_consent_from_pairs;
use crate::{
    BrowserLaunchMode, Error, ExternalBrowserConfig, Result, SnowflakeClientConfig,
    SnowflakeConnectionConfig, WithCallbackListenerConfig, WithoutCallbackListenerConfig,
};

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
    external_browser_config: &ExternalBrowserConfig,
) -> Result<ExternalBrowserResult> {
    match external_browser_config {
        ExternalBrowserConfig::WithCallbackListener(config_with_listener) => {
            run_external_browser_flow_with_listener(
                http,
                username,
                config,
                connection_config,
                config_with_listener,
            )
            .await
        }
        ExternalBrowserConfig::WithoutCallbackListener(config_without_listener) => {
            run_external_browser_flow_without_listener(
                http,
                username,
                config,
                connection_config,
                config_without_listener,
            )
            .await
        }
    }
}

async fn run_external_browser_flow_with_listener(
    http: &Client,
    username: &str,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
    config_with_listener: &WithCallbackListenerConfig,
) -> Result<ExternalBrowserResult> {
    let listener_config = ListenerConfig {
        application: Some(super::client::client_app_id().to_string()),
        host: config_with_listener.callback_socket_addr(),
        port: config_with_listener.callback_socket_port(),
        protocol: "http".to_string(),
    };
    let listener = spawn_listener(listener_config)
        .await
        .map_err(|e| Error::Communication(e.to_string()))?;
    let redirect_port = listener.addr.port();

    let auth = match request_authenticator(http, username, config, connection_config, redirect_port)
        .await
    {
        Ok(data) => data,
        Err(err) => {
            shutdown_listener(listener).await;
            return Err(err);
        }
    };

    let launch_mode = config_with_listener.browser_launch_mode();
    if let Err(err) = open_auth_page(&auth.sso_url, launch_mode) {
        shutdown_listener(listener).await;
        return Err(err);
    }

    let timeout = config.timeout.unwrap_or_else(|| Duration::from_secs(60));
    let payload = resolve_payload_with_listener(listener, timeout).await?;
    Ok(ExternalBrowserResult {
        token: payload.token,
        proof_key: auth.proof_key,
    })
}

async fn run_external_browser_flow_without_listener(
    http: &Client,
    username: &str,
    config: &SnowflakeClientConfig,
    connection_config: &Option<SnowflakeConnectionConfig>,
    config_without_listener: &WithoutCallbackListenerConfig,
) -> Result<ExternalBrowserResult> {
    let redirect_port = config_without_listener.redirect_port().get();
    let auth =
        request_authenticator(http, username, config, connection_config, redirect_port).await?;
    let launch_mode = config_without_listener.browser_launch_mode();
    open_auth_page(&auth.sso_url, launch_mode)?;

    let payload = resolve_payload_without_listener(redirect_port).await?;
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

fn open_auth_page(sso_url: &str, mode: BrowserLaunchMode) -> Result<()> {
    match mode {
        BrowserLaunchMode::Manual => {
            eprintln!(
                "{}",
                BrowserLauncher::<SystemCommandRunner>::manual_open_message(sso_url)
            );
            Ok(())
        }
        BrowserLaunchMode::Auto => {
            let outcome = BrowserLauncher::new()
                .open(sso_url)
                .map_err(|err| Error::Communication(format!("failed to open browser: {err}")))?;

            if let LaunchOutcome::ManualOpen { url } = outcome {
                eprintln!(
                    "{}",
                    BrowserLauncher::<SystemCommandRunner>::manual_open_message(&url)
                );
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallbackWaitError {
    TimedOut,
    ListenerStopped,
}

async fn resolve_payload_with_listener(
    listener: RunningListener,
    timeout: Duration,
) -> Result<CallbackPayload> {
    let callback_result = wait_for_token(listener.payloads.clone(), timeout).await;
    shutdown_listener(listener).await;

    match callback_result {
        Ok(payload) => Ok(payload),
        Err(CallbackWaitError::TimedOut) => {
            eprintln!("Callback was not received in time. Falling back to manual URL input.");
            manual_token_flow().await
        }
        Err(CallbackWaitError::ListenerStopped) => {
            eprintln!(
                "Local callback listener stopped before receiving token. Continue with manual URL input."
            );
            manual_token_flow().await
        }
    }
}

async fn resolve_payload_without_listener(redirect_port: u16) -> Result<CallbackPayload> {
    eprintln!(
        "The browser may display a connection error page for localhost:{redirect_port}; this is expected in this mode."
    );
    manual_token_flow().await
}

async fn wait_for_token(
    rx: tokio::sync::watch::Receiver<Option<CallbackPayload>>,
    timeout: Duration,
) -> std::result::Result<CallbackPayload, CallbackWaitError> {
    match time::timeout(timeout, wait_for_token_inner(rx)).await {
        Ok(res) => res,
        Err(_) => Err(CallbackWaitError::TimedOut),
    }
}

async fn wait_for_token_inner(
    rx: tokio::sync::watch::Receiver<Option<CallbackPayload>>,
) -> std::result::Result<CallbackPayload, CallbackWaitError> {
    let mut rx = rx.clone();
    if let Some(payload) = rx.borrow().clone() {
        return Ok(payload);
    }

    loop {
        if rx.changed().await.is_err() {
            return Err(CallbackWaitError::ListenerStopped);
        }
        if let Some(payload) = rx.borrow().clone() {
            return Ok(payload);
        }
    }
}

async fn shutdown_listener(listener: RunningListener) {
    let _ = listener.shutdown.send(());
    let _ = listener.handle.await;
}

async fn manual_token_flow() -> Result<CallbackPayload> {
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
    let query = parse_token_and_consent_from_pairs(parsed.query_pairs());
    let fragment = parsed
        .fragment()
        .map(|frag| {
            parse_token_and_consent_from_pairs(url::form_urlencoded::parse(frag.as_bytes()))
        })
        .unwrap_or_default();

    let token = query.token.or(fragment.token)?;
    let consent = query.consent.or(fragment.consent);

    Some(CallbackPayload { token, consent })
}

fn payload_from_redirect_input(input: &str) -> Result<CallbackPayload> {
    extract_payload_from_url(input).ok_or_else(|| {
        Error::Communication(
            "Unable to extract token from redirected URL (expected query or fragment token=...)"
                .to_string(),
        )
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
    fn payload_from_redirect_input_extracts_token_from_fragment() {
        let url = "https://example.test/callback#token=abc123&consent=true";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, Some(true));
    }

    #[test]
    fn payload_from_redirect_input_prefers_query_token_over_fragment_token() {
        let url = "https://example.test/callback?token=query_token#token=fragment_token";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "query_token");
    }

    #[test]
    fn payload_from_redirect_input_keeps_query_token_and_query_consent() {
        let url = "https://example.test/callback?token=query_token&consent=false#consent=true";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "query_token");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn payload_from_redirect_input_fails_without_token() {
        let url = "https://example.test/callback?missing=true";
        let err = payload_from_redirect_input(url).unwrap_err();
        assert!(format!("{err}").contains("Unable to extract token"));
    }
}
