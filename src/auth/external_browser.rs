use std::{future::Future, time::Duration};

use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::json;

mod config;
mod launcher;
mod listener;
#[cfg(unix)]
mod manual_input_unix;
mod manual_redirect_input;
mod payload;

use crate::{
    Result,
    auth::client::AUTH_REQUEST_TIMEOUT,
    error::{AuthError, ConfigError, NetworkError, ProtocolError, TimeoutError},
};

use launcher::{BrowserLauncher, LaunchOutcome, SystemCommandRunner};
use listener::{CallbackWaitError, ListenerConfig, RunningListener, spawn_listener};
use payload::BrowserCallbackPayload;

pub use config::{
    BrowserLaunchMode, ExternalBrowserConfig, WithCallbackListenerConfig,
    WithoutCallbackListenerConfig,
};

const EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const ALLOWED_BROWSER_URL_SCHEMES: &[&str] = &["http", "https"];

pub(crate) struct ExternalBrowserResult {
    pub(crate) token: String,
    pub(crate) proof_key: Option<String>,
}

impl ExternalBrowserResult {
    pub(crate) fn new(token: String, proof_key: Option<String>) -> Self {
        Self { token, proof_key }
    }
}

pub(in crate::auth) async fn run_external_browser_flow(
    http: &Client,
    username: &str,
    account: &str,
    base_url: &Url,
    external_browser_config: &ExternalBrowserConfig,
) -> Result<ExternalBrowserResult> {
    match external_browser_config {
        ExternalBrowserConfig::WithCallbackListener(config_with_listener) => {
            run_external_browser_flow_with_listener(
                http,
                username,
                account,
                base_url,
                config_with_listener,
            )
            .await
        }
        ExternalBrowserConfig::WithoutCallbackListener(config_without_listener) => {
            run_external_browser_flow_without_listener(
                http,
                username,
                account,
                base_url,
                config_without_listener,
            )
            .await
        }
    }
}

async fn run_external_browser_flow_with_listener(
    http: &Client,
    username: &str,
    account: &str,
    base_url: &Url,
    config_with_listener: &WithCallbackListenerConfig,
) -> Result<ExternalBrowserResult> {
    let listener_config = ListenerConfig::new(
        Some(super::client::client_app_id().to_string()),
        config_with_listener.callback_socket_addr(),
        config_with_listener.callback_socket_port(),
        "http".to_string(),
    );
    let listener = spawn_listener(listener_config).await.map_err(|e| {
        AuthError::external_browser_with_source("failed to spawn callback listener", e)
    })?;
    let redirect_port = listener.redirect_port();

    let auth =
        match request_external_browser_challenge(http, username, account, base_url, redirect_port)
            .await
        {
            Ok(data) => data,
            Err(err) => {
                listener.shutdown().await;
                return Err(err);
            }
        };

    let launch_mode = config_with_listener.browser_launch_mode();
    if let Err(err) = open_auth_page(&auth.sso_url, launch_mode) {
        listener.shutdown().await;
        return Err(err);
    }

    let payload = resolve_payload_with_listener_with_fallback(
        listener,
        EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT,
        manual_redirect_input::manual_token_flow,
    )
    .await?;
    Ok(ExternalBrowserResult::new(payload.token, auth.proof_key))
}

async fn run_external_browser_flow_without_listener(
    http: &Client,
    username: &str,
    account: &str,
    base_url: &Url,
    config_without_listener: &WithoutCallbackListenerConfig,
) -> Result<ExternalBrowserResult> {
    let redirect_port = config_without_listener.redirect_port().get();
    let auth = request_external_browser_challenge(http, username, account, base_url, redirect_port)
        .await?;
    let launch_mode = config_without_listener.browser_launch_mode();
    open_auth_page(&auth.sso_url, launch_mode)?;

    eprintln!(
        "The browser may display a connection error page for localhost:{redirect_port}; this is expected in this mode."
    );
    let payload = manual_redirect_input::manual_token_flow().await?;
    Ok(ExternalBrowserResult {
        token: payload.token,
        proof_key: auth.proof_key,
    })
}

async fn request_external_browser_challenge(
    http: &Client,
    username: &str,
    account: &str,
    base_url: &Url,
    redirect_port: u16,
) -> Result<ExternalBrowserChallenge> {
    let url = base_url
        .join("session/authenticator-request")
        .map_err(|e| ConfigError::invalid_url(e.to_string()))?;

    let body = challenge_request_body(username, account, redirect_port);

    let resp = http
        .post(url)
        .timeout(AUTH_REQUEST_TIMEOUT)
        .json(&body)
        .send()
        .await
        .map_err(NetworkError::request)?;
    let status = resp.status();
    let text = resp.text().await.map_err(NetworkError::request)?;
    if !status.is_success() {
        return Err(NetworkError::http_status(status.as_u16(), text.as_bytes()).into());
    }

    parse_external_browser_challenge_response(&text)
}

#[derive(Debug)]
struct ExternalBrowserChallenge {
    sso_url: Url,
    proof_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawExternalBrowserChallenge {
    #[serde(rename = "ssoUrl")]
    sso_url: Option<String>,
    #[serde(rename = "proofKey")]
    proof_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExternalBrowserChallengeResponse {
    data: Option<RawExternalBrowserChallenge>,
    message: Option<String>,
    success: bool,
}

fn parse_external_browser_challenge_response(body: &str) -> Result<ExternalBrowserChallenge> {
    let parsed: ExternalBrowserChallengeResponse =
        serde_json::from_str(body).map_err(|e| ProtocolError::json_parse(e, body))?;
    if !parsed.success {
        return Err(AuthError::login_rejected(parsed.message).into());
    }

    let data = parsed
        .data
        .ok_or_else(|| ProtocolError::missing_field("data"))?;

    let sso_url = data
        .sso_url
        .ok_or_else(|| ProtocolError::missing_field("data.ssoUrl"))?;

    if sso_url.trim().is_empty() {
        return Err(ProtocolError::invalid_field("data.ssoUrl", "must not be empty").into());
    }

    let sso_url = Url::parse(&sso_url)
        .map_err(|e| ProtocolError::invalid_response_url("data.ssoUrl", &sso_url, e))?;

    if !ALLOWED_BROWSER_URL_SCHEMES.contains(&sso_url.scheme()) {
        return Err(ProtocolError::invalid_field(
            "data.ssoUrl",
            format!(
                "unsupported URL scheme '{}' (expected one of: {})",
                sso_url.scheme(),
                ALLOWED_BROWSER_URL_SCHEMES.join(", ")
            ),
        )
        .into());
    }

    Ok(ExternalBrowserChallenge {
        sso_url,
        proof_key: data.proof_key,
    })
}

fn challenge_request_body(username: &str, account: &str, redirect_port: u16) -> serde_json::Value {
    json!({
        "data": {
            "ACCOUNT_NAME": account,
            "LOGIN_NAME": username,
            "CLIENT_ENVIRONMENT": super::client::client_environment(),
            "AUTHENTICATOR": "EXTERNALBROWSER",
            "BROWSER_MODE_REDIRECT_PORT": redirect_port.to_string(),
        }
    })
}

fn open_auth_page(sso_url: &Url, mode: BrowserLaunchMode) -> Result<()> {
    match mode {
        BrowserLaunchMode::Manual => {
            eprintln!(
                "{}",
                BrowserLauncher::<SystemCommandRunner>::manual_open_message(sso_url.as_str())
            );
            Ok(())
        }
        BrowserLaunchMode::Auto => {
            let outcome = BrowserLauncher::new()
                .open(sso_url.as_str())
                .map_err(|err| {
                    AuthError::external_browser_with_source("failed to open browser", err)
                })?;

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

async fn resolve_payload_with_listener_with_fallback<F, Fut>(
    mut listener: RunningListener,
    timeout: Duration,
    manual_fallback: F,
) -> Result<BrowserCallbackPayload>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<BrowserCallbackPayload>>,
{
    let callback_result = listener.wait_for_payload(timeout).await;
    listener.shutdown().await;
    let mut manual_fallback = Some(manual_fallback);

    match callback_result {
        Ok(payload) => Ok(payload),
        Err(CallbackWaitError::TimedOut) => {
            let timeout_error: crate::Error = TimeoutError::browser_callback().into();
            eprintln!("{timeout_error}. Continue with manual URL input.");
            manual_fallback
                .take()
                .expect("manual fallback is available")()
            .await
        }
        Err(CallbackWaitError::ListenerStopped) => {
            eprintln!(
                "Local callback listener stopped before receiving token. Continue with manual URL input."
            );
            manual_fallback
                .take()
                .expect("manual fallback is available")()
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, time::Duration};

    use super::*;
    use crate::ErrorKind;

    #[test]
    fn parse_external_browser_challenge_missing_data_is_protocol_error() {
        let err = parse_external_browser_challenge_response(r#"{"success":true}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.snowflake_message(), None);
    }

    #[test]
    fn parse_external_browser_challenge_missing_sso_url_is_protocol_error() {
        let err =
            parse_external_browser_challenge_response(r#"{"success":true,"data":{}}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.ssoUrl"
        );
    }

    #[test]
    fn parse_external_browser_challenge_empty_sso_url_is_protocol_error() {
        let err = parse_external_browser_challenge_response(
            r#"{"success":true,"data":{"ssoUrl":"   "}}"#,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.ssoUrl: must not be empty"
        );
    }

    #[test]
    fn parse_external_browser_challenge_invalid_sso_url_is_protocol_error() {
        let err = parse_external_browser_challenge_response(
            r#"{"success":true,"data":{"ssoUrl":"http://[::1"}}"#,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(err.to_string().contains("data.ssoUrl"));
    }

    #[test]
    fn parse_external_browser_challenge_rejects_non_http_schemes() {
        for raw in ["javascript:alert(1)", "file:///tmp/token.txt"] {
            let body = format!(r#"{{"success":true,"data":{{"ssoUrl":"{raw}"}}}}"#);
            let err = parse_external_browser_challenge_response(&body)
                .expect_err("non-http scheme must fail");

            assert_eq!(err.kind(), ErrorKind::Protocol);
            assert!(err.to_string().contains("unsupported URL scheme"));
        }
    }

    #[test]
    fn parse_external_browser_challenge_rejected_without_message_preserves_absence() {
        let err = parse_external_browser_challenge_response(r#"{"success":false}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), None);
        assert_eq!(err.to_string(), "authentication rejected");
    }

    #[tokio::test]
    async fn callback_wait_timeout_falls_back_to_manual_input() {
        let listener = spawn_listener(ListenerConfig::new(
            Some("test-app".to_string()),
            Ipv4Addr::LOCALHOST.into(),
            0,
            "http".to_string(),
        ))
        .await
        .unwrap();

        let expected = BrowserCallbackPayload {
            token: "fallback-token".to_string(),
            consent: Some(true),
        };
        let payload =
            resolve_payload_with_listener_with_fallback(listener, Duration::from_millis(10), {
                let expected = expected.clone();
                move || async move { Ok(expected) }
            })
            .await
            .unwrap();

        assert_eq!(payload, expected);
    }
}
