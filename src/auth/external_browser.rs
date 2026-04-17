use std::time::Duration;

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

use crate::auth::client::AUTH_REQUEST_TIMEOUT;
use crate::{Error, Result};

use launcher::{BrowserLauncher, LaunchOutcome, SystemCommandRunner};
use listener::{CallbackWaitError, ListenerConfig, RunningListener, spawn_listener};
use payload::BrowserCallbackPayload;

pub use config::{
    BrowserLaunchMode, ExternalBrowserConfig, WithCallbackListenerConfig,
    WithoutCallbackListenerConfig,
};

const EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

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
    let listener = spawn_listener(listener_config)
        .await
        .map_err(|e| Error::Communication(e.to_string()))?;
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

    let payload =
        resolve_payload_with_listener(listener, EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT).await?;
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
    let url = base_url.join("session/authenticator-request")?;

    let body = challenge_request_body(username, account, redirect_port);

    let resp = http
        .post(url)
        .timeout(AUTH_REQUEST_TIMEOUT)
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(format!("HTTP {status}: {text}")));
    }

    let parsed: ExternalBrowserChallengeResponse =
        serde_json::from_str(&text).map_err(|e| Error::Json(e, text.clone()))?;
    if !parsed.success {
        return Err(Error::Communication(parsed.message.unwrap_or_default()));
    }

    let data = parsed
        .data
        .ok_or_else(|| Error::Communication("missing challenge response data".to_string()))?;
    Ok(data)
}

#[derive(Debug, Deserialize)]
struct ExternalBrowserChallenge {
    #[serde(rename = "ssoUrl")]
    sso_url: String,
    #[serde(rename = "proofKey")]
    proof_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExternalBrowserChallengeResponse {
    data: Option<ExternalBrowserChallenge>,
    message: Option<String>,
    success: bool,
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

async fn resolve_payload_with_listener(
    mut listener: RunningListener,
    timeout: Duration,
) -> Result<BrowserCallbackPayload> {
    let callback_result = listener.wait_for_payload(timeout).await;
    listener.shutdown().await;

    match callback_result {
        Ok(payload) => Ok(payload),
        Err(CallbackWaitError::TimedOut) => {
            eprintln!("Callback was not received in time. Falling back to manual URL input.");
            manual_redirect_input::manual_token_flow().await
        }
        Err(CallbackWaitError::ListenerStopped) => {
            eprintln!(
                "Local callback listener stopped before receiving token. Continue with manual URL input."
            );
            manual_redirect_input::manual_token_flow().await
        }
    }
}
