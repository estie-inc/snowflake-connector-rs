use std::{future::Future, time::Duration};

use url::Url;

use crate::{
    Result,
    auth::{api::AuthApiClient, credential::LoginContext, wire::AuthenticatorRequest},
    error::{AuthError, TimeoutError},
};

use super::{
    config::{
        BrowserLaunchMode, CallbackListenerConfig, ExternalBrowserConfig, ExternalBrowserMode,
        ManualRedirectConfig,
    },
    launcher::{BrowserLauncher, LaunchOutcome, SystemCommandRunner},
    listener::{CallbackWaitError, ListenerConfig, RunningListener, spawn_listener},
    manual_redirect_input,
    payload::BrowserCallbackPayload,
};

const EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalBrowserCredential {
    pub(crate) token: String,
    pub(crate) proof_key: Option<String>,
}

pub(crate) async fn acquire_external_browser_credential(
    client: &AuthApiClient,
    context: LoginContext<'_>,
    config: &ExternalBrowserConfig,
) -> Result<ExternalBrowserCredential> {
    match config.mode() {
        ExternalBrowserMode::CallbackListener(config) => {
            run_with_listener(client, context, config).await
        }
        ExternalBrowserMode::ManualRedirect(config) => {
            run_without_listener(client, context, config).await
        }
    }
}

async fn run_with_listener(
    client: &AuthApiClient,
    context: LoginContext<'_>,
    config: &CallbackListenerConfig,
) -> Result<ExternalBrowserCredential> {
    let listener = spawn_listener(ListenerConfig::new(
        Some(env!("CARGO_PKG_NAME").to_string()),
        config.callback_socket_addr(),
        config.callback_socket_port(),
        "http".to_string(),
    ))
    .await
    .map_err(|e| AuthError::external_browser_with_source("failed to spawn callback listener", e))?;

    let redirect_port = listener.redirect_port();

    let challenge = match client
        .request_external_browser_challenge(AuthenticatorRequest {
            account_name: context.account,
            login_name: context.username,
            redirect_port,
        })
        .await
    {
        Ok(challenge) => challenge,
        Err(err) => {
            listener.shutdown().await;
            return Err(err);
        }
    };

    if let Err(err) = open_auth_page(&challenge.sso_url, config.browser_launch_mode()) {
        listener.shutdown().await;
        return Err(err);
    }

    let payload = resolve_payload_with_listener_with_fallback(
        listener,
        EXTERNAL_BROWSER_CALLBACK_WAIT_TIMEOUT,
        manual_redirect_input::manual_token_flow,
    )
    .await?;

    Ok(ExternalBrowserCredential {
        token: payload.token,
        proof_key: challenge.proof_key,
    })
}

async fn run_without_listener(
    client: &AuthApiClient,
    context: LoginContext<'_>,
    config: &ManualRedirectConfig,
) -> Result<ExternalBrowserCredential> {
    let redirect_port = config.redirect_port().get();
    let challenge = client
        .request_external_browser_challenge(AuthenticatorRequest {
            account_name: context.account,
            login_name: context.username,
            redirect_port,
        })
        .await?;

    open_auth_page(&challenge.sso_url, config.browser_launch_mode())?;
    eprintln!(
        "The browser may display a connection error page for localhost:{redirect_port}; this is expected in this mode."
    );

    let payload = manual_redirect_input::manual_token_flow().await?;
    Ok(ExternalBrowserCredential {
        token: payload.token,
        proof_key: challenge.proof_key,
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
    use crate::auth::external_browser::listener::spawn_listener;

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
