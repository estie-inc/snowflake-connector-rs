use std::{sync::Arc, time::Duration};

use http::Method;
use reqwest::{Url, header::ACCEPT};

use crate::{
    ApiContext, Result,
    error::{NetworkError, classify_request_error},
};

#[cfg(feature = "external-browser-sso")]
use super::wire::{
    AuthenticatorRequest, ClientEnvironment, ExternalBrowserChallenge, parse_authenticator_response,
};
use super::wire::{LoginRequest, LoginSession, parse_login_response};

const AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const LOGIN_REQUEST_ACCEPT: &str = "application/snowflake";
#[cfg(feature = "external-browser-sso")]
const AUTHENTICATOR_REQUEST_ACCEPT: &str = "application/json";

#[derive(Clone)]
pub(crate) struct AuthApiClient {
    api: Arc<ApiContext>,
    request_timeout: Duration,
}

impl AuthApiClient {
    pub(crate) fn new(api: Arc<ApiContext>) -> Self {
        Self {
            api,
            request_timeout: AUTH_REQUEST_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_request_timeout(api: Arc<ApiContext>, request_timeout: Duration) -> Self {
        Self {
            api,
            request_timeout,
        }
    }

    pub(crate) async fn login(&self, request: LoginRequest<'_>) -> Result<LoginSession> {
        let url = self.api.resolve("session/v1/login-request")?;

        let response = self
            .post(url, LOGIN_REQUEST_ACCEPT)
            .query(&request.query)
            .json(&request.body)
            .send()
            .await
            .map_err(classify_request_error)?;

        let status = response.status();
        let body = response.text().await.map_err(classify_request_error)?;
        if !status.is_success() {
            return Err(NetworkError::http_status(status.as_u16(), body.as_bytes()).into());
        }

        parse_login_response(&body)
    }

    #[cfg(feature = "external-browser-sso")]
    pub(crate) async fn request_external_browser_challenge(
        &self,
        request: AuthenticatorRequest<'_>,
    ) -> Result<ExternalBrowserChallenge> {
        let url = self.api.resolve("session/authenticator-request")?;

        let body = request.into_body(ClientEnvironment::auth_defaults(self.request_timeout));
        let response = self
            .post(url, AUTHENTICATOR_REQUEST_ACCEPT)
            .json(&body)
            .send()
            .await
            .map_err(classify_request_error)?;

        let status = response.status();
        let body = response.text().await.map_err(classify_request_error)?;
        if !status.is_success() {
            return Err(NetworkError::http_status(status.as_u16(), body.as_bytes()).into());
        }

        parse_authenticator_response(&body)
    }

    fn post(&self, url: Url, accept: &'static str) -> reqwest::RequestBuilder {
        self.api
            .request(Method::POST, url)
            .header(ACCEPT, accept)
            .timeout(self.request_timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use http::StatusCode;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        ErrorKind,
        api_context::DEFAULT_USER_AGENT,
        auth::wire::{LoginBody, LoginCredentialWire, LoginData, LoginQuery, LoginRequest},
        test_support::http::{base_url, read_http_request, write_json_response},
    };

    #[cfg(feature = "external-browser-sso")]
    use crate::auth::wire::AuthenticatorRequest;

    fn test_api_context(http: reqwest::Client, base_url: Url) -> Arc<ApiContext> {
        Arc::new(ApiContext::new(http, base_url))
    }

    fn auth_client(addr: SocketAddr) -> AuthApiClient {
        AuthApiClient::new(test_api_context(reqwest::Client::new(), base_url(addr)))
    }

    fn sample_login_request<'a>() -> LoginRequest<'a> {
        LoginRequest {
            query: LoginQuery {
                warehouse: Some("warehouse"),
                database_name: Some("database"),
                schema_name: Some("schema"),
                role_name: Some("role"),
                request_id: None,
            },
            body: LoginBody {
                data: LoginData {
                    account_name: "account",
                    login_name: "username",
                    credential: LoginCredentialWire::Password {
                        password: "secret",
                        passcode: None,
                    },
                    session_parameters: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn login_posts_wire_request_and_parses_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await.unwrap();
            assert!(
                request.starts_with(
                    "POST /session/v1/login-request?warehouse=warehouse&databaseName=database&schemaName=schema&roleName=role HTTP/1.1"
                ),
                "{request}"
            );

            let lowered = request.to_ascii_lowercase();
            assert!(lowered.contains("\r\naccept: application/snowflake\r\n"));
            assert!(lowered.contains(&format!(
                "\r\nuser-agent: {}\r\n",
                DEFAULT_USER_AGENT.to_ascii_lowercase()
            )));

            let body = request
                .split("\r\n\r\n")
                .nth(1)
                .expect("request must contain body");
            let body: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                body,
                json!({
                    "data": {
                        "ACCOUNT_NAME": "account",
                        "LOGIN_NAME": "username",
                        "PASSWORD": "secret",
                    }
                })
            );

            write_json_response(
                &mut socket,
                StatusCode::OK,
                r#"{"success":true,"data":{"token":"session-token"}}"#,
            )
            .await
            .unwrap();
        });

        let client = auth_client(addr);
        let session = client.login(sample_login_request()).await.unwrap();

        assert_eq!(session.token, "session-token");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn login_non_success_status_is_network_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut socket).await.unwrap();
            write_json_response(
                &mut socket,
                StatusCode::UNAUTHORIZED,
                r#"{"message":"nope"}"#,
            )
            .await
            .unwrap();
        });

        let client = auth_client(addr);
        let err = client.login(sample_login_request()).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Network);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn login_invalid_json_is_protocol_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut socket).await.unwrap();
            write_json_response(&mut socket, StatusCode::OK, "not-json")
                .await
                .unwrap();
        });

        let client = auth_client(addr);
        let err = client.login(sample_login_request()).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn login_timeout_is_timeout_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = AuthApiClient::with_request_timeout(
            test_api_context(reqwest::Client::new(), base_url(addr)),
            Duration::from_millis(50),
        );
        let err = client.login(sample_login_request()).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_timeout());

        server.abort();
        let _ = server.await;
    }

    #[cfg(feature = "external-browser-sso")]
    #[tokio::test]
    async fn request_external_browser_challenge_posts_wire_request_and_parses_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await.unwrap();
            assert!(
                request.starts_with("POST /session/authenticator-request HTTP/1.1"),
                "{request}"
            );
            let lowered = request.to_ascii_lowercase();
            assert!(lowered.contains("\r\naccept: application/json\r\n"));
            assert!(lowered.contains(&format!(
                "\r\nuser-agent: {}\r\n",
                DEFAULT_USER_AGENT.to_ascii_lowercase()
            )));

            let body = request
                .split("\r\n\r\n")
                .nth(1)
                .expect("request must contain body");
            let body: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                body,
                json!({
                    "data": {
                        "ACCOUNT_NAME": "account",
                        "LOGIN_NAME": "username",
                        "CLIENT_ENVIRONMENT": {
                            "OCSP_MODE": "FAIL_OPEN",
                            "TRACING": 0,
                            "LOGIN_TIMEOUT": 120,
                            "NETWORK_TIMEOUT": 120,
                            "SOCKET_TIMEOUT": 120,
                        },
                        "AUTHENTICATOR": "EXTERNALBROWSER",
                        "BROWSER_MODE_REDIRECT_PORT": "8080",
                    }
                })
            );

            write_json_response(
                &mut socket,
                StatusCode::OK,
                r#"{"success":true,"data":{"ssoUrl":"https://example.com/sso","proofKey":"proof-key"}}"#,
            )
            .await
            .unwrap();
        });

        let client = auth_client(addr);
        let challenge = client
            .request_external_browser_challenge(AuthenticatorRequest {
                account_name: "account",
                login_name: "username",
                redirect_port: 8080,
            })
            .await
            .unwrap();

        assert_eq!(challenge.sso_url.as_str(), "https://example.com/sso");
        assert_eq!(challenge.proof_key.as_deref(), Some("proof-key"));

        server.await.unwrap();
    }
}
