use std::time::Duration;

use reqwest::{
    Client, Url,
    header::{ACCEPT, USER_AGENT},
};

use crate::{
    Result,
    error::{ConfigError, NetworkError, classify_request_error},
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
    http: Client,
    base_url: Url,
    request_timeout: Duration,
}

impl AuthApiClient {
    pub(crate) fn new(http: Client, base_url: Url) -> Self {
        Self {
            http,
            base_url,
            request_timeout: AUTH_REQUEST_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_request_timeout(http: Client, base_url: Url, request_timeout: Duration) -> Self {
        Self {
            http,
            base_url,
            request_timeout,
        }
    }

    pub(crate) async fn login(&self, request: LoginRequest<'_>) -> Result<LoginSession> {
        let url = self
            .base_url
            .join("session/v1/login-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;

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
        let url = self
            .base_url
            .join("session/authenticator-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;

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
        self.http
            .post(url)
            .header(ACCEPT, accept)
            .header(USER_AGENT, default_user_agent())
            .timeout(self.request_timeout)
    }
}

fn default_user_agent() -> String {
    format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use std::{io, net::SocketAddr, time::Duration};

    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::{
        ErrorKind,
        auth::wire::{LoginBody, LoginCredentialWire, LoginData, LoginQuery, LoginRequest},
    };

    #[cfg(feature = "external-browser-sso")]
    use crate::auth::wire::AuthenticatorRequest;

    async fn read_http_message(stream: &mut TcpStream) -> io::Result<String> {
        let mut buf = Vec::new();
        let mut header_end = None;

        while header_end.is_none() {
            let mut chunk = [0_u8; 1024];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed before headers completed",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
            header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
        }

        let header_end = header_end.expect("header end must exist") + 4;
        let headers = String::from_utf8_lossy(&buf[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);

        while buf.len() < header_end + content_length {
            let mut chunk = [0_u8; 1024];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed before body completed",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
        }

        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    async fn write_http_response(
        stream: &mut TcpStream,
        status: &str,
        body: &str,
    ) -> io::Result<()> {
        stream
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
    }

    fn base_url_for(addr: SocketAddr) -> Url {
        Url::parse(&format!("http://{addr}/")).expect("test base URL must parse")
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
                    credential: LoginCredentialWire::Password { password: "secret" },
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
            let request = read_http_message(&mut socket).await.unwrap();
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
                default_user_agent().to_ascii_lowercase()
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

            write_http_response(
                &mut socket,
                "200 OK",
                r#"{"success":true,"data":{"token":"session-token"}}"#,
            )
            .await
            .unwrap();
        });

        let client = AuthApiClient::new(Client::new(), base_url_for(addr));
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
            let _ = read_http_message(&mut socket).await.unwrap();
            write_http_response(&mut socket, "401 Unauthorized", r#"{"message":"nope"}"#)
                .await
                .unwrap();
        });

        let client = AuthApiClient::new(Client::new(), base_url_for(addr));
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
            let _ = read_http_message(&mut socket).await.unwrap();
            write_http_response(&mut socket, "200 OK", "not-json")
                .await
                .unwrap();
        });

        let client = AuthApiClient::new(Client::new(), base_url_for(addr));
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
            Client::new(),
            base_url_for(addr),
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
            let request = read_http_message(&mut socket).await.unwrap();
            assert!(
                request.starts_with("POST /session/authenticator-request HTTP/1.1"),
                "{request}"
            );
            let lowered = request.to_ascii_lowercase();
            assert!(lowered.contains("\r\naccept: application/json\r\n"));

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

            write_http_response(
                &mut socket,
                "200 OK",
                r#"{"success":true,"data":{"ssoUrl":"https://example.com/sso","proofKey":"proof-key"}}"#,
            )
            .await
            .unwrap();
        });

        let client = AuthApiClient::new(Client::new(), base_url_for(addr));
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
