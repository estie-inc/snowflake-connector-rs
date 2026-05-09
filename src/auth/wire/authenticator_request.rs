use std::time::Duration;

use serde::Serialize;

#[derive(Debug)]
pub(crate) struct AuthenticatorRequest<'a> {
    pub(crate) account_name: &'a str,
    pub(crate) login_name: &'a str,
    pub(crate) redirect_port: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ClientEnvironment {
    #[serde(rename = "OCSP_MODE")]
    pub(crate) ocsp_mode: &'static str,
    #[serde(rename = "TRACING")]
    pub(crate) tracing: u8,
    #[serde(rename = "LOGIN_TIMEOUT")]
    pub(crate) login_timeout: u64,
    #[serde(rename = "NETWORK_TIMEOUT")]
    pub(crate) network_timeout: u64,
    #[serde(rename = "SOCKET_TIMEOUT")]
    pub(crate) socket_timeout: u64,
}

impl ClientEnvironment {
    pub(crate) fn auth_defaults(timeout: Duration) -> Self {
        let secs = timeout.as_secs();
        Self {
            ocsp_mode: "FAIL_OPEN",
            tracing: 0,
            login_timeout: secs,
            network_timeout: secs,
            socket_timeout: secs,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct AuthenticatorBody<'a> {
    pub(crate) data: AuthenticatorData<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AuthenticatorData<'a> {
    #[serde(rename = "ACCOUNT_NAME")]
    pub(crate) account_name: &'a str,
    #[serde(rename = "LOGIN_NAME")]
    pub(crate) login_name: &'a str,
    #[serde(rename = "CLIENT_ENVIRONMENT")]
    pub(crate) client_environment: ClientEnvironment,
    #[serde(rename = "AUTHENTICATOR")]
    pub(crate) authenticator: &'static str,
    #[serde(rename = "BROWSER_MODE_REDIRECT_PORT")]
    pub(crate) redirect_port: String,
}

impl<'a> AuthenticatorRequest<'a> {
    pub(crate) fn into_body(self, client_environment: ClientEnvironment) -> AuthenticatorBody<'a> {
        AuthenticatorBody {
            data: AuthenticatorData {
                account_name: self.account_name,
                login_name: self.login_name,
                client_environment,
                authenticator: "EXTERNALBROWSER",
                redirect_port: self.redirect_port.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;

    #[test]
    fn authenticator_request_body_matches_wire_shape() {
        let body = AuthenticatorRequest {
            account_name: "account",
            login_name: "username",
            redirect_port: 8080,
        }
        .into_body(ClientEnvironment::auth_defaults(Duration::from_secs(120)));

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
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
    }
}
