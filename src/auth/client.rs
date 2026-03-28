use std::time::Duration;

#[cfg(feature = "external-browser-sso")]
use serde_json::json;

pub(super) const AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) fn client_app_id() -> &'static str {
    env!("CARGO_PKG_NAME")
}

pub(super) fn client_app_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(feature = "external-browser-sso")]
pub(super) fn client_environment() -> serde_json::Value {
    let auth_request_timeout_secs = AUTH_REQUEST_TIMEOUT.as_secs() as i64;
    json!({
        "OCSP_MODE": "FAIL_OPEN",
        "TRACING": 0,
        "LOGIN_TIMEOUT": auth_request_timeout_secs,
        "NETWORK_TIMEOUT": auth_request_timeout_secs,
        "SOCKET_TIMEOUT": auth_request_timeout_secs,
    })
}
