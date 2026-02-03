#[cfg(feature = "external-browser-sso")]
use std::time::Duration;

#[cfg(feature = "external-browser-sso")]
use serde_json::json;

pub(super) fn client_app_id() -> &'static str {
    env!("CARGO_PKG_NAME")
}

pub(super) fn client_app_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(feature = "external-browser-sso")]
pub(super) fn client_environment(timeout: Option<Duration>) -> serde_json::Value {
    json!({
        "OCSP_MODE": "FAIL_OPEN",
        "TRACING": 0,
        "LOGIN_TIMEOUT": timeout.map(|t| t.as_secs() as i64),
        "NETWORK_TIMEOUT": timeout.map(|t| t.as_secs() as i64),
        "SOCKET_TIMEOUT": timeout.map(|t| t.as_secs() as i64),
    })
}
