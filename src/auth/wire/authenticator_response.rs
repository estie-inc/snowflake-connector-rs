use url::Url;

use crate::{
    Result,
    error::{AuthError, ProtocolError},
};

const ALLOWED_BROWSER_URL_SCHEMES: &[&str] = &["http", "https"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalBrowserChallenge {
    pub(crate) sso_url: Url,
    pub(crate) proof_key: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct RawExternalBrowserChallenge {
    #[serde(rename = "ssoUrl")]
    sso_url: Option<String>,
    #[serde(rename = "proofKey")]
    proof_key: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct AuthenticatorResponse {
    data: Option<RawExternalBrowserChallenge>,
    message: Option<String>,
    success: bool,
}

pub(crate) fn parse_authenticator_response(body: &str) -> Result<ExternalBrowserChallenge> {
    let parsed: AuthenticatorResponse =
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn parse_external_browser_challenge_success_returns_challenge() {
        let challenge = parse_authenticator_response(
            r#"{"success":true,"data":{"ssoUrl":"https://example.com/sso","proofKey":"proof-key"}}"#,
        )
        .unwrap();

        assert_eq!(challenge.sso_url.as_str(), "https://example.com/sso");
        assert_eq!(challenge.proof_key.as_deref(), Some("proof-key"));
    }

    #[test]
    fn parse_external_browser_challenge_missing_data_is_protocol_error() {
        let err = parse_authenticator_response(r#"{"success":true}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.snowflake_message(), None);
    }

    #[test]
    fn parse_external_browser_challenge_missing_sso_url_is_protocol_error() {
        let err = parse_authenticator_response(r#"{"success":true,"data":{}}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.ssoUrl"
        );
    }

    #[test]
    fn parse_external_browser_challenge_empty_sso_url_is_protocol_error() {
        let err = parse_authenticator_response(r#"{"success":true,"data":{"ssoUrl":"   "}}"#)
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.ssoUrl: must not be empty"
        );
    }

    #[test]
    fn parse_external_browser_challenge_invalid_sso_url_is_protocol_error() {
        let err =
            parse_authenticator_response(r#"{"success":true,"data":{"ssoUrl":"http://[::1"}}"#)
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(err.to_string().contains("data.ssoUrl"));
    }

    #[test]
    fn parse_external_browser_challenge_rejects_non_http_schemes() {
        for raw in ["javascript:alert(1)", "file:///tmp/token.txt"] {
            let body = format!(r#"{{"success":true,"data":{{"ssoUrl":"{raw}"}}}}"#);
            let err = parse_authenticator_response(&body).expect_err("non-http scheme must fail");

            assert_eq!(err.kind(), ErrorKind::Protocol);
            assert!(err.to_string().contains("unsupported URL scheme"));
        }
    }

    #[test]
    fn parse_external_browser_challenge_rejected_without_message_preserves_absence() {
        let err = parse_authenticator_response(r#"{"success":false}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), None);
        assert_eq!(err.to_string(), "authentication rejected");
    }
}
