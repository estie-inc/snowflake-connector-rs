use std::fmt;

use crate::{
    Result,
    error::{AuthError, ProtocolError},
};

use super::usable_token;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct LoginSession {
    pub(crate) token: String,
    pub(crate) master_token: Option<String>,
}

impl fmt::Debug for LoginSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginSession")
            .field("token", &"<redacted>")
            .field(
                "master_token",
                &self.master_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponseData {
    token: Option<String>,
    master_token: Option<String>,
}

#[derive(serde::Deserialize)]
struct LoginResponseEnvelope {
    data: Option<LoginResponseData>,
    message: Option<String>,
    success: bool,
}

/// Parses a `session/v1/login-request` response.
///
/// UNVERIFIED (no public protocol reference): whether every authentication method returns data.masterToken.
/// The field is therefore optional: a missing, null, or empty value becomes None and login still succeeds,
/// matching the official connectors, which tolerate its absence. A session without a master token cannot renew
/// its session token and expires as it did before renewal support existed.
pub(crate) fn parse_login_response(body: &str) -> Result<LoginSession> {
    let parsed: LoginResponseEnvelope =
        serde_json::from_str(body).map_err(|e| ProtocolError::json_parse(e, body))?;
    if !parsed.success {
        return Err(AuthError::login_rejected(parsed.message).into());
    }

    let data = parsed
        .data
        .ok_or_else(|| ProtocolError::missing_field("data"))?;
    let token = data
        .token
        .ok_or_else(|| ProtocolError::missing_field("data.token"))?;

    Ok(LoginSession {
        token,
        master_token: usable_token(data.master_token),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn parse_login_response_success_returns_session() {
        let session =
            parse_login_response(r#"{"success":true,"data":{"token":"session-token"}}"#).unwrap();

        assert_eq!(
            session,
            LoginSession {
                token: "session-token".to_string(),
                master_token: None,
            }
        );
    }

    #[test]
    fn parse_login_response_keeps_master_token() {
        let session = parse_login_response(
            r#"{"success":true,"data":{"token":"session-token","masterToken":"master-token"}}"#,
        )
        .unwrap();

        assert_eq!(session.master_token.as_deref(), Some("master-token"));
    }

    #[test]
    fn parse_login_response_null_or_empty_master_token_is_none() {
        for body in [
            r#"{"success":true,"data":{"token":"session-token","masterToken":null}}"#,
            r#"{"success":true,"data":{"token":"session-token","masterToken":""}}"#,
        ] {
            let session = parse_login_response(body).unwrap();
            assert_eq!(session.master_token, None, "{body}");
        }
    }

    #[test]
    fn login_session_debug_redacts_tokens() {
        let session = LoginSession {
            token: "session-secret".to_string(),
            master_token: Some("master-secret".to_string()),
        };

        let rendered = format!("{session:?}");
        assert!(!rendered.contains("session-secret"), "{rendered}");
        assert!(!rendered.contains("master-secret"), "{rendered}");
    }

    #[test]
    fn parse_login_response_missing_data_is_protocol_error() {
        let err = parse_login_response(r#"{"success":true}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.snowflake_message(), None);
    }

    #[test]
    fn parse_login_response_missing_token_is_protocol_error() {
        let err = parse_login_response(r#"{"success":true,"data":{}}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.token"
        );
    }

    #[test]
    fn parse_login_response_rejected_keeps_auth_classification() {
        let err =
            parse_login_response(r#"{"success":false,"message":"bad credentials"}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), Some("bad credentials"));
    }

    #[test]
    fn parse_login_response_rejected_without_message_preserves_absence() {
        let err = parse_login_response(r#"{"success":false}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Auth);
        assert_eq!(err.snowflake_message(), None);
        assert_eq!(err.to_string(), "authentication rejected");
    }
}
