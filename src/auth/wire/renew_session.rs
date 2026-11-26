use serde::{Deserialize, Serialize};

use crate::{
    Result,
    error::{ProtocolError, SessionExpiredError},
};

/// Body of `POST /session/token-request`, which trades the master token for a new session token.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RenewSessionBody<'a> {
    pub(crate) old_session_token: &'a str,
    pub(crate) request_type: &'static str,
}

impl<'a> RenewSessionBody<'a> {
    pub(crate) fn renew(old_session_token: &'a str) -> Self {
        Self {
            old_session_token,
            request_type: "RENEW",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenewedSession {
    pub(crate) session_token: String,
    /// Snowflake may rotate the master token together with the session token.
    pub(crate) master_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenewSessionResponseData {
    session_token: Option<String>,
    master_token: Option<String>,
}

#[derive(Deserialize)]
struct RenewSessionResponseEnvelope {
    data: Option<RenewSessionResponseData>,
    code: Option<String>,
    message: Option<String>,
    success: bool,
}

/// Parses the token-request response.
///
/// A rejected renewal (for example an expired master token) is reported as a session-expired error: the caller can
/// no longer continue the session and has to log in again.
pub(crate) fn parse_renew_session_response(body: &str) -> Result<RenewedSession> {
    let parsed: RenewSessionResponseEnvelope =
        serde_json::from_str(body).map_err(|e| ProtocolError::json_parse(e, body))?;
    if !parsed.success {
        return Err(SessionExpiredError::new(parsed.code, parsed.message, None).into());
    }

    let data = parsed
        .data
        .ok_or_else(|| ProtocolError::missing_field("data"))?;
    let session_token = data
        .session_token
        .ok_or_else(|| ProtocolError::missing_field("data.sessionToken"))?;

    Ok(RenewedSession {
        session_token,
        master_token: data.master_token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn renew_body_uses_renew_request_type() {
        let body = serde_json::to_value(RenewSessionBody::renew("old-token")).unwrap();

        assert_eq!(
            body,
            serde_json::json!({"oldSessionToken": "old-token", "requestType": "RENEW"})
        );
    }

    #[test]
    fn parse_success_returns_new_tokens() {
        let renewed = parse_renew_session_response(
            r#"{"success":true,"data":{"sessionToken":"new-session","validityInSecondsST":3600,"masterToken":"new-master","validityInSecondsMT":14400,"sessionId":1}}"#,
        )
        .unwrap();

        assert_eq!(
            renewed,
            RenewedSession {
                session_token: "new-session".to_string(),
                master_token: Some("new-master".to_string()),
            }
        );
    }

    #[test]
    fn parse_success_without_master_token_keeps_it_absent() {
        let renewed = parse_renew_session_response(
            r#"{"success":true,"data":{"sessionToken":"new-session"}}"#,
        )
        .unwrap();

        assert_eq!(renewed.master_token, None);
    }

    #[test]
    fn parse_rejection_is_session_expired() {
        let err = parse_renew_session_response(
            r#"{"success":false,"code":"390114","message":"Authentication token has expired.  The user must authenticate again.","data":null}"#,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390114"));
    }

    #[test]
    fn parse_missing_session_token_is_protocol_error() {
        let err = parse_renew_session_response(r#"{"success":true,"data":{}}"#).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.sessionToken"
        );
    }
}
