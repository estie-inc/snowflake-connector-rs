use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;

use super::usable_token;

const REQUEST_TYPE_RENEW: &str = "RENEW";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TokenRequestBody<'a> {
    old_session_token: &'a str,
    request_type: &'static str,
}

impl<'a> TokenRequestBody<'a> {
    pub(crate) fn renew(old_session_token: &'a str) -> Self {
        Self {
            old_session_token,
            request_type: REQUEST_TYPE_RENEW,
        }
    }
}

pub(crate) enum TokenResponse {
    Renewed(RenewedTokens),
    Rejected {
        code: Option<String>,
        message: Option<String>,
    },
}

pub(crate) struct RenewedTokens {
    pub(crate) session_token: String,
    pub(crate) master_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponseData {
    session_token: Option<String>,
    master_token: Option<String>,
}

#[derive(Deserialize)]
struct TokenResponseEnvelope {
    data: Option<TokenResponseData>,
    code: Option<String>,
    message: Option<String>,
    success: bool,
}

/// Parses a `session/token-request` response.
///
/// UNVERIFIED (no public protocol reference): whether Snowflake can omit `data.masterToken` from a successful renew
/// response for a session that received a master token at login. The field is therefore optional here, and a
/// missing, `null`, or empty value is reported as `None`.
pub(crate) fn parse_token_response(body: &[u8]) -> Result<TokenResponse, ProtocolError> {
    let parsed: TokenResponseEnvelope =
        serde_json::from_slice(body).map_err(|e| ProtocolError::json_parse(e, body))?;
    if !parsed.success {
        return Ok(TokenResponse::Rejected {
            code: parsed.code,
            message: parsed.message,
        });
    }

    let data = parsed
        .data
        .ok_or_else(|| ProtocolError::missing_field("data"))?;
    let session_token = usable_token(data.session_token)
        .ok_or_else(|| ProtocolError::missing_field("data.sessionToken"))?;

    Ok(TokenResponse::Renewed(RenewedTokens {
        session_token,
        master_token: usable_token(data.master_token),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renew_body_uses_wire_field_names() {
        let body = serde_json::to_value(TokenRequestBody::renew("old-session")).unwrap();

        assert_eq!(
            body,
            serde_json::json!({"oldSessionToken": "old-session", "requestType": "RENEW"})
        );
    }

    #[test]
    fn success_returns_both_tokens() {
        let parsed = parse_token_response(
            br#"{"success":true,"data":{"sessionToken":"new-session","masterToken":"new-master","sessionId":1}}"#,
        )
        .unwrap();

        let TokenResponse::Renewed(tokens) = parsed else {
            panic!("expected renewed tokens");
        };
        assert_eq!(tokens.session_token, "new-session");
        assert_eq!(tokens.master_token.as_deref(), Some("new-master"));
    }

    #[test]
    fn missing_null_or_empty_master_token_is_none() {
        for body in [
            r#"{"success":true,"data":{"sessionToken":"new-session"}}"#,
            r#"{"success":true,"data":{"sessionToken":"new-session","masterToken":null}}"#,
            r#"{"success":true,"data":{"sessionToken":"new-session","masterToken":""}}"#,
        ] {
            let TokenResponse::Renewed(tokens) = parse_token_response(body.as_bytes()).unwrap()
            else {
                panic!("expected renewed tokens");
            };
            assert_eq!(tokens.master_token, None, "{body}");
        }
    }

    #[test]
    fn missing_session_token_is_protocol_error() {
        for body in [
            r#"{"success":true}"#,
            r#"{"success":true,"data":{}}"#,
            r#"{"success":true,"data":{"sessionToken":""}}"#,
        ] {
            assert!(parse_token_response(body.as_bytes()).is_err(), "{body}");
        }
    }

    #[test]
    fn rejection_keeps_code_and_message() {
        let parsed = parse_token_response(
            br#"{"success":false,"code":"390114","message":"master token expired","data":null}"#,
        )
        .unwrap();

        let TokenResponse::Rejected { code, message } = parsed else {
            panic!("expected rejection");
        };
        assert_eq!(code.as_deref(), Some("390114"));
        assert_eq!(message.as_deref(), Some("master token expired"));
    }

    #[test]
    fn invalid_json_is_protocol_error() {
        assert!(parse_token_response(b"not-json").is_err());
    }
}
