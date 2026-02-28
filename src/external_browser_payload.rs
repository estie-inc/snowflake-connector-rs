use std::borrow::Cow;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ParsedTokenAndConsent {
    pub(crate) token: Option<String>,
    pub(crate) consent: Option<bool>,
}

/// Parse `token` and `consent` from key/value pairs shared by both external-browser flows.
///
/// This parser is intentionally reused in:
/// - callback listener flow (`GET` query / `POST` form body parsing in `external_browser_listener`)
/// - manual redirected-URL flow (query/fragment parsing in `auth::external_browser`)
///
/// Keeping this in one place guarantees both flows use identical parsing rules
/// (case-insensitive keys, first non-empty token wins, consent parsing behavior).
pub(crate) fn parse_token_and_consent_from_pairs<'a, I>(pairs: I) -> ParsedTokenAndConsent
where
    I: IntoIterator<Item = (Cow<'a, str>, Cow<'a, str>)>,
{
    pairs.into_iter().fold(
        ParsedTokenAndConsent::default(),
        accumulate_token_and_consent,
    )
}

fn accumulate_token_and_consent(
    parsed: ParsedTokenAndConsent,
    (key, value): (Cow<'_, str>, Cow<'_, str>),
) -> ParsedTokenAndConsent {
    let ParsedTokenAndConsent {
        token: current_token,
        consent: current_consent,
    } = parsed;

    match key.to_ascii_lowercase().as_str() {
        "token" => {
            let token = if current_token.is_none() && !value.is_empty() {
                Some(value.into_owned())
            } else {
                current_token
            };
            ParsedTokenAndConsent {
                token,
                consent: current_consent,
            }
        }
        "consent" => {
            let consent = parse_consent_value(value.as_ref()).or(current_consent);
            ParsedTokenAndConsent {
                token: current_token,
                consent,
            }
        }
        _ => ParsedTokenAndConsent {
            token: current_token,
            consent: current_consent,
        },
    }
}

fn parse_consent_value(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_token_and_consent_from_pairs;

    #[test]
    fn picks_first_non_empty_token() {
        let parsed = parse_token_and_consent_from_pairs(url::form_urlencoded::parse(
            b"token=&token=first&token=second",
        ));
        assert_eq!(parsed.token.as_deref(), Some("first"));
    }

    #[test]
    fn parses_case_insensitive_keys() {
        let parsed = parse_token_and_consent_from_pairs(url::form_urlencoded::parse(
            b"ToKeN=abc&CoNsEnT=true",
        ));
        assert_eq!(parsed.token.as_deref(), Some("abc"));
        assert_eq!(parsed.consent, Some(true));
    }

    #[test]
    fn consent_uses_latest_valid_value() {
        let parsed = parse_token_and_consent_from_pairs(url::form_urlencoded::parse(
            b"consent=false&consent=maybe&consent=true&token=t",
        ));
        assert_eq!(parsed.token.as_deref(), Some("t"));
        assert_eq!(parsed.consent, Some(true));
    }

    #[test]
    fn returns_none_token_when_missing_or_empty() {
        let parsed = parse_token_and_consent_from_pairs(url::form_urlencoded::parse(
            b"consent=false&token=&other=value",
        ));
        assert_eq!(parsed.token, None);
        assert_eq!(parsed.consent, Some(false));
    }
}
