use std::io::{self, Write};

use reqwest::Url;

use super::payload::{BrowserCallbackPayload, parse_token_and_consent_from_pairs};
use crate::{Error, Result};

#[cfg(unix)]
use super::manual_input_unix;

pub(super) async fn manual_token_flow() -> Result<BrowserCallbackPayload> {
    tokio::task::spawn_blocking(manual_token_flow_blocking)
        .await
        .map_err(|e| Error::Communication(format!("manual input task failed: {e}")))?
}

fn manual_token_flow_blocking() -> Result<BrowserCallbackPayload> {
    eprintln!(
        "After completing authentication, paste the URL you were redirected to (not logged)."
    );

    eprint!("Redirected URL: ");
    let _ = io::stderr().flush();
    let input = read_redirected_url_line()?;

    eprintln!("Received redirected URL input. Continuing authentication...");

    payload_from_redirect_input(input.trim())
}

fn read_redirected_url_line() -> Result<String> {
    #[cfg(unix)]
    if let Some(result) = manual_input_unix::try_read_redirected_url_line_noncanonical() {
        return result;
    }

    // TODO(windows): investigate whether long redirected URLs can be truncated in
    // canonical console input mode and add a Windows-specific raw/non-canonical fallback.

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::Communication(format!("failed to read input: {e}")))?;

    validate_redirected_url_input(input)
}

pub(super) fn validate_redirected_url_input(input: String) -> Result<String> {
    if input.trim().is_empty() {
        return Err(Error::Communication(
            "No redirected URL was provided".to_string(),
        ));
    }
    Ok(input)
}

fn extract_payload_from_url(url: &str) -> Option<BrowserCallbackPayload> {
    let parsed = Url::parse(url).ok()?;
    let query = parse_token_and_consent_from_pairs(parsed.query_pairs());
    let fragment = parsed
        .fragment()
        .map(|frag| {
            parse_token_and_consent_from_pairs(url::form_urlencoded::parse(frag.as_bytes()))
        })
        .unwrap_or_default();

    let token = query.token.or(fragment.token)?;
    let consent = query.consent.or(fragment.consent);

    Some(BrowserCallbackPayload { token, consent })
}

fn payload_from_redirect_input(input: &str) -> Result<BrowserCallbackPayload> {
    extract_payload_from_url(input).ok_or_else(|| {
        Error::Communication(
            "Unable to extract token from redirected URL (expected query or fragment token=...)"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::payload_from_redirect_input;

    #[test]
    fn payload_from_redirect_input_extracts_token() {
        let url = "https://example.test/callback?token=abc123&other=1";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, None);
    }

    #[test]
    fn payload_from_redirect_input_extracts_consent() {
        let url = "https://example.test/callback?token=abc123&consent=false";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn payload_from_redirect_input_extracts_token_from_fragment() {
        let url = "https://example.test/callback#token=abc123&consent=true";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "abc123");
        assert_eq!(payload.consent, Some(true));
    }

    #[test]
    fn payload_from_redirect_input_prefers_query_token_over_fragment_token() {
        let url = "https://example.test/callback?token=query_token#token=fragment_token";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "query_token");
    }

    #[test]
    fn payload_from_redirect_input_keeps_query_token_and_query_consent() {
        let url = "https://example.test/callback?token=query_token&consent=false#consent=true";
        let payload = payload_from_redirect_input(url).unwrap();
        assert_eq!(payload.token, "query_token");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn payload_from_redirect_input_fails_without_token() {
        let url = "https://example.test/callback?missing=true";
        let err = payload_from_redirect_input(url).unwrap_err();
        assert!(format!("{err}").contains("Unable to extract token"));
    }
}
