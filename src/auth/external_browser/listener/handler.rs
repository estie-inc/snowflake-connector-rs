use std::{convert::Infallible, sync::Mutex};

use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    header::{ACCESS_CONTROL_REQUEST_HEADERS, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN, VARY},
    http::StatusCode,
    {Method, Request, Response},
};
use serde::Deserialize;
use tokio::sync::watch;
use url::form_urlencoded;

use super::super::payload::{
    BrowserCallbackPayload, ParsedTokenAndConsent, parse_token_and_consent_from_pairs,
};

#[derive(Debug, Deserialize)]
struct TokenPayload {
    token: Option<String>,
    consent: Option<bool>,
}

pub(super) struct CallbackHandler {
    expected_origin: String,
    application: Option<String>,
    payload_tx: watch::Sender<Option<BrowserCallbackPayload>>,
    validated_origin: Mutex<Option<String>>,
}

impl CallbackHandler {
    pub(super) fn new(
        expected_origin: String,
        application: Option<String>,
        payload_tx: watch::Sender<Option<BrowserCallbackPayload>>,
    ) -> Self {
        Self {
            expected_origin,
            application,
            payload_tx,
            validated_origin: Mutex::new(None),
        }
    }

    pub(super) async fn handle(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let request_origin = req
            .headers()
            .get(ORIGIN)
            .and_then(|origin| origin.to_str().ok())
            .map(ToString::to_string);

        match *req.method() {
            Method::OPTIONS => self.handle_preflight(request_origin, &req).await,
            Method::GET => {
                let payload = extract_payload_from_query(req.uri().query());
                self.handle_callback(payload).await
            }
            Method::POST => {
                let whole_body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                let payload = extract_payload_from_body(&whole_body);
                self.handle_callback(payload).await
            }
            _ => Ok(method_not_allowed_response()),
        }
    }

    async fn handle_preflight(
        &self,
        request_origin: Option<String>,
        req: &Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let Some(allowed_origin) =
            request_origin.filter(|origin| origin.eq_ignore_ascii_case(&self.expected_origin))
        else {
            return Ok(forbidden_response());
        };

        let allow_headers =
            requested_headers(req).unwrap_or_else(|| "Content-Type, Origin".to_string());
        let response = preflight_cors_response(&allowed_origin, &allow_headers);

        self.set_validated_origin(allowed_origin);

        Ok(response)
    }

    async fn handle_callback(
        &self,
        payload: Option<BrowserCallbackPayload>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        self.publish_payload(&payload);
        let stored_origin = self.validated_origin();

        let response = match payload.as_ref() {
            None => missing_token_response(),
            Some(p) if stored_origin.is_some() => {
                cors_consent_response(p, stored_origin.as_deref().expect("checked above"))
            }
            Some(_) => html_close_page_response(self.application.as_deref()),
        };
        Ok(response)
    }

    fn publish_payload(&self, payload: &Option<BrowserCallbackPayload>) {
        if let Some(payload) = payload {
            let _ = self.payload_tx.send(Some(payload.clone()));
        }
    }

    fn set_validated_origin(&self, origin: String) {
        let mut guard = self
            .validated_origin
            .lock()
            .expect("validated_origin lock poisoned");
        *guard = Some(origin);
    }

    fn validated_origin(&self) -> Option<String> {
        self.validated_origin
            .lock()
            .expect("validated_origin lock poisoned")
            .clone()
    }
}

fn extract_payload_from_body(body: &Bytes) -> Option<BrowserCallbackPayload> {
    // 1) JSON: {"token": "...", "consent": true}
    serde_json::from_slice::<TokenPayload>(body)
        .ok()
        .and_then(|parsed| {
            parsed.token.map(|token| BrowserCallbackPayload {
                token,
                consent: parsed.consent,
            })
        })
        // 2) x-www-form-urlencoded / key=value fallback
        .or_else(|| {
            std::str::from_utf8(body)
                .ok()
                .map(|body_str| {
                    parse_token_and_consent_from_pairs(form_urlencoded::parse(body_str.as_bytes()))
                })
                .and_then(ParsedTokenAndConsent::into_payload)
        })
}

fn extract_payload_from_query(query: Option<&str>) -> Option<BrowserCallbackPayload> {
    query
        .map(|q| parse_token_and_consent_from_pairs(form_urlencoded::parse(q.as_bytes())))
        .and_then(ParsedTokenAndConsent::into_payload)
}

fn requested_headers(req: &Request<Incoming>) -> Option<String> {
    req.headers()
        .get(ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

fn method_not_allowed_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Full::new(Bytes::from_static(b"method not allowed")))
        .expect("building method not allowed response failed")
}

fn forbidden_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Full::new(Bytes::from_static(b"origin not allowed")))
        .expect("building forbidden response failed")
}

fn preflight_cors_response(allowed_origin: &str, allow_headers: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Access-Control-Allow-Origin", allowed_origin)
        .header("Access-Control-Allow-Methods", "POST, GET")
        .header("Access-Control-Allow-Headers", allow_headers)
        .header("Access-Control-Max-Age", "86400")
        .header(VARY, "Accept-Encoding, Origin")
        .header(CONTENT_LENGTH, "0")
        .body(Full::new(Bytes::new()))
        .expect("building preflight response failed")
}

fn missing_token_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain")
        .header(CONTENT_LENGTH, "17")
        .body(Full::new(Bytes::from_static(b"no token provided")))
        .expect("building missing token response failed")
}

fn html_close_page_response(application: Option<&str>) -> Response<Full<Bytes>> {
    let app_line = application
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|app| format!("Client application: {app}.\n"))
        .unwrap_or_default();
    let body = format!(
        r#"<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>
<link rel=\"icon\" href=\"data:,\">
<title>SAML Response for Snowflake</title></head>
<body>
Your identity was confirmed and propagated to Snowflake.
{app_line}You can close this window now and go back where you started from.
</body></html>"#
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/html")
        .header(CONTENT_LENGTH, body.len().to_string())
        .body(Full::new(Bytes::from(body)))
        .expect("building HTML close page response failed")
}

fn cors_consent_response(payload: &BrowserCallbackPayload, origin: &str) -> Response<Full<Bytes>> {
    let consent = payload.consent.unwrap_or(true);
    let body = serde_json::json!({"consent": consent}).to_string();

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_LENGTH, body.len().to_string())
        .header("Access-Control-Allow-Origin", origin)
        .header(VARY, "Accept-Encoding, Origin")
        .body(Full::new(Bytes::from(body)))
        .expect("building CORS consent response failed")
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;
    use hyper::{
        body::Bytes,
        header::{CONTENT_LENGTH, CONTENT_TYPE, VARY},
        http::StatusCode,
    };

    use super::super::super::payload::BrowserCallbackPayload;

    use super::{
        cors_consent_response, extract_payload_from_body, extract_payload_from_query,
        html_close_page_response,
    };

    #[test]
    fn extract_payload_from_body_json() {
        let body = Bytes::from_static(br#"{"token":"t","consent":false}"#);
        let payload = extract_payload_from_body(&body).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn extract_payload_from_body_form_urlencoded() {
        let body = Bytes::from_static(b"token=t&consent=true");
        let payload = extract_payload_from_body(&body).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(true));
    }

    #[test]
    fn extract_payload_from_query_parses_consent() {
        let payload = extract_payload_from_query(Some("token=t&consent=false")).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn extract_payload_from_query_prefers_first_token() {
        let payload = extract_payload_from_query(Some("token=first&token=second")).unwrap();
        assert_eq!(payload.token, "first");
    }

    #[tokio::test]
    async fn cors_consent_response_returns_json_without_token() {
        let payload = BrowserCallbackPayload {
            token: "secret".to_string(),
            consent: Some(false),
        };
        let resp = cors_consent_response(&payload, "http://localhost:1");
        assert_eq!(resp.status(), StatusCode::OK);

        let headers = resp.headers().clone();
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "application/json"
        );
        assert_eq!(
            headers
                .get("access-control-allow-origin")
                .unwrap()
                .to_str()
                .unwrap(),
            "http://localhost:1"
        );
        assert_eq!(
            headers.get(VARY).unwrap().to_str().unwrap(),
            "Accept-Encoding, Origin"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("\"consent\":false"));
        assert!(!body_str.contains("secret"));

        let expected_len = body.len().to_string();
        assert_eq!(
            headers.get(CONTENT_LENGTH).unwrap().to_str().unwrap(),
            expected_len
        );
    }

    #[tokio::test]
    async fn html_close_page_response_includes_app_line() {
        let resp = html_close_page_response(Some("myapp"));
        assert_eq!(resp.status(), StatusCode::OK);

        let headers = resp.headers().clone();
        assert!(headers.get("access-control-allow-origin").is_none());
        assert!(headers.get(VARY).is_none());

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("You can close this window now"));
        assert!(body_str.contains("Client application: myapp"));
        assert!(!body_str.contains("token"));
    }
}
