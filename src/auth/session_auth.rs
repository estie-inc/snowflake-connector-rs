use std::{
    fmt,
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};

use crate::{
    ApiContext,
    error::{NetworkError, ProtocolError, QueryScopedRepr, SessionExpiredError, TimeoutError},
};

use super::{
    api::AuthApiClient,
    wire::{LoginSession, TokenResponse},
};

pub(crate) struct SessionAuth {
    current: RwLock<Arc<TokenSet>>,
    /// Serializes renewals so concurrent requests that observe the same expiry trigger a single token request.
    renew_gate: Mutex<()>,
}

pub(crate) struct TokenSet {
    /// Starts at 0 on login and increases by one per successful renewal. A `u64` cannot realistically overflow at
    /// one renewal per HTTP round trip.
    generation: u64,
    session_token: String,
    master_token: Option<String>,
}

impl TokenSet {
    pub(crate) fn authorization(&self) -> String {
        snowflake_token_header(&self.session_token)
    }
}

pub(crate) fn snowflake_token_header(token: &str) -> String {
    format!(r#"Snowflake Token="{token}""#)
}

impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSet")
            .field("generation", &self.generation)
            .field("session_token", &"<redacted>")
            .field(
                "master_token",
                &self.master_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl fmt::Debug for SessionAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAuth")
            .field("current", &self.snapshot())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum RenewError {
    NoMasterToken,
    Rejected {
        code: Option<String>,
        message: Option<String>,
    },
    Network(NetworkError),
    RequestTimeout(reqwest::Error),
    Protocol(ProtocolError),
    DeadlineElapsed,
    RenewalTimeout,
}

impl RenewError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Network(NetworkError::Http(_)) | Self::RequestTimeout(_) => true,
            Self::Network(NetworkError::HttpStatus { status, .. }) => {
                matches!(status, 408 | 429 | 500..=599)
            }
            _ => false,
        }
    }

    pub(crate) fn into_scoped(
        self,
        observed: SessionExpiredError,
        deadline_error: TimeoutError,
    ) -> QueryScopedRepr {
        // UNVERIFIED (no public protocol reference): the full set of codes a token request can be rejected with. Only
        // 390112-390115 are known, and the official Python connector treats further codes as "log in again" too. Every
        // rejection therefore maps to a session-expired error instead of branching on the code, because the caller's
        // recovery is the same in each case: create a new session.
        match self {
            Self::NoMasterToken => observed.into(),
            Self::Rejected { code, message } => {
                SessionExpiredError::new(code, message, None).into()
            }
            Self::Network(error) => error.into(),
            Self::RequestTimeout(error) => TimeoutError::request(error).into(),
            Self::Protocol(error) => error.into(),
            Self::DeadlineElapsed => deadline_error.into(),
            Self::RenewalTimeout => TimeoutError::session_renewal().into(),
        }
    }
}

pub(crate) enum Exchange<T> {
    Done(T),
    Expired(T),
}

pub(crate) enum RenewedSendError<T, E> {
    Send(E),
    Renew { response: T, error: RenewError },
}

impl SessionAuth {
    pub(crate) fn new(session_token: String, master_token: Option<String>) -> Self {
        Self {
            current: RwLock::new(Arc::new(TokenSet {
                generation: 0,
                session_token,
                master_token,
            })),
            renew_gate: Mutex::new(()),
        }
    }

    pub(crate) fn from_login(login: LoginSession) -> Self {
        Self::new(login.token, login.master_token)
    }

    pub(crate) fn snapshot(&self) -> Arc<TokenSet> {
        Arc::clone(&self.current.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) async fn send_with_renewal<T, E, F, Fut>(
        &self,
        api: &Arc<ApiContext>,
        renew_budget: impl FnOnce() -> Duration,
        mut send: F,
    ) -> Result<Exchange<T>, RenewedSendError<T, E>>
    where
        F: FnMut(Arc<TokenSet>) -> Fut,
        Fut: Future<Output = Result<Exchange<T>, E>>,
    {
        let tokens = self.snapshot();
        let response = match send(Arc::clone(&tokens)).await {
            Ok(Exchange::Expired(response)) => response,
            Ok(done) => return Ok(done),
            Err(error) => return Err(RenewedSendError::Send(error)),
        };

        if let Err(error) = self.renew(api, &tokens, renew_budget()).await {
            return Err(RenewedSendError::Renew { response, error });
        }

        send(self.snapshot()).await.map_err(RenewedSendError::Send)
    }

    async fn renew(
        &self,
        api: &Arc<ApiContext>,
        observed: &TokenSet,
        budget: Duration,
    ) -> Result<Arc<TokenSet>, RenewError> {
        const LIMITS: RenewLimits = RenewLimits {
            total: Duration::from_secs(30),
            attempt: Duration::from_secs(10),
        };
        self.renew_with_limits(api, observed, budget, LIMITS).await
    }

    async fn renew_with_limits(
        &self,
        api: &Arc<ApiContext>,
        observed: &TokenSet,
        budget: Duration,
        limits: RenewLimits,
    ) -> Result<Arc<TokenSet>, RenewError> {
        let caller_deadline_first = budget < limits.total;
        let budget = budget.min(limits.total);
        let deadline = Instant::now() + budget;
        match timeout(
            budget,
            self.renew_until(api, observed, deadline, limits.attempt),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) if caller_deadline_first => Err(RenewError::DeadlineElapsed),
            Err(_elapsed) => Err(RenewError::RenewalTimeout),
        }
    }

    async fn renew_until(
        &self,
        api: &Arc<ApiContext>,
        observed: &TokenSet,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<Arc<TokenSet>, RenewError> {
        const MAX_RENEW_ATTEMPTS: usize = 8;

        let _gate = self.renew_gate.lock().await;

        let current = self.snapshot();
        if current.generation != observed.generation {
            return Ok(current);
        }
        let Some(master_token) = current.master_token.as_deref() else {
            return Err(RenewError::NoMasterToken);
        };

        let client = AuthApiClient::new(Arc::clone(api));
        let mut backoff = RenewBackoff::new();
        let mut attempt = 0;
        // UNVERIFIED (no public protocol reference): whether Snowflake accepts a second token request carrying the
        // same old session token and master token when it already processed the first one but the response was
        // lost. Retries here, and renewals restarted after a dropped future, resend the unchanged credentials
        // without guarding against that case. Both official connectors do the same: they route the token request
        // through their generic HTTP retry layer.
        let renewed = loop {
            attempt += 1;
            let remaining = deadline.saturating_duration_since(Instant::now());
            match client
                .renew_session(
                    &current.session_token,
                    master_token,
                    remaining.min(attempt_timeout),
                )
                .await
            {
                Ok(TokenResponse::Renewed(renewed)) => break renewed,
                Ok(TokenResponse::Rejected { code, message }) => {
                    return Err(RenewError::Rejected { code, message });
                }
                Err(error) if error.is_retryable() && attempt < MAX_RENEW_ATTEMPTS => {
                    sleep(backoff.next_delay()).await;
                }
                Err(error) => return Err(error),
            }
        };

        // UNVERIFIED (no public protocol reference): whether a successful renew response can omit the master token.
        // When it does, the current master token is kept. This is this crate's own choice: the official connectors
        // overwrite it unconditionally (Python with `None`, Go with an empty string), which leaves the session
        // unable to renew again. Keeping the old value is harmless, because if Snowflake did invalidate it, the next
        // renewal is simply rejected.
        let master_token = renewed
            .master_token
            .or_else(|| current.master_token.clone());
        let next = Arc::new(TokenSet {
            generation: current.generation + 1,
            session_token: renewed.session_token,
            master_token,
        });
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&next);
        Ok(next)
    }

    #[cfg(test)]
    pub(crate) fn for_test(session_token: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::new(session_token.into(), None))
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_master(
        session_token: impl Into<String>,
        master_token: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self::new(session_token.into(), Some(master_token.into())))
    }
}

#[derive(Clone, Copy)]
struct RenewLimits {
    total: Duration,
    attempt: Duration,
}

struct RenewBackoff {
    step: usize,
}

impl RenewBackoff {
    const STEPS: [Duration; 4] = [
        Duration::from_millis(200),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(2),
    ];

    fn new() -> Self {
        Self { step: 0 }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = Self::STEPS[self.step];
        self.step = (self.step + 1).min(Self::STEPS.len() - 1);
        delay
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        ErrorKind,
        error::with_optional_query_id,
        test_support::http::{base_url, read_http_request, write_json_response},
    };

    const BUDGET: Duration = Duration::from_secs(10);
    const RENEWED: &str =
        r#"{"success":true,"data":{"sessionToken":"session-2","masterToken":"master-2"}}"#;

    fn api(listener: &TcpListener) -> Arc<ApiContext> {
        Arc::new(ApiContext::new(
            reqwest::Client::new(),
            base_url(listener.local_addr().unwrap()),
        ))
    }

    fn unreachable_api() -> Arc<ApiContext> {
        Arc::new(ApiContext::new(
            reqwest::Client::new(),
            reqwest::Url::parse("http://127.0.0.1:1/").unwrap(),
        ))
    }

    fn serve(
        listener: TcpListener,
        responses: Vec<(u16, &'static str)>,
    ) -> tokio::task::JoinHandle<Vec<String>> {
        tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                requests.push(read_http_request(&mut socket).await.unwrap());
                write_json_response(&mut socket, StatusCode::from_u16(status).unwrap(), body)
                    .await
                    .unwrap();
            }
            requests
        })
    }

    #[tokio::test]
    async fn renew_sends_master_token_and_swaps_both_tokens() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(listener, vec![(200, RENEWED)]);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let renewed = auth.renew(&api, &observed, BUDGET).await.unwrap();

        assert_eq!(renewed.authorization(), r#"Snowflake Token="session-2""#);
        assert_eq!(renewed.master_token.as_deref(), Some("master-2"));
        assert_eq!(renewed.generation, observed.generation + 1);
        assert_eq!(
            auth.snapshot().authorization(),
            r#"Snowflake Token="session-2""#
        );

        let requests = server.await.unwrap();
        let request = &requests[0];
        assert!(
            request.starts_with("POST /session/token-request?requestId="),
            "{request}"
        );
        assert!(request.contains(r#"authorization: Snowflake Token="master-1""#));
        assert!(request.contains("accept: application/snowflake"));
        let body: serde_json::Value =
            serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"oldSessionToken": "session-1", "requestType": "RENEW"})
        );
    }

    #[tokio::test]
    async fn renew_with_already_replaced_token_sends_nothing() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(listener, vec![(200, RENEWED)]);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        auth.renew(&api, &observed, BUDGET).await.unwrap();
        server.await.unwrap();

        // The listener is gone, so a second token request would fail instead of returning credentials.
        let current = auth.renew(&api, &observed, BUDGET).await.unwrap();
        assert_eq!(current.authorization(), r#"Snowflake Token="session-2""#);
    }

    #[tokio::test]
    async fn concurrent_renewals_of_one_token_send_a_single_request() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await.unwrap();
            // Hold the first renewal open long enough for the second caller to queue behind it.
            sleep(Duration::from_millis(100)).await;
            write_json_response(&mut socket, StatusCode::OK, RENEWED)
                .await
                .unwrap();
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        });

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let (first, second) = tokio::join!(
            auth.renew(&api, &observed, BUDGET),
            auth.renew(&api, &observed, BUDGET),
        );

        assert_eq!(first.unwrap().generation, 1);
        assert_eq!(second.unwrap().generation, 1);
        assert!(server.await.unwrap(), "only one token request must be sent");
    }

    #[tokio::test]
    async fn renew_without_master_token_sends_nothing() {
        let auth = SessionAuth::for_test("session-1");
        let observed = auth.snapshot();

        let error = auth
            .renew(&unreachable_api(), &observed, BUDGET)
            .await
            .unwrap_err();

        assert!(matches!(error, RenewError::NoMasterToken), "{error:?}");
    }

    #[tokio::test]
    async fn rejected_renewal_keeps_code_and_current_tokens() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(
            listener,
            vec![(
                200,
                r#"{"success":false,"code":"390114","message":"master token expired"}"#,
            )],
        );

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let error = auth.renew(&api, &observed, BUDGET).await.unwrap_err();

        let RenewError::Rejected { code, message } = error else {
            panic!("expected rejection");
        };
        assert_eq!(code.as_deref(), Some("390114"));
        assert_eq!(message.as_deref(), Some("master token expired"));
        assert_eq!(auth.snapshot().generation, 0);
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn renew_retries_transient_http_status_with_same_credentials() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(listener, vec![(503, "unavailable"), (200, RENEWED)]);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let renewed = auth.renew(&api, &observed, BUDGET).await.unwrap();

        assert_eq!(renewed.generation, 1);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert!(request.contains(r#"authorization: Snowflake Token="master-1""#));
            assert!(request.contains(r#""oldSessionToken":"session-1""#));
        }
    }

    #[tokio::test]
    async fn renew_does_not_retry_non_transient_http_status() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(listener, vec![(400, "bad request")]);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let error = auth.renew(&api, &observed, BUDGET).await.unwrap_err();

        assert!(
            matches!(
                error,
                RenewError::Network(NetworkError::HttpStatus { status: 400, .. })
            ),
            "{error:?}"
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }

    /// Accepts every connection and never answers.
    fn serve_silence(listener: TcpListener) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                sockets.push(listener.accept().await.unwrap().0);
            }
        })
    }

    fn expired() -> SessionExpiredError {
        SessionExpiredError::new(Some("390112".to_string()), None, None)
    }

    #[tokio::test]
    async fn renew_cut_short_by_the_caller_deadline_reports_the_caller_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve_silence(listener);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let error = auth
            .renew(&api, &observed, Duration::from_millis(100))
            .await
            .unwrap_err();

        assert!(matches!(error, RenewError::DeadlineElapsed), "{error:?}");
        assert_eq!(auth.snapshot().generation, 0);
        let error =
            with_optional_query_id(error.into_scoped(expired(), TimeoutError::query()), None);
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.to_string(), "timed out waiting for query response");
        server.abort();
    }

    #[tokio::test]
    async fn renew_exhausting_its_own_limit_reports_a_renewal_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve_silence(listener);

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let limits = RenewLimits {
            total: Duration::from_millis(300),
            attempt: Duration::from_millis(100),
        };
        // The caller still has an hour left, so its own timeout must not be blamed.
        let error = auth
            .renew_with_limits(&api, &observed, Duration::from_secs(3600), limits)
            .await
            .unwrap_err();

        assert!(matches!(error, RenewError::RenewalTimeout), "{error:?}");
        assert_eq!(auth.snapshot().generation, 0);
        let error =
            with_optional_query_id(error.into_scoped(expired(), TimeoutError::query()), None);
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.to_string(), "timed out renewing the session token");
        server.abort();
    }

    #[tokio::test]
    async fn renew_retries_after_a_token_request_that_never_answers() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = tokio::spawn(async move {
            // Keep the first connection open without answering; answer the second.
            let (_stalled, _) = listener.accept().await.unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await.unwrap();
            write_json_response(&mut socket, StatusCode::OK, RENEWED)
                .await
                .unwrap();
        });

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let observed = auth.snapshot();
        let limits = RenewLimits {
            total: Duration::from_secs(10),
            attempt: Duration::from_millis(100),
        };
        let renewed = auth
            .renew_with_limits(&api, &observed, Duration::from_secs(3600), limits)
            .await
            .unwrap();

        assert_eq!(renewed.generation, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn renew_response_without_master_token_keeps_current_one() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api = api(&listener);
        let server = serve(
            listener,
            vec![
                (
                    200,
                    r#"{"success":true,"data":{"sessionToken":"session-2"}}"#,
                ),
                (
                    200,
                    r#"{"success":true,"data":{"sessionToken":"session-3"}}"#,
                ),
            ],
        );

        let auth = SessionAuth::for_test_with_master("session-1", "master-1");
        let first = auth.snapshot();
        let second = auth.renew(&api, &first, BUDGET).await.unwrap();
        assert_eq!(second.master_token.as_deref(), Some("master-1"));
        auth.renew(&api, &second, BUDGET).await.unwrap();

        let requests = server.await.unwrap();
        assert!(requests[1].contains(r#"authorization: Snowflake Token="master-1""#));
        assert!(requests[1].contains(r#""oldSessionToken":"session-2""#));
    }

    #[test]
    fn debug_redacts_both_tokens() {
        let auth = SessionAuth::for_test_with_master("session-secret", "master-secret");

        let rendered = format!("{auth:?} {:?}", auth.snapshot());
        assert!(!rendered.contains("session-secret"), "{rendered}");
        assert!(!rendered.contains("master-secret"), "{rendered}");
    }
}
