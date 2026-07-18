use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use uuid::Uuid;

use crate::{
    ClientShared, IntoStatement, Result,
    result_cursor::{ResultCursor, TypedResultCursor},
    result_table::FromRow,
    statement::{QueryControl, QueryHandle, StatementExecutor, builder::into_statement_parts},
};

pub struct Session {
    pub(crate) shared: Arc<ClientShared>,
    pub(crate) auth: Arc<SessionAuth>,
}

/// Per-query overrides for a single [`Session::query_with_options`] / [`Session::query_as_with_options`] /
/// [`Session::query_handle_with_options`] call.
///
/// Unset options inherit the defaults from [`QueryConfig`](crate::QueryConfig). These options sit between the
/// client-wide query defaults and per-collect [`CollectOptions`](crate::CollectOptions).
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub(crate) query_response_timeout: Option<Duration>,
    pub(crate) query_cancel_request_timeout: Option<Duration>,
    pub(crate) collect_prefetch_concurrency: Option<NonZeroUsize>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides [`QueryConfig::with_query_response_timeout`](crate::QueryConfig::with_query_response_timeout) for
    /// this query.
    pub fn with_query_response_timeout(mut self, timeout: Duration) -> Self {
        self.query_response_timeout = Some(timeout);
        self
    }

    /// Overrides [`QueryConfig::with_query_cancel_request_timeout`](crate::QueryConfig::with_query_cancel_request_timeout)
    /// for this query's explicit cancellation requests.
    pub fn with_query_cancel_request_timeout(mut self, timeout: Duration) -> Self {
        self.query_cancel_request_timeout = Some(timeout);
        self
    }

    /// Overrides [`QueryConfig::with_collect_prefetch_concurrency`](crate::QueryConfig::with_collect_prefetch_concurrency)
    /// for the cursor returned by this query.
    ///
    /// This sets the default collect policy embedded in the returned [`ResultCursor`]; a later
    /// [`CollectOptions::with_prefetch_concurrency`](crate::CollectOptions::with_prefetch_concurrency) still has the
    /// final say for a specific collection call.
    pub fn with_collect_prefetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.collect_prefetch_concurrency = Some(concurrency);
        self
    }
}

/// Per-session authentication state.
pub(crate) struct SessionAuth {
    pub(crate) session_token: String,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The session token authenticates every request; never print it.
        f.debug_struct("Session")
            .field("base_url", &self.shared.base_url)
            .field("session_token", &"<redacted>")
            .field("query", &self.shared.query)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl SessionAuth {
    /// Build a per-session auth handle for unit tests.
    pub(crate) fn for_test(session_token: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            session_token: session_token.into(),
        })
    }
}

impl Session {
    /// Submit a statement and return a `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::BindEncode`, `ErrorKind::Network`, `ErrorKind::Server`, `ErrorKind::SessionExpired`,
    /// `ErrorKind::Timeout`, or `ErrorKind::Protocol`.
    pub async fn query<S: IntoStatement>(&self, statement: S) -> Result<ResultCursor> {
        self.query_with_options(statement, QueryOptions::default())
            .await
    }

    /// Submit a statement with per-query [`QueryOptions`] and return a `ResultCursor` for streaming partition access.
    ///
    /// Unset options inherit the configured [`QueryConfig`](crate::QueryConfig) defaults.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`].
    pub async fn query_with_options<S>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> Result<ResultCursor>
    where
        S: IntoStatement,
    {
        self.query_handle_with_options(statement, options)?
            .execute()
            .await
    }

    /// Submit a statement and return a typed `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`], including `ErrorKind::BindEncode` when bind values cannot be encoded or
    /// validated before the request is sent. After the statement succeeds, this also propagates any error returned by
    /// [`FromRow::build_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use `ErrorKind::Decode` when the result schema does not match `T`;
    /// inspect the detail via [`Error::as_schema_error`](crate::Error::as_schema_error). For custom plan-time validation
    /// from hand-written decoders, inspect [`Error::as_custom_plan_error`](crate::Error::as_custom_plan_error).
    pub async fn query_as<T, S>(&self, statement: S) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        self.query_as_with_options(statement, QueryOptions::default())
            .await
    }

    /// Submit a statement with per-query [`QueryOptions`] and return a typed `ResultCursor`.
    ///
    /// Unset options inherit the configured [`QueryConfig`](crate::QueryConfig) defaults.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`]. After the statement succeeds, this also propagates plan-time
    /// decode failures from [`FromRow::build_plan`] for `T`: use [`Error::as_schema_error`](crate::Error::as_schema_error)
    /// for schema mismatches and [`Error::as_custom_plan_error`](crate::Error::as_custom_plan_error) for custom
    /// hand-written decoder validation.
    pub async fn query_as_with_options<T, S>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        self.query_handle_with_options(statement, options)?
            .execute_as::<T>()
            .await
    }

    /// Create a query whose execution can be cancelled explicitly.
    ///
    /// Use this instead of [`Session::query`] when a running query may have to be cancelled: dropping a query future
    /// never cancels the query on Snowflake, so explicit cancellation requires retaining a
    /// [`QueryCanceller`](crate::QueryCanceller) via [`QueryHandle::canceller`](crate::QueryHandle::canceller) before
    /// driving [`QueryHandle::execute`]. The canceller then aborts the query from any task, independently of the
    /// execution future. If you never need to cancel, [`Session::query`] is equivalent and simpler.
    ///
    /// No HTTP request is sent until [`QueryHandle::execute`] is polled.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::BindEncode` when bind values cannot be validated before the request is sent.
    pub fn query_handle<S: IntoStatement>(&self, statement: S) -> Result<QueryHandle> {
        self.query_handle_with_options(statement, QueryOptions::default())
    }

    /// Create a query whose execution can be cancelled explicitly, with per-query [`QueryOptions`].
    ///
    /// Unset options inherit the configured [`QueryConfig`](crate::QueryConfig) defaults.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query_handle`].
    pub fn query_handle_with_options<S>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> Result<QueryHandle>
    where
        S: IntoStatement,
    {
        let parts = into_statement_parts(statement)?;
        let settings = self.shared.query.resolve_options(options);
        let query_request_id = Arc::from(Uuid::new_v4().to_string());
        let control = QueryControl::new(query_request_id);
        let executor = StatementExecutor::new(self, settings);
        Ok(QueryHandle::new(parts, executor, control))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::Url;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
        time::timeout,
    };

    use super::*;
    use crate::{
        ClientShared, ErrorKind, QueryCancelStatus, QueryConfig, Statement, runtime::QueryRuntime,
    };

    fn test_session(base_url: Url) -> Session {
        test_session_with_config(base_url, QueryConfig::default())
    }

    fn test_session_with_config(base_url: Url, config: QueryConfig) -> Session {
        Session {
            shared: ClientShared::for_test_with(
                reqwest::Client::new(),
                base_url,
                config.into(),
                QueryRuntime::new(),
            ),
            auth: SessionAuth::for_test("test-token"),
        }
    }

    #[tokio::test]
    async fn cancel_before_execute_sends_neither_query_nor_abort() {
        let session = test_session(Url::parse("http://127.0.0.1:1/").unwrap());
        let handle = session.query_handle("SELECT 1").unwrap();
        let canceller = handle.canceller();

        assert_eq!(
            handle.request_id(),
            canceller.request_id(),
            "handle and canceller must share the request identity"
        );
        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::NotSubmitted
        );

        let error = handle.execute().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert!(error.is_cancelled());
        assert_eq!(error.snowflake_code(), None);
        assert_eq!(error.snowflake_message(), None);
        assert_eq!(error.query_id(), None);
    }

    #[tokio::test]
    async fn cancel_after_submit_returns_accepted_and_execution_is_cancelled() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            let query_request = read_http_request(&mut query_socket).await;
            query_ready_tx.send(query_request.clone()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            let abort_request = read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;
            write_http_response(
                &mut query_socket,
                200,
                r#"{"code":"000604","message":"SQL execution canceled","success":false,"data":{"queryId":"query-id"}}"#,
            )
            .await;
            (query_request, abort_request)
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let request_id = handle.request_id().to_owned();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        let _query_request = query_ready_rx.await.unwrap();
        let outcome = canceller.cancel().await.unwrap();
        let error = execution.await.unwrap().unwrap_err();
        let (query_request, abort_request) = server.await.unwrap();

        assert_eq!(outcome, QueryCancelStatus::Accepted);
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert_eq!(error.snowflake_code(), Some("000604"));
        assert_eq!(error.snowflake_message(), Some("SQL execution canceled"));
        assert_eq!(error.query_id(), Some("query-id"));

        let query_target = query_request.split_whitespace().nth(1).unwrap();
        let query_url = Url::parse(&format!("http://localhost{query_target}")).unwrap();
        assert_eq!(
            query_url
                .query_pairs()
                .find_map(|(key, value)| (key == "requestId").then(|| value.into_owned()))
                .as_deref(),
            Some(request_id.as_str())
        );
        assert_eq!(
            abort_request.split("\r\n\r\n").nth(1).unwrap(),
            format!(r#"{{"requestId":"{request_id}"}}"#)
        );
    }

    #[tokio::test]
    async fn canceller_can_abort_after_execution_future_is_dropped() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            let query_request = read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();
            let (mut abort_socket, _) = listener.accept().await.unwrap();
            let abort_request = read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;
            (query_request, abort_request)
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let task = tokio::spawn(handle.execute());
        query_ready_rx.await.unwrap();
        task.abort();
        let _ = task.await;

        let outcome = canceller.cancel().await.unwrap();
        assert_eq!(outcome, QueryCancelStatus::Accepted);
        let (_query_request, abort_request) = server.await.unwrap();
        assert!(abort_request.contains(r#"{"requestId":""#));
    }

    #[tokio::test]
    async fn finished_query_cancel_is_local_and_does_not_send_abort() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _request = read_http_request(&mut socket).await;
            write_http_response(
                &mut socket,
                200,
                r#"{"success":true,"data":{"queryId":"query-id","rowset":[],"rowtype":[],"queryResultFormat":"json"}}"#,
            )
            .await;
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT 1").unwrap();
        let canceller = handle.canceller();
        let _cursor = handle.execute().await.unwrap();

        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::AlreadyFinished
        );
        assert!(server.await.unwrap());
    }

    #[test]
    fn query_handle_validates_named_bind_keys_synchronously() {
        let session = test_session(Url::parse("http://127.0.0.1:1/").unwrap());
        let error = match session.query_handle(Statement::new("SELECT :id").bind_named("", 1_i64)) {
            Ok(_) => panic!("empty bind names must fail synchronously"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::BindEncode);
    }

    const CANCELLED_QUERY_RESPONSE: &str = r#"{"code":"000604","sqlState":"57014","message":"SQL execution canceled","success":false,"data":{"queryId":"query-id"}}"#;

    fn abort_request_id(request: &str) -> String {
        let target = request.split_whitespace().nth(1).unwrap();
        Url::parse(&format!("http://localhost{target}"))
            .unwrap()
            .query_pairs()
            .find_map(|(key, value)| (key == "requestId").then(|| value.into_owned()))
            .unwrap()
    }

    #[tokio::test]
    async fn concurrent_cancel_sends_a_single_abort() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;

            // The first accepted abort caches its outcome, so a concurrent caller must not open a second connection.
            let no_second_abort = timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err();
            write_http_response(&mut query_socket, 200, CANCELLED_QUERY_RESPONSE).await;
            no_second_abort
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller_a = handle.canceller();
        let canceller_b = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        let (first, second) = tokio::join!(canceller_a.cancel(), canceller_b.cancel());
        assert_eq!(first.unwrap(), QueryCancelStatus::Accepted);
        assert_eq!(second.unwrap(), QueryCancelStatus::Accepted);

        let error = execution.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert!(
            server.await.unwrap(),
            "single-flight must send exactly one abort for concurrent cancels"
        );
    }

    #[tokio::test]
    async fn public_cancel_retry_uses_a_fresh_request_id_after_a_failed_attempt() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut first_abort, _) = listener.accept().await.unwrap();
            let first_request = read_http_request(&mut first_abort).await;
            write_http_response(&mut first_abort, 400, r#"{"error":"bad request"}"#).await;

            let (mut second_abort, _) = listener.accept().await.unwrap();
            let second_request = read_http_request(&mut second_abort).await;
            write_http_response(&mut second_abort, 200, r#"{"success":true}"#).await;

            write_http_response(&mut query_socket, 200, CANCELLED_QUERY_RESPONSE).await;
            (first_request, second_request)
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let request_id = handle.request_id().to_owned();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        // A non-retryable abort failure must not cache success, so the next explicit call retries with a new id.
        assert_eq!(
            canceller.cancel().await.unwrap_err().kind(),
            ErrorKind::Network
        );
        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::Accepted
        );

        let error = execution.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);

        let (first_request, second_request) = server.await.unwrap();
        assert_ne!(
            abort_request_id(&first_request),
            abort_request_id(&second_request),
            "each explicit cancel call must use a fresh cancel-request-id"
        );
        let expected_body = format!(r#"{{"requestId":"{request_id}"}}"#);
        assert_eq!(
            first_request.split("\r\n\r\n").nth(1).unwrap(),
            expected_body,
            "the abort body must keep targeting the original query-request-id"
        );
        assert_eq!(
            second_request.split("\r\n\r\n").nth(1).unwrap(),
            expected_body
        );
    }

    #[tokio::test]
    async fn dropping_the_cancel_future_issues_no_further_abort() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            // Accept the one abort and never answer it; the client drops the future while still awaiting the response.
            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;

            let no_retry = timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err();
            drop(abort_socket);
            drop(query_socket);
            no_retry
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        let dropped = timeout(Duration::from_millis(150), canceller.cancel()).await;
        assert!(
            dropped.is_err(),
            "cancel must still be awaiting the unanswered abort when dropped"
        );

        assert!(
            server.await.unwrap(),
            "a dropped cancel future must not leave a background retry running"
        );
        execution.abort();
        let _ = execution.await;
    }

    const SUCCESS_QUERY_RESPONSE: &str = r#"{"success":true,"data":{"queryId":"query-id","rowset":[],"rowtype":[],"queryResultFormat":"json"}}"#;

    #[tokio::test]
    async fn cancel_after_response_timeout_still_sends_abort() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // Accept the query but never answer it, so the client-side query-response deadline elapses and the
            // execution guard drops into `OutcomeUnknown`.
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            let abort_request = read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;
            drop(query_socket);
            abort_request
        });

        let session = test_session_with_config(
            Url::parse(&format!("http://{addr}/")).unwrap(),
            QueryConfig::default().with_query_response_timeout(Duration::from_millis(150)),
        );
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let request_id = handle.request_id().to_owned();
        let canceller = handle.canceller();

        let error = handle.execute().await.unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::Timeout,
            "the unanswered submit must time out"
        );

        // The dropped guard left the query in `OutcomeUnknown`, so the retained canceller must still send an abort.
        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::Accepted
        );
        let abort_request = server.await.unwrap();
        assert_eq!(
            abort_request.split("\r\n\r\n").nth(1).unwrap(),
            format!(r#"{{"requestId":"{request_id}"}}"#)
        );
    }

    #[tokio::test]
    async fn normal_completion_after_cancel_intent_yields_result_and_accepted() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;

            // The query completes successfully before its abort is answered: a recorded cancel intent must not turn a
            // non-cancellation terminal response into a `Cancelled` error.
            write_http_response(&mut query_socket, 200, SUCCESS_QUERY_RESPONSE).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        let outcome = canceller.cancel().await.unwrap();
        let cursor = execution.await.unwrap();

        assert_eq!(outcome, QueryCancelStatus::Accepted);
        assert!(
            cursor.is_ok(),
            "a successful terminal response must build a cursor despite the cancel intent"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_intent_does_not_cancel_a_successful_response_carrying_a_marker() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;

            // A `success:true` payload is authoritative even when it carries a cancellation code, so the cancel intent
            // must not discard it as a `Cancelled` error.
            write_http_response(
                &mut query_socket,
                200,
                r#"{"code":"000604","success":true,"data":{"queryId":"query-id","rowset":[],"rowtype":[],"queryResultFormat":"json"}}"#,
            )
            .await;
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::Accepted
        );

        let cursor = execution.await.unwrap();
        assert!(
            cursor.is_ok(),
            "a successful marked response must build a cursor, not a Cancelled error"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_recognized_from_sql_state_only_terminal_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;
            write_http_response(&mut abort_socket, 200, r#"{"success":true}"#).await;

            // No top-level `code`; cancellation is signaled only through `sqlState`.
            write_http_response(
                &mut query_socket,
                200,
                r#"{"sqlState":"57014","message":"SQL execution canceled","success":false,"data":{"queryId":"query-id"}}"#,
            )
            .await;
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        assert_eq!(
            canceller.cancel().await.unwrap(),
            QueryCancelStatus::Accepted
        );

        let error = execution.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert!(error.is_cancelled());
        assert_eq!(
            error.snowflake_code(),
            None,
            "a sqlState-only response carries no Snowflake code"
        );
        assert_eq!(error.snowflake_message(), Some("SQL execution canceled"));
        assert_eq!(error.query_id(), Some("query-id"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn abort_session_expired_response_is_session_expired_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (query_ready_tx, query_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut query_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut query_socket).await;
            query_ready_tx.send(()).unwrap();

            let (mut abort_socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut abort_socket).await;
            write_http_response(
                &mut abort_socket,
                200,
                r#"{"success":false,"code":"390112","message":"session expired"}"#,
            )
            .await;
            drop(query_socket);
        });

        let session = test_session(Url::parse(&format!("http://{addr}/")).unwrap());
        let handle = session.query_handle("SELECT SYSTEM$WAIT(120)").unwrap();
        let canceller = handle.canceller();
        let execution = tokio::spawn(handle.execute());

        query_ready_rx.await.unwrap();
        let error = canceller.cancel().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SessionExpired);
        assert_eq!(error.snowflake_code(), Some("390112"));

        execution.abort();
        let _ = execution.await;
        server.await.unwrap();
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        let body_start;
        let body_length;

        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before request completed");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                body_start = index + 4;
                let headers = String::from_utf8_lossy(&bytes[..index]);
                body_length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .or_else(|| line.strip_prefix("Content-Length:"))
                    })
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                break;
            }
        }

        while bytes.len() < body_start + body_length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before request body completed");
            bytes.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    async fn write_http_response(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
        let response = format!(
            "HTTP/1.1 {status} OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}
