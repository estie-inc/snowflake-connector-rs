use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    error::{ProtocolError, QueryScopedError, QueryScopedResult, ServerError, SessionExpiredError},
    result_cursor::{CollectPolicy, RemotePartitionSource, ResultCursor},
    runtime::QueryRuntime,
    statement::StatementParts,
    {Error, Result, Session},
};

use super::{
    client::{QueryResponseDeadline, StatementApiClient},
    manifest::ResultManifest,
    wire::response::{
        QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, RawQueryResponse, SESSION_EXPIRED,
        SnowflakeResponse,
    },
};

pub(crate) struct StatementExecutor {
    api: StatementApiClient,
    query_response_timeout: Duration,
    default_collect_concurrency: NonZeroUsize,
    runtime: QueryRuntime,
}

struct QueryScopedResponse {
    query_id: Arc<str>,
    response: SnowflakeResponse,
}

impl QueryScopedResponse {
    fn from_submit(response: SnowflakeResponse) -> Result<Self> {
        match response
            .data
            .as_ref()
            .map(|data| Arc::clone(&data.query_id))
        {
            Some(query_id) => Ok(Self { query_id, response }),
            None if response.code.as_deref() == Some(SESSION_EXPIRED) => {
                Err(SessionExpiredError::new(response.code, response.message, None).into())
            }
            None if !response.success => {
                Err(ServerError::new(response.code, response.message, None).into())
            }
            None => Err(ProtocolError::missing_field("data").into()),
        }
    }

    fn into_parts(self) -> (Arc<str>, SnowflakeResponse) {
        (self.query_id, self.response)
    }
}

impl StatementExecutor {
    pub(crate) fn new(session: &Session) -> Self {
        Self {
            api: StatementApiClient::new(Arc::clone(&session.shared), Arc::clone(&session.auth)),
            query_response_timeout: session.shared.query.query_response_timeout(),
            default_collect_concurrency: session.shared.query.collect_prefetch_concurrency(),
            runtime: session.shared.runtime.clone(),
        }
    }

    pub(crate) async fn execute(self, parts: StatementParts) -> Result<ResultCursor> {
        let deadline = QueryResponseDeadline::new(self.query_response_timeout);
        let response = QueryScopedResponse::from_submit(self.api.submit(&parts, deadline).await?)?;
        self.execute_query_scoped(response, deadline)
            .await
            .map_err(Error::from)
    }

    async fn execute_query_scoped(
        self,
        response: QueryScopedResponse,
        deadline: QueryResponseDeadline,
    ) -> QueryScopedResult<ResultCursor> {
        let (query_id, mut response) = response.into_parts();

        let response_code = response.code.as_deref();
        if response_code == Some(QUERY_IN_PROGRESS_ASYNC_CODE)
            || response_code == Some(QUERY_IN_PROGRESS_CODE)
        {
            let data = response
                .data
                .take()
                .expect("query-scoped responses always carry a query id in data");
            let Some(result_url) = data.get_result_url else {
                return Err(QueryScopedError::new(
                    query_id,
                    ProtocolError::no_polling_url(),
                ));
            };

            response = self
                .api
                .poll_async_results(&result_url, deadline, Arc::clone(&query_id))
                .await?;
        }

        if let Some(SESSION_EXPIRED) = response.code.as_deref() {
            return Err(QueryScopedError::new(
                query_id,
                SessionExpiredError::new(
                    response.code,
                    response.message,
                    response.data.as_ref().map(|data| data.query_id.clone()),
                ),
            ));
        }

        if !response.success {
            return Err(QueryScopedError::new(
                query_id,
                ServerError::new(
                    response.code,
                    response.message,
                    response.data.as_ref().map(|data| data.query_id.clone()),
                ),
            ));
        }

        let Some(data) = response.data else {
            return Err(QueryScopedError::new(
                query_id,
                ProtocolError::missing_field("data"),
            ));
        };

        if let Some(ref format) = data.query_result_format
            && format != "json"
        {
            return Err(QueryScopedError::new(
                query_id,
                ProtocolError::unsupported_result_format(format.clone()),
            ));
        }

        self.build_result_set(data)
    }

    fn build_result_set(self, data: RawQueryResponse) -> QueryScopedResult<ResultCursor> {
        let manifest = ResultManifest::try_from(data)?;

        let source = RemotePartitionSource::new(manifest.lease, self.api.http_client());
        let default_collect_policy = CollectPolicy::new(self.default_collect_concurrency);

        Ok(ResultCursor::new(
            manifest.snapshot,
            manifest.inline_rowset,
            source,
            self.runtime,
            default_collect_policy,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener as StdTcpListener,
        num::NonZeroUsize,
        thread,
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use reqwest::{Client, Url};
    use tokio::time::timeout;

    use super::*;
    use crate::{
        ClientShared, ErrorKind, QueryConfig, Statement,
        rowset::BLOCKING_PARSE_CELLS,
        runtime::QueryRuntime,
        session::SessionAuth,
        statement::{
            StatementParts,
            builder::into_statement_parts,
            client::StatementApiClient,
            wire::response::{RawQueryResponse, RawQueryResponseRowType},
        },
    };

    fn test_statement_api(base_url: Url) -> StatementApiClient {
        StatementApiClient::new(
            ClientShared::for_test(base_url),
            SessionAuth::for_test("test-token"),
        )
    }

    fn test_session(base_url: Url) -> Session {
        Session {
            shared: ClientShared::for_test(base_url),
            auth: SessionAuth::for_test("test-token"),
        }
    }

    fn executor() -> StatementExecutor {
        StatementExecutor {
            api: test_statement_api(Url::parse("https://example.com/").unwrap()),
            query_response_timeout: crate::config::DEFAULT_QUERY_RESPONSE_TIMEOUT,
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime: QueryRuntime::new(),
        }
    }

    fn executor_with_timeout(base_url: Url, timeout: Duration) -> StatementExecutor {
        StatementExecutor {
            api: test_statement_api(base_url),
            query_response_timeout: timeout,
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime: QueryRuntime::new(),
        }
    }

    fn spawn_single_response_server(body: &'static str) -> Url {
        let listener = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        Url::parse(&format!("http://{addr}/")).unwrap()
    }

    fn spawn_disconnect_server() -> Url {
        let listener = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
        });

        Url::parse(&format!("http://{addr}/")).unwrap()
    }

    /// One accepted connection's behavior for [`spawn_scripted_server`].
    enum ServerStep {
        /// Read the request and immediately respond with the given JSON body.
        Respond(&'static str),
        /// Read the request, wait, then respond with the given JSON body.
        RespondAfter(Duration, &'static str),
        /// Read the request and hold the connection open without responding, emulating a server-side long-poll that
        /// outlives the client-side deadline.
        Block,
    }

    fn spawn_scripted_server(steps: Vec<ServerStep>) -> Url {
        let listener = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            for step in steps {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0_u8; 4096];
                let _ = stream.read(&mut buf);

                let body = match step {
                    ServerStep::Respond(body) => body,
                    ServerStep::RespondAfter(delay, body) => {
                        thread::sleep(delay);
                        body
                    }
                    ServerStep::Block => {
                        // Keep the connection open so the client's own deadline is what returns control.
                        thread::sleep(Duration::from_secs(30));
                        continue;
                    }
                };

                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        Url::parse(&format!("http://{addr}/")).unwrap()
    }

    fn text_row_type(name: &str) -> RawQueryResponseRowType {
        RawQueryResponseRowType {
            name: name.to_string(),
            nullable: false,
            scale: None,
            length: Some(16),
            precision: None,
            data_type: "text".to_string(),
        }
    }

    fn whitespace_empty_inline_response(rowset: &'static [u8]) -> RawQueryResponse {
        RawQueryResponse {
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: None,
            total: None,
            row_set_bytes: Some(Bytes::from_static(rowset)),
            row_types: Some(vec![text_row_type("X")]),
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        }
    }

    fn select_1_parts() -> StatementParts {
        into_statement_parts(Statement::from("select 1"))
    }

    async fn execute_single_response_err(body: &'static str) -> crate::Error {
        let session = test_session(spawn_single_response_server(body));

        match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("expected statement execution to fail"),
            Err(err) => err,
        }
    }

    #[test]
    fn statement_executor_reuses_session_query_runtime() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let session = Session {
            shared: ClientShared::for_test_with(
                Client::new(),
                Url::parse("https://example.com/").unwrap(),
                QueryConfig::default().into(),
                runtime.clone(),
            ),
            auth: SessionAuth::for_test("test-token"),
        };

        let executor = StatementExecutor::new(&session);
        assert!(
            executor
                .runtime
                .blocking_parse_limiter()
                .ptr_eq(&runtime.blocking_parse_limiter())
        );
    }

    #[tokio::test]
    async fn execute_async_response_invalid_polling_urls_are_protocol_errors() {
        for (label, body, expected_message) in [
            (
                "malformed absolute URL",
                r#"{"code":"333334","success":true,"data":{"queryId":"query-id","getResultUrl":"http://[::1"}}"#,
                "invalid URL in Snowflake response field data.getResultUrl; value: http://[::1",
            ),
            (
                "cross-origin absolute URL",
                r#"{"code":"333334","success":true,"data":{"queryId":"query-id","getResultUrl":"https://attacker.example/query"}}"#,
                "invalid Snowflake response field data.getResultUrl: must be relative or same-origin absolute URL",
            ),
            (
                "empty polling URL",
                r#"{"code":"333334","success":true,"data":{"queryId":"query-id","getResultUrl":"   "}}"#,
                "invalid Snowflake response field data.getResultUrl: must not be empty",
            ),
        ] {
            let err = execute_single_response_err(body).await;
            assert_eq!(err.kind(), ErrorKind::Protocol, "{label}");
            assert_eq!(err.to_string(), expected_message, "{label}");
            assert_eq!(err.query_id(), Some("query-id"), "{label}");
        }
    }

    #[tokio::test]
    async fn execute_async_response_without_polling_url_preserves_query_id() {
        let err = execute_single_response_err(
            r#"{"code":"333334","success":true,"data":{"queryId":"query-id"}}"#,
        )
        .await;

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "async response doesn't contain a URL to poll for results"
        );
        assert_eq!(err.query_id(), Some("query-id"));
    }

    // These two are intentionally byte-identical: an async submit response and an in-progress poll response both carry
    // code 333334 with the same query id and poll URL. The distinct names document which step each server reply plays.
    const ASYNC_SUBMIT_RESPONSE: &str =
        r#"{"code":"333334","success":true,"data":{"queryId":"query-id","getResultUrl":"/poll"}}"#;
    const ASYNC_IN_PROGRESS_RESPONSE: &str =
        r#"{"code":"333334","success":true,"data":{"queryId":"query-id","getResultUrl":"/poll"}}"#;
    const FINAL_INLINE_RESPONSE: &str = r#"{"success":true,"data":{"queryId":"query-id","rowset":[["x"]],"rowtype":[{"name":"X","nullable":false,"length":16,"type":"text"}],"queryResultFormat":"json"}}"#;

    #[test]
    fn query_response_deadline_clamps_overflowing_timeout() {
        // An extreme timeout from the public setter must not panic on `Instant + Duration`; it clamps to a far-off
        // deadline that leaves effectively unbounded remaining budget.
        let deadline = QueryResponseDeadline::new(Duration::from_secs(u64::MAX));
        assert!(deadline.remaining() > Duration::from_secs(60 * 60 * 24 * 300));
    }

    #[tokio::test]
    async fn execute_submit_timeout_has_no_query_id() {
        // The submit request never gets a response, so no query id is known when the deadline elapses.
        let executor = executor_with_timeout(
            spawn_scripted_server(vec![ServerStep::Block]),
            Duration::from_millis(100),
        );

        let err = match executor.execute(select_1_parts()).await {
            Ok(_) => panic!("submit timeout must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(err.to_string(), "timed out waiting for query response");
        assert_eq!(err.query_id(), None);
    }

    #[tokio::test]
    async fn execute_async_poll_timeout_preserves_query_id() {
        // Submit resolves to an async response, one poll returns in-progress, then the deadline elapses.
        let executor = executor_with_timeout(
            spawn_scripted_server(vec![
                ServerStep::Respond(ASYNC_SUBMIT_RESPONSE),
                ServerStep::Respond(ASYNC_IN_PROGRESS_RESPONSE),
            ]),
            Duration::from_millis(150),
        );

        let err = match executor.execute(select_1_parts()).await {
            Ok(_) => panic!("poll timeout must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[tokio::test]
    async fn execute_blocking_poll_is_interrupted_by_deadline() {
        // A single poll GET blocks past the deadline (server-side long-poll); the client-side timeout returns control
        // and the query id from the async submit response is preserved.
        let executor = executor_with_timeout(
            spawn_scripted_server(vec![
                ServerStep::Respond(ASYNC_SUBMIT_RESPONSE),
                ServerStep::Block,
            ]),
            Duration::from_millis(150),
        );

        let err = match executor.execute(select_1_parts()).await {
            Ok(_) => panic!("blocking poll must time out"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[tokio::test]
    async fn execute_timeout_budget_spans_submit_and_poll() {
        // Submit consumes half of the budget before responding, then the poll blocks. If the budget were reset when
        // polling began, the total wait would be the submit delay plus a fresh full budget; instead it tracks the
        // single shared deadline.
        let executor = executor_with_timeout(
            spawn_scripted_server(vec![
                ServerStep::RespondAfter(Duration::from_millis(200), ASYNC_SUBMIT_RESPONSE),
                ServerStep::Block,
            ]),
            Duration::from_millis(400),
        );

        let start = Instant::now();
        let err = match executor.execute(select_1_parts()).await {
            Ok(_) => panic!("blocking poll must time out"),
            Err(err) => err,
        };
        let elapsed = start.elapsed();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(err.query_id(), Some("query-id"));
        assert!(
            elapsed >= Duration::from_millis(350),
            "deadline fired before spanning submit and poll: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(550),
            "poll appears to have reset the timeout budget: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn execute_async_final_response_within_deadline_succeeds() {
        let executor = executor_with_timeout(
            spawn_scripted_server(vec![
                ServerStep::Respond(ASYNC_SUBMIT_RESPONSE),
                ServerStep::Respond(FINAL_INLINE_RESPONSE),
            ]),
            Duration::from_secs(30),
        );

        let mut result = executor
            .execute(select_1_parts())
            .await
            .expect("final response within deadline must build a cursor");
        let table = result
            .next_table()
            .await
            .unwrap()
            .expect("inline partition");
        assert_eq!(table.row_count(), 1);
    }

    #[tokio::test]
    async fn execute_unsupported_result_format_preserves_query_id() {
        let session = test_session(spawn_single_response_server(
            r#"{"success":true,"data":{"queryId":"query-id","queryResultFormat":"arrow"}}"#,
        ));

        let err = match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("unsupported result format must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.to_string(), "unsupported result format: arrow");
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[tokio::test]
    async fn execute_session_expired_preserves_snowflake_fields() {
        let session = test_session(spawn_single_response_server(
            r#"{"code":"390112","message":"Your session has expired. Please login again.","success":false,"data":{"queryId":"query-id"}}"#,
        ));

        let err = match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("session expired response must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert!(err.is_session_expired());
        assert_eq!(err.snowflake_code(), Some("390112"));
        assert_eq!(
            err.snowflake_message(),
            Some("Your session has expired. Please login again.")
        );
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[tokio::test]
    async fn execute_session_expired_without_query_id_preserves_snowflake_fields() {
        let session = test_session(spawn_single_response_server(
            r#"{"code":"390112","message":"Your session has expired. Please login again.","success":false,"data":null}"#,
        ));

        let err = match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("session expired response must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert!(err.is_session_expired());
        assert_eq!(err.snowflake_code(), Some("390112"));
        assert_eq!(
            err.snowflake_message(),
            Some("Your session has expired. Please login again.")
        );
        assert_eq!(err.query_id(), None);
    }

    #[tokio::test]
    async fn execute_server_error_preserves_query_id() {
        let session = test_session(spawn_single_response_server(
            r#"{"code":"123456","message":"statement failed","success":false,"data":{"queryId":"query-id"}}"#,
        ));

        let err = match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("server error response must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Server);
        assert_eq!(err.snowflake_code(), Some("123456"));
        assert_eq!(err.snowflake_message(), Some("statement failed"));
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[tokio::test]
    async fn execute_network_failure_has_no_query_id() {
        let session = test_session(spawn_disconnect_server());

        let err = match StatementExecutor::new(&session)
            .execute(select_1_parts())
            .await
        {
            Ok(_) => panic!("network failure must not yield a result set"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), None);
    }

    #[tokio::test]
    async fn inline_result_set_build_does_not_wait_for_blocking_parse_permit() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let permit = runtime.blocking_parse_limiter().acquire_owned().await;
        let executor = StatementExecutor {
            api: test_statement_api(Url::parse("https://example.com/").unwrap()),
            query_response_timeout: crate::config::DEFAULT_QUERY_RESPONSE_TIMEOUT,
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: Some(BLOCKING_PARSE_CELLS as i64),
            total: None,
            row_set_bytes: Some(Bytes::from_static(br#"[["x"]]"#)),
            row_types: Some(vec![text_row_type("X")]),
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        };

        let mut result = match timeout(Duration::from_millis(20), async move {
            executor.build_result_set(response)
        })
        .await
        {
            Err(_) => panic!("expected build_result_set to return before inline parse"),
            Ok(Err(err)) => panic!("expected build_result_set to succeed, got {err:?}"),
            Ok(Ok(result)) => result,
        };

        drop(permit);

        let table = result
            .next_table()
            .await
            .unwrap()
            .expect("inline partition");
        assert_eq!(table.row_count(), 1);
    }

    #[tokio::test]
    async fn inline_next_table_waits_for_blocking_parse_permit() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let permit = runtime.blocking_parse_limiter().acquire_owned().await;
        let executor = StatementExecutor {
            api: test_statement_api(Url::parse("https://example.com/").unwrap()),
            query_response_timeout: crate::config::DEFAULT_QUERY_RESPONSE_TIMEOUT,
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: Some(BLOCKING_PARSE_CELLS as i64),
            total: None,
            row_set_bytes: Some(Bytes::from_static(br#"[["x"]]"#)),
            row_types: Some(vec![text_row_type("X")]),
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        };

        let mut result = executor.build_result_set(response).unwrap();
        let next = result.next_table();
        tokio::pin!(next);

        match timeout(Duration::from_millis(20), &mut next).await {
            Err(_) => {}
            Ok(_) => panic!("expected next_table to wait for blocking-parse permit"),
        }

        drop(permit);

        let table = next.await.unwrap().expect("inline partition");
        assert_eq!(table.row_count(), 1);
    }

    #[tokio::test]
    async fn inline_collect_table_waits_for_blocking_parse_permit() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let permit = runtime.blocking_parse_limiter().acquire_owned().await;
        let executor = StatementExecutor {
            api: test_statement_api(Url::parse("https://example.com/").unwrap()),
            query_response_timeout: crate::config::DEFAULT_QUERY_RESPONSE_TIMEOUT,
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: Some(BLOCKING_PARSE_CELLS as i64),
            total: None,
            row_set_bytes: Some(Bytes::from_static(br#"[["x"]]"#)),
            row_types: Some(vec![text_row_type("X")]),
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        };

        let result = executor.build_result_set(response).unwrap();
        let collect = result.collect_table();
        tokio::pin!(collect);

        match timeout(Duration::from_millis(20), &mut collect).await {
            Err(_) => {}
            Ok(_) => panic!("expected collect_table to wait for blocking-parse permit"),
        }

        drop(permit);

        let table = collect.await.unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[tokio::test]
    async fn whitespace_only_empty_inline_rowset_does_not_create_inline_partition() {
        for rowset in [b"[ ]".as_slice(), b"[\n]".as_slice()] {
            let mut result = executor()
                .build_result_set(whitespace_empty_inline_response(rowset))
                .unwrap();
            assert_eq!(result.schema().len(), 1);
            assert!(result.is_exhausted());
            assert!(result.next_table().await.unwrap().is_none());
        }
    }
}
