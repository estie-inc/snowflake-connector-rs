use bytes::Bytes;
use http::{
    Method,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
};
use reqwest::Url;
use tokio::time::timeout;

use crate::{
    Error, Result,
    auth::RenewError,
    error::{
        NetworkError, SessionExpiredError, TimeoutError, classify_request_error,
        with_optional_query_id,
    },
    statement::{
        StatementParts,
        wire::{
            request::WireQueryBody,
            response::{SESSION_EXPIRED, WireQueryResponse, parse_query_response},
        },
    },
};

use super::{QueryApiClient, QueryResponseDeadline};

pub(crate) struct PreparedSubmit {
    url: Url,
    body: Bytes,
}

impl QueryApiClient {
    pub(crate) fn prepare_submit(
        &self,
        parts: &StatementParts,
        query_request_id: &str,
    ) -> Result<PreparedSubmit> {
        let mut url = self.api.resolve("queries/v1/query-request");
        url.query_pairs_mut()
            .append_pair("requestId", query_request_id);

        let body = Bytes::from(
            serde_json::to_vec(&WireQueryBody::from_statement_parts(parts)).map_err(|error| {
                Error::other(format!("failed to serialize query request: {error}"))
            })?,
        );

        Ok(PreparedSubmit { url, body })
    }

    pub(crate) async fn send_prepared_submit(
        &self,
        prepared: PreparedSubmit,
        deadline: QueryResponseDeadline,
    ) -> Result<WireQueryResponse> {
        let mut resent_after_renewal = false;

        loop {
            let remaining = deadline.remaining_or_timeout()?;
            let tokens = self.auth.snapshot();
            let request = self
                .api
                .request(Method::POST, prepared.url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(CONTENT_TYPE, "application/json")
                .header(AUTHORIZATION, tokens.authorization())
                .body(prepared.body.clone());

            let body = match timeout(remaining, async {
                let response = request.send().await.map_err(classify_request_error)?;
                let status = response.status();
                let body = response.bytes().await.map_err(classify_request_error)?;
                if !status.is_success() {
                    return Err(Error::from(NetworkError::http_status(
                        status.as_u16(),
                        &body,
                    )));
                }
                Ok(body)
            })
            .await
            {
                Ok(result) => result?,
                Err(_elapsed) => return Err(TimeoutError::query().into()),
            };

            let response = parse_query_response(body).map_err(Error::from)?;
            if response.code.as_deref() != Some(SESSION_EXPIRED) || resent_after_renewal {
                return Ok(response);
            }

            // UNVERIFIED (no public protocol reference): the assumption that a "session expired" answer guarantees
            // Snowflake did not start the query. The submission is resent without checking for a duplicate execution,
            // with the same `requestId` and no retry parameters, exactly as both official connectors do. Keeping the
            // `requestId` also keeps cancellation working, since abort addresses the query by that id.
            match self
                .auth
                .renew(&self.api, &tokens, deadline.remaining())
                .await
            {
                Ok(_) => resent_after_renewal = true,
                Err(RenewError::NoMasterToken) => return Ok(response),
                Err(error) => {
                    let query_id = response.data.map(|data| data.query_id);
                    let observed = SessionExpiredError::new(response.code, response.message, None);
                    return Err(with_optional_query_id(
                        error.into_scoped(observed, TimeoutError::query()),
                        query_id,
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use http::StatusCode;
    use reqwest::Url;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        ApiContext, ErrorKind, Statement,
        api_context::DEFAULT_USER_AGENT,
        auth::SessionAuth,
        statement::{
            api::test_support::{
                QUERY_DONE_BODY, RENEWED_BODY, SESSION_EXPIRED_BODY, renewable_query_api,
                request_target, serve_script, test_query_api,
            },
            builder::into_statement_parts,
        },
        test_support::http::{read_http_request, write_json_response},
    };

    #[tokio::test]
    async fn submit_posts_wire_request_contract() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(
                &mut socket,
                StatusCode::OK,
                r#"{"success":true,"data":{"queryId":"q","rowtype":[],"rowset":[],"queryResultFormat":"json"}}"#,
            )
            .await
            .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let parts = into_statement_parts(Statement::from("select 1")).unwrap();
        let deadline = QueryResponseDeadline::new(Duration::from_secs(30));
        let prepared = client.prepare_submit(&parts, "query-request-id").unwrap();
        client
            .send_prepared_submit(prepared, deadline)
            .await
            .unwrap();

        let request = server.await.unwrap();
        let request_line = request.lines().next().unwrap();
        assert!(
            request_line
                .starts_with("POST /queries/v1/query-request?requestId=query-request-id HTTP/1.1"),
            "{request_line}"
        );
        assert!(request.contains("accept: application/snowflake"));
        assert!(request.contains("content-type: application/json"));
        assert!(request.contains(r#"authorization: Snowflake Token="test-token""#));
        assert!(request.to_ascii_lowercase().contains(&format!(
            "user-agent: {}",
            DEFAULT_USER_AGENT.to_ascii_lowercase()
        )));
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["sqlText"], "select 1");
    }

    #[tokio::test]
    async fn submit_timeout_is_timeout_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = QueryApiClient::new(
            Arc::new(ApiContext::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_millis(50))
                    .build()
                    .unwrap(),
                Url::parse(&format!("http://{addr}/")).unwrap(),
            )),
            SessionAuth::for_test("test-token"),
        );

        let parts = into_statement_parts(Statement::from("select 1")).unwrap();
        // A generous deadline leaves the reqwest client-wide timeout as the trigger for this case.
        let deadline = QueryResponseDeadline::new(Duration::from_secs(30));
        let prepared = client.prepare_submit(&parts, "query-request-id").unwrap();
        let err = client
            .send_prepared_submit(prepared, deadline)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_timeout());

        server.abort();
        let _ = server.await;
    }

    async fn submit(client: &QueryApiClient) -> Result<WireQueryResponse> {
        let parts = into_statement_parts(Statement::from("select 1")).unwrap();
        let deadline = QueryResponseDeadline::new(Duration::from_secs(30));
        let prepared = client.prepare_submit(&parts, "query-request-id").unwrap();
        client.send_prepared_submit(prepared, deadline).await
    }

    #[tokio::test]
    async fn submit_renews_expired_session_and_resends_same_request() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![SESSION_EXPIRED_BODY, RENEWED_BODY, QUERY_DONE_BODY],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let response = submit(&client).await.unwrap();
        assert!(response.success);

        let (requests, quiet) = server.await.unwrap();
        assert!(quiet);
        assert!(requests[0].contains(r#"authorization: Snowflake Token="test-token""#));
        assert!(request_target(&requests[1]).starts_with("/session/token-request?requestId="));
        assert!(requests[1].contains(r#"authorization: Snowflake Token="master-token""#));
        assert!(requests[2].contains(r#"authorization: Snowflake Token="renewed-token""#));
        // The resend is the same submission: same request id, no retry parameters, same body.
        assert_eq!(
            request_target(&requests[0]),
            "/queries/v1/query-request?requestId=query-request-id"
        );
        assert_eq!(request_target(&requests[0]), request_target(&requests[2]));
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1).unwrap(),
            requests[2].split("\r\n\r\n").nth(1).unwrap()
        );
    }

    #[tokio::test]
    async fn submit_without_master_token_returns_expired_response_without_renewing() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(listener, vec![SESSION_EXPIRED_BODY]);

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        // The executor turns this response into the session-expired error.
        let response = submit(&client).await.unwrap();

        assert_eq!(response.code.as_deref(), Some("390112"));
        assert_eq!(response.message.as_deref(), Some("session expired"));
        let (requests, quiet) = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(quiet, "no token request may be sent without a master token");
    }

    #[tokio::test]
    async fn submit_rejected_renewal_is_session_expired_with_renewal_code() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![
                SESSION_EXPIRED_BODY,
                r#"{"success":false,"code":"390114","message":"master token expired"}"#,
            ],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = submit(&client).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390114"));
        assert_eq!(err.snowflake_message(), Some("master token expired"));
        let (_requests, quiet) = server.await.unwrap();
        assert!(quiet, "the submission must not be resent after a rejection");
    }

    #[tokio::test]
    async fn submit_still_expired_after_renewal_is_not_renewed_again() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![SESSION_EXPIRED_BODY, RENEWED_BODY, SESSION_EXPIRED_BODY],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        // The executor turns this response into the session-expired error.
        let response = submit(&client).await.unwrap();

        assert_eq!(response.code.as_deref(), Some("390112"));
        let (requests, quiet) = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(quiet, "a second renewal must not be attempted");
    }

    #[tokio::test]
    async fn submit_renewal_past_the_deadline_is_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await.unwrap();
            write_json_response(&mut socket, StatusCode::OK, SESSION_EXPIRED_BODY)
                .await
                .unwrap();
            // Accept the token request and never answer it.
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let parts = into_statement_parts(Statement::from("select 1")).unwrap();
        let deadline = QueryResponseDeadline::new(Duration::from_millis(500));
        let prepared = client.prepare_submit(&parts, "query-request-id").unwrap();
        let err = client
            .send_prepared_submit(prepared, deadline)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        // The query deadline is what ran out, so that is what gets reported.
        assert_eq!(err.to_string(), "timed out waiting for query response");
        server.abort();
    }
}
