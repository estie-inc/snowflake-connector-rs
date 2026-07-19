use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use tokio::time::timeout;

use crate::{
    Error, Result,
    error::{ConfigError, NetworkError, TimeoutError, classify_request_error},
    statement::{
        StatementParts,
        wire::{
            request::WireQueryBody,
            response::{WireQueryResponse, parse_query_response},
        },
    },
};

use super::{QueryApiClient, QueryResponseDeadline};

pub(crate) struct PreparedSubmit {
    request: reqwest::RequestBuilder,
}

impl QueryApiClient {
    pub(crate) fn prepare_submit(
        &self,
        parts: &StatementParts,
        query_request_id: &str,
    ) -> Result<PreparedSubmit> {
        let mut url = self
            .shared
            .base_url
            .join("queries/v1/query-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("requestId", query_request_id);

        let body = Bytes::from(
            serde_json::to_vec(&WireQueryBody::from_statement_parts(parts)).map_err(|error| {
                Error::other(format!("failed to serialize query request: {error}"))
            })?,
        );

        let request = self
            .shared
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(CONTENT_TYPE, "application/json")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.auth.session_token),
            )
            .body(body);

        Ok(PreparedSubmit { request })
    }

    pub(crate) async fn send_prepared_submit(
        &self,
        prepared: PreparedSubmit,
        deadline: QueryResponseDeadline,
    ) -> Result<WireQueryResponse> {
        let remaining = deadline.remaining_or_timeout()?;

        let body = match timeout(remaining, async {
            let response = prepared
                .request
                .send()
                .await
                .map_err(classify_request_error)?;
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

        parse_query_response(body).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::Url;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        ClientSharedPartial, ErrorKind, Statement,
        session::SessionAuth,
        statement::{
            api::test_support::{read_http_request, test_query_api, write_http_response},
            builder::into_statement_parts,
        },
    };

    #[tokio::test]
    async fn submit_posts_wire_request_contract() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            write_http_response(
                &mut socket,
                200,
                r#"{"success":true,"data":{"queryId":"q","rowtype":[],"rowset":[],"queryResultFormat":"json"}}"#,
            )
            .await;
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
            ClientSharedPartial::new()
                .with_http(
                    reqwest::Client::builder()
                        .timeout(Duration::from_millis(50))
                        .build()
                        .unwrap(),
                )
                .with_base_url(Url::parse(&format!("http://{addr}/")).unwrap())
                .build(),
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
}
