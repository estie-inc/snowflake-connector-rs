use std::{num::NonZeroUsize, time::Duration};

use serde::de::Error as _;

use crate::{
    chunk::ChunkDownloader,
    query::QueryRequest,
    query_result::{
        CollectPolicy, InlineRowset, ResultSet,
        partition_source::{PartitionSource, StaticPartitionSource},
        snapshot::PartitionCursor,
    },
    runtime::QueryRuntime,
    {Error, Result, SnowflakeSession},
};

use super::{
    client::StatementApiClient,
    manifest::ResultManifest,
    response::{
        DEFAULT_TIMEOUT_SECONDS, QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE,
        RawQueryResponse, SESSION_EXPIRED,
    },
};

pub(crate) struct StatementExecutor {
    api: StatementApiClient,
    async_completion_timeout: Duration,
    default_collect_concurrency: NonZeroUsize,
    runtime: QueryRuntime,
}

impl StatementExecutor {
    pub(crate) fn new(sess: &SnowflakeSession) -> Self {
        let timeout = sess
            .query
            .async_query_completion_timeout()
            .unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS));

        Self {
            api: StatementApiClient::new(
                sess.http.clone(),
                sess.base_url.clone(),
                sess.session_token.clone(),
            ),
            async_completion_timeout: timeout,
            default_collect_concurrency: sess.query.collect_prefetch_concurrency(),
            runtime: sess.runtime.clone(),
        }
    }

    pub(crate) async fn execute(self, request: QueryRequest) -> Result<ResultSet> {
        let mut response = self.api.submit(&request).await?;

        let response_code = response.code.as_deref();
        if response_code == Some(QUERY_IN_PROGRESS_ASYNC_CODE)
            || response_code == Some(QUERY_IN_PROGRESS_CODE)
        {
            let Some(data) = response.data else {
                return Err(Error::Json(
                    serde_json::Error::custom("missing data field in async query response"),
                    "".to_string(),
                ));
            };
            match data.get_result_url {
                Some(raw_url) => {
                    let poll_url = self.api.resolve_result_url(&raw_url)?;
                    response = self
                        .api
                        .poll_async_results(poll_url, self.async_completion_timeout)
                        .await?;
                }
                None => {
                    return Err(Error::NoPollingUrlAsyncQuery);
                }
            }
        }

        if let Some(SESSION_EXPIRED) = response.code.as_deref() {
            return Err(Error::SessionExpired);
        }

        if !response.success {
            return Err(Error::Communication(response.message.unwrap_or_default()));
        }

        let Some(data) = response.data else {
            return Err(Error::Json(
                serde_json::Error::custom("missing data field in query response"),
                "".to_string(),
            ));
        };

        if let Some(ref format) = data.query_result_format {
            if format != "json" {
                return Err(Error::UnsupportedFormat(format.clone()));
            }
        }

        self.build_result_set(data)
    }

    fn build_result_set(self, data: RawQueryResponse) -> Result<ResultSet> {
        let manifest = ResultManifest::try_from(data)?;
        let cursor = PartitionCursor::new(manifest.snapshot.partitions.len());

        let inline_rowset = manifest
            .inline_rowset
            .map(|rowset| InlineRowset::new(rowset.bytes, rowset.row_count_hint));

        let downloader = ChunkDownloader::new(self.api.http_client());
        let source =
            PartitionSource::Static(StaticPartitionSource::new(manifest.lease, downloader));
        let default_collect_policy = CollectPolicy::new(self.default_collect_concurrency);

        Ok(ResultSet::new(
            manifest.snapshot,
            cursor,
            inline_rowset,
            source,
            self.runtime,
            default_collect_policy,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use bytes::Bytes;
    use reqwest::{Client, Url};
    use tokio::time::timeout;

    use super::super::client::StatementApiClient;
    use super::*;
    use crate::{
        Error,
        rowset::BLOCKING_PARSE_CELLS,
        runtime::QueryRuntime,
        statement::response::{RawQueryResponse, RawQueryResponseChunk, RawQueryResponseRowType},
    };

    fn executor() -> StatementExecutor {
        StatementExecutor {
            api: StatementApiClient::new(
                Client::new(),
                Url::parse("https://example.com/").unwrap(),
                "test-token".to_string(),
            ),
            async_completion_timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECONDS),
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime: QueryRuntime::new(),
        }
    }

    fn chunk_only_response(row_types: Option<Vec<RawQueryResponseRowType>>) -> RawQueryResponse {
        RawQueryResponse {
            parameters: None,
            query_id: "query-id".to_string(),
            get_result_url: None,
            returned: None,
            total: None,
            row_set_bytes: Some(Bytes::from_static(b"[]")),
            row_types,
            chunk_headers: None,
            qrmk: None,
            chunks: Some(vec![RawQueryResponseChunk {
                url: "https://example.com/chunk/0".to_string(),
                row_count: 1,
                uncompressed_size: 16,
                compressed_size: 8,
            }]),
            query_result_format: Some("json".to_string()),
        }
    }

    fn text_row_type(name: &str) -> RawQueryResponseRowType {
        RawQueryResponseRowType {
            database: String::new(),
            name: name.to_string(),
            nullable: false,
            scale: None,
            byte_length: Some(16),
            length: Some(16),
            schema: String::new(),
            table: String::new(),
            precision: None,
            data_type: "text".to_string(),
        }
    }

    fn whitespace_empty_inline_response(rowset: &'static [u8]) -> RawQueryResponse {
        RawQueryResponse {
            parameters: None,
            query_id: "query-id".to_string(),
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

    #[test]
    fn statement_executor_reuses_session_query_runtime() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let session = SnowflakeSession {
            http: Client::new(),
            base_url: Url::parse("https://example.com/").unwrap(),
            session_token: "test-token".to_string(),
            query: crate::SnowflakeQueryConfig::default(),
            runtime: runtime.clone(),
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
    async fn inline_result_set_build_does_not_wait_for_blocking_parse_permit() {
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let permit = runtime.blocking_parse_limiter().acquire_owned().await;
        let executor = StatementExecutor {
            api: StatementApiClient::new(
                Client::new(),
                Url::parse("https://example.com/").unwrap(),
                "test-token".to_string(),
            ),
            async_completion_timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECONDS),
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            parameters: None,
            query_id: "query-id".to_string(),
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
            api: StatementApiClient::new(
                Client::new(),
                Url::parse("https://example.com/").unwrap(),
                "test-token".to_string(),
            ),
            async_completion_timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECONDS),
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            parameters: None,
            query_id: "query-id".to_string(),
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
            api: StatementApiClient::new(
                Client::new(),
                Url::parse("https://example.com/").unwrap(),
                "test-token".to_string(),
            ),
            async_completion_timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECONDS),
            default_collect_concurrency: NonZeroUsize::new(1).unwrap(),
            runtime,
        };
        let response = RawQueryResponse {
            parameters: None,
            query_id: "query-id".to_string(),
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

    #[test]
    fn chunk_only_results_require_rowtype_metadata_when_missing_or_empty() {
        for (label, row_types) in [
            ("missing", None),
            ("empty", Some(Vec::<RawQueryResponseRowType>::new())),
        ] {
            match executor().build_result_set(chunk_only_response(row_types)) {
                Err(Error::UnsupportedFormat(message))
                    if message == "response has result data but no rowtype metadata" => {}
                Err(other) => panic!("expected UnsupportedFormat, got {other:?}"),
                Ok(_) => {
                    panic!("expected chunk-only response with {label} rowtype metadata to fail")
                }
            }
        }
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
