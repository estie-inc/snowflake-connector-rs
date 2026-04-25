use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use http::header::{ACCEPT, AUTHORIZATION};
use reqwest::{Client, Url};
use serde::de::Error as _;
use tokio::time::sleep;

use crate::{Error, Result, SnowflakeColumn, SnowflakeSession, row::SnowflakeColumnType};

use super::{
    capability::{BackendResultCapability, RefreshInvariant},
    chunk_downloader::ChunkDownloader,
    expiry::LeaseExpiryClassifier,
    partition_source::{PartitionSource, RefreshingPartitionSource, StaticPartitionSource},
    refresh::{LeaseRefresher, RefreshCapability, RefreshHandle},
    request::QueryRequest,
    response::{
        DEFAULT_TIMEOUT_SECONDS, QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE,
        RawQueryResponse, SESSION_EXPIRED, SnowflakeResponse, resolve_download_headers,
    },
    result_set::{CollectPolicy, ResultSet},
    snapshot::{
        DownloadLocator, PartitionCursor, PartitionSpec, ResolvedLease, ResultIdentity,
        ResultSnapshot,
    },
};

/// Low-level HTTP client for the Snowflake statement API.
pub(crate) struct StatementApiClient {
    pub(super) http: Client,
    pub(super) base_url: Url,
    pub(super) session_token: String,
}

impl StatementApiClient {
    fn authorization_header_value(&self) -> String {
        format!(r#"Snowflake Token="{}""#, self.session_token)
    }

    /// Send a pre-built `reqwest::RequestBuilder` to Snowflake, applying the
    /// shared success/JSON-decode envelope used by submit / poll / refresh.
    async fn send_snowflake_request(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<SnowflakeResponse> {
        let response = builder.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(Error::Communication(format!("HTTP {status}: {body}")));
        }
        serde_json::from_str(&body).map_err(|e| Error::Json(e, body))
    }

    async fn submit(&self, request: &QueryRequest) -> Result<SnowflakeResponse> {
        let request_id = uuid::Uuid::new_v4();
        let mut url = self.base_url.join("queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let builder = self
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(AUTHORIZATION, self.authorization_header_value())
            .json(request);

        self.send_snowflake_request(builder).await
    }

    async fn poll_async_results(
        &self,
        poll_url: Url,
        timeout: Duration,
    ) -> Result<SnowflakeResponse> {
        const POLL_INTERVAL: Duration = Duration::from_secs(10);

        let deadline = Instant::now() + timeout;
        loop {
            let builder = self
                .http
                .get(poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(AUTHORIZATION, self.authorization_header_value());

            let response = self.send_snowflake_request(builder).await?;
            if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
                && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE)
            {
                return Ok(response);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::TimedOut);
            }
            sleep(remaining.min(POLL_INTERVAL)).await;
        }
    }

    /// Re-fetch the result envelope for an already-finished query. Used by
    /// `LeaseRefresher` to regenerate expired chunk locators.
    ///
    /// Validation against the original snapshot (partition shape, column
    /// metadata) is the refresher's responsibility; this method only performs
    /// the HTTP call, shared envelope decode, and mapping of well-known
    /// response codes to `Error` variants.
    pub(super) async fn refresh(&self, handle: &RefreshHandle) -> Result<RawQueryResponse> {
        let RefreshHandle::QueryResultPath { path, .. } = handle;
        let url = self.base_url.join(path)?;

        let builder = self
            .http
            .get(url)
            .header(ACCEPT, "application/snowflake")
            .header(AUTHORIZATION, self.authorization_header_value());

        let response = self.send_snowflake_request(builder).await?;

        if response.code.as_deref() == Some(SESSION_EXPIRED) {
            return Err(Error::SessionExpired);
        }
        if !response.success {
            return Err(Error::Communication(response.message.unwrap_or_default()));
        }
        response.data.ok_or_else(|| {
            Error::Json(
                serde_json::Error::custom("missing data field in refresh response"),
                String::new(),
            )
        })
    }

    /// Resolve a result URL that may be absolute or relative to `base_url`.
    fn resolve_result_url(&self, raw: &str) -> Result<Url> {
        Url::parse(raw).or_else(|_| self.base_url.join(raw).map_err(Into::into))
    }
}

/// Orchestrates query submission, polling, and ResultSet construction.
pub(crate) struct StatementExecutor {
    api: Arc<StatementApiClient>,
    async_completion_timeout: Duration,
    default_collect_concurrency: NonZeroUsize,
    capability: BackendResultCapability,
}

impl StatementExecutor {
    pub(crate) fn new(sess: &SnowflakeSession) -> Self {
        let timeout = sess
            .query
            .async_query_completion_timeout()
            .unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS));

        let capability = if sess.query.result_chunk_refresh() {
            BackendResultCapability::Refreshing {
                invariant: RefreshInvariant::StableRemainingPartitions,
            }
        } else {
            BackendResultCapability::StaticOnly
        };

        Self {
            api: Arc::new(StatementApiClient {
                http: sess.http.clone(),
                base_url: sess.base_url.clone(),
                session_token: sess.session_token.clone(),
            }),
            async_completion_timeout: timeout,
            default_collect_concurrency: sess.query.collect_prefetch_concurrency(),
            capability,
        }
    }

    fn capability(&self) -> BackendResultCapability {
        self.capability
    }

    pub(crate) async fn execute(self, request: QueryRequest) -> Result<ResultSet> {
        let mut response = self.api.submit(&request).await?;

        // Handle async query polling
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
        // Build column metadata
        let row_types = data.row_types.ok_or_else(|| {
            Error::UnsupportedFormat("the response doesn't contain 'rowtype'".to_string())
        })?;

        let columns = row_types
            .into_iter()
            .enumerate()
            .map(|(index, row_type)| {
                SnowflakeColumn::new(
                    row_type.name.to_ascii_uppercase(),
                    index,
                    SnowflakeColumnType {
                        snowflake_type: row_type.data_type,
                        nullable: row_type.nullable,
                        length: row_type.length,
                        precision: row_type.precision,
                        scale: row_type.scale,
                    },
                )
            })
            .collect::<Vec<_>>();

        let column_indices = columns
            .iter()
            .map(|column| (column.name().to_string(), column.index()))
            .collect::<HashMap<_, _>>();
        let column_indices = Arc::new(column_indices);
        let columns: Arc<[SnowflakeColumn]> = Arc::from(columns);

        let row_set = data.row_set.ok_or_else(|| {
            Error::UnsupportedFormat("the response doesn't contain 'rowset'".to_string())
        })?;
        let chunks = data.chunks.unwrap_or_default();

        let download_headers = resolve_download_headers(&data.qrmk, &data.chunk_headers)?;

        let has_inline = !row_set.is_empty();
        let mut partitions = Vec::new();
        let mut locators = HashMap::new();

        if has_inline {
            partitions.push(PartitionSpec::Inline);
        }

        for chunk in chunks {
            let ordinal = partitions.len();
            partitions.push(PartitionSpec::Remote {
                row_count: chunk.row_count,
                compressed_size: chunk.compressed_size,
                uncompressed_size: chunk.uncompressed_size,
            });
            locators.insert(
                ordinal,
                DownloadLocator {
                    url: chunk.url,
                    headers: Arc::clone(&download_headers),
                },
            );
        }

        let snapshot = Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: data.query_id,
            },
            columns,
            column_indices,
            partitions,
        });

        // 0-row normalization: if no partitions, start exhausted
        let total = snapshot.partitions.len();
        let cursor = PartitionCursor::new(total);

        let inline_rows = if has_inline { Some(row_set) } else { None };

        let lease = ResolvedLease {
            generation: 0,
            locators,
        };
        let downloader = ChunkDownloader::new(self.api.http.clone());

        let source = match self.capability() {
            BackendResultCapability::StaticOnly => {
                PartitionSource::Static(StaticPartitionSource::new(lease, downloader))
            }
            BackendResultCapability::Refreshing { invariant } => {
                let capability = RefreshCapability {
                    handle: extract_refresh_handle(&snapshot.identity),
                    invariant,
                };
                let refresher = LeaseRefresher::new(Arc::clone(&self.api));
                PartitionSource::Refreshing(RefreshingPartitionSource::new(
                    Arc::clone(&snapshot),
                    lease,
                    capability,
                    refresher,
                    downloader,
                    LeaseExpiryClassifier::new(),
                ))
            }
        };

        let default_collect_policy = CollectPolicy {
            prefetch_concurrency: self.default_collect_concurrency,
        };

        Ok(ResultSet::new(
            snapshot,
            cursor,
            inline_rows,
            source,
            default_collect_policy,
        ))
    }
}

/// Compose the `RefreshHandle` that anchors a `RefreshingPartitionSource` to
/// a concrete query result. `path` is a `base_url`-relative fragment (no
/// leading `/`), so `StatementApiClient::refresh` joins it with the session's
/// `base_url` unchanged.
fn extract_refresh_handle(identity: &ResultIdentity) -> RefreshHandle {
    RefreshHandle::QueryResultPath {
        query_id: Arc::from(identity.query_id.as_str()),
        path: Arc::from(format!("queries/{}/result", identity.query_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::test_server::{CannedResponse, ServerHandle, start_server};

    fn make_executor(capability: BackendResultCapability) -> StatementExecutor {
        StatementExecutor {
            api: Arc::new(StatementApiClient {
                http: reqwest::Client::new(),
                base_url: Url::parse("http://localhost/").unwrap(),
                session_token: "token".to_string(),
            }),
            async_completion_timeout: Duration::from_secs(30),
            default_collect_concurrency: NonZeroUsize::new(8).unwrap(),
            capability,
        }
    }

    fn refreshing_capability() -> BackendResultCapability {
        BackendResultCapability::Refreshing {
            invariant: RefreshInvariant::StableRemainingPartitions,
        }
    }

    fn api_client_for(server: &ServerHandle) -> StatementApiClient {
        StatementApiClient {
            http: reqwest::Client::new(),
            base_url: server.base_url(),
            session_token: "the-token".to_string(),
        }
    }

    fn refresh_success_body(query_id: &str) -> String {
        serde_json::json!({
            "success": true,
            "message": null,
            "code": null,
            "data": {
                "queryId": query_id,
                "queryResultFormat": "json",
                "rowtype": [],
                "rowset": [],
                "chunks": [],
            },
        })
        .to_string()
    }

    #[test]
    fn capability_defaults_to_static_only() {
        let executor = make_executor(BackendResultCapability::StaticOnly);
        assert_eq!(executor.capability(), BackendResultCapability::StaticOnly);
    }

    #[test]
    fn capability_is_refreshing_when_result_chunk_refresh_enabled() {
        let executor = make_executor(refreshing_capability());
        assert_eq!(executor.capability(), refreshing_capability());
    }

    #[test]
    fn extract_refresh_handle_composes_relative_query_result_path() {
        let identity = ResultIdentity {
            query_id: "01c3c195-0000-0001-0000-abcdef".into(),
        };
        match extract_refresh_handle(&identity) {
            RefreshHandle::QueryResultPath { query_id, path } => {
                assert_eq!(&*query_id, "01c3c195-0000-0001-0000-abcdef");
                assert_eq!(&*path, "queries/01c3c195-0000-0001-0000-abcdef/result");
                assert!(
                    !path.starts_with('/'),
                    "path must stay relative so base_url.join keeps the session prefix",
                );
            }
        }
    }

    #[tokio::test]
    async fn refresh_issues_get_to_query_result_path_with_auth_and_accept_headers() {
        let server = start_server(vec![CannedResponse::ok_json(refresh_success_body("q-1"))]);
        let api = api_client_for(&server);
        let handle = RefreshHandle::QueryResultPath {
            query_id: Arc::from("q-1"),
            path: Arc::from("queries/q-1/result"),
        };

        let resp = api.refresh(&handle).await.expect("refresh succeeds");
        assert_eq!(resp.query_id, "q-1");
        assert_eq!(resp.query_result_format.as_deref(), Some("json"));

        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        let req = &recorded[0];
        assert_eq!(req.method, http::Method::GET);
        assert_eq!(req.path, "/queries/q-1/result");
        assert!(
            req.query.is_none(),
            "refresh GET must carry no query params"
        );
        assert_eq!(
            req.headers.get(http::header::ACCEPT).unwrap(),
            "application/snowflake",
        );
        assert_eq!(
            req.headers.get(http::header::AUTHORIZATION).unwrap(),
            r#"Snowflake Token="the-token""#,
        );
    }

    fn sample_raw_response() -> RawQueryResponse {
        serde_json::from_value(serde_json::json!({
            "queryId": "q-42",
            "queryResultFormat": "json",
            "rowtype": [{
                "database": "", "schema": "", "table": "",
                "name": "X", "type": "fixed", "nullable": false,
                "precision": 10, "scale": 0, "length": null, "byteLength": null,
            }],
            "rowset": [[Some("1".to_string())]],
            "chunks": [{
                "url": "https://example.invalid/chunk-0",
                "rowCount": 1,
                "uncompressedSize": 0,
                "compressedSize": 0,
            }],
        }))
        .expect("fixture parses")
    }

    #[test]
    fn build_result_set_produces_static_source_by_default() {
        let executor = make_executor(BackendResultCapability::StaticOnly);
        let result_set = executor
            .build_result_set(sample_raw_response())
            .expect("builds");
        let source = result_set.source();
        assert!(
            matches!(source, PartitionSource::Static(_)),
            "default capability must construct PartitionSource::Static, got a different variant",
        );
    }

    #[test]
    fn build_result_set_produces_refreshing_source_when_capability_is_refreshing() {
        let executor = make_executor(refreshing_capability());
        let result_set = executor
            .build_result_set(sample_raw_response())
            .expect("builds");
        let source = result_set.source();
        assert!(
            matches!(source, PartitionSource::Refreshing(_)),
            "refreshing capability must construct PartitionSource::Refreshing, got a different variant",
        );
        assert_eq!(source.refreshing_lease_generation(), Some(0));
    }

    #[tokio::test]
    async fn refresh_maps_session_expired_code_to_error() {
        let body = serde_json::json!({
            "success": false,
            "message": "Your session has expired.",
            "code": "390112",
            "data": null,
        })
        .to_string();
        let server = start_server(vec![CannedResponse::ok_json(body)]);
        let api = api_client_for(&server);
        let handle = RefreshHandle::QueryResultPath {
            query_id: Arc::from("q-1"),
            path: Arc::from("queries/q-1/result"),
        };

        let err = api.refresh(&handle).await.expect_err("should error");
        assert!(
            matches!(err, Error::SessionExpired),
            "expected SessionExpired, got {err:?}",
        );
    }
}
