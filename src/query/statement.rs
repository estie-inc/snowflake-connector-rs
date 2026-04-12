use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::header::{ACCEPT, AUTHORIZATION};
use reqwest::{Client, Url};
use serde::de::Error as _;
use tokio::time::sleep;

use crate::{Error, Result, SnowflakeColumn, SnowflakeSession, row::SnowflakeColumnType};

use super::chunk_downloader::ChunkDownloader;
use super::partition_source::{PartitionSource, StaticPartitionSource};
use super::request::QueryRequest;
use super::response::{
    DEFAULT_TIMEOUT_SECONDS, QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE,
    RawQueryResponse, SESSION_EXPIRED, SnowflakeResponse, resolve_download_headers,
};
use super::result_set::{CollectPolicy, ResultSet};
use super::snapshot::{
    DownloadLocator, PartitionCursor, PartitionSpec, ResolvedLease, ResultIdentity, ResultSnapshot,
};

/// Low-level HTTP client for the Snowflake statement API.
pub(crate) struct StatementApiClient {
    http: Client,
    base_url: Url,
    session_token: String,
}

impl StatementApiClient {
    async fn submit(&self, request: &QueryRequest) -> Result<SnowflakeResponse> {
        let request_id = uuid::Uuid::new_v4();
        let mut url = self.base_url.join("queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let response = self
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.session_token),
            )
            .json(request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(Error::Communication(format!("HTTP {status}: {body}")));
        }

        serde_json::from_str(&body).map_err(|e| Error::Json(e, body))
    }

    async fn poll_async_results(
        &self,
        poll_url: Url,
        timeout: Duration,
    ) -> Result<SnowflakeResponse> {
        const POLL_INTERVAL: Duration = Duration::from_secs(10);

        let deadline = Instant::now() + timeout;
        loop {
            let resp = self
                .http
                .get(poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(
                    AUTHORIZATION,
                    format!(r#"Snowflake Token="{}""#, self.session_token),
                )
                .send()
                .await?;

            let status = resp.status();
            let body = resp.text().await?;
            if !status.is_success() {
                return Err(Error::Communication(format!("HTTP {status}: {body}")));
            }

            let response: SnowflakeResponse =
                serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;
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

    /// Resolve a result URL that may be absolute or relative to `base_url`.
    fn resolve_result_url(&self, raw: &str) -> Result<Url> {
        Url::parse(raw).or_else(|_| self.base_url.join(raw).map_err(Into::into))
    }
}

/// Orchestrates query submission, polling, and ResultSet construction.
pub(crate) struct StatementExecutor {
    api: StatementApiClient,
    async_completion_timeout: Duration,
    default_collect_concurrency: NonZeroUsize,
}

impl StatementExecutor {
    pub(crate) fn new(sess: &SnowflakeSession) -> Self {
        let timeout = sess
            .query
            .async_query_completion_timeout()
            .unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS));

        Self {
            api: StatementApiClient {
                http: sess.http.clone(),
                base_url: sess.base_url.clone(),
                session_token: sess.session_token.clone(),
            },
            async_completion_timeout: timeout,
            default_collect_concurrency: sess.query.collect_prefetch_concurrency(),
        }
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

        let lease = ResolvedLease { locators };
        let downloader = ChunkDownloader::new(self.api.http.clone());
        let source = PartitionSource::Static(StaticPartitionSource::new(lease, downloader));

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
