use std::time::{Duration, Instant};
use std::{collections::HashMap, mem, sync::Arc};

use http::{
    HeaderMap,
    header::{ACCEPT, AUTHORIZATION},
};
use reqwest::{Client, Url};
use serde::de::Error as _;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

use crate::SnowflakeSession;
use crate::row::SnowflakeColumnType;
use crate::{Error, Result, SnowflakeRow, chunk::download_chunk};

pub(super) const SESSION_EXPIRED: &str = "390112";
pub(super) const QUERY_IN_PROGRESS_CODE: &str = "333333";
pub(super) const QUERY_IN_PROGRESS_ASYNC_CODE: &str = "333334";
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

pub struct QueryExecutor {
    http: Client,
    qrmk: String,
    chunks: Mutex<Vec<RawQueryResponseChunk>>,
    chunk_headers: HeaderMap,
    column_types: Arc<Vec<SnowflakeColumnType>>,
    column_indices: Arc<HashMap<String, usize>>,
    row_set: Mutex<Option<Vec<Vec<Option<String>>>>>,
}

fn get_base_url(sess: &SnowflakeSession) -> Result<Url> {
    let host = sess
        .host
        .clone()
        .unwrap_or_else(|| format!("{}.snowflakecomputing.com", sess.account));
    let protocol = sess.protocol.clone().unwrap_or_else(|| "https".to_string());
    let mut url = Url::parse(&format!("{protocol}://{host}"))?;
    if let Some(port) = sess.port {
        url.set_port(Some(port))
            .map_err(|_| Error::Url("invalid base url port".to_string()))?;
    }
    Ok(url)
}

impl QueryExecutor {
    pub(super) async fn create<Q: Into<QueryRequest>>(
        sess: &SnowflakeSession,
        request: Q,
    ) -> Result<Self> {
        let SnowflakeSession {
            http,
            session_token,
            timeout,
            ..
        } = sess;
        let timeout = timeout.unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS));

        let request_id = uuid::Uuid::new_v4();
        let base_url = get_base_url(sess)?;
        let mut url = base_url.join("queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let request: QueryRequest = request.into();
        let response = http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, session_token),
            )
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(Error::Communication(body));
        }

        let mut response: SnowflakeResponse =
            serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;

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
                Some(result_url) => {
                    response = poll_for_async_results(
                        http,
                        &result_url,
                        session_token,
                        timeout,
                        base_url.clone(),
                    )
                    .await?
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

        let Some(response_data) = response.data else {
            return Err(Error::Json(
                serde_json::Error::custom("missing data field in query response"),
                "".to_string(),
            ));
        };

        if let Some(format) = response_data.query_result_format {
            if format != "json" {
                return Err(Error::UnsupportedFormat(format.clone()));
            }
        }

        let http = http.clone();
        let qrmk = response_data.qrmk.unwrap_or_default();
        let chunks = response_data.chunks.unwrap_or_default();
        let chunks = Mutex::new(chunks);
        let row_types = response_data.row_types.ok_or_else(|| {
            Error::UnsupportedFormat("the response doesn't contain 'rowtype'".to_string())
        })?;
        let row_set = response_data.row_set.ok_or_else(|| {
            Error::UnsupportedFormat("the response doesn't contain 'rowset'".to_string())
        })?;
        let row_set = Mutex::new(Some(row_set));

        let column_indices = row_types
            .iter()
            .enumerate()
            .map(|(i, row_type)| (row_type.name.to_ascii_uppercase(), i))
            .collect::<HashMap<_, _>>();
        let column_indices = Arc::new(column_indices);

        let column_types = row_types
            .into_iter()
            .map(|row_type| SnowflakeColumnType {
                snowflake_type: row_type.data_type,
                nullable: row_type.nullable,
                length: row_type.length,
                precision: row_type.precision,
                scale: row_type.scale,
            })
            .collect::<Vec<_>>();
        let column_types = Arc::new(column_types);

        let chunk_headers = response_data.chunk_headers.unwrap_or_default();
        let chunk_headers: HeaderMap = HeaderMap::try_from(&chunk_headers)?;

        Ok(Self {
            http,
            qrmk,
            chunks,
            chunk_headers,
            column_types,
            column_indices,
            row_set,
        })
    }

    /// Check if there are no more rows to fetch
    pub async fn eof(&self) -> bool {
        let row_set = &*self.row_set.lock().await;
        let chunks = &*self.chunks.lock().await;
        row_set.is_none() && chunks.is_empty()
    }

    /// Fetch a single chunk
    pub async fn fetch_next_chunk(&self) -> Result<Option<Vec<SnowflakeRow>>> {
        let row_set = &mut *self.row_set.lock().await;
        if let Some(row_set) = row_set.take() {
            let rows = row_set.into_iter().map(|r| self.convert_row(r)).collect();
            return Ok(Some(rows));
        }

        let http = self.http.clone();
        let chunk_headers = self.chunk_headers.clone();
        let qrmk = self.qrmk.clone();
        let chunks = &mut *self.chunks.lock().await;
        let Some(chunk) = chunks.pop() else {
            // Nothing to fetch
            return Ok(None);
        };

        let rows = download_chunk(http, chunk.url, chunk_headers, qrmk).await?;
        let rows = rows.into_iter().map(|r| self.convert_row(r)).collect();
        Ok(Some(rows))
    }

    /// Fetch all the remaining chunks at once
    pub async fn fetch_all(&self) -> Result<Vec<SnowflakeRow>> {
        self.fetch_all_with_limit(usize::MAX).await
    }

    /// Fetch all remaining chunks while capping concurrent downloads.
    ///
    /// `max_concurrency` values below `1` are treated as `1` to ensure progress.
    pub async fn fetch_all_with_limit(&self, max_concurrency: usize) -> Result<Vec<SnowflakeRow>> {
        let mut rows = Vec::new();
        {
            let row_set = &mut *self.row_set.lock().await;
            if let Some(row_set) = row_set.take() {
                rows.extend(row_set.into_iter().map(|r| self.convert_row(r)));
            }
        }

        let mut chunks = {
            let chunks = &mut *self.chunks.lock().await;
            if chunks.is_empty() {
                return Ok(rows);
            }

            mem::take(chunks)
        };

        let max_concurrency = max_concurrency.max(1);
        let concurrency = chunks.len().clamp(1, max_concurrency);

        // The semaphore ensures that no more than `concurrency` downloads are in flight.
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut handles = Vec::with_capacity(chunks.len());
        while let Some(chunk) = chunks.pop() {
            let http = self.http.clone();
            let chunk_headers = self.chunk_headers.clone();
            let qrmk = self.qrmk.clone();
            let semaphore = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let permit = semaphore.acquire_owned().await?;
                let result = download_chunk(http, chunk.url, chunk_headers, qrmk).await;

                // drop the permit ASAP. It's not needed right now, but is clearer, and in case
                // extra work is added later it's good to have.
                drop(permit);

                result
            }));
        }

        for fut in handles {
            let result = fut.await?;
            rows.extend(result?.into_iter().map(|r| self.convert_row(r)));
        }

        Ok(rows)
    }

    fn convert_row(&self, row: Vec<Option<String>>) -> SnowflakeRow {
        SnowflakeRow {
            row,
            column_indices: Arc::clone(&self.column_indices),
            column_types: Arc::clone(&self.column_types),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinSet,
        time::{Duration, sleep},
    };

    async fn run_fetch_all_limit_test(limit: usize, chunk_count: usize) {
        // Spin up a tiny HTTP server that simulates chunk downloads. Each incoming
        // connection increments the "active" counter before pausing for a short
        // delay, mimicking a request that is in-flight. This lets us observe the
        // maximum number of concurrent downloads driven by fetch_all_with_limit.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let server_active = Arc::clone(&active);
        let server_peak = Arc::clone(&peak);

        let server = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            for _ in 0..chunk_count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let active = Arc::clone(&server_active);
                let peak = Arc::clone(&server_peak);
                handlers.spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = socket.read(&mut buf).await;
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    assert!(current <= limit, "observed {current} > limit {limit}");

                    sleep(Duration::from_millis(25)).await;
                    active.fetch_sub(1, Ordering::SeqCst);

                    let body = b"[\"ok\"]";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    let _ = socket.shutdown().await;
                });
            }

            while let Some(res) = handlers.join_next().await {
                res.unwrap();
            }
        });

        let mut chunks = Vec::new();
        for i in 0..chunk_count {
            chunks.push(RawQueryResponseChunk {
                url: format!("http://{addr}/chunk{i}"),
                row_count: 0,
                uncompressed_size: 0,
                compressed_size: 0,
            });
        }

        let mut column_indices = HashMap::new();
        column_indices.insert("COL1".to_string(), 0);

        let executor = QueryExecutor {
            http: reqwest::Client::new(),
            qrmk: "dummy-key".to_string(),
            chunks: Mutex::new(chunks),
            chunk_headers: HeaderMap::new(),
            column_types: Arc::new(vec![SnowflakeColumnType::new(
                "text".to_string(),
                true,
                None,
                None,
                None,
            )]),
            column_indices: Arc::new(column_indices),
            row_set: Mutex::new(None),
        };

        // The server records how many requests overlap at once; after fetch_all completes
        // we assert that the observed maximum never exceeds the requested concurrency limit.
        let fetch = executor.fetch_all_with_limit(limit);
        let rows = fetch.await.unwrap();
        server.await.unwrap();

        assert_eq!(rows.len(), chunk_count);
        let observed_peak = peak.load(Ordering::SeqCst);

        // The limit should be low enough that we can safely assert that the observed peak
        // is exactly the limit. I.e. the peak was reached.
        assert_eq!(observed_peak, limit);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fetch_all_with_limit_caps_concurrency_stress() {
        // increase beyond 10 to try harder to find a concurrency bug
        for _ in 0..10 {
            run_fetch_all_limit_test(2, 8).await;
        }
    }
}

async fn poll_for_async_results(
    http: &Client,
    result_url: &str,
    session_token: &str,
    timeout: Duration,
    base_url: Url,
) -> Result<SnowflakeResponse> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        sleep(Duration::from_secs(10)).await;
        let url = if let Ok(url) = Url::parse(result_url) {
            url
        } else {
            base_url.join(result_url)?
        };

        let resp = http
            .get(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, session_token),
            )
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(Error::Communication(body));
        }

        let response: SnowflakeResponse =
            serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;
        if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
            && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE)
        {
            return Ok(response);
        }
    }

    Err(Error::TimedOut)
}

#[derive(Debug, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    /// SQL statement to execute against Snowflake.
    pub sql_text: String,
}

impl From<&str> for QueryRequest {
    fn from(sql_text: &str) -> Self {
        Self {
            sql_text: sql_text.to_string(),
        }
    }
}
impl From<&QueryRequest> for QueryRequest {
    fn from(request: &QueryRequest) -> Self {
        request.clone()
    }
}

impl From<String> for QueryRequest {
    fn from(sql_text: String) -> Self {
        Self { sql_text }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawQueryResponse {
    #[allow(unused)]
    parameters: Option<Vec<RawQueryResponseParameter>>,
    #[allow(unused)]
    query_id: String,
    #[allow(unused)]
    get_result_url: Option<String>,
    #[allow(unused)]
    returned: Option<i64>,
    #[allow(unused)]
    total: Option<i64>,

    #[serde(rename = "rowset")]
    row_set: Option<Vec<Vec<Option<String>>>>,

    #[serde(rename = "rowtype")]
    row_types: Option<Vec<RawQueryResponseRowType>>,

    chunk_headers: Option<HashMap<String, String>>,

    qrmk: Option<String>,

    chunks: Option<Vec<RawQueryResponseChunk>>,
    query_result_format: Option<String>,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawQueryResponseRowType {
    #[allow(unused)]
    database: String,
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    nullable: bool,
    #[allow(unused)]
    scale: Option<i64>,
    #[allow(unused)]
    byte_length: Option<i64>,
    #[allow(unused)]
    length: Option<i64>,
    #[allow(unused)]
    schema: String,
    #[allow(unused)]
    table: String,
    #[allow(unused)]
    precision: Option<i64>,

    #[allow(unused)]
    #[serde(rename = "type")]
    data_type: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawQueryResponseParameter {
    #[allow(unused)]
    name: String,

    #[allow(unused)]
    value: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawQueryResponseChunk {
    url: String,

    #[allow(unused)]
    row_count: i64,

    #[allow(unused)]
    uncompressed_size: i64,

    #[allow(unused)]
    compressed_size: i64,
}

#[derive(serde::Deserialize, Debug)]
struct SnowflakeResponse {
    /// Response data payload. May be null when session has expired (e.g., code 390112).
    data: Option<RawQueryResponse>,
    message: Option<String>,
    success: bool,
    code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_session_expired() {
        let json = serde_json::json!({
            "data": null,
            "code": "390112",
            "message": "Your session has expired. Please login again.",
            "success": false,
            "headers": null
        });
        let resp: SnowflakeResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.code.as_deref(), Some("390112"));
    }
}
