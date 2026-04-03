use std::collections::VecDeque;
use std::time::{Duration, Instant};
use std::{collections::HashMap, mem, sync::Arc};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use http::{
    HeaderMap,
    header::{ACCEPT, AUTHORIZATION},
};
use reqwest::{Client, Url};
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
    /// Updated on refresh because it may be rotated.
    qrmk: Mutex<String>,
    chunks: Mutex<VecDeque<RawQueryResponseChunk>>,
    /// Updated on refresh because it may change.
    chunk_headers: Mutex<HeaderMap>,
    column_types: Arc<Vec<SnowflakeColumnType>>,
    column_indices: Arc<HashMap<String, usize>>,
    row_set: Mutex<Option<Vec<Vec<Option<String>>>>>,
    session_token: String,
    base_url: Url,
    /// Path to re-fetch query-result metadata with fresh presigned URLs.
    /// This does **not** re-execute the query.
    result_path: String,
    total_chunks: usize,
}

impl QueryExecutor {
    pub(super) async fn create<Q: Into<QueryRequest>>(
        sess: &SnowflakeSession,
        request: Q,
    ) -> Result<Self> {
        let SnowflakeSession {
            http,
            session_token,
            query,
            ..
        } = sess;
        let query_timeout = query
            .async_query_completion_timeout()
            .unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS));

        let request_id = uuid::Uuid::new_v4();
        let base_url = sess.base_url.clone();
        let mut url = base_url.join("queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let request: QueryRequest = request.into();
        let mut response_data = send_snowflake_request(
            snowflake_request(http, http::Method::POST, url, session_token).json(&request),
        )
        .await?;

        if let Some(result_url) = response_data.get_result_url.take() {
            response_data = poll_for_async_results(
                http,
                &result_url,
                session_token,
                query_timeout,
                base_url.clone(),
            )
            .await?;
        }

        if let Some(format) = response_data.query_result_format {
            if format != "json" {
                return Err(Error::UnsupportedFormat(format.clone()));
            }
        }

        let http = http.clone();
        let session_token = session_token.clone();
        let qrmk = response_data.qrmk.unwrap_or_default();
        let result_path = response_data
            .get_result_url
            .unwrap_or_else(|| format!("/queries/{}/result", response_data.query_id));
        let chunks_vec = response_data.chunks.unwrap_or_default();
        let total_chunks = chunks_vec.len();
        let chunks = Mutex::new(VecDeque::from(chunks_vec));
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
            qrmk: Mutex::new(qrmk),
            chunks,
            chunk_headers: Mutex::new(chunk_headers),
            column_types,
            column_indices,
            row_set,
            session_token,
            base_url,
            result_path,
            total_chunks,
        })
    }

    /// Check if there are no more rows to fetch
    pub async fn eof(&self) -> bool {
        let row_set = &*self.row_set.lock().await;
        let chunks = &*self.chunks.lock().await;
        row_set.is_none() && chunks.is_empty()
    }

    /// Fetch a single chunk.
    ///
    /// When a presigned URL has expired, this method transparently refreshes
    /// all remaining URLs (without re-executing the query) and retries.
    pub async fn fetch_next_chunk(&self) -> Result<Option<Vec<SnowflakeRow>>> {
        {
            let mut row_set = self.row_set.lock().await;
            if let Some(rs) = row_set.take() {
                let rows = rs.into_iter().map(|r| self.convert_row(r)).collect();
                return Ok(Some(rows));
            }
        }

        // pop_front yields chunks in the original order (0, 1, 2, …).
        // total_chunks - remaining_after_pop gives the original index.
        let (chunk_url, original_index) = {
            let mut chunks = self.chunks.lock().await;
            let Some(chunk) = chunks.pop_front() else {
                return Ok(None);
            };
            (chunk.url, self.total_chunks - chunks.len() - 1)
        };

        let http = self.http.clone();
        let chunk_headers = self.chunk_headers.lock().await.clone();
        let qrmk = self.qrmk.lock().await.clone();

        match download_chunk(http.clone(), chunk_url, chunk_headers, qrmk).await {
            Ok(rows) => {
                let rows = rows.into_iter().map(|r| self.convert_row(r)).collect();
                Ok(Some(rows))
            }
            Err(Error::ChunkUrlExpired) => {
                let (fresh_url, chunk_headers, qrmk) =
                    self.refresh_presigned_urls(original_index).await?;
                let rows = download_chunk(http, fresh_url, chunk_headers, qrmk).await?;
                let rows = rows.into_iter().map(|r| self.convert_row(r)).collect();
                Ok(Some(rows))
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch all the remaining chunks at once
    pub async fn fetch_all(&self) -> Result<Vec<SnowflakeRow>> {
        self.fetch_all_with_concurrency_limit(usize::MAX).await
    }

    /// Fetch all remaining chunks while capping concurrent downloads.
    ///
    /// `max_concurrency` values below `1` are treated as `1` to ensure progress.
    pub async fn fetch_all_with_concurrency_limit(
        &self,
        max_concurrency: usize,
    ) -> Result<Vec<SnowflakeRow>> {
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

        let chunk_headers = self.chunk_headers.lock().await.clone();
        let qrmk = self.qrmk.lock().await.clone();

        let mut handles = Vec::with_capacity(chunks.len());
        while let Some(chunk) = chunks.pop_front() {
            let http = self.http.clone();
            let chunk_headers = chunk_headers.clone();
            let qrmk = qrmk.clone();
            let semaphore = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await?;
                download_chunk(http, chunk.url, chunk_headers, qrmk).await
            }));
        }

        for fut in handles {
            let result = fut.await?;
            rows.extend(result?.into_iter().map(|r| self.convert_row(r)));
        }

        Ok(rows)
    }

    /// Re-fetch result metadata from Snowflake to obtain fresh presigned URLs,
    /// update internal state (`qrmk`, `chunk_headers`, remaining `chunks`),
    /// and return the fresh URL + headers + qrmk for `retry_chunk_index`.
    ///
    /// This does **not** re-execute the query.  Snowflake persists query
    /// results for 24 hours; this call simply returns the same result set
    /// with newly signed download URLs.
    async fn refresh_presigned_urls(
        &self,
        retry_chunk_index: usize,
    ) -> Result<(String, HeaderMap, String)> {
        let url = resolve_url(&self.base_url, &self.result_path)?;
        let data = send_snowflake_request(snowflake_request(
            &self.http,
            http::Method::GET,
            url,
            &self.session_token,
        ))
        .await?;

        let fresh_qrmk = data.qrmk.unwrap_or_default();
        let fresh_headers: HeaderMap =
            HeaderMap::try_from(&data.chunk_headers.unwrap_or_default())?;
        let fresh_chunks = data.chunks.unwrap_or_default();

        let fresh_url = fresh_chunks
            .get(retry_chunk_index)
            .ok_or_else(|| {
                Error::Communication(format!(
                    "refreshed result missing chunk index {} (total: {})",
                    retry_chunk_index,
                    fresh_chunks.len()
                ))
            })?
            .url
            .clone();

        *self.qrmk.lock().await = fresh_qrmk.clone();
        *self.chunk_headers.lock().await = fresh_headers.clone();
        *self.chunks.lock().await = fresh_chunks
            .into_iter()
            .skip(retry_chunk_index + 1)
            .collect();

        Ok((fresh_url, fresh_headers, fresh_qrmk))
    }

    /// Replace all remaining chunk URLs with expired ones so that
    /// `is_presigned_url_expired` returns `true` on the next download attempt.
    #[cfg(test)]
    async fn expire_chunk_urls_for_testing(&self) {
        let mut chunks = self.chunks.lock().await;
        for chunk in chunks.iter_mut() {
            chunk.url = "https://expired.s3.amazonaws.com/chunk\
                ?X-Amz-Date=20200101T000000Z&X-Amz-Expires=3600"
                .to_string();
        }
    }

    fn convert_row(&self, row: Vec<Option<String>>) -> SnowflakeRow {
        SnowflakeRow {
            row,
            column_indices: Arc::clone(&self.column_indices),
            column_types: Arc::clone(&self.column_types),
        }
    }
}

/// Build a request with Snowflake's standard auth headers.
fn snowflake_request(
    http: &Client,
    method: http::Method,
    url: Url,
    session_token: &str,
) -> reqwest::RequestBuilder {
    http.request(method, url)
        .header(ACCEPT, "application/snowflake")
        .header(
            AUTHORIZATION,
            format!(r#"Snowflake Token="{session_token}""#),
        )
}

/// Send a Snowflake API request, validate the response (HTTP errors,
/// session expiry, API-level failures), and return the `data` payload.
async fn send_snowflake_request(request: reqwest::RequestBuilder) -> Result<RawQueryResponse> {
    let resp = request.send().await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        return Err(Error::Communication(format!("HTTP {status}: {body}")));
    }
    let response: SnowflakeResponse =
        serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;
    if response.code.as_deref() == Some(SESSION_EXPIRED) {
        return Err(Error::SessionExpired);
    }
    if !response.success {
        return Err(Error::Communication(response.message.unwrap_or_default()));
    }
    response
        .data
        .ok_or_else(|| Error::Communication("missing data field in response".to_string()))
}

fn resolve_url(base_url: &Url, path: &str) -> Result<Url> {
    if let Ok(absolute) = Url::parse(path) {
        Ok(absolute)
    } else {
        Ok(base_url.join(path)?)
    }
}

/// Poll until the async query completes, checking the response code.
async fn poll_for_async_results(
    http: &Client,
    result_url: &str,
    session_token: &str,
    timeout: Duration,
    base_url: Url,
) -> Result<RawQueryResponse> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        sleep(Duration::from_secs(10)).await;
        let url = resolve_url(&base_url, result_url)?;
        let resp = snowflake_request(http, http::Method::GET, url, session_token)
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
            if response.code.as_deref() == Some(SESSION_EXPIRED) {
                return Err(Error::SessionExpired);
            }
            if !response.success {
                return Err(Error::Communication(response.message.unwrap_or_default()));
            }
            return response
                .data
                .ok_or_else(|| Error::Communication("missing data field in response".to_string()));
        }
    }

    Err(Error::TimedOut)
}

/// Snowflake bind parameter type.
///
/// See <https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement>
#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq)]
pub enum BindingType {
    #[serde(rename = "FIXED")]
    Fixed,
    #[serde(rename = "REAL")]
    Real,
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "BOOLEAN")]
    Boolean,
    #[serde(rename = "DATE")]
    Date,
    #[serde(rename = "TIME")]
    Time,
    #[serde(rename = "TIMESTAMP_NTZ")]
    TimestampNtz,
    #[serde(rename = "TIMESTAMP_LTZ")]
    TimestampLtz,
    #[serde(rename = "TIMESTAMP_TZ")]
    TimestampTz,
    #[serde(rename = "BINARY")]
    Binary,
}

mod sealed {
    pub trait Sealed {}
}

/// Types accepted by [`Binding::fixed`] (Snowflake `FIXED` / exact numeric).
pub trait SnowflakeFixedType: sealed::Sealed + ToString {}

/// Types accepted by [`Binding::real`] (Snowflake `REAL` / floating-point).
pub trait SnowflakeRealType: sealed::Sealed + ToString {}

macro_rules! impl_sealed_binding {
    ($trait:ident => $($t:ty),*) => { $(
        impl sealed::Sealed for $t {}
        impl $trait for $t {}
    )* };
}
impl_sealed_binding!(SnowflakeFixedType => i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);
impl_sealed_binding!(SnowflakeRealType => f32, f64);

/// A single bind parameter for a Snowflake query.
///
/// Use [`Binding::null`] to represent SQL NULL.
#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq)]
pub struct Binding {
    #[serde(rename = "type")]
    binding_type: BindingType,
    value: Option<String>,
}

impl Binding {
    pub(crate) fn new(binding_type: BindingType, value: impl Into<String>) -> Self {
        Self {
            binding_type,
            value: Some(value.into()),
        }
    }

    /// NULL bind parameter with the given type.
    pub fn null(binding_type: BindingType) -> Self {
        Self {
            binding_type,
            value: None,
        }
    }

    pub fn fixed(value: impl SnowflakeFixedType) -> Self {
        Self::new(BindingType::Fixed, value.to_string())
    }

    /// Snowflake's REST API rejects `"inf"` / `"-inf"` for REAL bind type
    /// but accepts `"Infinity"` / `"-Infinity"`. `"NaN"` is accepted as-is.
    pub fn real(value: impl SnowflakeRealType) -> Self {
        let s = value.to_string();
        let s = match s.as_str() {
            "inf" => "Infinity".to_string(),
            "-inf" => "-Infinity".to_string(),
            _ => s,
        };
        Self::new(BindingType::Real, s)
    }

    pub fn text(value: impl Into<String>) -> Self {
        Self::new(BindingType::Text, value)
    }

    pub fn boolean(value: bool) -> Self {
        Self::new(BindingType::Boolean, value.to_string())
    }

    /// Snowflake REST API expects milliseconds since the Unix epoch for DATE.
    pub fn date(value: NaiveDate) -> Self {
        let ms = value
            .and_hms_opt(0, 0, 0)
            .expect("and_hms_opt(0, 0, 0) is always valid")
            .and_utc()
            .timestamp_millis();
        Self::new(BindingType::Date, ms.to_string())
    }

    /// Snowflake REST API expects nanoseconds since midnight for TIME.
    pub fn time(value: NaiveTime) -> Self {
        let total_nanos = u64::from(value.num_seconds_from_midnight()) * 1_000_000_000
            + u64::from(value.nanosecond() % 1_000_000_000);
        Self::new(BindingType::Time, total_nanos.to_string())
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch for TIMESTAMP_NTZ.
    pub fn timestamp_ntz(value: NaiveDateTime) -> Self {
        Self::new(BindingType::TimestampNtz, format_epoch_nanos(value))
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch for TIMESTAMP_LTZ.
    pub fn timestamp_ltz(value: DateTime<Utc>) -> Self {
        Self::new(
            BindingType::TimestampLtz,
            format_epoch_nanos(value.naive_utc()),
        )
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch followed by
    /// a space and the timezone offset encoded as `1440 + offset_minutes`.
    pub fn timestamp_tz(value: DateTime<FixedOffset>) -> Self {
        let nanos = format_epoch_nanos(value.naive_utc());
        let offset_minutes = value.offset().local_minus_utc() / 60;
        let sf_tz = 1440 + offset_minutes;
        Self::new(BindingType::TimestampTz, format!("{nanos} {sf_tz}"))
    }

    pub fn binary(value: &[u8]) -> Self {
        let hex = value
            .iter()
            .fold(String::with_capacity(value.len() * 2), |mut s, b| {
                use std::fmt::Write;
                let _ = write!(s, "{b:02X}");
                s
            });
        Self::new(BindingType::Binary, hex)
    }
}

/// Falls back to manual calculation when `timestamp_nanos_opt()` returns None
/// (dates outside ~1678–2262 overflow i64 nanoseconds).
fn format_epoch_nanos(value: NaiveDateTime) -> String {
    let ts = value.and_utc();
    match ts.timestamp_nanos_opt() {
        Some(nanos) => nanos.to_string(),
        None => {
            let secs = ts.timestamp();
            let total_nanos =
                i128::from(secs) * 1_000_000_000 + i128::from(ts.timestamp_subsec_nanos());
            total_nanos.to_string()
        }
    }
}

#[derive(Debug, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    /// SQL statement to execute against Snowflake.
    pub sql_text: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,
}

impl QueryRequest {
    pub fn bindings(&self) -> &HashMap<String, Binding> {
        &self.bindings
    }

    /// Build a query with positional bind parameters.
    /// The iterator index order maps to `"1"`, `"2"`, … (1-origin keys).
    pub fn with_bindings(
        sql_text: impl Into<String>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Self {
        let map: HashMap<String, Binding> = bindings
            .into_iter()
            .enumerate()
            .map(|(i, b)| ((i + 1).to_string(), b))
            .collect();
        Self {
            sql_text: sql_text.into(),
            bindings: map,
        }
    }
}

impl From<&str> for QueryRequest {
    fn from(sql_text: &str) -> Self {
        Self {
            sql_text: sql_text.to_string(),
            bindings: HashMap::new(),
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
        Self {
            sql_text,
            bindings: HashMap::new(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawQueryResponse {
    #[allow(unused)]
    parameters: Option<Vec<RawQueryResponseParameter>>,
    query_id: String,
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

#[derive(Debug, Clone, serde::Deserialize)]
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

    /// Create a Snowflake session using environment variables.
    /// Returns `None` when credentials are not available so that
    /// integration tests can be silently skipped.
    async fn create_test_session() -> Option<crate::SnowflakeSession> {
        let username = std::env::var("SNOWFLAKE_USERNAME").ok()?;
        let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
        let private_key = std::env::var("SNOWFLAKE_PRIVATE_KEY").ok()?;
        let password = std::env::var("SNOWFLAKE_PRIVATE_KEY_PASSWORD").ok()?;

        let mut session_config = crate::SnowflakeSessionConfig::default();
        if let Ok(v) = std::env::var("SNOWFLAKE_ROLE") {
            session_config = session_config.with_role(v);
        }
        if let Ok(v) = std::env::var("SNOWFLAKE_WAREHOUSE") {
            session_config = session_config.with_warehouse(v);
        }
        if let Ok(v) = std::env::var("SNOWFLAKE_DATABASE") {
            session_config = session_config.with_database(v);
        }
        if let Ok(v) = std::env::var("SNOWFLAKE_SCHEMA") {
            session_config = session_config.with_schema(v);
        }

        let config = crate::SnowflakeClientConfig::new(
            &username,
            &account,
            crate::SnowflakeAuthMethod::KeyPair {
                encrypted_pem: private_key,
                password: password.into_bytes(),
            },
        )
        .with_session(session_config);

        let client = crate::SnowflakeClient::new(config).ok()?;
        client.create_session().await.ok()
    }

    /// Verify that `fetch_next_chunk` transparently refreshes expired
    /// presigned URLs by calling `refresh_presigned_urls`.
    ///
    /// 1. Execute a Snowflake query large enough to produce multiple chunks.
    /// 2. Consume the initial inline row-set.
    /// 3. Replace remaining chunk URLs with expired ones.
    /// 4. Call `fetch_next_chunk` and assert it succeeds — proving that
    ///    the refresh endpoint returned fresh URLs and the download worked.
    ///
    /// Skipped when Snowflake credentials are not set.
    #[tokio::test]
    async fn test_fetch_next_chunk_refreshes_expired_urls() {
        let Some(session) = create_test_session().await else {
            return;
        };

        let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS PAD \
                     FROM TABLE(GENERATOR(ROWCOUNT=>200000))";
        let executor = session.execute(query).await.unwrap();

        // Consume the initial row_set.
        let first = executor.fetch_next_chunk().await.unwrap();
        assert!(first.is_some(), "expected initial row_set");

        // We need remaining chunks for this test.
        assert!(
            !executor.eof().await,
            "need remaining chunks to test refresh"
        );

        // Poison all remaining chunk URLs so they appear expired.
        executor.expire_chunk_urls_for_testing().await;

        // fetch_next_chunk should detect the expired URL, call
        // refresh_presigned_urls (GET /queries/{id}/result), obtain
        // fresh URLs, and download the chunk successfully.
        let rows = executor.fetch_next_chunk().await.unwrap();
        assert!(rows.is_some(), "expected rows after URL refresh");
        assert!(
            !rows.unwrap().is_empty(),
            "refreshed chunk should contain rows"
        );
    }

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

    #[test]
    fn test_query_request_with_bindings_serializes_correctly() {
        let request = QueryRequest::with_bindings(
            "INSERT INTO t (c1, c2) VALUES (?, ?)",
            vec![Binding::fixed(123), Binding::text("hello")],
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["sqlText"], "INSERT INTO t (c1, c2) VALUES (?, ?)");
        assert_eq!(json["bindings"]["1"]["type"], "FIXED");
        assert_eq!(json["bindings"]["1"]["value"], "123");
        assert_eq!(json["bindings"]["2"]["type"], "TEXT");
        assert_eq!(json["bindings"]["2"]["value"], "hello");
    }

    #[test]
    fn test_query_request_without_bindings_omits_field() {
        let request = QueryRequest::from("SELECT 1");
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["sqlText"], "SELECT 1");
        assert!(json.get("bindings").is_none());
    }

    #[test]
    fn test_from_str_has_no_bindings() {
        let request = QueryRequest::from("SELECT 1");
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_from_string_has_no_bindings() {
        let request = QueryRequest::from("SELECT 1".to_string());
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_with_bindings_indices_are_1_origin() {
        let request = QueryRequest::with_bindings(
            "SELECT ?, ?, ?",
            vec![
                Binding::fixed(1),
                Binding::text("two"),
                Binding::boolean(true),
            ],
        );
        let json = serde_json::to_value(&request).unwrap();
        let bindings = json["bindings"].as_object().unwrap();
        assert_eq!(bindings.len(), 3);
        assert_eq!(json["bindings"]["1"]["type"], "FIXED");
        assert_eq!(json["bindings"]["1"]["value"], "1");
        assert_eq!(json["bindings"]["2"]["type"], "TEXT");
        assert_eq!(json["bindings"]["2"]["value"], "two");
        assert_eq!(json["bindings"]["3"]["type"], "BOOLEAN");
        assert_eq!(json["bindings"]["3"]["value"], "true");
    }

    #[test]
    fn test_null_binding_serializes_correctly() {
        let request = QueryRequest::with_bindings(
            "INSERT INTO t (c1) VALUES (?)",
            vec![Binding::null(BindingType::Text)],
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["bindings"]["1"]["type"], "TEXT");
        assert!(json["bindings"]["1"]["value"].is_null());
    }

    #[test]
    fn test_with_bindings_empty_vec_produces_empty_map() {
        let request = QueryRequest::with_bindings("SELECT 1", vec![]);
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_binding_constructors() {
        let cases: Vec<(Binding, &str, &str)> = vec![
            (Binding::fixed(42), "FIXED", "42"),
            (Binding::real(1.5), "REAL", "1.5"),
            (Binding::text("hi"), "TEXT", "hi"),
            (Binding::boolean(true), "BOOLEAN", "true"),
            (Binding::boolean(false), "BOOLEAN", "false"),
            (
                Binding::date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
                "DATE",
                "1718409600000",
            ),
            (
                Binding::time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()),
                "TIME",
                "45296000000000",
            ),
            (
                Binding::timestamp_ntz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap(),
                ),
                "TIMESTAMP_NTZ",
                "1718454645000000000",
            ),
            (
                Binding::timestamp_ltz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc(),
                ),
                "TIMESTAMP_LTZ",
                "1718454645000000000",
            ),
            (
                Binding::timestamp_tz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc()
                        .fixed_offset(),
                ),
                "TIMESTAMP_TZ",
                "1718454645000000000 1440",
            ),
            (Binding::binary(b"Hello"), "BINARY", "48656C6C6F"),
        ];
        for (binding, expected_type, expected_value) in cases {
            let json = serde_json::to_value(&binding).unwrap();
            assert_eq!(json["type"], expected_type);
            assert_eq!(json["value"], expected_value);
        }
    }

    #[test]
    fn test_timestamp_tz_non_utc_offsets() {
        // UTC+9 (e.g. JST): 2024-06-15 21:30:45+09:00 = 2024-06-15 12:30:45 UTC
        let offset_east = FixedOffset::east_opt(9 * 3600).unwrap();
        let dt_east = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(21, 30, 45)
            .unwrap()
            .and_local_timezone(offset_east)
            .unwrap();
        let json = serde_json::to_value(Binding::timestamp_tz(dt_east)).unwrap();
        assert_eq!(json["type"], "TIMESTAMP_TZ");
        // UTC nanos for 2024-06-15 12:30:45 UTC, offset = 1440 + 540 = 1980
        assert_eq!(json["value"], "1718454645000000000 1980");

        // UTC-5 (e.g. EST): 2024-06-15 07:30:45-05:00 = 2024-06-15 12:30:45 UTC
        let offset_west = FixedOffset::west_opt(5 * 3600).unwrap();
        let dt_west = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(7, 30, 45)
            .unwrap()
            .and_local_timezone(offset_west)
            .unwrap();
        let json = serde_json::to_value(Binding::timestamp_tz(dt_west)).unwrap();
        assert_eq!(json["type"], "TIMESTAMP_TZ");
        // offset = 1440 + (-300) = 1140
        assert_eq!(json["value"], "1718454645000000000 1140");
    }

    #[test]
    fn test_real_nan_uses_real_type() {
        let json = serde_json::to_value(Binding::real(f64::NAN)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "NaN");
    }

    #[test]
    fn test_real_infinity_rewrites_to_full_word() {
        let json = serde_json::to_value(Binding::real(f64::INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "Infinity");

        let json = serde_json::to_value(Binding::real(f64::NEG_INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "-Infinity");

        let json = serde_json::to_value(Binding::real(f32::INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "Infinity");

        let json = serde_json::to_value(Binding::real(f32::NEG_INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "-Infinity");
    }

    #[test]
    fn test_fixed_boundary_values() {
        let cases: Vec<(Binding, String)> = vec![
            (Binding::fixed(-1_i64), "-1".to_string()),
            (Binding::fixed(0_i64), "0".to_string()),
            (Binding::fixed(i64::MAX), i64::MAX.to_string()),
            (Binding::fixed(i64::MIN), i64::MIN.to_string()),
            (Binding::fixed(u64::MAX), u64::MAX.to_string()),
        ];
        for (binding, expected_value) in cases {
            let json = serde_json::to_value(&binding).unwrap();
            assert_eq!(json["type"], "FIXED");
            assert_eq!(json["value"], expected_value);
        }
    }

    #[test]
    fn test_real_zero() {
        let json = serde_json::to_value(Binding::real(0.0_f64)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "0");
    }
}
