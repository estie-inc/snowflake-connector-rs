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
                format!(r#"Snowflake Token="{session_token}""#),
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

        let mut handles = Vec::with_capacity(chunks.len());
        while let Some(chunk) = chunks.pop() {
            let http = self.http.clone();
            let chunk_headers = self.chunk_headers.clone();
            let qrmk = self.qrmk.clone();
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

    fn convert_row(&self, row: Vec<Option<String>>) -> SnowflakeRow {
        SnowflakeRow {
            row,
            column_indices: Arc::clone(&self.column_indices),
            column_types: Arc::clone(&self.column_types),
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
                format!(r#"Snowflake Token="{session_token}""#),
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

/// A single bind parameter for a Snowflake query.
///
/// `value` is `None` for SQL NULL.
#[derive(Debug, serde::Serialize, Clone)]
pub struct Binding {
    #[serde(rename = "type")]
    pub binding_type: BindingType,
    pub value: Option<String>,
}

impl Binding {
    pub fn new(binding_type: BindingType, value: impl Into<String>) -> Self {
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

    pub fn fixed(value: impl ToString) -> Self {
        Self::new(BindingType::Fixed, value.to_string())
    }

    pub fn real(value: impl ToString) -> Self {
        Self::new(BindingType::Real, value.to_string())
    }

    pub fn text(value: impl Into<String>) -> Self {
        Self::new(BindingType::Text, value)
    }

    pub fn boolean(value: bool) -> Self {
        Self::new(BindingType::Boolean, value.to_string())
    }

    pub fn date(value: impl ToString) -> Self {
        Self::new(BindingType::Date, value.to_string())
    }

    pub fn time(value: impl ToString) -> Self {
        Self::new(BindingType::Time, value.to_string())
    }

    pub fn timestamp_ntz(value: impl ToString) -> Self {
        Self::new(BindingType::TimestampNtz, value.to_string())
    }

    pub fn timestamp_ltz(value: impl ToString) -> Self {
        Self::new(BindingType::TimestampLtz, value.to_string())
    }

    pub fn timestamp_tz(value: impl ToString) -> Self {
        Self::new(BindingType::TimestampTz, value.to_string())
    }

    pub fn binary(value: impl Into<String>) -> Self {
        Self::new(BindingType::Binary, value)
    }
}

#[derive(Debug, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub sql_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bindings: Option<HashMap<String, Binding>>,
}

impl QueryRequest {
    /// Build a query with positional bind parameters.
    /// The `Vec` index order maps to `"1"`, `"2"`, … (1-origin keys).
    pub fn with_bindings(sql_text: impl Into<String>, bindings: Vec<Binding>) -> Self {
        let map: HashMap<String, Binding> = bindings
            .into_iter()
            .enumerate()
            .map(|(i, b)| ((i + 1).to_string(), b))
            .collect();
        Self {
            sql_text: sql_text.into(),
            bindings: if map.is_empty() { None } else { Some(map) },
        }
    }
}

impl From<&str> for QueryRequest {
    fn from(sql_text: &str) -> Self {
        Self {
            sql_text: sql_text.to_string(),
            bindings: None,
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
            bindings: None,
        }
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
        assert!(request.bindings.is_none());
    }

    #[test]
    fn test_from_string_has_no_bindings() {
        let request = QueryRequest::from("SELECT 1".to_string());
        assert!(request.bindings.is_none());
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
        let bindings = request.bindings.as_ref().unwrap();
        assert_eq!(bindings.len(), 3);
        assert_eq!(bindings["1"].binding_type, BindingType::Fixed);
        assert_eq!(bindings["1"].value.as_deref(), Some("1"));
        assert_eq!(bindings["2"].binding_type, BindingType::Text);
        assert_eq!(bindings["2"].value.as_deref(), Some("two"));
        assert_eq!(bindings["3"].binding_type, BindingType::Boolean);
        assert_eq!(bindings["3"].value.as_deref(), Some("true"));
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
    fn test_with_bindings_empty_vec_produces_none() {
        let request = QueryRequest::with_bindings("SELECT 1", vec![]);
        assert!(request.bindings.is_none());
    }

    #[test]
    fn test_binding_constructors() {
        assert_eq!(Binding::fixed(42).binding_type, BindingType::Fixed);
        assert_eq!(Binding::real(1.5).binding_type, BindingType::Real);
        assert_eq!(Binding::text("hi").binding_type, BindingType::Text);
        assert_eq!(Binding::boolean(true).binding_type, BindingType::Boolean);
        assert_eq!(Binding::boolean(true).value.as_deref(), Some("true"));
        assert_eq!(Binding::boolean(false).value.as_deref(), Some("false"));
        assert_eq!(Binding::date("19000").binding_type, BindingType::Date);
        assert_eq!(Binding::time("123456789").binding_type, BindingType::Time);
        assert_eq!(
            Binding::timestamp_ntz("123456789").binding_type,
            BindingType::TimestampNtz
        );
        assert_eq!(
            Binding::timestamp_ltz("123456789").binding_type,
            BindingType::TimestampLtz
        );
        assert_eq!(
            Binding::timestamp_tz("123456789").binding_type,
            BindingType::TimestampTz
        );
        assert_eq!(
            Binding::binary("48656C6C6F").binding_type,
            BindingType::Binary
        );
    }
}
