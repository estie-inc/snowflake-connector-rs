use std::{collections::HashMap, sync::Arc};

use http::{
    header::{ACCEPT, AUTHORIZATION},
    HeaderMap,
};
use reqwest::Client;

use crate::{chunk::download_chunk, Error, Result, SnowflakeRow};

pub(super) const SESSION_EXPIRED: &str = "390112";

pub(super) async fn query<Q: Into<QueryRequest>>(
    http: &Client,
    account: &str,
    request: Q,
    session_token: &str,
) -> Result<Vec<SnowflakeRow>> {
    let request_id = uuid::Uuid::new_v4();
    let url = format!(
        r"https://{account}.snowflakecomputing.com/queries/v1/query-request?requestId={request_id}"
    );

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

    let response: SnowflakeResponse =
        serde_json::from_str(&body).map_err(|e| Error::Json(e, body))?;

    if let Some(SESSION_EXPIRED) = response.code.as_deref() {
        return Err(Error::SessionExpired);
    }

    if !response.success {
        return Err(Error::Communication(response.message.unwrap_or_default()));
    }

    if response.data.query_result_format != "json" {
        return Err(Error::UnsupportedFormat(response.data.query_result_format));
    }

    let http = http.clone();
    let qrmk = response.data.qrmk.unwrap_or_default();
    let chunks = response.data.chunks.unwrap_or_default();
    let row_types = response.data.row_types;
    let mut row_set = response.data.row_set;

    let chunk_headers = response.data.chunk_headers.unwrap_or_default();
    let chunk_headers: HeaderMap = HeaderMap::try_from(&chunk_headers)?;

    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let http = http.clone();
        let chunk_headers = chunk_headers.clone();
        let qrmk = qrmk.clone();
        handles.push(tokio::spawn(async move {
            download_chunk(http, chunk.url, chunk_headers, qrmk).await
        }));
    }

    for fut in handles {
        let result = fut.await?;
        let rows = result?;
        row_set.extend(rows);
    }

    let column_names = row_types
        .into_iter()
        .enumerate()
        .map(|(i, name)| (name.name.to_ascii_uppercase(), i))
        .collect::<HashMap<_, _>>();
    let column_names = Arc::new(column_names);
    Ok(row_set
        .into_iter()
        .map(|row| SnowflakeRow {
            row,
            column_names: Arc::clone(&column_names),
        })
        .collect())
}

#[derive(Debug, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
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
    parameters: Vec<RawQueryResponseParameter>,
    #[allow(unused)]
    query_id: String,
    #[allow(unused)]
    returned: i64,
    #[allow(unused)]
    total: i64,

    #[serde(rename = "rowset")]
    row_set: Vec<Vec<Option<String>>>,

    #[serde(rename = "rowtype")]
    row_types: Vec<RawQueryResponseRowType>,

    chunk_headers: Option<HashMap<String, String>>,

    qrmk: Option<String>,

    chunks: Option<Vec<RawQueryResponseChunk>>,
    query_result_format: String,
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
    data: RawQueryResponse,
    message: Option<String>,
    success: bool,
    code: Option<String>,
}
