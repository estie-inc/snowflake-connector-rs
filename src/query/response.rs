use std::collections::HashMap;
use std::sync::Arc;

use http::HeaderMap;
use serde::Deserialize;

use crate::Result;

pub(crate) const SESSION_EXPIRED: &str = "390112";
pub(crate) const QUERY_IN_PROGRESS_CODE: &str = "333333";
pub(crate) const QUERY_IN_PROGRESS_ASYNC_CODE: &str = "333334";

pub(crate) const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

#[derive(Deserialize, Debug)]
pub(crate) struct SnowflakeResponse {
    pub(crate) data: Option<RawQueryResponse>,
    pub(crate) message: Option<String>,
    pub(crate) success: bool,
    pub(crate) code: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponse {
    #[allow(unused)]
    pub(crate) parameters: Option<Vec<RawQueryResponseParameter>>,
    pub(crate) query_id: String,
    pub(crate) get_result_url: Option<String>,
    #[allow(unused)]
    pub(crate) returned: Option<i64>,
    #[allow(unused)]
    pub(crate) total: Option<i64>,

    #[serde(rename = "rowset")]
    pub(crate) row_set: Option<Vec<Vec<Option<String>>>>,

    #[serde(rename = "rowtype")]
    pub(crate) row_types: Option<Vec<RawQueryResponseRowType>>,

    pub(crate) chunk_headers: Option<HashMap<String, String>>,

    pub(crate) qrmk: Option<String>,

    pub(crate) chunks: Option<Vec<RawQueryResponseChunk>>,
    pub(crate) query_result_format: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponseRowType {
    #[allow(unused)]
    pub(crate) database: String,
    pub(crate) name: String,
    pub(crate) nullable: bool,
    pub(crate) scale: Option<i64>,
    #[allow(unused)]
    pub(crate) byte_length: Option<i64>,
    pub(crate) length: Option<i64>,
    #[allow(unused)]
    pub(crate) schema: String,
    #[allow(unused)]
    pub(crate) table: String,
    pub(crate) precision: Option<i64>,

    #[serde(rename = "type")]
    pub(crate) data_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponseParameter {
    #[allow(unused)]
    pub(crate) name: String,
    #[allow(unused)]
    pub(crate) value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponseChunk {
    pub(crate) url: String,
    pub(crate) row_count: i64,
    pub(crate) uncompressed_size: i64,
    pub(crate) compressed_size: i64,
}

/// Resolve `qrmk` and raw `chunk_headers` into a fully-formed `HeaderMap`
/// suitable for downloading remote partitions.
pub(crate) fn resolve_download_headers(
    qrmk: &Option<String>,
    chunk_headers: &Option<HashMap<String, String>>,
) -> Result<Arc<HeaderMap>> {
    // Branch on chunk_headers presence, not emptiness: if Snowflake
    // returns `chunkHeaders: {}` that empty map is used as-is, without
    // falling back to qrmk-derived SSE-C headers.
    let headers = match (chunk_headers, qrmk) {
        (Some(ch), _) => HeaderMap::try_from(ch)?,
        (None, Some(qrmk)) => {
            let mut h = HeaderMap::with_capacity(2);
            h.append(HEADER_SSE_C_ALGORITHM, AES256.parse()?);
            h.append(HEADER_SSE_C_KEY, qrmk.parse()?);
            h
        }
        _ => HeaderMap::new(),
    };

    Ok(Arc::new(headers))
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
    fn resolve_headers_none_with_qrmk_synthesizes_sse_c() {
        let headers = resolve_download_headers(&Some("my-qrmk".into()), &None).unwrap();
        assert_eq!(headers.get(HEADER_SSE_C_ALGORITHM).unwrap(), AES256,);
        assert_eq!(headers.get(HEADER_SSE_C_KEY).unwrap(), "my-qrmk",);
    }

    #[test]
    fn resolve_headers_some_empty_map_does_not_fall_back_to_qrmk() {
        let empty = Some(HashMap::new());
        let headers = resolve_download_headers(&Some("my-qrmk".into()), &empty).unwrap();
        assert!(
            headers.is_empty(),
            "chunkHeaders: {{}} must be used as-is without qrmk fallback"
        );
    }

    #[test]
    fn resolve_headers_some_non_empty_uses_chunk_headers() {
        let mut map = HashMap::new();
        map.insert("x-custom".to_string(), "value".to_string());
        let headers = resolve_download_headers(&Some("my-qrmk".into()), &Some(map)).unwrap();
        assert_eq!(headers.get("x-custom").unwrap(), "value");
        assert!(
            headers.get(HEADER_SSE_C_KEY).is_none(),
            "qrmk headers must not be present when chunkHeaders is provided"
        );
    }
}
