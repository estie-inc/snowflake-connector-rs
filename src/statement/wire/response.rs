use std::{collections::HashMap, io, sync::Arc};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::error::ProtocolError;

pub(crate) const SESSION_EXPIRED: &str = "390112";
pub(crate) const QUERY_IN_PROGRESS_CODE: &str = "333333";
pub(crate) const QUERY_IN_PROGRESS_ASYNC_CODE: &str = "333334";

pub(crate) const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

#[derive(Debug)]
pub(crate) struct SnowflakeResponse {
    pub(crate) data: Option<RawQueryResponse>,
    pub(crate) message: Option<String>,
    pub(crate) success: bool,
    pub(crate) code: Option<String>,
}

// Response metadata the runtime ignores is omitted from the deserialize DTOs:
// - data.parameters: [{ "name": "...", "value": ... }]
// - data.rowtype[]: { "database": "...", "schema": "...", "table": "...", "byteLength": ... }
#[derive(Debug)]
pub(crate) struct RawQueryResponse {
    pub(crate) query_id: Arc<str>,
    pub(crate) get_result_url: Option<String>,
    pub(crate) returned: Option<i64>,
    pub(crate) total: Option<i64>,
    pub(crate) row_set_bytes: Option<Bytes>,
    pub(crate) row_types: Option<Vec<RawQueryResponseRowType>>,
    pub(crate) chunk_headers: Option<HashMap<String, String>>,
    pub(crate) qrmk: Option<String>,
    pub(crate) chunks: Option<Vec<RawQueryResponseChunk>>,
    pub(crate) query_result_format: Option<String>,
}

#[derive(Deserialize)]
struct BorrowedSnowflakeResponse<'a> {
    #[serde(borrow)]
    data: Option<BorrowedRawQueryResponse<'a>>,
    message: Option<String>,
    success: bool,
    code: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BorrowedRawQueryResponse<'a> {
    query_id: Option<String>,
    get_result_url: Option<String>,
    returned: Option<i64>,
    total: Option<i64>,

    #[serde(rename = "rowset", borrow)]
    row_set: Option<&'a RawValue>,

    #[serde(rename = "rowtype")]
    row_types: Option<Vec<RawQueryResponseRowType>>,

    chunk_headers: Option<HashMap<String, String>>,
    qrmk: Option<String>,
    chunks: Option<Vec<RawQueryResponseChunk>>,
    query_result_format: Option<String>,
}

/// Parse a response body, keeping the rowset as a zero-copy `Bytes` slice of the input.
pub(crate) fn parse_response(body: Bytes) -> Result<SnowflakeResponse, ProtocolError> {
    let borrowed: BorrowedSnowflakeResponse<'_> =
        serde_json::from_slice(strip_utf8_bom(body.as_ref()))
            .map_err(|e| ProtocolError::json_parse(e, &body))?;

    let data = borrowed
        .data
        .map(|d| {
            let row_set_bytes = d
                .row_set
                .map(|row_set| row_set_bytes_from_raw_value(&body, row_set))
                .transpose()?;
            Ok::<RawQueryResponse, ProtocolError>(RawQueryResponse {
                query_id: d
                    .query_id
                    .map(Arc::from)
                    .ok_or_else(|| ProtocolError::missing_field("data.queryId"))?,
                get_result_url: d.get_result_url,
                returned: d.returned,
                total: d.total,
                row_set_bytes,
                row_types: d.row_types,
                chunk_headers: d.chunk_headers,
                qrmk: d.qrmk,
                chunks: d.chunks,
                query_result_format: d.query_result_format,
            })
        })
        .transpose()?;

    Ok(SnowflakeResponse {
        data,
        message: borrowed.message,
        success: borrowed.success,
        code: borrowed.code,
    })
}

fn row_set_bytes_from_raw_value(body: &Bytes, raw: &RawValue) -> Result<Bytes, ProtocolError> {
    let raw_bytes = raw.get().as_bytes();
    // `BorrowedRawQueryResponse<'_>` uses `#[serde(borrow)] &'a RawValue`, so `raw_bytes` is expected to alias the original
    // response buffer. We rely on that invariant to reconstruct a zero-copy `Bytes::slice`.
    let body_start = body.as_ptr() as usize;
    let raw_start = raw_bytes.as_ptr() as usize;
    let start = raw_start
        .checked_sub(body_start)
        .ok_or_else(|| json_scan_error(body, "rowset slice must alias the response body"))?;
    let end = start
        .checked_add(raw_bytes.len())
        .ok_or_else(|| json_scan_error(body, "rowset slice overflow"))?;
    if end > body.len() {
        return Err(json_scan_error(
            body,
            "rowset slice must stay within the response body",
        ));
    }
    debug_assert_eq!(&body[start..end], raw_bytes);
    Ok(body.slice(start..end))
}

fn json_scan_error(body: &Bytes, message: impl Into<String>) -> ProtocolError {
    ProtocolError::json_parse(
        serde_json::Error::io(io::Error::new(io::ErrorKind::InvalidData, message.into())),
        body,
    )
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponseRowType {
    pub(crate) name: String,
    pub(crate) nullable: bool,
    pub(crate) scale: Option<u8>,
    pub(crate) length: Option<u32>,
    pub(crate) precision: Option<u8>,

    #[serde(rename = "type")]
    pub(crate) data_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResponseChunk {
    pub(crate) url: String,
    pub(crate) row_count: i64,
    pub(crate) uncompressed_size: i64,
    pub(crate) compressed_size: i64,
}

/// Resolve `qrmk` and raw `chunk_headers` into a fully-formed `HeaderMap` suitable for downloading remote partitions.
pub(crate) fn resolve_download_headers(
    qrmk: &Option<String>,
    chunk_headers: &Option<HashMap<String, String>>,
) -> Result<Arc<HeaderMap>, ProtocolError> {
    // Branch on chunk_headers presence, not emptiness: if Snowflake returns `chunkHeaders: {}` that empty map is used as-is,
    // without falling back to qrmk-derived SSE-C headers.
    let headers = match (chunk_headers, qrmk) {
        (Some(ch), _) => HeaderMap::try_from(ch).map_err(ProtocolError::header_conversion)?,
        (None, Some(qrmk)) => {
            let mut h = HeaderMap::with_capacity(2);
            h.append(HEADER_SSE_C_ALGORITHM, HeaderValue::from_static(AES256));
            h.append(
                HEADER_SSE_C_KEY,
                qrmk.parse()
                    .map_err(ProtocolError::invalid_response_header_value)?,
            );
            h
        }
        _ => HeaderMap::new(),
    };

    Ok(Arc::new(headers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, ErrorKind};

    #[test]
    fn test_deserialize_session_expired() {
        let body = Bytes::from(
            r#"{
                "data": null,
                "code": "390112",
                "message": "Your session has expired. Please login again.",
                "success": false,
                "headers": null
            }"#,
        );
        let resp = parse_response(body).unwrap();
        assert_eq!(resp.code.as_deref(), Some("390112"));
    }

    #[test]
    fn parse_response_ignores_unconsumed_metadata_fields() {
        // Unconsumed keys must be skipped while the consumed rowtype metadata still parses.
        let body = Bytes::from(
            r#"{
                "data": {
                    "queryId": "q1",
                    "parameters": [{"name": "TIMEZONE", "value": "UTC"}],
                    "rowset": [["x"]],
                    "rowtype": [{
                        "name": "COL",
                        "nullable": true,
                        "scale": 0,
                        "length": 16,
                        "precision": 38,
                        "type": "fixed",
                        "database": "DB",
                        "schema": "SC",
                        "table": "TBL",
                        "byteLength": 16
                    }],
                    "queryResultFormat": "json"
                },
                "success": true
            }"#,
        );
        let resp = parse_response(body).unwrap();
        let data = resp.data.expect("data");
        let row_types = data.row_types.expect("row_types");
        assert_eq!(row_types.len(), 1);
        let row_type = &row_types[0];
        assert_eq!(row_type.name, "COL");
        assert!(row_type.nullable);
        assert_eq!(row_type.scale, Some(0));
        assert_eq!(row_type.length, Some(16));
        assert_eq!(row_type.precision, Some(38));
        assert_eq!(row_type.data_type, "fixed");
    }

    #[test]
    fn parse_response_extracts_zero_copy_rowset_slice() {
        let body = Bytes::from(
            r#"{"data":{"queryId":"q1","rowset":[["a","b"],["c","d"]]},"success":true}"#,
        );
        let body_ptr = body.as_ptr() as usize;
        let body_len = body.len();
        let resp = parse_response(body).unwrap();
        let data = resp.data.expect("data");
        let rowset = data.row_set_bytes.expect("rowset_bytes");
        // The slice must point inside the original body, proving zero-copy.
        let rs_ptr = rowset.as_ptr() as usize;
        assert!(
            rs_ptr >= body_ptr && rs_ptr + rowset.len() <= body_ptr + body_len,
            "rowset slice must alias the original body bytes"
        );
        assert_eq!(&rowset[..], br#"[["a","b"],["c","d"]]"#);
    }

    #[test]
    fn parse_response_rowset_scanner_ignores_rowset_text_inside_strings() {
        let body = Bytes::from(
            r#"{"message":"rowset in top-level string","data":{"queryId":"q1","rowset":[["literal \"rowset\" text","brace: { }"]],"rowtype":[],"queryResultFormat":"json"},"success":true}"#,
        );
        let resp = parse_response(body).unwrap();
        let rowset = resp
            .data
            .expect("data")
            .row_set_bytes
            .expect("rowset bytes");
        assert_eq!(
            &rowset[..],
            br#"[["literal \"rowset\" text","brace: { }"]]"#,
        );
    }

    #[test]
    fn parse_response_rowset_scanner_handles_surrogate_pair_escape() {
        let body = Bytes::from(
            r#"{"data":{"queryId":"q1","rowset":[["\uD83D\uDE80"]],"rowtype":[],"queryResultFormat":"json"},"success":true}"#,
        );
        let resp = parse_response(body).unwrap();
        let rowset = resp
            .data
            .expect("data")
            .row_set_bytes
            .expect("rowset bytes");
        assert_eq!(&rowset[..], br#"[["\uD83D\uDE80"]]"#);
    }

    #[test]
    fn parse_response_rejects_invalid_surrogate_pair_in_string() {
        let body = Bytes::from(
            r#"{"message":"\uD83D","data":{"queryId":"q1","rowset":[["ok"]],"rowtype":[],"queryResultFormat":"json"},"success":true}"#,
        );
        let err: Error = parse_response(body).unwrap_err().into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
    }

    #[test]
    fn parse_response_treats_null_rowset_as_absent() {
        let body = Bytes::from(
            r#"{"data":{"queryId":"q1","rowset":null,"rowtype":[],"queryResultFormat":"json"},"success":true}"#,
        );
        let resp = parse_response(body).unwrap();
        assert!(resp.data.expect("data").row_set_bytes.is_none());
    }

    #[test]
    fn parse_response_missing_query_id_is_protocol_error() {
        let body = Bytes::from(r#"{"data":{"rowset":null},"success":true}"#);

        let err: Error = parse_response(body).unwrap_err().into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.queryId"
        );
    }

    #[test]
    fn parse_response_allows_leading_utf8_bom() {
        let body = Bytes::from_static(
            b"\xEF\xBB\xBF{\"data\":{\"queryId\":\"q1\",\"rowset\":[[\"a\"]],\"rowtype\":[],\"queryResultFormat\":\"json\"},\"success\":true}",
        );
        let body_ptr = body.as_ptr() as usize;
        let body_len = body.len();
        let resp = parse_response(body).unwrap();
        let rowset = resp
            .data
            .expect("data")
            .row_set_bytes
            .expect("rowset bytes");
        let rowset_ptr = rowset.as_ptr() as usize;
        assert!(
            rowset_ptr >= body_ptr && rowset_ptr + rowset.len() <= body_ptr + body_len,
            "rowset slice must alias the original body bytes"
        );
        assert_eq!(&rowset[..], br#"[["a"]]"#);
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

    #[test]
    fn resolve_headers_invalid_chunk_headers_are_protocol_errors() {
        let mut map = HashMap::new();
        map.insert("x-custom".to_string(), "bad\nvalue".to_string());

        let err: Error = resolve_download_headers(&None, &Some(map))
            .unwrap_err()
            .into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
    }

    #[test]
    fn resolve_headers_invalid_qrmk_is_protocol_error() {
        let err: Error = resolve_download_headers(&Some("bad\nvalue".into()), &None)
            .unwrap_err()
            .into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
    }
}
