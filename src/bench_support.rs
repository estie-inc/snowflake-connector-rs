//! Unstable: no semver guarantee. Used by internal benchmarks and
//! `tests/derive_smoke.rs` (gated behind the `bench-internals` feature).

use std::sync::Arc;

use bytes::Bytes;

use crate::{
    Result, ResultTable, Schema,
    rowset::{self, parser},
    statement::parse_response,
};

pub use crate::result::test_data::{make_result_table_from_rows, make_schema};
pub use crate::rowset::parser::inline_rows_to_result_table;

#[derive(Debug, Clone)]
pub struct StatementEnvelopeParts {
    pub rowset_bytes: Option<Bytes>,
    pub row_type_count: usize,
    pub chunk_count: usize,
}

pub fn parse_statement_envelope(body: Bytes) -> Result<StatementEnvelopeParts> {
    let response = parse_response(body)?;
    let Some(data) = response.data else {
        return Ok(StatementEnvelopeParts {
            rowset_bytes: None,
            row_type_count: 0,
            chunk_count: 0,
        });
    };
    let row_type_count = data.row_types.as_ref().map_or(0, Vec::len);
    let chunk_count = data.chunks.as_ref().map_or(0, Vec::len);
    Ok(StatementEnvelopeParts {
        rowset_bytes: data.row_set_bytes,
        row_type_count,
        chunk_count,
    })
}

pub fn parse_inline_result_table(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
) -> Result<ResultTable> {
    parser::parse_inline_result_table(schema, query_id, body)
}

pub fn decode_gzip_chunk(body: Bytes) -> Result<Bytes> {
    rowset::decode_gzip_chunk(body)
}

pub fn parse_remote_chunk_result_table(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
) -> Result<ResultTable> {
    parser::parse_remote_chunk_result_table(schema, query_id, body)
}

pub async fn parse_remote_chunk_result_table_async(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
) -> Result<ResultTable> {
    parse_remote_chunk_result_table_async_with_workload(schema, query_id, body, None, None).await
}

pub async fn parse_remote_chunk_result_table_async_with_workload(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
    row_count: Option<u64>,
    uncompressed_bytes: Option<usize>,
) -> Result<ResultTable> {
    let gzip_encoded = body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b;
    let workload = rowset::ParseWorkload {
        input_bytes: body.len(),
        row_count,
        column_count: schema.len(),
        gzip_encoded,
        compressed_bytes: gzip_encoded.then_some(body.len()),
        uncompressed_bytes: if gzip_encoded {
            uncompressed_bytes
        } else {
            Some(uncompressed_bytes.unwrap_or(body.len()))
        },
    };
    parser::parse_remote_chunk_result_table_async(schema, query_id, body, workload, None).await
}
