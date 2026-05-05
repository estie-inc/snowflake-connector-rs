use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;

use crate::{
    error::{ProtocolError, QueryScopedError, QueryScopedResult},
    query_result::snapshot::{
        DownloadLocator, PartitionSpec, ResolvedLease, ResultIdentity, ResultSnapshot,
    },
    result::{Column, ColumnType, Schema},
    rowset::parser::inline_rowset_has_rows_inner,
};

use super::response::{RawQueryResponse, resolve_download_headers};

pub(crate) struct ResultManifest {
    pub(crate) snapshot: Arc<ResultSnapshot>,
    pub(crate) lease: ResolvedLease,
    pub(crate) inline_rowset: Option<InlineRowset>,
}

pub(crate) struct InlineRowset {
    pub(crate) bytes: Bytes,
    pub(crate) row_count_hint: Option<u64>,
}

impl TryFrom<RawQueryResponse> for ResultManifest {
    type Error = QueryScopedError;

    fn try_from(value: RawQueryResponse) -> QueryScopedResult<Self> {
        let RawQueryResponse {
            query_id,
            returned,
            total,
            row_set_bytes,
            row_types,
            chunk_headers,
            qrmk,
            chunks,
            ..
        } = value;

        let has_inline = match row_set_bytes.as_ref() {
            None => false,
            Some(bytes) => match inline_rowset_has_rows_inner(bytes) {
                Ok(has_rows) => has_rows,
                Err(err) => {
                    return Err(QueryScopedError::new(query_id, err));
                }
            },
        };

        let chunks = chunks.unwrap_or_default();
        let inline_row_count = if !chunks.is_empty() {
            returned.and_then(|v| u64::try_from(v).ok())
        } else {
            returned.or(total).and_then(|v| u64::try_from(v).ok())
        };

        if has_inline || !chunks.is_empty() {
            match row_types.as_ref() {
                None => {
                    return Err(QueryScopedError::new(
                        query_id,
                        ProtocolError::missing_field("data.rowtype"),
                    ));
                }
                Some(row_types) if row_types.is_empty() => {
                    return Err(QueryScopedError::new(
                        query_id,
                        ProtocolError::invalid_field(
                            "data.rowtype",
                            "must not be empty when result data is present",
                        ),
                    ));
                }
                Some(_) => {}
            }
        }

        let columns = row_types
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(index, row_type)| {
                let ty = ColumnType::from_driver_metadata(
                    &row_type.data_type,
                    row_type.length,
                    row_type.precision,
                    row_type.scale,
                );
                Column::new(row_type.name, index as u32, row_type.nullable, ty)
            })
            .collect();
        let schema = match Schema::from_columns(columns) {
            Ok(schema) => schema,
            Err(err) => {
                return Err(QueryScopedError::new(query_id, err));
            }
        };
        let schema = Arc::new(schema);

        let download_headers = match resolve_download_headers(&qrmk, &chunk_headers) {
            Ok(headers) => headers,
            Err(err) => {
                return Err(QueryScopedError::new(query_id, err));
            }
        };
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
            identity: ResultIdentity { query_id },
            schema: Arc::clone(&schema),
            partitions,
        });

        Ok(Self {
            inline_rowset: has_inline.then(|| InlineRowset {
                bytes: row_set_bytes
                    .expect("inline rowset bytes must exist when inline rows are present"),
                row_count_hint: inline_row_count,
            }),
            lease: ResolvedLease { locators },
            snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::statement::response::{RawQueryResponseChunk, RawQueryResponseRowType};

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

    #[test]
    fn manifest_requires_rowtype_when_inline_data_is_present() {
        let response = RawQueryResponse {
            parameters: None,
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: Some(1),
            total: None,
            row_set_bytes: Some(Bytes::from_static(br#"[["x"]]"#)),
            row_types: None,
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        };

        let err = match ResultManifest::try_from(response) {
            Ok(_) => panic!("missing rowtype metadata must fail"),
            Err(err) => crate::Error::from(err),
        };
        assert_eq!(err.kind(), crate::ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "missing required field in Snowflake response: data.rowtype"
        );
    }

    #[test]
    fn manifest_rejects_empty_rowtype_when_result_data_is_present() {
        let response = RawQueryResponse {
            parameters: None,
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: None,
            total: None,
            row_set_bytes: None,
            row_types: Some(Vec::new()),
            chunk_headers: None,
            qrmk: None,
            chunks: Some(vec![RawQueryResponseChunk {
                url: "https://example.com/chunk/0".to_string(),
                row_count: 1,
                uncompressed_size: 16,
                compressed_size: 8,
            }]),
            query_result_format: Some("json".to_string()),
        };

        let err = match ResultManifest::try_from(response) {
            Ok(_) => panic!("empty rowtype metadata must fail"),
            Err(err) => crate::Error::from(err),
        };
        assert_eq!(err.kind(), crate::ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.rowtype: must not be empty when result data is present"
        );
    }

    #[test]
    fn manifest_accepts_non_empty_rowtype_when_result_data_is_present() {
        let response = RawQueryResponse {
            parameters: None,
            query_id: Arc::from("query-id"),
            get_result_url: None,
            returned: Some(1),
            total: None,
            row_set_bytes: Some(Bytes::from_static(br#"[["x"]]"#)),
            row_types: Some(vec![text_row_type("X")]),
            chunk_headers: None,
            qrmk: None,
            chunks: None,
            query_result_format: Some("json".to_string()),
        };

        let manifest = ResultManifest::try_from(response).unwrap();
        assert_eq!(manifest.snapshot.schema.len(), 1);
        assert_eq!(manifest.snapshot.partitions.len(), 1);
        assert!(matches!(
            manifest.snapshot.partitions.first(),
            Some(PartitionSpec::Inline)
        ));
    }
}
