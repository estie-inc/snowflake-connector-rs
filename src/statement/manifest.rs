use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;

use crate::{
    Error, Result,
    query_result::snapshot::{
        DownloadLocator, PartitionSpec, ResolvedLease, ResultIdentity, ResultSnapshot,
    },
    result::{Column, ColumnType, Schema},
    rowset::parser::inline_rowset_has_rows,
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
    type Error = Error;

    fn try_from(value: RawQueryResponse) -> Result<Self> {
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
            Some(bytes) => inline_rowset_has_rows(bytes)?,
        };

        let chunks = chunks.unwrap_or_default();
        let inline_row_count = if !chunks.is_empty() {
            returned.and_then(|v| u64::try_from(v).ok())
        } else {
            returned.or(total).and_then(|v| u64::try_from(v).ok())
        };

        if (has_inline || !chunks.is_empty())
            && row_types
                .as_ref()
                .is_none_or(|row_types| row_types.is_empty())
        {
            return Err(Error::UnsupportedFormat(
                "response has result data but no rowtype metadata".to_string(),
            ));
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
        let schema = Arc::new(Schema::from_columns(columns)?);

        let download_headers = resolve_download_headers(&qrmk, &chunk_headers)?;
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

        let query_id: Arc<str> = Arc::from(query_id);
        let snapshot = Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: Arc::clone(&query_id),
            },
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
