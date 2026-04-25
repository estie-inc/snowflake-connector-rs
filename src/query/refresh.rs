use std::collections::HashMap;
use std::sync::Arc;

use crate::Error;

use super::capability::RefreshInvariant;
use super::response::resolve_download_headers;
use super::snapshot::{DownloadLocator, PartitionSpec, ResolvedLease, ResultSnapshot};
use super::statement::StatementApiClient;

/// Internal outcome of [`LeaseRefresher::refresh`] that distinguishes
/// "the refresh response disagrees with the snapshot" (a contract-level
/// failure that callers must lift to [`Error::ResultSetExpired`]) from
/// "the refresh call itself failed" (transport / session / decode errors
/// that should stay as-is).
///
/// This enum stays internal and has no blanket conversion into the public
/// [`Error`]; callers must pattern-match on the variant so that the
/// validation/api split cannot be accidentally flattened into a generic
/// communication error.
#[derive(Debug)]
pub(crate) enum RefreshError {
    /// The refresh response could not be reconciled with the snapshot
    /// taken at query submission. The attached string is a human-readable
    /// reason retained for `Debug` output and test assertions; production
    /// callers surface this as [`Error::ResultSetExpired`] regardless.
    Validation(#[allow(dead_code)] String),
    /// Any other failure reached while performing the refresh call —
    /// session expiry, transport hiccup, response decode, etc.
    Api(Error),
}

impl RefreshError {
    fn validation(reason: impl Into<String>) -> Self {
        Self::Validation(reason.into())
    }
}

impl From<Error> for RefreshError {
    fn from(err: Error) -> Self {
        Self::Api(err)
    }
}

pub(crate) type RefreshResult<T> = std::result::Result<T, RefreshError>;

/// Opaque identity of a refreshable result batch, handed from the adapter
/// that produced the initial response to the refresh machinery. Consumed
/// exclusively by `StatementApiClient::refresh`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefreshHandle {
    /// `path` is a URL path fragment relative to `StatementApiClient::base_url`,
    /// such as `"queries/<query_id>/result"`. `query_id` is not used for the
    /// HTTP call itself — it disambiguates refresh handles of the same shape.
    QueryResultPath { query_id: Arc<str>, path: Arc<str> },
}

/// Pair of `RefreshHandle` and `RefreshInvariant` carried by a refresh-capable
/// `PartitionSource`. The invariant describes which properties of the result
/// are guaranteed stable across refreshes; `LeaseRefresher::refresh` enforces
/// exactly those properties.
#[derive(Debug, Clone)]
pub(crate) struct RefreshCapability {
    pub(crate) handle: RefreshHandle,
    /// Stored for future classifiers that branch on which refresh guarantee
    /// the adapter advertises. `LeaseRefresher` currently enforces the
    /// strictest invariant (`StableRemainingPartitions`) regardless.
    #[allow(dead_code)]
    pub(crate) invariant: RefreshInvariant,
}

/// Regenerates a `ResolvedLease` by re-reading the query result envelope and
/// cross-validating its shape against the original snapshot.
///
/// The refresher never rewrites the snapshot. Failures are surfaced as
/// [`RefreshError`] so callers can distinguish snapshot-mismatch from
/// transport / session failure without relying on error message content.
pub(crate) struct LeaseRefresher {
    api: Arc<StatementApiClient>,
}

impl LeaseRefresher {
    pub(crate) fn new(api: Arc<StatementApiClient>) -> Self {
        Self { api }
    }

    pub(crate) async fn refresh(
        &self,
        snapshot: &ResultSnapshot,
        handle: &RefreshHandle,
        committed_before: usize,
        next_generation: u64,
    ) -> RefreshResult<ResolvedLease> {
        debug_assert!(
            committed_before <= snapshot.partitions.len(),
            "committed_before must not exceed total partitions",
        );

        let response = self.api.refresh(handle).await?;

        if response.query_id != snapshot.identity.query_id {
            return Err(RefreshError::validation(format!(
                "query_id mismatch: expected {}, got {}",
                snapshot.identity.query_id, response.query_id,
            )));
        }

        // `queryResultFormat` is not part of the documented `GET /queries/
        // {queryId}/result` contract, so its absence is not evidence of a
        // shape mismatch. Mirror the execute path: only reject when the
        // field is present *and* explicitly non-JSON; a missing field is
        // treated as an implicit JSON result set.
        if let Some(format) = response.query_result_format.as_deref()
            && format != "json"
        {
            return Err(RefreshError::validation(format!(
                "unexpected query_result_format: {format}",
            )));
        }

        let row_types = response
            .row_types
            .ok_or_else(|| RefreshError::validation("missing rowtype in refresh response"))?;
        let row_set = response
            .row_set
            .ok_or_else(|| RefreshError::validation("missing rowset in refresh response"))?;
        let chunks = response.chunks.unwrap_or_default();

        let has_inline = !row_set.is_empty();
        let mut fresh_partitions = Vec::new();
        if has_inline {
            fresh_partitions.push(PartitionSpec::Inline);
        }
        for chunk in &chunks {
            fresh_partitions.push(PartitionSpec::Remote {
                row_count: chunk.row_count,
                compressed_size: chunk.compressed_size,
                uncompressed_size: chunk.uncompressed_size,
            });
        }

        if fresh_partitions.len() != snapshot.partitions.len() {
            return Err(RefreshError::validation(format!(
                "partition count mismatch: expected {}, got {}",
                snapshot.partitions.len(),
                fresh_partitions.len(),
            )));
        }

        for (ordinal, (snap, fresh)) in snapshot
            .partitions
            .iter()
            .zip(fresh_partitions.iter())
            .enumerate()
        {
            match (snap, fresh) {
                (PartitionSpec::Inline, PartitionSpec::Inline) => {}
                (
                    PartitionSpec::Remote {
                        row_count: snap_rc, ..
                    },
                    PartitionSpec::Remote {
                        row_count: fresh_rc,
                        ..
                    },
                ) => {
                    if snap_rc != fresh_rc {
                        return Err(RefreshError::validation(format!(
                            "partition {ordinal} row_count mismatch: expected {snap_rc}, got {fresh_rc}",
                        )));
                    }
                }
                _ => {
                    return Err(RefreshError::validation(format!(
                        "partition {ordinal} inline/remote kind differs between snapshot and refresh response",
                    )));
                }
            }
        }

        if row_types.len() != snapshot.columns.len() {
            return Err(RefreshError::validation(format!(
                "rowtype count mismatch: expected {}, got {}",
                snapshot.columns.len(),
                row_types.len(),
            )));
        }

        for (ordinal, (fresh, column)) in row_types.iter().zip(snapshot.columns.iter()).enumerate()
        {
            let column_type = column.column_type();
            let expected_name = column.name();
            let fresh_name = fresh.name.to_ascii_uppercase();
            if expected_name != fresh_name {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} name mismatch: expected {expected_name}, got {fresh_name}",
                )));
            }
            if fresh.data_type != column_type.snowflake_type() {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} data_type mismatch: expected {}, got {}",
                    column_type.snowflake_type(),
                    fresh.data_type,
                )));
            }
            if fresh.nullable != column_type.nullable() {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} nullable mismatch: expected {}, got {}",
                    column_type.nullable(),
                    fresh.nullable,
                )));
            }
            if fresh.precision != column_type.precision() {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} precision mismatch",
                )));
            }
            if fresh.scale != column_type.scale() {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} scale mismatch",
                )));
            }
            if fresh.length != column_type.length() {
                return Err(RefreshError::validation(format!(
                    "column {ordinal} length mismatch",
                )));
            }
        }

        let download_headers = resolve_download_headers(&response.qrmk, &response.chunk_headers)?;
        let chunks_start_ordinal = usize::from(has_inline);
        let mut locators: HashMap<usize, DownloadLocator> = HashMap::new();
        for (idx, chunk) in chunks.into_iter().enumerate() {
            let ordinal = chunks_start_ordinal + idx;
            if ordinal < committed_before {
                continue;
            }
            locators.insert(
                ordinal,
                DownloadLocator {
                    url: chunk.url,
                    headers: Arc::clone(&download_headers),
                },
            );
        }

        Ok(ResolvedLease {
            generation: next_generation,
            locators,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{SnowflakeColumn, row::SnowflakeColumnType};

    use super::super::{
        snapshot::{PartitionSpec, ResultIdentity, ResultSnapshot},
        statement::StatementApiClient,
        test_server::{CannedResponse, ServerHandle, start_server},
    };
    use super::{LeaseRefresher, RefreshError, RefreshHandle};

    const QID: &str = "abc-123";

    fn sample_snapshot(
        columns: Vec<SnowflakeColumn>,
        partitions: Vec<PartitionSpec>,
    ) -> ResultSnapshot {
        let columns: Arc<[SnowflakeColumn]> = Arc::from(columns);
        let column_indices = columns
            .iter()
            .map(|c| (c.name().to_string(), c.index()))
            .collect();
        ResultSnapshot {
            identity: ResultIdentity {
                query_id: QID.to_string(),
            },
            columns,
            column_indices: Arc::new(column_indices),
            partitions,
        }
    }

    fn two_partition_inline_remote_snapshot() -> ResultSnapshot {
        sample_snapshot(
            vec![SnowflakeColumn::new(
                "X".to_string(),
                0,
                SnowflakeColumnType::new("fixed".to_string(), false, None, Some(10), Some(0)),
            )],
            vec![
                PartitionSpec::Inline,
                PartitionSpec::Remote {
                    row_count: 7,
                    compressed_size: 0,
                    uncompressed_size: 0,
                },
            ],
        )
    }

    fn matching_handle() -> RefreshHandle {
        RefreshHandle::QueryResultPath {
            query_id: Arc::from(QID),
            path: Arc::from("queries/abc-123/result"),
        }
    }

    fn refresher_for(server: &ServerHandle) -> LeaseRefresher {
        let api = Arc::new(StatementApiClient {
            http: reqwest::Client::new(),
            base_url: server.base_url(),
            session_token: "token".to_string(),
        });
        LeaseRefresher::new(api)
    }

    fn json_body(data: serde_json::Value) -> CannedResponse {
        let envelope = serde_json::json!({
            "success": true,
            "message": null,
            "code": null,
            "data": data,
        });
        CannedResponse::ok_json(envelope.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    fn two_partition_response(
        query_id: &str,
        format: Option<&str>,
        column_type: &str,
        column_name: &str,
        nullable: bool,
        precision: Option<i64>,
        scale: Option<i64>,
        length: Option<i64>,
        row_set: Vec<Vec<Option<String>>>,
        chunks: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        let mut data = serde_json::json!({
            "queryId": query_id,
            "rowtype": [{
                "database": "",
                "schema": "",
                "table": "",
                "name": column_name,
                "type": column_type,
                "nullable": nullable,
                "precision": precision,
                "scale": scale,
                "length": length,
                "byteLength": null,
            }],
            "rowset": row_set,
            "chunks": chunks,
        });
        if let Some(f) = format {
            data["queryResultFormat"] = serde_json::Value::String(f.to_string());
        }
        data
    }

    fn ok_chunk(url: &str, row_count: i64) -> serde_json::Value {
        serde_json::json!({
            "url": url,
            "rowCount": row_count,
            "uncompressedSize": 0,
            "compressedSize": 0,
        })
    }

    #[tokio::test]
    async fn refresh_success_returns_frontier_locators() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        let server = start_server(vec![json_body(data)]);
        let refresher = refresher_for(&server);

        let lease = refresher
            .refresh(&snapshot, &matching_handle(), 1, 42)
            .await
            .expect("refresh succeeds");

        assert_eq!(lease.generation, 42);
        assert_eq!(lease.locators.len(), 1);
        let locator = lease.locators.get(&1).expect("remote ordinal 1 present");
        assert_eq!(locator.url, "https://example.invalid/chunk-1");
        assert!(!lease.locators.contains_key(&0));
    }

    async fn assert_refresh_rejects(
        snapshot: &ResultSnapshot,
        data: serde_json::Value,
        expected_reason_fragment: &str,
    ) {
        let server = start_server(vec![json_body(data)]);
        let refresher = refresher_for(&server);
        let err = refresher
            .refresh(snapshot, &matching_handle(), 1, 1)
            .await
            .expect_err("refresh must fail validation");
        match err {
            RefreshError::Validation(reason) => {
                assert!(
                    reason.contains(expected_reason_fragment),
                    "expected reason fragment `{expected_reason_fragment}` in: {reason}",
                );
            }
            RefreshError::Api(err) => {
                panic!("expected Validation, got Api({err:?})");
            }
        }
    }

    #[tokio::test]
    async fn refresh_rejects_query_id_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            "other-qid",
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "query_id mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_non_json_format() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("arrow"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "unexpected query_result_format").await;
    }

    #[tokio::test]
    async fn refresh_accepts_missing_format() {
        // The `GET /queries/{queryId}/result` response shape is not publicly
        // documented and does not guarantee `queryResultFormat`. Missing
        // format must not be treated as a shape mismatch; the refresh must
        // succeed as long as everything else matches the snapshot.
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            None,
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        let server = start_server(vec![json_body(data)]);
        let refresher = refresher_for(&server);

        let lease = refresher
            .refresh(&snapshot, &matching_handle(), 1, 42)
            .await
            .expect("refresh succeeds without explicit queryResultFormat");

        assert_eq!(lease.generation, 42);
        let locator = lease.locators.get(&1).expect("remote ordinal 1 present");
        assert_eq!(locator.url, "https://example.invalid/chunk-1");
    }

    #[tokio::test]
    async fn refresh_rejects_column_order_change() {
        let snapshot = sample_snapshot(
            vec![
                SnowflakeColumn::new(
                    "A".into(),
                    0,
                    SnowflakeColumnType::new("fixed".into(), false, None, Some(10), Some(0)),
                ),
                SnowflakeColumn::new(
                    "B".into(),
                    1,
                    SnowflakeColumnType::new("text".into(), true, Some(16), None, None),
                ),
            ],
            vec![
                PartitionSpec::Inline,
                PartitionSpec::Remote {
                    row_count: 1,
                    compressed_size: 0,
                    uncompressed_size: 0,
                },
            ],
        );
        // Swap the response's column order: B first, A second.
        let data = serde_json::json!({
            "queryId": QID,
            "queryResultFormat": "json",
            "rowtype": [
                {
                    "database": "", "schema": "", "table": "",
                    "name": "B", "type": "text", "nullable": true,
                    "precision": null, "scale": null, "length": 16, "byteLength": null,
                },
                {
                    "database": "", "schema": "", "table": "",
                    "name": "A", "type": "fixed", "nullable": false,
                    "precision": 10, "scale": 0, "length": null, "byteLength": null,
                }
            ],
            "rowset": [[null, null]],
            "chunks": [ok_chunk("https://example.invalid/chunk-1", 1)],
        });
        assert_refresh_rejects(&snapshot, data, "name mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_nullable_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            true, // snapshot has false
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "nullable mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_data_type_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "text", // snapshot has "fixed"
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "data_type mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_precision_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(38), // snapshot has 10
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "precision mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_scale_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(2), // snapshot has 0
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "scale mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_length_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            Some(64), // snapshot has None
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 7)],
        );
        assert_refresh_rejects(&snapshot, data, "length mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_inline_remote_boundary_change() {
        let snapshot = two_partition_inline_remote_snapshot();
        // Response has no inline partition but a remote one.
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![],
            vec![
                ok_chunk("https://example.invalid/chunk-0", 1),
                ok_chunk("https://example.invalid/chunk-1", 7),
            ],
        );
        assert_refresh_rejects(&snapshot, data, "kind differs").await;
    }

    #[tokio::test]
    async fn refresh_rejects_partition_count_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![
                ok_chunk("https://example.invalid/chunk-1", 7),
                ok_chunk("https://example.invalid/chunk-2", 3),
            ],
        );
        assert_refresh_rejects(&snapshot, data, "partition count mismatch").await;
    }

    #[tokio::test]
    async fn refresh_rejects_remote_row_count_mismatch() {
        let snapshot = two_partition_inline_remote_snapshot();
        let data = two_partition_response(
            QID,
            Some("json"),
            "fixed",
            "X",
            false,
            Some(10),
            Some(0),
            None,
            vec![vec![Some("1".into())]],
            vec![ok_chunk("https://example.invalid/chunk-1", 99)], // snapshot expects 7
        );
        assert_refresh_rejects(&snapshot, data, "row_count mismatch").await;
    }
}
