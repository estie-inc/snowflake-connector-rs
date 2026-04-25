use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::{Error, Result};

use super::chunk_downloader::{ChunkDownloader, DownloadFailure};
use super::expiry::{
    LeaseDisposition, LeaseExpiryClassifier, is_strictly_expired_locator, predict_expired_locator,
};
use super::refresh::{LeaseRefresher, RefreshCapability, RefreshError, RefreshResult};
use super::snapshot::{DownloadLocator, RawPartitionRows, ResolvedLease, ResultSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FetchContext {
    pub(crate) ordinal: usize,
    /// Ordinal frontier committed by the caller at the time of this fetch.
    /// A partition source may assume no ordinal `< committed_before` will be
    /// requested again. Used by refresh-capable sources to avoid holding
    /// locators for already-consumed partitions.
    pub(crate) committed_before: usize,
}

pub(crate) enum PartitionSource {
    Static(StaticPartitionSource),
    Refreshing(RefreshingPartitionSource),
    #[cfg(test)]
    Fake(tests::FakePartitionSource),
}

impl PartitionSource {
    pub(crate) async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
        match self {
            Self::Static(s) => s.fetch(ctx).await,
            Self::Refreshing(s) => s.fetch(ctx).await,
            #[cfg(test)]
            Self::Fake(f) => f.fetch(ctx).await,
        }
    }

    #[cfg(test)]
    pub(crate) fn refreshing_lease_generation(&self) -> Option<u64> {
        match self {
            Self::Refreshing(s) => Some(s.current_generation()),
            _ => None,
        }
    }
}

pub(crate) struct StaticPartitionSource {
    lease: ResolvedLease,
    downloader: ChunkDownloader,
}

impl StaticPartitionSource {
    pub(crate) fn new(lease: ResolvedLease, downloader: ChunkDownloader) -> Self {
        Self { lease, downloader }
    }

    async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
        let locator = self
            .lease
            .locators
            .get(&ctx.ordinal)
            .expect("locator must exist for every Remote partition ordinal");

        self.downloader.download(locator).await.map_err(Error::from)
    }
}

/// `PartitionSource` backed by a refreshable lease. Downloads use the current
/// lease; expired locators are regenerated via `refresh_if_stale`, which runs
/// one refresh at a time via a tokio `Mutex` gate.
///
/// `fetch` retries a partition at most once after refreshing the lease. If
/// the post-refresh attempt still looks expired, or if the refresh response
/// fails snapshot validation, the error is lifted to `Error::ResultSetExpired`
/// to signal that the query must be re-run.
pub(crate) struct RefreshingPartitionSource {
    snapshot: Arc<ResultSnapshot>,
    lease: ArcSwap<ResolvedLease>,
    capability: RefreshCapability,
    refresh_gate: tokio::sync::Mutex<()>,
    refresher: LeaseRefresher,
    downloader: ChunkDownloader,
    expiry_classifier: LeaseExpiryClassifier,
}

impl RefreshingPartitionSource {
    pub(crate) fn new(
        snapshot: Arc<ResultSnapshot>,
        lease: ResolvedLease,
        capability: RefreshCapability,
        refresher: LeaseRefresher,
        downloader: ChunkDownloader,
        expiry_classifier: LeaseExpiryClassifier,
    ) -> Self {
        Self {
            snapshot,
            lease: ArcSwap::new(Arc::new(lease)),
            capability,
            refresh_gate: tokio::sync::Mutex::new(()),
            refresher,
            downloader,
            expiry_classifier,
        }
    }

    async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
        let initial_lease = self.lease.load();
        let expected_generation = initial_lease.generation;
        let locator = locator_for_ordinal(&initial_lease, ctx.ordinal);
        let first_url = locator.url.clone();
        drop(initial_lease);

        let first_failure = match self.preflight_then_download(&locator).await {
            Ok(rows) => return Ok(rows),
            Err(failure) => failure,
        };

        match self.expiry_classifier.classify(&first_failure) {
            LeaseDisposition::ExpiredLease => {}
            LeaseDisposition::RetryableTransport | LeaseDisposition::Fatal => {
                return Err(Error::from(first_failure));
            }
        }

        match self
            .refresh_if_stale(expected_generation, ctx.committed_before)
            .await
        {
            Ok(()) => {}
            Err(RefreshError::Validation(_)) => return Err(Error::ResultSetExpired),
            Err(RefreshError::Api(err)) => return Err(err),
        }

        // Another task may have won the refresh race, so reload the lease
        // before looking up the locator again.
        let refreshed_lease = self.lease.load();
        let retry_locator = locator_for_ordinal(&refreshed_lease, ctx.ordinal);
        drop(refreshed_lease);

        // If the refresh returned the same URL as before, we have no
        // evidence that the backend actually re-signed the locator. Two
        // sub-cases:
        //
        // - The URL's encoded expiry is strictly in the past. The URL
        //   itself says it is expired; a normal download attempt would
        //   also fail, so surface `ResultSetExpired` directly.
        // - The URL is only within the preflight skew buffer (not yet
        //   strictly past). We would re-flag it as expired if we
        //   preflighted, but a few minutes of real validity may remain,
        //   so skip the preflight, attempt the download, and let the raw
        //   failure (if any) surface as-is rather than escalating to
        //   `ResultSetExpired`.
        let same_url_as_initial = retry_locator.url == first_url;
        if same_url_as_initial && is_strictly_expired_locator(&retry_locator) {
            return Err(Error::ResultSetExpired);
        }

        let retry_result = if same_url_as_initial {
            self.downloader.download(&retry_locator).await
        } else {
            self.preflight_then_download(&retry_locator).await
        };

        match retry_result {
            Ok(rows) => Ok(rows),
            Err(retry_failure) if same_url_as_initial => Err(Error::from(retry_failure)),
            Err(retry_failure) => match self.expiry_classifier.classify(&retry_failure) {
                LeaseDisposition::ExpiredLease => Err(Error::ResultSetExpired),
                _ => Err(Error::from(retry_failure)),
            },
        }
    }

    /// Run the provider-aware expiry predictor before dispatching to the
    /// network. If the predictor thinks the locator is already expired,
    /// synthesize a `DownloadFailure::PredictedExpired` instead of issuing
    /// an HTTP request that would have failed anyway. This preflight only
    /// runs on refresh-capable sources — `StaticPartitionSource` keeps its
    /// original "always hit the network" contract.
    async fn preflight_then_download(
        &self,
        locator: &DownloadLocator,
    ) -> std::result::Result<RawPartitionRows, DownloadFailure> {
        if let Some(reason) = predict_expired_locator(locator) {
            return Err(DownloadFailure::PredictedExpired {
                locator: locator.clone(),
                reason,
            });
        }
        self.downloader.download(locator).await
    }

    /// Refresh the current lease iff `expected_generation` still matches.
    /// Exactly one refresh runs at a time; concurrent callers see the winner's
    /// generation on their second load and skip redundant refreshes.
    async fn refresh_if_stale(
        &self,
        expected_generation: u64,
        committed_before: usize,
    ) -> RefreshResult<()> {
        let _guard = self.refresh_gate.lock().await;

        let current = self.lease.load();
        if current.generation != expected_generation {
            return Ok(());
        }
        drop(current);

        let new_lease = self
            .refresher
            .refresh(
                &self.snapshot,
                &self.capability.handle,
                committed_before,
                expected_generation + 1,
            )
            .await?;
        self.lease.store(Arc::new(new_lease));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn current_generation(&self) -> u64 {
        self.lease.load().generation
    }
}

fn locator_for_ordinal(lease: &ResolvedLease, ordinal: usize) -> DownloadLocator {
    lease
        .locators
        .get(&ordinal)
        .cloned()
        .expect("locator must exist for requested remote ordinal in current lease frontier")
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Test-only partition source that returns pre-configured results
    /// keyed by partition ordinal. Supports multiple calls per ordinal
    /// (e.g. first call fails, second succeeds) and optionally records the
    /// `FetchContext` of each call.
    pub(crate) struct FakePartitionSource {
        responses: Mutex<HashMap<usize, VecDeque<Result<RawPartitionRows>>>>,
        recorder: Option<Arc<Mutex<Vec<FetchContext>>>>,
    }

    impl FakePartitionSource {
        pub(crate) fn new(responses: HashMap<usize, VecDeque<Result<RawPartitionRows>>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                recorder: None,
            }
        }

        pub(crate) fn with_recorder(
            responses: HashMap<usize, VecDeque<Result<RawPartitionRows>>>,
            recorder: Arc<Mutex<Vec<FetchContext>>>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses),
                recorder: Some(recorder),
            }
        }

        pub(crate) async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
            if let Some(recorder) = &self.recorder {
                recorder
                    .lock()
                    .expect("FakePartitionSource recorder poisoned")
                    .push(ctx);
            }
            let mut map = self
                .responses
                .lock()
                .expect("FakePartitionSource: poisoned");
            let queue = map
                .get_mut(&ctx.ordinal)
                .expect("FakePartitionSource: no responses configured for ordinal");
            queue
                .pop_front()
                .expect("FakePartitionSource: response queue exhausted for ordinal")
        }
    }
}

#[cfg(test)]
mod refreshing_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use http::{HeaderMap, StatusCode};

    use super::super::{
        capability::RefreshInvariant,
        expiry::LeaseExpiryClassifier,
        refresh::{LeaseRefresher, RefreshCapability, RefreshHandle},
        snapshot::{DownloadLocator, PartitionSpec, ResultIdentity, ResultSnapshot},
        statement::StatementApiClient,
        test_server::{CannedResponse, ServerHandle, start_server},
    };
    use super::*;
    use crate::{Error, SnowflakeColumn, row::SnowflakeColumnType};

    const QID: &str = "abc-123";

    fn snapshot() -> Arc<ResultSnapshot> {
        let columns: Arc<[SnowflakeColumn]> = Arc::from(vec![SnowflakeColumn::new(
            "X".into(),
            0,
            SnowflakeColumnType::new("fixed".into(), false, None, Some(10), Some(0)),
        )]);
        let column_indices = columns
            .iter()
            .map(|c| (c.name().to_string(), c.index()))
            .collect();
        Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: QID.to_string(),
            },
            columns,
            column_indices: Arc::new(column_indices),
            partitions: vec![
                PartitionSpec::Inline,
                PartitionSpec::Remote {
                    row_count: 2,
                    compressed_size: 0,
                    uncompressed_size: 0,
                },
            ],
        })
    }

    fn initial_lease(url: String) -> ResolvedLease {
        let mut locators = HashMap::new();
        locators.insert(
            1,
            DownloadLocator {
                url,
                headers: Arc::new(HeaderMap::new()),
            },
        );
        ResolvedLease {
            generation: 0,
            locators,
        }
    }

    fn capability() -> RefreshCapability {
        RefreshCapability {
            handle: RefreshHandle::QueryResultPath {
                query_id: Arc::from(QID),
                path: Arc::from("queries/abc-123/result"),
            },
            invariant: RefreshInvariant::StableRemainingPartitions,
        }
    }

    fn api_client(server: &ServerHandle) -> Arc<StatementApiClient> {
        Arc::new(StatementApiClient {
            http: reqwest::Client::new(),
            base_url: server.base_url(),
            session_token: "token".into(),
        })
    }

    fn refresh_body_with_chunk(chunk_url: &str) -> CannedResponse {
        let data = serde_json::json!({
            "queryId": QID,
            "queryResultFormat": "json",
            "rowtype": [{
                "database": "", "schema": "", "table": "",
                "name": "X", "type": "fixed", "nullable": false,
                "precision": 10, "scale": 0, "length": null, "byteLength": null,
            }],
            "rowset": [[Some("1".to_string())]],
            "chunks": [{
                "url": chunk_url,
                "rowCount": 2,
                "uncompressedSize": 0,
                "compressedSize": 0,
            }],
        });
        CannedResponse::ok_json(
            serde_json::json!({
                "success": true,
                "message": null,
                "code": null,
                "data": data,
            })
            .to_string(),
        )
    }

    // Chunk bodies are wrapped as `[ <body> ]` by the downloader; a single
    // row `["a"]` comes back as `[["a"]]` — one partition with one row.
    fn chunk_body(rows: &[&str]) -> CannedResponse {
        let body = rows
            .iter()
            .map(|s| format!(r#"[{}]"#, serde_json::to_string(s).unwrap()))
            .collect::<Vec<_>>()
            .join(",");
        CannedResponse {
            status: StatusCode::OK,
            body: hyper::body::Bytes::from(body),
            headers: Vec::new(),
        }
    }

    fn source_for(server: &ServerHandle, lease: ResolvedLease) -> RefreshingPartitionSource {
        let api = api_client(server);
        let refresher = LeaseRefresher::new(Arc::clone(&api));
        let downloader = ChunkDownloader::new(api.http.clone());
        RefreshingPartitionSource::new(
            snapshot(),
            lease,
            capability(),
            refresher,
            downloader,
            LeaseExpiryClassifier::new(),
        )
    }

    /// Build an S3-style URL whose `X-Amz-Date` / `X-Amz-Expires` are
    /// strictly in the past, so the chunk downloader's preflight predictor
    /// short-circuits to `DownloadFailure::PredictedExpired` without
    /// touching the network and the same-URL strict-expiry guard also
    /// fires.
    fn expired_s3_url(path: &str) -> String {
        format!(
            "https://expired.invalid{path}?{query}",
            query = s3_amz_query(/* signed_offset_secs = */ -7_200, 3_600),
        )
    }

    /// Like `expired_s3_url`, but pointed at the in-process test server so a
    /// fetch that bypasses the preflight resolves against a reachable host
    /// instead of waiting on DNS.
    fn expired_s3_url_on(server: &ServerHandle, path: &str) -> String {
        format!(
            "http://{addr}{path}?{query}",
            addr = server.addr,
            query = s3_amz_query(-7_200, 3_600),
        )
    }

    /// S3-style URL whose encoded expiry is **still in the future** but
    /// within the preflight's 5-minute skew buffer, so the preflight flags
    /// it as expired even though `is_strictly_expired_locator` returns
    /// `false`. Used to exercise the same-URL-but-not-strictly-past branch
    /// of the retry guard.
    fn buffer_window_s3_url_on(server: &ServerHandle, path: &str) -> String {
        format!(
            "http://{addr}{path}?{query}",
            addr = server.addr,
            query = s3_amz_query(-3_400, 3_600),
        )
    }

    fn s3_amz_query(signed_offset_secs: i64, expires_secs: i64) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let signed = chrono::DateTime::<chrono::Utc>::from_timestamp(
            now_secs + signed_offset_secs,
            0,
        )
        .unwrap()
        .format("%Y%m%dT%H%M%SZ")
        .to_string();
        format!("X-Amz-Date={signed}&X-Amz-Expires={expires_secs}")
    }

    fn refresh_body_with_raw_url(raw_url: serde_json::Value) -> CannedResponse {
        let data = serde_json::json!({
            "queryId": QID,
            "queryResultFormat": "json",
            "rowtype": [{
                "database": "", "schema": "", "table": "",
                "name": "X", "type": "fixed", "nullable": false,
                "precision": 10, "scale": 0, "length": null, "byteLength": null,
            }],
            "rowset": [[Some("1".to_string())]],
            "chunks": [{
                "url": raw_url,
                "rowCount": 2,
                "uncompressedSize": 0,
                "compressedSize": 0,
            }],
        });
        CannedResponse::ok_json(
            serde_json::json!({
                "success": true,
                "message": null,
                "code": null,
                "data": data,
            })
            .to_string(),
        )
    }

    fn refresh_body_with_row_count_mismatch() -> CannedResponse {
        let data = serde_json::json!({
            "queryId": QID,
            "queryResultFormat": "json",
            "rowtype": [{
                "database": "", "schema": "", "table": "",
                "name": "X", "type": "fixed", "nullable": false,
                "precision": 10, "scale": 0, "length": null, "byteLength": null,
            }],
            "rowset": [[Some("1".to_string())]],
            "chunks": [{
                "url": "https://example.invalid/new",
                "rowCount": 999, // snapshot says 2
                "uncompressedSize": 0,
                "compressedSize": 0,
            }],
        });
        CannedResponse::ok_json(
            serde_json::json!({
                "success": true,
                "message": null,
                "code": null,
                "data": data,
            })
            .to_string(),
        )
    }

    #[tokio::test]
    async fn fetch_fast_path_returns_rows_from_current_lease() {
        let server = start_server(vec![chunk_body(&["row-1", "row-2"])]);
        let lease_url = format!("http://{}/chunk-a", server.addr);
        let source = source_for(&server, initial_lease(lease_url));

        let rows = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect("fetch succeeds");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_deref(), Some("row-1"));
        assert_eq!(rows[1][0].as_deref(), Some("row-2"));
    }

    #[tokio::test]
    async fn refresh_if_stale_refreshes_and_bumps_generation() {
        let server = start_server(vec![refresh_body_with_chunk("https://example.invalid/new")]);
        let lease_url = format!("http://{}/chunk-old", server.addr);
        let source = source_for(&server, initial_lease(lease_url));

        source
            .refresh_if_stale(0, 1)
            .await
            .expect("refresh succeeds");

        assert_eq!(source.current_generation(), 1);
        let lease = source.lease.load_full();
        let locator = lease
            .locators
            .get(&1)
            .expect("ordinal 1 locator present after refresh");
        assert_eq!(locator.url, "https://example.invalid/new");

        // The server should have received exactly one refresh request.
        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].path, "/queries/abc-123/result");
    }

    #[tokio::test]
    async fn refresh_if_stale_skips_when_generation_already_moved() {
        // Canned responses empty: any unexpected request returns 503 which
        // would surface as a test failure. The test asserts no request is
        // issued at all.
        let server = start_server(vec![]);
        let lease_url = format!("http://{}/chunk-a", server.addr);
        let mut lease = initial_lease(lease_url);
        lease.generation = 5;
        let source = source_for(&server, lease);

        source
            .refresh_if_stale(3, 1)
            .await
            .expect("stale generation short-circuits to Ok");

        assert_eq!(source.current_generation(), 5);
        assert!(
            server.recorded.lock().unwrap().is_empty(),
            "no HTTP call must be made when generation is stale",
        );
    }

    #[tokio::test]
    async fn refresh_if_stale_is_single_flight_under_concurrent_callers() {
        let server = start_server(vec![refresh_body_with_chunk("https://example.invalid/new")]);
        let lease_url = format!("http://{}/chunk-old", server.addr);
        let source = Arc::new(source_for(&server, initial_lease(lease_url)));

        let s1 = Arc::clone(&source);
        let s2 = Arc::clone(&source);
        let (r1, r2) = tokio::join!(async move { s1.refresh_if_stale(0, 1).await }, async move {
            s2.refresh_if_stale(0, 1).await
        },);
        r1.expect("first refresh succeeds");
        r2.expect("second refresh short-circuits successfully");

        assert_eq!(source.current_generation(), 1);
        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(
            recorded.len(),
            1,
            "single-flight must coalesce to exactly one refresh HTTP call, got {}",
            recorded.len(),
        );
    }

    fn server_chunk_url(server: &ServerHandle, path: &str) -> String {
        format!("http://{}{path}", server.addr)
    }

    #[tokio::test]
    async fn fetch_expired_lease_refreshes_and_retries_download() {
        let server = start_server(vec![]);
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            server_chunk_url(&server, "/new-chunk"),
        )));
        server.push_response(chunk_body(&["row-a", "row-b"]));

        let source = source_for(&server, initial_lease(expired_s3_url("/orig")));

        let rows = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect("fetch should succeed after refresh");
        assert_eq!(rows.len(), 2);
        assert_eq!(source.current_generation(), 1);

        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/queries/abc-123/result")
                .count(),
            1,
            "refresh endpoint hit exactly once",
        );
        assert_eq!(
            recorded.iter().filter(|r| r.path == "/new-chunk").count(),
            1,
            "post-refresh retry downloads the refreshed chunk URL exactly once",
        );
    }

    #[tokio::test]
    async fn fetch_maps_refresh_validation_failure_to_result_set_expired() {
        let server = start_server(vec![refresh_body_with_row_count_mismatch()]);

        let source = source_for(&server, initial_lease(expired_s3_url("/orig")));

        let err = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect_err("validation rejection must bubble up");
        assert!(
            matches!(err, Error::ResultSetExpired),
            "expected ResultSetExpired, got {err:?}",
        );
    }

    #[tokio::test]
    async fn fetch_maps_second_expired_download_to_result_set_expired() {
        // Refresh response returns a fresh chunk URL that is itself already
        // expired, so the post-refresh retry preflights to PredictedExpired.
        let server = start_server(vec![]);
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            expired_s3_url("/still-expired"),
        )));

        let source = source_for(&server, initial_lease(expired_s3_url("/orig")));

        let err = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect_err("still-expired retry must surface as ResultSetExpired");
        assert!(
            matches!(err, Error::ResultSetExpired),
            "expected ResultSetExpired, got {err:?}",
        );

        // Refresh happened exactly once; we do not re-refresh inside a single fetch.
        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/queries/abc-123/result")
                .count(),
            1,
        );
    }

    #[tokio::test]
    async fn fetch_propagates_non_expiry_failure_from_retry_download() {
        let server = start_server(vec![]);
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            server_chunk_url(&server, "/not-found"),
        )));
        // Retry chunk download returns 404 — non-retryable, non-expired.
        server.push_response(CannedResponse::with_status(
            http::StatusCode::NOT_FOUND,
            "missing",
        ));

        let source = source_for(&server, initial_lease(expired_s3_url("/orig")));

        let err = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect_err("retry 404 must bubble up as ChunkDownload, not ResultSetExpired");
        match err {
            Error::ChunkDownload(body) => assert_eq!(body, "missing"),
            other => panic!("expected ChunkDownload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_does_not_escalate_to_result_set_expired_when_refresh_returns_same_url() {
        // The initial URL is only within the preflight's skew buffer (not
        // strictly past), so the predictor flags it as expired but its
        // encoded expiry is still minutes away. The refresh response then
        // returns that *same* URL — the backend did not hand us a freshly
        // signed locator and we have no evidence it was rotated.
        //
        // In that case the post-refresh retry must skip the preflight, hit
        // the network, and surface whatever failure the download returns
        // unchanged instead of escalating to `ResultSetExpired`.
        let server = start_server(vec![]);
        let original_url = buffer_window_s3_url_on(&server, "/same-chunk");
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            original_url.clone(),
        )));
        server.push_response(CannedResponse::with_status(
            http::StatusCode::NOT_FOUND,
            "same-url 404",
        ));
        let source = source_for(&server, initial_lease(original_url));

        let err = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect_err("same-URL refresh must not mask a real download failure");
        match err {
            Error::ChunkDownload(body) => assert_eq!(body, "same-url 404"),
            Error::ResultSetExpired => {
                panic!("same-URL (buffer-window) refresh must not be escalated to ResultSetExpired")
            }
            other => panic!("expected ChunkDownload, got {other:?}"),
        }

        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/queries/abc-123/result")
                .count(),
            1,
            "refresh endpoint hit exactly once",
        );
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/same-chunk")
                .count(),
            1,
            "retry path bypasses preflight and issues exactly one GET against the same URL",
        );
    }

    #[tokio::test]
    async fn fetch_maps_same_url_strictly_past_expiry_to_result_set_expired() {
        // The refresh response returns the same URL as before *and* that
        // URL's encoded expiry is strictly in the past. The URL itself
        // advertises that it is expired — a normal download would also
        // fail — so we short-circuit the retry and surface
        // `Error::ResultSetExpired` instead of wasting a network round trip.
        let server = start_server(vec![]);
        let original_url = expired_s3_url_on(&server, "/strict-past");
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            original_url.clone(),
        )));
        let source = source_for(&server, initial_lease(original_url));

        let err = source
            .fetch(FetchContext {
                ordinal: 1,
                committed_before: 1,
            })
            .await
            .expect_err("strictly-past same-URL refresh must surface ResultSetExpired");
        assert!(
            matches!(err, Error::ResultSetExpired),
            "expected ResultSetExpired, got {err:?}",
        );

        let recorded = server.recorded.lock().unwrap().clone();
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/queries/abc-123/result")
                .count(),
            1,
            "refresh endpoint hit exactly once",
        );
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r.path == "/strict-past")
                .count(),
            0,
            "strict-past same-URL must short-circuit without an HTTP attempt",
        );
    }

    #[tokio::test]
    async fn fetch_is_single_flight_under_concurrent_expired_leases() {
        let server = start_server(vec![]);
        server.push_response(refresh_body_with_raw_url(serde_json::Value::String(
            server_chunk_url(&server, "/new-chunk"),
        )));
        // Both concurrent fetches will retry against /new-chunk.
        server.push_response(chunk_body(&["row-a", "row-b"]));
        server.push_response(chunk_body(&["row-a", "row-b"]));

        let source = Arc::new(source_for(&server, initial_lease(expired_s3_url("/orig"))));

        let s1 = Arc::clone(&source);
        let s2 = Arc::clone(&source);
        let (r1, r2) = tokio::join!(
            async move {
                s1.fetch(FetchContext {
                    ordinal: 1,
                    committed_before: 1,
                })
                .await
            },
            async move {
                s2.fetch(FetchContext {
                    ordinal: 1,
                    committed_before: 1,
                })
                .await
            },
        );
        r1.expect("first concurrent fetch succeeds");
        r2.expect("second concurrent fetch succeeds");

        let recorded = server.recorded.lock().unwrap().clone();
        let refresh_hits = recorded
            .iter()
            .filter(|r| r.path == "/queries/abc-123/result")
            .count();
        assert_eq!(
            refresh_hits, 1,
            "single-flight must coalesce to exactly one refresh call under concurrent fetches",
        );
        let chunk_hits = recorded
            .iter()
            .filter(|r| r.path == "/new-chunk")
            .count();
        assert_eq!(chunk_hits, 2, "each fetch retries the chunk once");
    }

    #[tokio::test]
    async fn refresh_if_stale_releases_gate_after_failure() {
        // First canned response: HTTP 500 (communication error). Second:
        // valid refresh body. The gate must be released after the first call
        // so the second one proceeds.
        let server = start_server(vec![
            CannedResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR, "boom"),
            refresh_body_with_chunk("https://example.invalid/new"),
        ]);
        let lease_url = format!("http://{}/chunk-old", server.addr);
        let source = source_for(&server, initial_lease(lease_url));

        let first = source.refresh_if_stale(0, 1).await;
        assert!(
            matches!(first, Err(RefreshError::Api(Error::Communication(_)))),
            "first refresh must bubble up the HTTP error as RefreshError::Api",
        );
        assert_eq!(
            source.current_generation(),
            0,
            "failed refresh must not bump the generation",
        );

        source
            .refresh_if_stale(0, 1)
            .await
            .expect("second refresh after a failure must succeed");
        assert_eq!(source.current_generation(), 1);
    }
}
