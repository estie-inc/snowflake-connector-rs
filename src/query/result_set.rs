use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::task::JoinSet;

use crate::{Error, Result, SnowflakeColumn, SnowflakeRow};

use super::partition_source::{FetchContext, PartitionSource};
use super::snapshot::{PartitionCursor, PartitionSpec, RawPartitionRows, ResultSnapshot};

/// Options for controlling how [`ResultSet::collect_all_with_options`] fetches
/// remaining partitions.
pub struct CollectOptions {
    prefetch_concurrency: Option<NonZeroUsize>,
}

impl CollectOptions {
    pub fn new() -> Self {
        Self {
            prefetch_concurrency: None,
        }
    }

    /// Override the maximum number of partitions to download concurrently.
    /// When not set, the default from [`SnowflakeQueryConfig`](crate::SnowflakeQueryConfig) is used.
    pub fn with_prefetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.prefetch_concurrency = Some(concurrency);
        self
    }
}

impl Default for CollectOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CollectPolicy {
    pub(crate) prefetch_concurrency: NonZeroUsize,
}

pub struct ResultSet {
    snapshot: Arc<ResultSnapshot>,
    cursor: PartitionCursor,
    inline_rows: Option<RawPartitionRows>,
    source: PartitionSource,
    default_collect_policy: CollectPolicy,
}

impl ResultSet {
    pub(crate) fn new(
        snapshot: Arc<ResultSnapshot>,
        cursor: PartitionCursor,
        inline_rows: Option<RawPartitionRows>,
        source: PartitionSource,
        default_collect_policy: CollectPolicy,
    ) -> Self {
        Self {
            snapshot,
            cursor,
            inline_rows,
            source,
            default_collect_policy,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.snapshot.identity.query_id
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> &PartitionSource {
        &self.source
    }

    /// Column metadata for this result set, available even when there are no rows.
    pub fn columns(&self) -> &[SnowflakeColumn] {
        self.snapshot.columns.as_ref()
    }

    pub fn is_exhausted(&self) -> bool {
        self.cursor.is_exhausted()
    }

    /// Fetch the next partition as a batch of rows.
    ///
    /// Returns `Ok(None)` when all partitions have been consumed.
    /// On failure the cursor is **not** advanced, so the caller may retry.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<SnowflakeRow>>> {
        if self.cursor.is_exhausted() {
            return Ok(None);
        }

        let ordinal = self.cursor.next_ordinal;
        let spec = &self.snapshot.partitions[ordinal];

        let raw = match spec {
            PartitionSpec::Inline => self
                .inline_rows
                .take()
                .expect("inline_rows must be Some when partition spec is Inline"),
            PartitionSpec::Remote { .. } => {
                let ctx = FetchContext {
                    ordinal,
                    committed_before: ordinal,
                };
                self.source.fetch(ctx).await?
            }
        };

        self.cursor.advance();
        Ok(Some(convert_rows(
            raw,
            &self.snapshot.columns,
            &self.snapshot.column_indices,
        )))
    }

    /// Consume this result set and collect all remaining rows.
    ///
    /// Uses the default prefetch concurrency from
    /// [`SnowflakeQueryConfig`](crate::SnowflakeQueryConfig).
    pub async fn collect_all(self) -> Result<Vec<SnowflakeRow>> {
        let policy = self.default_collect_policy;
        self.collect_with_policy(policy).await
    }

    /// Consume this result set and collect all remaining rows with custom options.
    pub async fn collect_all_with_options(
        self,
        options: CollectOptions,
    ) -> Result<Vec<SnowflakeRow>> {
        let policy = CollectPolicy {
            prefetch_concurrency: options
                .prefetch_concurrency
                .unwrap_or(self.default_collect_policy.prefetch_concurrency),
        };
        self.collect_with_policy(policy).await
    }

    async fn collect_with_policy(self, policy: CollectPolicy) -> Result<Vec<SnowflakeRow>> {
        let Self {
            snapshot,
            cursor,
            inline_rows,
            source,
            ..
        } = self;

        if cursor.is_exhausted() {
            return Ok(Vec::new());
        }

        let mut all_rows = Vec::new();
        let start = cursor.next_ordinal;
        let total = snapshot.partitions.len();

        // Consume inline partition synchronously if it's next
        let mut next_remote_ordinal = start;
        if next_remote_ordinal < total {
            if let PartitionSpec::Inline = &snapshot.partitions[next_remote_ordinal] {
                let raw =
                    inline_rows.expect("inline_rows must be Some when partition spec is Inline");
                all_rows.extend(convert_rows(
                    raw,
                    &snapshot.columns,
                    &snapshot.column_indices,
                ));
                next_remote_ordinal += 1;
            }
        }

        if next_remote_ordinal >= total {
            return Ok(all_rows);
        }

        // Concurrent download of remote partitions using JoinSet
        let source = Arc::new(source);
        let mut window = CollectWindow::new(
            next_remote_ordinal,
            total,
            policy.prefetch_concurrency.get(),
        );
        window.fill(&source);

        while let Some(result) = window.join_next().await {
            let (ordinal, raw) = result?;
            all_rows.extend(window.commit(
                ordinal,
                raw,
                &snapshot.columns,
                &snapshot.column_indices,
            ));
            window.fill(&source);
        }

        Ok(all_rows)
    }
}

struct CollectWindow {
    next_spawn_ordinal: usize,
    next_commit_ordinal: usize,
    total: usize,
    max_in_flight: usize,
    tasks: JoinSet<(usize, Result<RawPartitionRows>)>,
    buffered: BTreeMap<usize, RawPartitionRows>,
}

impl CollectWindow {
    fn new(start_ordinal: usize, total: usize, max_in_flight: usize) -> Self {
        Self {
            next_spawn_ordinal: start_ordinal,
            next_commit_ordinal: start_ordinal,
            total,
            max_in_flight,
            tasks: JoinSet::new(),
            buffered: BTreeMap::new(),
        }
    }

    /// Spawn tasks up to the concurrency limit.
    fn fill(&mut self, source: &Arc<PartitionSource>) {
        while self.tasks.len() < self.max_in_flight && self.next_spawn_ordinal < self.total {
            let ordinal = self.next_spawn_ordinal;
            // `committed_before` is the commit frontier at the moment this task
            // is spawned. We intentionally do not update it later, so refresh
            // sees the frontier from the task's own perspective.
            let ctx = FetchContext {
                ordinal,
                committed_before: self.next_commit_ordinal,
            };
            let source = Arc::clone(source);
            self.tasks.spawn(async move {
                let result = source.fetch(ctx).await;
                (ordinal, result)
            });
            self.next_spawn_ordinal += 1;
        }
    }

    /// Wait for the next task to complete.
    async fn join_next(&mut self) -> Option<Result<(usize, RawPartitionRows)>> {
        let join_result = self.tasks.join_next().await?;
        Some(
            join_result
                .map_err(Error::FutureJoin)
                .and_then(|(ordinal, r)| r.map(|rows| (ordinal, rows))),
        )
    }

    /// Buffer a completed partition and flush the contiguous prefix
    /// that is ready to commit, converting raw rows to `SnowflakeRow`.
    fn commit(
        &mut self,
        ordinal: usize,
        raw: RawPartitionRows,
        columns: &Arc<[SnowflakeColumn]>,
        column_indices: &Arc<HashMap<String, usize>>,
    ) -> Vec<SnowflakeRow> {
        self.buffered.insert(ordinal, raw);
        let mut committed = Vec::new();
        while let Some(rows) = self.buffered.remove(&self.next_commit_ordinal) {
            committed.extend(convert_rows(rows, columns, column_indices));
            self.next_commit_ordinal += 1;
        }
        committed
    }
}

fn convert_rows(
    raw: RawPartitionRows,
    columns: &Arc<[SnowflakeColumn]>,
    column_indices: &Arc<HashMap<String, usize>>,
) -> Vec<SnowflakeRow> {
    raw.into_iter()
        .map(|row| SnowflakeRow {
            row,
            columns: Arc::clone(columns),
            column_indices: Arc::clone(column_indices),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use super::*;
    use crate::query::{
        partition_source::tests::FakePartitionSource,
        snapshot::{PartitionCursor, PartitionSpec, ResultIdentity, ResultSnapshot},
    };
    use crate::row::SnowflakeColumnType;

    fn make_columns() -> (Arc<[SnowflakeColumn]>, Arc<HashMap<String, usize>>) {
        let col = SnowflakeColumn::new(
            "X".to_string(),
            0,
            SnowflakeColumnType::new("fixed".to_string(), false, None, None, None),
        );
        let columns: Arc<[SnowflakeColumn]> = Arc::from(vec![col]);
        let column_indices = Arc::new(
            [("X".to_string(), 0_usize)]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        (columns, column_indices)
    }

    fn dummy_snapshot(num_remote: usize) -> Arc<ResultSnapshot> {
        let (columns, column_indices) = make_columns();
        let partitions = (0..num_remote)
            .map(|_| PartitionSpec::Remote {
                row_count: 1,
                compressed_size: 0,
                uncompressed_size: 0,
            })
            .collect();
        Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: "test".to_string(),
            },
            columns,
            column_indices,
            partitions,
        })
    }

    /// Snapshot with one inline partition followed by `num_remote` remote partitions.
    fn snapshot_with_inline(num_remote: usize) -> Arc<ResultSnapshot> {
        let (columns, column_indices) = make_columns();
        let mut partitions = vec![PartitionSpec::Inline];
        for _ in 0..num_remote {
            partitions.push(PartitionSpec::Remote {
                row_count: 1,
                compressed_size: 0,
                uncompressed_size: 0,
            });
        }
        Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: "test".to_string(),
            },
            columns,
            column_indices,
            partitions,
        })
    }

    fn ok_rows(n: usize) -> crate::Result<RawPartitionRows> {
        Ok((0..n).map(|i| vec![Some(i.to_string())]).collect())
    }

    fn fail() -> crate::Result<RawPartitionRows> {
        Err(crate::Error::ChunkDownload("simulated failure".into()))
    }

    fn fake_source(
        responses: Vec<(usize, Vec<crate::Result<RawPartitionRows>>)>,
    ) -> PartitionSource {
        let map = responses
            .into_iter()
            .map(|(ord, results)| (ord, VecDeque::from(results)))
            .collect();
        PartitionSource::Fake(FakePartitionSource::new(map))
    }

    fn build_result_set(snapshot: Arc<ResultSnapshot>, source: PartitionSource) -> ResultSet {
        let total = snapshot.partitions.len();
        ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            None,
            source,
            CollectPolicy {
                prefetch_concurrency: NonZeroUsize::new(8).unwrap(),
            },
        )
    }

    /// next_batch failure must not advance the cursor; the same
    /// partition can be retried on a subsequent call.
    #[tokio::test]
    async fn next_batch_failure_does_not_advance_cursor() {
        let snapshot = dummy_snapshot(2);
        let source = fake_source(vec![(0, vec![fail(), ok_rows(1)]), (1, vec![ok_rows(2)])]);
        let mut rs = build_result_set(snapshot, source);

        // First call fails — cursor must stay at ordinal 0
        let err = rs.next_batch().await;
        assert!(err.is_err());
        assert_eq!(rs.cursor.next_ordinal, 0);
        assert!(!rs.is_exhausted());

        // Retry succeeds on the same ordinal
        let batch = rs.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(rs.cursor.next_ordinal, 1);

        // Next partition succeeds normally
        let batch = rs.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len(), 2);
        assert!(rs.is_exhausted());
    }

    /// collect_all must return Err when any partition fails, without
    /// exposing partially collected rows.
    #[tokio::test]
    async fn collect_all_failure_returns_err_without_partial_data() {
        let snapshot = dummy_snapshot(3);
        let source = fake_source(vec![
            (0, vec![ok_rows(1)]),
            (1, vec![fail()]),
            (2, vec![ok_rows(1)]),
        ]);
        let rs = build_result_set(snapshot, source);

        let result = rs.collect_all().await;
        assert!(result.is_err());
    }

    /// next_batch must yield inline rows first, then remote partitions
    /// starting at ordinal 1.
    #[tokio::test]
    async fn next_batch_inline_then_remote() {
        let snapshot = snapshot_with_inline(2);
        let inline = vec![vec![Some("10".into())], vec![Some("11".into())]];
        let source = fake_source(vec![(1, vec![ok_rows(1)]), (2, vec![ok_rows(1)])]);
        let total = snapshot.partitions.len();
        let mut rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline),
            source,
            CollectPolicy {
                prefetch_concurrency: NonZeroUsize::new(8).unwrap(),
            },
        );

        // First batch is inline
        let batch = rs.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].get::<String>("X").unwrap(), "10");
        assert_eq!(batch[1].get::<String>("X").unwrap(), "11");

        // Remaining are remote
        let batch = rs.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        let batch = rs.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);

        assert!(rs.is_exhausted());
        assert!(rs.next_batch().await.unwrap().is_none());
    }

    fn recording_source(
        responses: Vec<(usize, Vec<crate::Result<RawPartitionRows>>)>,
    ) -> (PartitionSource, Arc<Mutex<Vec<FetchContext>>>) {
        let recorder = Arc::new(Mutex::new(Vec::<FetchContext>::new()));
        let map = responses
            .into_iter()
            .map(|(ord, results)| (ord, VecDeque::from(results)))
            .collect();
        let fake = FakePartitionSource::with_recorder(map, Arc::clone(&recorder));
        (PartitionSource::Fake(fake), recorder)
    }

    /// `next_batch` must pass `committed_before == ordinal` for each remote
    /// fetch: the caller cannot have committed the partition it's about to
    /// fetch, but every earlier ordinal is already consumed.
    #[tokio::test]
    async fn next_batch_passes_committed_before_equal_to_ordinal() {
        let snapshot = dummy_snapshot(3);
        let (source, recorder) = recording_source(vec![
            (0, vec![ok_rows(1)]),
            (1, vec![ok_rows(1)]),
            (2, vec![ok_rows(1)]),
        ]);
        let mut rs = build_result_set(snapshot, source);
        for _ in 0..3 {
            rs.next_batch().await.unwrap();
        }
        let observed = recorder.lock().unwrap().clone();
        let expected: Vec<FetchContext> = (0..3)
            .map(|o| FetchContext {
                ordinal: o,
                committed_before: o,
            })
            .collect();
        assert_eq!(observed, expected);
    }

    /// `collect_all` spawns tasks greedily up to the prefetch concurrency.
    /// Every recorded `committed_before` must be <= the spawned ordinal
    /// (it can be lower because spawns are issued before prior ones commit).
    #[tokio::test]
    async fn collect_all_records_committed_before_leq_ordinal_with_monotone_frontier() {
        let snapshot = dummy_snapshot(4);
        let (source, recorder) = recording_source(vec![
            (0, vec![ok_rows(1)]),
            (1, vec![ok_rows(1)]),
            (2, vec![ok_rows(1)]),
            (3, vec![ok_rows(1)]),
        ]);
        let rs = build_result_set(snapshot, source);
        rs.collect_all().await.unwrap();

        let mut observed = recorder.lock().unwrap().clone();
        observed.sort_by_key(|ctx| ctx.ordinal);
        assert_eq!(observed.len(), 4);

        let mut prev_committed: Option<usize> = None;
        for ctx in &observed {
            assert!(
                ctx.committed_before <= ctx.ordinal,
                "committed_before must not exceed ordinal: {ctx:?}",
            );
            if let Some(prev) = prev_committed {
                assert!(
                    ctx.committed_before >= prev,
                    "committed_before must be monotone non-decreasing across spawns: {ctx:?} after prev={prev}",
                );
            }
            prev_committed = Some(ctx.committed_before);
        }
    }

    /// collect_all must consume inline rows synchronously first, then
    /// fetch remote partitions, returning all in order.
    #[tokio::test]
    async fn collect_all_inline_then_remote() {
        let snapshot = snapshot_with_inline(2);
        let inline = vec![vec![Some("10".into())], vec![Some("11".into())]];
        let source = fake_source(vec![(1, vec![ok_rows(3)]), (2, vec![ok_rows(2)])]);
        let total = snapshot.partitions.len();
        let rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline),
            source,
            CollectPolicy {
                prefetch_concurrency: NonZeroUsize::new(8).unwrap(),
            },
        );

        let rows = rs.collect_all().await.unwrap();
        // 2 inline + 3 remote(1) + 2 remote(2) = 7
        assert_eq!(rows.len(), 7);
        // First two are inline
        assert_eq!(rows[0].get::<String>("X").unwrap(), "10");
        assert_eq!(rows[1].get::<String>("X").unwrap(), "11");
    }
}
