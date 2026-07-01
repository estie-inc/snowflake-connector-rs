use std::{fmt, num::NonZeroUsize, sync::Arc};

use bytes::Bytes;

use crate::{
    Result,
    error::QueryScopedResult,
    result::{DynamicRow, ResultTable, Schema},
    rowset::parser::parse_inline_result_table_async,
    runtime::QueryRuntime,
};

use super::{
    collect::{CollectPolicy, CollectWindow},
    partition_source::{PartitionSource, remote_fetch_context},
    snapshot::{PartitionCursor, PartitionSpec, ResultSnapshot},
};

/// Options for controlling how result-set collection fetches remaining partitions.
#[derive(Clone, Debug)]
pub struct CollectOptions {
    prefetch_concurrency: Option<NonZeroUsize>,
}

impl CollectOptions {
    pub fn new() -> Self {
        Self {
            prefetch_concurrency: None,
        }
    }

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

pub struct ResultSet {
    snapshot: Arc<ResultSnapshot>,
    cursor: PartitionCursor,
    inline_rowset: Option<InlineRowset>,
    source: PartitionSource,
    runtime: QueryRuntime,
    default_collect_policy: CollectPolicy,
}

impl fmt::Debug for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResultSet")
            .field("query_id", &self.query_id())
            .field("is_exhausted", &self.is_exhausted())
            .finish_non_exhaustive()
    }
}

pub(crate) struct InlineRowset {
    bytes: Bytes,
    row_count_hint: Option<u64>,
}

impl InlineRowset {
    pub(crate) fn new(bytes: Bytes, row_count_hint: Option<u64>) -> Self {
        Self {
            bytes,
            row_count_hint,
        }
    }
}

impl ResultSet {
    pub(crate) fn new(
        snapshot: Arc<ResultSnapshot>,
        cursor: PartitionCursor,
        inline_rowset: Option<InlineRowset>,
        source: PartitionSource,
        runtime: QueryRuntime,
        default_collect_policy: CollectPolicy,
    ) -> Self {
        Self {
            snapshot,
            cursor,
            inline_rowset,
            source,
            runtime,
            default_collect_policy,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.snapshot.identity.query_id
    }

    pub fn schema(&self) -> &Schema {
        &self.snapshot.schema
    }

    pub(crate) fn schema_arc(&self) -> &Arc<Schema> {
        &self.snapshot.schema
    }

    pub fn is_exhausted(&self) -> bool {
        self.cursor.is_exhausted()
    }

    /// Fetch the next partition as a `ResultTable`.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if fetching or
    /// materializing the next partition fails.
    pub async fn next_table(&mut self) -> Result<Option<ResultTable>> {
        self.next_table_inner().await.map_err(crate::Error::from)
    }

    async fn next_table_inner(&mut self) -> QueryScopedResult<Option<ResultTable>> {
        if self.cursor.is_exhausted() {
            return Ok(None);
        }
        let ordinal = self.cursor.next_ordinal;
        let spec = self.snapshot.partitions[ordinal];

        let table = match spec {
            PartitionSpec::Inline => {
                let (bytes, row_count_hint) = {
                    let inline = self
                        .inline_rowset
                        .as_ref()
                        .expect("inline_rowset must be Some when partition spec is Inline");
                    (inline.bytes.clone(), inline.row_count_hint)
                };

                let table = parse_inline_result_table_async(
                    Arc::clone(&self.snapshot.schema),
                    Arc::clone(&self.snapshot.identity.query_id),
                    bytes,
                    row_count_hint,
                    Some(self.runtime.blocking_parse_limiter()),
                )
                .await?;

                self.inline_rowset = None;
                table
            }
            PartitionSpec::Remote { .. } => {
                self.source
                    .fetch_table(
                        remote_fetch_context(
                            self.snapshot.as_ref(),
                            ordinal,
                            Some(self.runtime.blocking_parse_limiter()),
                        ),
                        Arc::clone(&self.snapshot.schema),
                    )
                    .await?
            }
        };

        self.cursor.advance();
        Ok(Some(table))
    }

    /// Consume this result set and merge all remaining partitions into a single `ResultTable`.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
    /// partition fails to materialize.
    pub async fn collect_table(self) -> Result<ResultTable> {
        let policy = self.default_collect_policy;
        self.collect_with_policy(policy)
            .await
            .map_err(crate::Error::from)
    }

    /// Consume this result set and merge all remaining partitions into a single `ResultTable`.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
    /// partition fails to materialize.
    pub async fn collect_table_with_options(self, options: CollectOptions) -> Result<ResultTable> {
        let policy = CollectPolicy::new(
            options
                .prefetch_concurrency
                .unwrap_or(self.default_collect_policy.prefetch_concurrency),
        );
        self.collect_with_policy(policy)
            .await
            .map_err(crate::Error::from)
    }

    /// Consume this result set and decode all rows into any collection implementing [`FromIterator<DynamicRow>`](FromIterator).
    ///
    /// The target collection is inferred from context (e.g. a `let` binding type or a `.collect::<Vec<_>>()` turbofish),
    /// matching [`Iterator::collect`]'s usual inference.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, `ErrorKind::Internal`, or
    /// `ErrorKind::Decode`; inspect the detail via [`crate::Error::as_cell_decode_error`].
    pub async fn collect<C: FromIterator<DynamicRow>>(self) -> Result<C> {
        let table = self.collect_table().await?;
        table.dynamic_rows()?.collect()
    }

    /// Consume this result set and decode all rows into any collection implementing [`FromIterator<DynamicRow>`](FromIterator).
    ///
    /// The target collection is inferred from context, as with [`ResultSet::collect`].
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, `ErrorKind::Internal`, or
    /// `ErrorKind::Decode`; inspect the detail via [`crate::Error::as_cell_decode_error`].
    pub async fn collect_with_options<C: FromIterator<DynamicRow>>(
        self,
        options: CollectOptions,
    ) -> Result<C> {
        let table = self.collect_table_with_options(options).await?;
        table.dynamic_rows()?.collect()
    }

    async fn collect_with_policy(self, policy: CollectPolicy) -> QueryScopedResult<ResultTable> {
        let Self {
            snapshot,
            cursor,
            inline_rowset,
            source,
            runtime,
            ..
        } = self;

        let schema = Arc::clone(&snapshot.schema);
        if cursor.is_exhausted() {
            return Ok(ResultTable::empty(
                schema,
                Arc::clone(&snapshot.identity.query_id),
            ));
        }

        let mut tables = Vec::new();
        let total = snapshot.partitions.len();
        let inline_ordinal = cursor.next_ordinal;
        let inline_first = matches!(
            snapshot.partitions.get(inline_ordinal),
            Some(PartitionSpec::Inline)
        );
        let first_remote_ordinal = inline_ordinal + usize::from(inline_first);

        // Start remote prefetch before awaiting inline parsing so network I/O can
        // overlap any blocking-parse permit wait on the inline partition.
        let source = (first_remote_ordinal < total).then(|| Arc::new(source));
        let blocking_parse_limiter = source.as_ref().map(|_| runtime.blocking_parse_limiter());
        let mut window = source.as_ref().map(|source| {
            let blocking_parse_limiter = blocking_parse_limiter
                .as_ref()
                .expect("limiter exists when source exists");
            let mut window = CollectWindow::new(
                Arc::clone(&snapshot.identity.query_id),
                first_remote_ordinal,
                total,
                policy.prefetch_concurrency.get(),
            );
            window.fill(source, &snapshot, blocking_parse_limiter);
            window
        });

        if inline_first {
            let inline =
                inline_rowset.expect("inline_rowset must be Some when partition spec is Inline");
            let table = parse_inline_result_table_async(
                Arc::clone(&snapshot.schema),
                Arc::clone(&snapshot.identity.query_id),
                inline.bytes,
                inline.row_count_hint,
                Some(runtime.blocking_parse_limiter()),
            )
            .await?;
            tables.push(table);
        }

        if let (Some(source), Some(blocking_parse_limiter), Some(mut window)) =
            (source, blocking_parse_limiter, window.take())
        {
            while let Some(result) = window.join_next().await {
                let (ordinal, table) = result?;
                window.commit(ordinal, table, &mut tables);
                window.fill(&source, &snapshot, &blocking_parse_limiter);
            }
        }

        if tables.is_empty() {
            return Ok(ResultTable::empty(
                schema,
                Arc::clone(&snapshot.identity.query_id),
            ));
        }
        Ok(ResultTable::concat_same_schema(tables))
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::VecDeque, ptr, sync::atomic::Ordering, time::Duration};

    use super::*;
    use crate::{
        ErrorKind, MissingColumnError, SchemaError,
        error::QueryScopedRepr,
        query_result::{
            TypedResultSet,
            collect::CollectPolicy,
            partition_source::tests::{BlockingFetchProbe, FakePartitionSource, FakeResponse},
            snapshot::{PartitionCursor, PartitionSpec, ResultIdentity, ResultSnapshot},
        },
        result::{
            CellDecodeIssue, CellDecodeResult, CellRef, Column, ColumnIndex, ColumnType,
            DynamicRow, FromCell, FromRow, RowPlanContext, RowRef, Schema,
        },
        rowset::BLOCKING_PARSE_CELLS,
        runtime::QueryRuntime,
    };

    thread_local! {
        static BUILD_PLAN_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    fn reset_build_plan_calls() {
        BUILD_PLAN_CALLS.with(|calls| calls.set(0));
    }

    fn record_build_plan_call() {
        BUILD_PLAN_CALLS.with(|calls| calls.set(calls.get() + 1));
    }

    fn build_plan_calls() -> usize {
        BUILD_PLAN_CALLS.with(Cell::get)
    }

    fn make_schema() -> Arc<Schema> {
        Arc::new(
            Schema::from_columns(vec![Column::new(
                "X",
                0,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            )])
            .unwrap(),
        )
    }

    fn dummy_snapshot(num_remote: usize) -> Arc<ResultSnapshot> {
        let schema = make_schema();
        let partitions = (0..num_remote)
            .map(|_| PartitionSpec::Remote {
                row_count: 1,
                compressed_size: 0,
                uncompressed_size: 0,
            })
            .collect();
        Arc::new(ResultSnapshot {
            identity: ResultIdentity {
                query_id: Arc::from("test"),
            },
            schema,
            partitions,
        })
    }

    fn snapshot_with_inline(num_remote: usize) -> Arc<ResultSnapshot> {
        let schema = make_schema();
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
                query_id: Arc::from("test"),
            },
            schema,
            partitions,
        })
    }

    fn ok_rows(n: usize) -> std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr> {
        Ok((0..n).map(|i| vec![Some(i.to_string())]).collect())
    }

    fn fail() -> std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr> {
        Err(crate::error::NetworkError::chunk_download(500, "simulated failure").into())
    }

    fn fail_timeout() -> std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr> {
        Err(crate::error::TimeoutError::query().into())
    }

    fn fail_protocol() -> std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr> {
        Err(crate::error::ProtocolError::missing_field("data").into())
    }

    async fn fail_internal() -> std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr> {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();

        Err(crate::error::InternalError::future_join(handle.await.unwrap_err()).into())
    }

    type FakeRows = Vec<Vec<Option<String>>>;

    fn fake_source(
        responses: Vec<(usize, Vec<std::result::Result<FakeRows, QueryScopedRepr>>)>,
    ) -> PartitionSource {
        let map = responses
            .into_iter()
            .map(|(ord, results)| {
                (
                    ord,
                    results
                        .into_iter()
                        .map(FakeResponse::from)
                        .collect::<VecDeque<_>>(),
                )
            })
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
            QueryRuntime::new(),
            CollectPolicy::new(NonZeroUsize::new(8).unwrap()),
        )
    }

    fn inline_rowset(json: &'static [u8], row_count_hint: Option<u64>) -> InlineRowset {
        InlineRowset::new(Bytes::from_static(json), row_count_hint)
    }

    fn build_typed_result_set<T: FromRow>(
        result_set: ResultSet,
    ) -> crate::Result<TypedResultSet<T>> {
        let plan = T::build_plan(RowPlanContext::new(result_set.schema_arc()))?;
        Ok(TypedResultSet::new(result_set, plan))
    }

    #[derive(Debug)]
    struct PlanErrorRow;

    impl FromRow for PlanErrorRow {
        type Plan = ();

        fn build_plan(_: RowPlanContext<'_>) -> crate::Result<Self::Plan> {
            Err(SchemaError::MissingColumn(MissingColumnError::new("MISSING")).into())
        }

        fn from_row_with_plan(_: RowRef<'_>, _: &Self::Plan) -> crate::Result<Self> {
            unreachable!("plan build should fail before any row is decoded")
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct CountingRow;

    impl FromRow for CountingRow {
        type Plan = ();

        fn build_plan(_: RowPlanContext<'_>) -> crate::Result<Self::Plan> {
            record_build_plan_call();
            Ok(())
        }

        fn from_row_with_plan(_: RowRef<'_>, _: &Self::Plan) -> crate::Result<Self> {
            Ok(Self)
        }
    }

    #[derive(Debug)]
    struct DecodeErrorRow;

    #[derive(Debug)]
    struct DecodeErrorCell;

    impl FromCell for DecodeErrorCell {
        fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
            let _ = cell.required_raw()?;
            Err(CellDecodeIssue::builder("simulated decode failure").build())
        }
    }

    impl FromRow for DecodeErrorRow {
        type Plan = ();

        fn build_plan(_: RowPlanContext<'_>) -> crate::Result<Self::Plan> {
            Ok(())
        }

        fn from_row_with_plan(row: RowRef<'_>, _: &Self::Plan) -> crate::Result<Self> {
            row.get::<DecodeErrorCell>(ColumnIndex::new(0))?;
            unreachable!("decode failure should bubble out before constructing DecodeErrorRow")
        }
    }

    #[tokio::test]
    async fn next_table_failure_does_not_advance_cursor() {
        let snapshot = dummy_snapshot(2);
        let source = fake_source(vec![(0, vec![fail(), ok_rows(1)]), (1, vec![ok_rows(2)])]);
        let mut rs = build_result_set(snapshot, source);
        let err = rs.next_table().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
        assert_eq!(rs.query_id(), "test");
        assert_eq!(rs.cursor.next_ordinal, 0);
        let t = rs.next_table().await.unwrap().unwrap();
        assert_eq!(t.row_count(), 1);
        let t = rs.next_table().await.unwrap().unwrap();
        assert_eq!(t.row_count(), 2);
        assert!(rs.is_exhausted());
    }

    #[tokio::test]
    async fn collect_table_failure_returns_err() {
        let snapshot = dummy_snapshot(3);
        let source = fake_source(vec![
            (0, vec![ok_rows(1)]),
            (1, vec![fail()]),
            (2, vec![ok_rows(1)]),
        ]);
        let rs = build_result_set(snapshot, source);
        let err = rs.collect_table().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
    }

    #[tokio::test]
    async fn collect_table_query_scoped_failures_preserve_query_id() {
        let cases = vec![
            (
                "timeout",
                fake_source(vec![(0, vec![fail_timeout()])]),
                ErrorKind::Timeout,
            ),
            (
                "protocol",
                fake_source(vec![(0, vec![fail_protocol()])]),
                ErrorKind::Protocol,
            ),
            (
                "internal",
                fake_source(vec![(0, vec![fail_internal().await])]),
                ErrorKind::Internal,
            ),
        ];

        for (label, source, expected_kind) in cases {
            let err = build_result_set(dummy_snapshot(1), source)
                .collect_table()
                .await
                .unwrap_err();

            assert_eq!(err.kind(), expected_kind, "{label}");
            assert_eq!(err.query_id(), Some("test"), "{label}");
        }
    }

    #[tokio::test]
    async fn next_table_inline_then_remote() {
        let snapshot = snapshot_with_inline(2);
        let source = fake_source(vec![(1, vec![ok_rows(1)]), (2, vec![ok_rows(1)])]);
        let total = snapshot.partitions.len();
        let mut rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline_rowset(br#"[["10"],["11"]]"#, Some(2))),
            source,
            QueryRuntime::new(),
            CollectPolicy::new(NonZeroUsize::new(8).unwrap()),
        );

        let t = rs.next_table().await.unwrap().unwrap();
        assert_eq!(t.row_count(), 2);
        let t = rs.next_table().await.unwrap().unwrap();
        assert_eq!(t.row_count(), 1);
        let t = rs.next_table().await.unwrap().unwrap();
        assert_eq!(t.row_count(), 1);
        assert!(rs.next_table().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn collect_table_inline_then_remote() {
        let snapshot = snapshot_with_inline(2);
        let source = fake_source(vec![(1, vec![ok_rows(3)]), (2, vec![ok_rows(2)])]);
        let total = snapshot.partitions.len();
        let rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline_rowset(br#"[["10"],["11"]]"#, Some(2))),
            source,
            QueryRuntime::new(),
            CollectPolicy::new(NonZeroUsize::new(8).unwrap()),
        );
        let t = rs.collect_table().await.unwrap();
        assert_eq!(t.row_count(), 7);
    }

    #[tokio::test]
    async fn next_table_inline_parse_failure_does_not_advance_cursor() {
        let snapshot = snapshot_with_inline(0);
        let total = snapshot.partitions.len();
        let mut rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline_rowset(br#"[["10"]"#, Some(1))),
            fake_source(vec![]),
            QueryRuntime::new(),
            CollectPolicy::new(NonZeroUsize::new(8).unwrap()),
        );

        let err = rs.next_table().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(err.query_id(), Some("test"));
        assert_eq!(rs.query_id(), "test");
        assert_eq!(rs.cursor.next_ordinal, 0);
        assert!(rs.inline_rowset.is_some());

        rs.inline_rowset = Some(inline_rowset(br#"[["10"]]"#, Some(1)));

        let table = rs.next_table().await.unwrap().unwrap();
        assert_eq!(table.row_count(), 1);
        assert!(rs.is_exhausted());
    }

    #[tokio::test]
    async fn collect_table_limits_blocking_parse_concurrency() {
        let snapshot = dummy_snapshot(4);
        let probe = Arc::new(BlockingFetchProbe::default());
        let map = (0..4)
            .map(|ordinal| {
                (
                    ordinal,
                    VecDeque::from(vec![FakeResponse::BlockingRows {
                        rows: ok_rows(1),
                        probe: Arc::clone(&probe),
                        delay: Duration::from_millis(50),
                    }]),
                )
            })
            .collect();
        let source = PartitionSource::Fake(FakePartitionSource::new(map));
        let rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(4),
            None,
            source,
            QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(2).unwrap()),
            CollectPolicy::new(NonZeroUsize::new(4).unwrap()),
        );

        let table = rs.collect_table().await.unwrap();
        assert_eq!(table.row_count(), 4);
        assert_eq!(probe.max_active(), 2);
    }

    #[tokio::test]
    async fn collect_table_starts_remote_prefetch_while_inline_parse_waits_for_permit() {
        let snapshot = snapshot_with_inline(1);
        let total = snapshot.partitions.len();
        let source = FakePartitionSource::new(
            vec![(1, VecDeque::from(vec![FakeResponse::Rows(ok_rows(1))]))]
                .into_iter()
                .collect(),
        );
        let fetch_count = source.fetch_count();
        let runtime = QueryRuntime::with_blocking_parse_concurrency(NonZeroUsize::new(1).unwrap());
        let permit = runtime.blocking_parse_limiter().acquire_owned().await;
        let rs = ResultSet::new(
            snapshot,
            PartitionCursor::new(total),
            Some(inline_rowset(br#"[["10"]]"#, Some(BLOCKING_PARSE_CELLS))),
            PartitionSource::Fake(source),
            runtime,
            CollectPolicy::new(NonZeroUsize::new(1).unwrap()),
        );

        let collect = rs.collect_table();
        tokio::pin!(collect);

        match tokio::time::timeout(Duration::from_millis(20), &mut collect).await {
            Err(_) => {}
            Ok(_) => panic!("expected collect_table to wait for the inline blocking-parse permit"),
        }

        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        drop(permit);

        let table = collect.await.unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[tokio::test]
    async fn typed_result_set_collect_builds_plan_once() {
        reset_build_plan_calls();
        let snapshot = dummy_snapshot(1);
        let rs = build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(2)])]),
        ))
        .unwrap();

        let rows = rs.collect::<Vec<_>>().await.unwrap();

        assert_eq!(rows, vec![CountingRow, CountingRow]);
        assert_eq!(build_plan_calls(), 1);
    }

    #[tokio::test]
    async fn collect_failure_propagates_query_id_from_consumed_result_set() {
        let snapshot = dummy_snapshot(1);
        let err = build_result_set(snapshot, fake_source(vec![(0, vec![fail()])]))
            .collect::<Vec<_>>()
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
    }

    #[tokio::test]
    async fn collect_table_with_options_failure_propagates_query_id_from_consumed_result_set() {
        let snapshot = dummy_snapshot(1);
        let err = build_result_set(snapshot, fake_source(vec![(0, vec![fail()])]))
            .collect_table_with_options(
                CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(1).unwrap()),
            )
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
    }

    #[tokio::test]
    async fn collect_matches_collect_table_dynamic_rows() {
        let snapshot = dummy_snapshot(2);
        let collected_table = build_result_set(
            Arc::clone(&snapshot),
            fake_source(vec![(0, vec![ok_rows(1)]), (1, vec![ok_rows(2)])]),
        )
        .collect_table()
        .await
        .unwrap();
        let collected_rows = build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(1)]), (1, vec![ok_rows(2)])]),
        )
        .collect::<Vec<_>>()
        .await
        .unwrap();

        let expected_rows = collected_table
            .dynamic_rows()
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(collected_rows.len(), expected_rows.len());
        for (actual, expected) in collected_rows.iter().zip(expected_rows.iter()) {
            assert_eq!(actual.values(), expected.values());
        }
    }

    #[tokio::test]
    async fn typed_result_set_collect_with_options_builds_plan_once() {
        reset_build_plan_calls();
        let snapshot = dummy_snapshot(2);
        let rs = build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(1)]), (1, vec![ok_rows(1)])]),
        ))
        .unwrap();

        let rows = rs
            .collect_with_options::<Vec<_>>(
                CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(1).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(rows, vec![CountingRow, CountingRow]);
        assert_eq!(build_plan_calls(), 1);
    }

    #[tokio::test]
    async fn typed_collect_failure_propagates_query_id_from_consumed_result_set() {
        let snapshot = dummy_snapshot(1);
        let err = build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![fail()])]),
        ))
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
    }

    #[tokio::test]
    async fn typed_collect_table_with_options_failure_propagates_query_id_from_consumed_result_set()
    {
        let snapshot = dummy_snapshot(1);
        let err = match build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![fail()])]),
        ))
        .unwrap()
        .collect_table_with_options(
            CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(1).unwrap()),
        )
        .await
        {
            Ok(_) => panic!("typed collect_table_with_options should fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("test"));
    }

    #[tokio::test]
    async fn typed_collect_decode_error_keeps_query_id_on_materialized_table() {
        let snapshot = dummy_snapshot(1);
        let err = build_typed_result_set::<DecodeErrorRow>(build_result_set(
            Arc::clone(&snapshot),
            fake_source(vec![(0, vec![ok_rows(1)])]),
        ))
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.as_cell_decode_error().is_some());
        assert_eq!(err.query_id(), None);

        let table = build_typed_result_set::<DecodeErrorRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(1)])]),
        ))
        .unwrap()
        .collect_table()
        .await
        .unwrap();
        assert_eq!(table.query_id(), "test");

        let err = table.rows().collect::<crate::Result<Vec<_>>>().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.as_cell_decode_error().is_some());
        assert_eq!(err.query_id(), None);
        assert_eq!(table.query_id(), "test");
    }

    #[tokio::test]
    async fn explicit_table_flow_preserves_query_id_for_schema_errors() {
        let table = build_result_set(dummy_snapshot(1), fake_source(vec![(0, vec![ok_rows(1)])]))
            .collect_table()
            .await
            .unwrap();
        assert_eq!(table.query_id(), "test");

        let err = match table.rows::<PlanErrorRow>() {
            Ok(_) => panic!("schema planning should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::MissingColumn(_))
        ));
        assert_eq!(err.query_id(), None);
        assert_eq!(table.query_id(), "test");
    }

    #[tokio::test]
    async fn typed_result_set_preflights_schema_before_fetching_remote_partitions() {
        let snapshot = dummy_snapshot(1);
        let source = FakePartitionSource::new(Default::default());
        let fetch_count = source.fetch_count();
        let source = PartitionSource::Fake(source);
        let rs = build_result_set(snapshot, source);

        let err = match build_typed_result_set::<PlanErrorRow>(rs) {
            Ok(_) => panic!("typed result set construction should fail"),
            Err(err) => err,
        };

        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::MissingColumn(_))
        ));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn typed_table_rows_reuse_typed_result_set_plan() {
        reset_build_plan_calls();
        let snapshot = dummy_snapshot(2);
        let mut result = build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(1)]), (1, vec![ok_rows(1)])]),
        ))
        .unwrap();

        let first = result.next_table().await.unwrap().unwrap();
        let second = result.next_table().await.unwrap().unwrap();

        assert_eq!(
            first.rows().collect::<crate::Result<Vec<_>>>().unwrap(),
            vec![CountingRow]
        );
        assert_eq!(
            second.rows().collect::<crate::Result<Vec<_>>>().unwrap(),
            vec![CountingRow]
        );
        assert_eq!(build_plan_calls(), 1);
    }

    #[tokio::test]
    async fn typed_result_set_dynamic_row_shares_snapshot_schema() {
        let snapshot = dummy_snapshot(1);
        let collected_table = build_typed_result_set::<DynamicRow>(build_result_set(
            Arc::clone(&snapshot),
            fake_source(vec![(0, vec![ok_rows(1)])]),
        ))
        .unwrap()
        .collect_table()
        .await
        .unwrap();
        let row = collected_table
            .rows()
            .next()
            .expect("row should be present")
            .unwrap();

        assert!(ptr::eq(collected_table.schema(), snapshot.schema.as_ref()));
        assert!(ptr::eq(row.schema(), snapshot.schema.as_ref()));
        assert!(ptr::eq(row.schema(), collected_table.schema()));
    }

    #[tokio::test]
    async fn untyped_result_set_does_not_build_a_plan() {
        reset_build_plan_calls();
        let snapshot = dummy_snapshot(1);
        let table = build_result_set(snapshot, fake_source(vec![(0, vec![ok_rows(1)])]))
            .collect_table()
            .await
            .unwrap();

        assert_eq!(table.row_count(), 1);
        assert_eq!(build_plan_calls(), 0);
    }
}
