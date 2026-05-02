use std::{num::NonZeroUsize, sync::Arc};

use bytes::Bytes;

use crate::{
    Result,
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
    pub async fn next_table(&mut self) -> Result<Option<ResultTable>> {
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
    pub async fn collect_table(self) -> Result<ResultTable> {
        let policy = self.default_collect_policy;
        self.collect_with_policy(policy).await
    }

    pub async fn collect_table_with_options(self, options: CollectOptions) -> Result<ResultTable> {
        let policy = CollectPolicy::new(
            options
                .prefetch_concurrency
                .unwrap_or(self.default_collect_policy.prefetch_concurrency),
        );
        self.collect_with_policy(policy).await
    }

    pub async fn collect(self) -> Result<Vec<DynamicRow>> {
        let table = self.collect_table().await?;
        table.dynamic_rows()?.collect()
    }

    pub async fn collect_with_options(self, options: CollectOptions) -> Result<Vec<DynamicRow>> {
        let table = self.collect_table_with_options(options).await?;
        table.dynamic_rows()?.collect()
    }

    async fn collect_with_policy(self, policy: CollectPolicy) -> Result<ResultTable> {
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
        let mut next_remote_ordinal = cursor.next_ordinal;

        if next_remote_ordinal < total {
            if let PartitionSpec::Inline = &snapshot.partitions[next_remote_ordinal] {
                let inline = inline_rowset
                    .expect("inline_rowset must be Some when partition spec is Inline");
                let table = parse_inline_result_table_async(
                    Arc::clone(&snapshot.schema),
                    Arc::clone(&snapshot.identity.query_id),
                    inline.bytes,
                    inline.row_count_hint,
                    Some(runtime.blocking_parse_limiter()),
                )
                .await?;
                tables.push(table);
                next_remote_ordinal += 1;
            }
        }

        if next_remote_ordinal < total {
            let source = Arc::new(source);
            let blocking_parse_limiter = runtime.blocking_parse_limiter();
            let mut window = CollectWindow::new(
                next_remote_ordinal,
                total,
                policy.prefetch_concurrency.get(),
            );
            window.fill(&source, &snapshot, &blocking_parse_limiter);
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
        ResultTable::concat_same_schema(tables)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::VecDeque, ptr, sync::atomic::Ordering, time::Duration};

    use super::*;
    use crate::{
        Error, SchemaError,
        query_result::{
            TypedResultSet,
            collect::CollectPolicy,
            partition_source::tests::{BlockingFetchProbe, FakePartitionSource, FakeResponse},
            snapshot::{PartitionCursor, PartitionSpec, ResultIdentity, ResultSnapshot},
        },
        result::{Column, ColumnType, DynamicRow, FromRow, RowPlanContext, RowRef, Schema},
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

    fn ok_rows(n: usize) -> crate::Result<Vec<Vec<Option<String>>>> {
        Ok((0..n).map(|i| vec![Some(i.to_string())]).collect())
    }

    fn fail() -> crate::Result<Vec<Vec<Option<String>>>> {
        Err(crate::Error::ChunkDownload("simulated failure".into()))
    }

    type FakeRows = Vec<Vec<Option<String>>>;

    fn fake_source(responses: Vec<(usize, Vec<crate::Result<FakeRows>>)>) -> PartitionSource {
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
            Err(Error::Schema(SchemaError::MissingColumn {
                name: Box::from("MISSING"),
            }))
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

    #[tokio::test]
    async fn next_table_failure_does_not_advance_cursor() {
        let snapshot = dummy_snapshot(2);
        let source = fake_source(vec![(0, vec![fail(), ok_rows(1)]), (1, vec![ok_rows(2)])]);
        let mut rs = build_result_set(snapshot, source);
        assert!(rs.next_table().await.is_err());
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
        assert!(rs.collect_table().await.is_err());
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

        assert!(rs.next_table().await.is_err());
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
    async fn typed_result_set_collect_builds_plan_once() {
        reset_build_plan_calls();
        let snapshot = dummy_snapshot(1);
        let rs = build_typed_result_set::<CountingRow>(build_result_set(
            snapshot,
            fake_source(vec![(0, vec![ok_rows(2)])]),
        ))
        .unwrap();

        let rows = rs.collect().await.unwrap();

        assert_eq!(rows, vec![CountingRow, CountingRow]);
        assert_eq!(build_plan_calls(), 1);
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
        .collect()
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
            .collect_with_options(
                CollectOptions::default().with_prefetch_concurrency(NonZeroUsize::new(1).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(rows, vec![CountingRow, CountingRow]);
        assert_eq!(build_plan_calls(), 1);
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
            err,
            Error::Schema(SchemaError::MissingColumn { .. })
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
