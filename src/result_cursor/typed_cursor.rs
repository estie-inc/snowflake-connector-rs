use std::{fmt, marker::PhantomData, sync::Arc};

use crate::{
    Result,
    result_table::{FromRow, Schema, TypedResultTable},
};

use super::{collect::CollectOptions, cursor::ResultCursor};

/// Typed wrapper over a [`ResultCursor`] that owns a precomputed decode plan.
pub struct TypedResultCursor<T: FromRow> {
    inner: ResultCursor,
    plan: Arc<T::Plan>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: FromRow> fmt::Debug for TypedResultCursor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // No `T: Debug` bound: surface the same shape as `ResultCursor`.
        f.debug_struct("TypedResultCursor")
            .field("query_id", &self.query_id())
            .field("is_exhausted", &self.is_exhausted())
            .finish_non_exhaustive()
    }
}

impl<T: FromRow> TypedResultCursor<T> {
    pub(crate) fn new(inner: ResultCursor, plan: T::Plan) -> Self {
        Self {
            inner,
            plan: Arc::new(plan),
            _marker: PhantomData,
        }
    }

    pub fn query_id(&self) -> &str {
        self.inner.query_id()
    }

    pub fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    pub fn is_exhausted(&self) -> bool {
        self.inner.is_exhausted()
    }

    /// Return the underlying [`ResultCursor`], discarding the decode plan.
    pub fn into_inner(self) -> ResultCursor {
        self.inner
    }

    /// Fetch the next partition as a typed table.
    ///
    /// # Cancellation safety
    ///
    /// Dropping this future before it resolves leaves the cursor unchanged;
    /// the next call to `next_table` retries the same partition.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, or `ErrorKind::Internal` if fetching or
    /// materializing the next partition fails.
    pub async fn next_table(&mut self) -> Result<Option<TypedResultTable<T>>> {
        self.inner
            .next_table()
            .await
            .map(|table| table.map(|table| TypedResultTable::new(table, Arc::clone(&self.plan))))
    }

    /// Consume this result set and decode all rows into any collection implementing [`FromIterator<T>`](FromIterator).
    ///
    /// Rows are decoded as partitions arrive, so a decode error may be returned before every partition has been fetched.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`TypedResultCursor::collect_table`].
    /// Decoding rows then propagates any error returned by [`FromRow::from_row_with_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use `ErrorKind::Decode` for per-row conversion failures; inspect the
    /// detail via [`crate::Error::as_cell_decode_error`].
    pub async fn collect<C: FromIterator<T>>(self) -> Result<C> {
        let Self { inner, plan, .. } = self;
        let policy = inner.collect_policy();
        inner.collect_typed_rows::<T, C>(policy, plan).await
    }

    /// Like [`collect`](Self::collect), but overrides the connection's default prefetch concurrency via `options`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`collect`](Self::collect).
    pub async fn collect_with_options<C: FromIterator<T>>(
        self,
        options: CollectOptions,
    ) -> Result<C> {
        let Self { inner, plan, .. } = self;
        let policy = inner.resolve_policy(&options);
        inner.collect_typed_rows::<T, C>(policy, plan).await
    }

    /// Consume this result set and collect all remaining partitions into a typed table.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
    /// partition fails to materialize.
    pub async fn collect_table(self) -> Result<TypedResultTable<T>> {
        let Self { inner, plan, .. } = self;
        let table = inner.collect_table().await?;
        Ok(TypedResultTable::new(table, plan))
    }

    /// Consume this result set and collect all remaining partitions into a typed table.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
    /// partition fails to materialize.
    pub async fn collect_table_with_options(
        self,
        options: CollectOptions,
    ) -> Result<TypedResultTable<T>> {
        let Self { inner, plan, .. } = self;
        let table = inner.collect_table_with_options(options).await?;
        Ok(TypedResultTable::new(table, plan))
    }
}
