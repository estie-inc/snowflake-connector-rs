use std::{fmt, marker::PhantomData, sync::Arc};

use crate::{
    Result,
    result::{FromRow, Schema, TypedResultTable},
};

use super::result_set::{CollectOptions, ResultSet};

/// Typed wrapper over a [`ResultSet`] that owns a precomputed decode plan.
pub struct TypedResultSet<T: FromRow> {
    inner: ResultSet,
    plan: Arc<T::Plan>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: FromRow> fmt::Debug for TypedResultSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // No `T: Debug` bound: surface the same shape as `ResultSet`.
        f.debug_struct("TypedResultSet")
            .field("query_id", &self.query_id())
            .field("is_exhausted", &self.is_exhausted())
            .finish_non_exhaustive()
    }
}

impl<T: FromRow> TypedResultSet<T> {
    pub(crate) fn new(inner: ResultSet, plan: T::Plan) -> Self {
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

    /// Fetch the next partition as a typed table.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if fetching or
    /// materializing the next partition fails.
    pub async fn next_table(&mut self) -> Result<Option<TypedResultTable<T>>> {
        self.inner
            .next_table()
            .await
            .map(|table| table.map(|table| TypedResultTable::new(table, Arc::clone(&self.plan))))
    }

    /// Consume this result set and decode all rows into `T`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`TypedResultSet::collect_table`]. Decoding
    /// rows then propagates any error returned by [`FromRow::from_row_with_plan`]
    /// for `T`.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` for per-row conversion failures; inspect the
    /// detail via [`crate::Error::as_cell_decode_error`].
    pub async fn collect(self) -> Result<Vec<T>> {
        let table = self.collect_table().await?;
        table.rows().collect()
    }

    /// Consume this result set and decode all rows into `T`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`TypedResultSet::collect_table_with_options`].
    /// Decoding rows then propagates any error returned by
    /// [`FromRow::from_row_with_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` for per-row conversion failures; inspect the
    /// detail via [`crate::Error::as_cell_decode_error`].
    pub async fn collect_with_options(self, options: CollectOptions) -> Result<Vec<T>> {
        let table = self.collect_table_with_options(options).await?;
        table.rows().collect()
    }

    /// Consume this result set and collect all remaining partitions into a typed table.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
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
    /// Returns `ErrorKind::Network`, `ErrorKind::Timeout`,
    /// `ErrorKind::Protocol`, or `ErrorKind::Internal` if any remaining
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
