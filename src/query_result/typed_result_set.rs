use std::{marker::PhantomData, sync::Arc};

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

    pub async fn next_table(&mut self) -> Result<Option<TypedResultTable<T>>> {
        self.inner
            .next_table()
            .await
            .map(|table| table.map(|table| TypedResultTable::new(table, Arc::clone(&self.plan))))
    }

    pub async fn collect(self) -> Result<Vec<T>> {
        let table = self.collect_table().await?;
        table.rows().collect()
    }

    pub async fn collect_with_options(self, options: CollectOptions) -> Result<Vec<T>> {
        let table = self.collect_table_with_options(options).await?;
        table.rows().collect()
    }

    pub async fn collect_table(self) -> Result<TypedResultTable<T>> {
        let Self { inner, plan, .. } = self;
        let table = inner.collect_table().await?;
        Ok(TypedResultTable::new(table, plan))
    }

    pub async fn collect_table_with_options(
        self,
        options: CollectOptions,
    ) -> Result<TypedResultTable<T>> {
        let Self { inner, plan, .. } = self;
        let table = inner.collect_table_with_options(options).await?;
        Ok(TypedResultTable::new(table, plan))
    }
}
