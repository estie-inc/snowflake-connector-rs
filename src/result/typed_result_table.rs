use std::{marker::PhantomData, sync::Arc};

use crate::result::{FromRow, ResultTable, Rows, Schema};

/// Typed view over a materialized [`ResultTable`] paired with a precomputed decode plan.
pub struct TypedResultTable<T: FromRow> {
    table: ResultTable,
    plan: Arc<T::Plan>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: FromRow> TypedResultTable<T> {
    pub(crate) fn new(table: ResultTable, plan: Arc<T::Plan>) -> Self {
        Self {
            table,
            plan,
            _marker: PhantomData,
        }
    }

    pub fn query_id(&self) -> &str {
        self.table.query_id()
    }

    pub fn schema(&self) -> &Schema {
        self.table.schema()
    }

    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn rows(&self) -> Rows<'_, T> {
        Rows::from_arc_plan(&self.table, Arc::clone(&self.plan))
    }

    pub fn into_inner(self) -> ResultTable {
        self.table
    }
}
