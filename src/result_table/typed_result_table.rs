use std::{marker::PhantomData, sync::Arc};

use crate::result_table::{FromRow, ResultTable, Rows, Schema};

/// A [`ResultTable`] paired with a pre-built decode plan for row type `T`.
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

    /// Snowflake-assigned query id for this table.
    pub fn query_id(&self) -> &str {
        self.table.query_id()
    }

    /// Borrow the schema describing the result-set columns.
    pub fn schema(&self) -> &Schema {
        self.table.schema()
    }

    /// Total number of rows across this table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Returns `true` when [`row_count`](Self::row_count) is zero.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Iterate the table as typed rows of `T`.
    pub fn rows(&self) -> Rows<'_, T> {
        Rows::from_arc_plan(&self.table, Arc::clone(&self.plan))
    }

    /// Return the underlying [`ResultTable`].
    pub fn into_inner(self) -> ResultTable {
        self.table
    }
}
