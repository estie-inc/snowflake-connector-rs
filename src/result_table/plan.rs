use std::sync::Arc;

use crate::result_table::{Column, Schema};

/// Context for building a row decode plan.
#[derive(Clone, Copy)]
pub struct RowPlanContext<'a> {
    schema: &'a Arc<Schema>,
}

impl<'a> RowPlanContext<'a> {
    pub(crate) fn new(schema: &'a Arc<Schema>) -> Self {
        Self { schema }
    }

    /// Borrow the schema describing the result-set columns.
    pub fn schema(&self) -> &'a Schema {
        self.schema.as_ref()
    }

    /// Returns the shared `Arc<Schema>`.
    pub fn shared_schema(&self) -> Arc<Schema> {
        Arc::clone(self.schema)
    }
}

/// Context for building a single cell decode plan.
#[derive(Clone, Copy)]
pub struct CellPlanContext<'a> {
    row_ctx: RowPlanContext<'a>,
    column: &'a Column,
}

impl<'a> CellPlanContext<'a> {
    pub(crate) fn new(row_ctx: RowPlanContext<'a>, column: &'a Column) -> Self {
        Self { row_ctx, column }
    }

    /// Borrow the column this cell plan is being built for.
    pub fn column(&self) -> &'a Column {
        self.column
    }

    /// Borrow the schema describing the result-set columns.
    pub fn schema(&self) -> &'a Schema {
        self.row_ctx.schema()
    }

    /// Returns the shared `Arc<Schema>`.
    pub fn shared_schema(&self) -> Arc<Schema> {
        self.row_ctx.shared_schema()
    }
}
