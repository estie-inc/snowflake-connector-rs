use std::sync::Arc;

use crate::result::Schema;

/// Schema context for building a row decode plan.
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
