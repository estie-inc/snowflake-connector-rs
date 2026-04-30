use std::sync::Arc;

use crate::result::Schema;

/// Shared context available while building a [`crate::FromRow`] plan.
#[derive(Clone, Copy)]
pub struct RowPlanContext<'a> {
    schema: &'a Arc<Schema>,
}

impl<'a> RowPlanContext<'a> {
    pub(crate) fn new(schema: &'a Arc<Schema>) -> Self {
        Self { schema }
    }

    pub fn schema(&self) -> &'a Schema {
        self.schema.as_ref()
    }

    pub fn clone_schema(&self) -> Arc<Schema> {
        Arc::clone(self.schema)
    }
}
