mod cell;
mod decode;
mod dynamic;
mod plan;
mod result_table;
mod row;
mod schema;
mod typed_result_table;

pub use cell::CellRef;
pub use decode::{FromCell, FromRow};
pub use dynamic::{DecimalValue, DynamicRow, SnowflakeValue};
pub use plan::RowPlanContext;
pub use result_table::ResultTable;
pub use row::{RowRef, Rows};
pub use schema::{Column, ColumnIndex, ColumnType, Schema};
pub use typed_result_table::TypedResultTable;

pub use crate::error::decode::{CellDecodeIssue, CellDecodeResult};
pub(crate) use cell::RawSpan;
pub(crate) use result_table::ResultTableBuilder;

#[cfg(any(test, feature = "bench-internals"))]
#[doc(hidden)]
pub mod test_data {
    use std::sync::Arc;

    use super::{
        result_table::{ResultTable, ResultTableBuilder},
        schema::{Column, ColumnType, Schema},
    };

    pub fn make_schema(columns: Vec<(String, ColumnType, bool)>) -> Arc<Schema> {
        let cols = columns
            .into_iter()
            .enumerate()
            .map(|(i, (name, ty, nullable))| Column::new(name, i as u32, nullable, ty))
            .collect::<Vec<_>>();
        Arc::new(Schema::from_columns(cols).expect("schema build"))
    }

    pub fn make_result_table_from_rows(
        schema: Arc<Schema>,
        rows: Vec<Vec<Option<String>>>,
    ) -> crate::Result<ResultTable> {
        let mut builder =
            ResultTableBuilder::new(schema, Arc::from("test"), None, Some(rows.len()))?;

        for row in rows {
            for cell in row {
                match cell {
                    Some(s) => builder.push_owned_text(s),
                    None => builder.push_null(),
                }
            }
            builder.finish_row()?;
        }
        Ok(builder.finish()?)
    }
}

#[cfg(test)]
mod tests {
    use super::{DynamicRow, ResultTable, RowPlanContext, Rows, TypedResultTable};

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn result_row_types_are_send_sync() {
        assert_send_sync::<RowPlanContext<'static>>();
        assert_send_sync::<ResultTable>();
        assert_send_sync::<TypedResultTable<(String,)>>();
        assert_send_sync::<DynamicRow>();
        assert_send_sync::<Rows<'static, (String,)>>();
    }
}
