mod cell;
mod decode;
mod dynamic;
mod plan;
mod row;
mod schema;
mod table;
mod typed_table;

pub use cell::CellRef;
pub use decode::{CellPlan, FromCell, FromRow, Json, TimePlan, TimestampPlan, UtcTimestampPlan};
pub use dynamic::{BinaryValue, CellValue, DecimalValue, DynamicRow};
pub use plan::RowPlanContext;
pub use row::{RowRef, Rows};
pub use schema::{Column, ColumnIndex, ColumnType, Schema};
pub use table::ResultTable;
pub use typed_table::TypedResultTable;

pub use crate::error::decode::{CellConversionError, CellDecodeResult};
pub(crate) use cell::RawSpan;
pub(crate) use table::ResultTableBuilder;

#[cfg(any(test, feature = "bench-internals"))]
#[doc(hidden)]
pub mod test_data {
    use std::sync::Arc;

    use super::{
        schema::{Column, ColumnType, Schema},
        table::{ResultTable, ResultTableBuilder},
    };

    pub fn make_schema(columns: Vec<(String, ColumnType, bool)>) -> Arc<Schema> {
        let cols = columns
            .into_iter()
            .enumerate()
            .map(|(i, (name, ty, nullable))| Column::new(name, i as u32, nullable, ty))
            .collect::<Vec<_>>();
        Arc::new(Schema::from_columns(cols).expect("schema build"))
    }

    #[cfg(feature = "bench-internals")]
    pub fn column_type(
        snowflake_type: &str,
        length: Option<u32>,
        precision: Option<u8>,
        scale: Option<u8>,
    ) -> ColumnType {
        ColumnType::from_driver_metadata(snowflake_type, length, precision, scale)
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
