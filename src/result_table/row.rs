use std::{any::type_name, marker::PhantomData, sync::Arc};

use crate::{
    Error, Result,
    error::{CellDecodeError, InvalidColumnIndexError, SchemaError},
    result_table::{
        cell::{CellBlock, CellRef},
        decode::{CellPlan, FromCell, FromRow},
        plan::RowPlanContext,
        schema::{Column, ColumnIndex, Schema},
        table::{ResultTable, ResultTableStorage},
    },
};

/// Borrowed view of a single row.
///
/// `RowRef` is copyable and does not own the underlying result data.
#[derive(Clone, Copy)]
pub struct RowRef<'a> {
    table: &'a ResultTable,
    block: &'a CellBlock,
    global_row: usize,
    local_row: usize,
}

impl<'a> RowRef<'a> {
    /// Zero-based index of this row within the result set.
    pub fn row_index(self) -> usize {
        self.global_row
    }

    /// Borrow a cell by resolved column index.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::Decode`](crate::ErrorKind::Decode) when `index` is out of bounds for this row's schema.
    pub fn cell_at(self, index: ColumnIndex) -> Result<CellRef<'a>> {
        let column = self.table.schema().column_at(index).ok_or_else(|| {
            Error::from(SchemaError::InvalidColumnIndex(
                InvalidColumnIndexError::new(index, self.table.schema().len()),
            ))
        })?;

        let cell = self.block.cell(self.local_row, index.as_usize());
        let raw = self.block.cell_text(cell);

        Ok(CellRef {
            row_index: self.global_row,
            column,
            raw,
        })
    }

    // A fast path for DynamicRow.
    // DynamicRow has columns in the correct order, so it can skip column lookups in `cell_at()`.
    pub(crate) fn cell_at_offset(self, column: &'a Column, offset: usize) -> CellRef<'a> {
        debug_assert_eq!(column.index().as_usize(), offset);

        let cell = self.block.cell(self.local_row, offset);
        let raw = self.block.cell_text(cell);

        CellRef {
            row_index: self.global_row,
            column,
            raw,
        }
    }

    /// Decode a cell using a prepared [`CellPlan`].
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::Decode`](crate::ErrorKind::Decode) when the cell cannot be decoded as `T`.
    pub fn get_with_plan<T: FromCell>(self, plan: &CellPlan<T>) -> Result<T> {
        let cell = self.block.cell(self.local_row, plan.offset);
        let raw = self.block.cell_text(cell);

        T::from_cell_with_plan(raw, &plan.decode_plan).map_err(|issue| {
            CellDecodeError::new(
                self.global_row,
                plan.column.index(),
                plan.column.name(),
                type_name::<T>(),
                plan.column.ty().clone(),
                raw,
                issue,
            )
            .into()
        })
    }

    /// Borrows the schema describing the result-set columns.
    pub fn schema(self) -> &'a Schema {
        self.table.schema()
    }
}

/// Typed iterator over result rows.
///
/// Iteration is non-allocating beyond what `T`'s decode requires: the `Arc`-shared schema and decode plan are reused across rows.
pub struct Rows<'a, T: FromRow> {
    table: &'a ResultTable,
    plan: Arc<T::Plan>,
    next: usize,
    chunk_index: usize,
    chunk_local: usize,
    _phantom: PhantomData<fn() -> T>,
}

impl<'a, T: FromRow> Rows<'a, T> {
    pub(crate) fn new(table: &'a ResultTable) -> Result<Self> {
        let plan = Arc::new(T::build_plan(RowPlanContext::new(table.shared_schema()))?);
        Ok(Self::from_arc_plan(table, plan))
    }

    /// Construct from a shared pre-built plan.
    pub(crate) fn from_arc_plan(table: &'a ResultTable, plan: Arc<T::Plan>) -> Self {
        Self {
            table,
            plan,
            next: 0,
            chunk_index: 0,
            chunk_local: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: FromRow> Iterator for Rows<'a, T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.table.row_count() {
            return None;
        }

        let block = match self.table.storage() {
            ResultTableStorage::Single(b) => b.as_ref(),
            ResultTableStorage::Chunks(blocks) => loop {
                let b = blocks.get(self.chunk_index)?.as_ref();
                if self.chunk_local >= b.row_count {
                    self.chunk_index += 1;
                    self.chunk_local = 0;
                    continue;
                }
                break b;
            },
        };

        let row_ref = RowRef {
            table: self.table,
            block,
            global_row: self.next,
            local_row: match self.table.storage() {
                ResultTableStorage::Single(_) => self.next,
                ResultTableStorage::Chunks(_) => self.chunk_local,
            },
        };
        self.next += 1;
        if matches!(self.table.storage(), ResultTableStorage::Chunks(_)) {
            self.chunk_local += 1;
        }

        Some(T::from_row_with_plan(row_ref, self.plan.as_ref()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.table.row_count().saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}

impl<T: FromRow> ExactSizeIterator for Rows<'_, T> {}

impl<T: FromRow> std::iter::FusedIterator for Rows<'_, T> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::result_table::{
        ColumnType,
        table::{ResultTable, ResultTableStorage},
        test_data::{make_result_table_from_rows, make_schema},
    };

    #[test]
    fn rows_len_tracks_remaining_items_for_single_and_chunked_tables() {
        let schema = make_schema(vec![(
            "X".to_string(),
            ColumnType::Text { length: None },
            true,
        )]);

        let single = make_result_table_from_rows(
            Arc::clone(&schema),
            ["a", "b", "c"]
                .into_iter()
                .map(|value| vec![Some(value.to_string())])
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let first = make_result_table_from_rows(
            Arc::clone(&schema),
            ["a", "b"]
                .into_iter()
                .map(|value| vec![Some(value.to_string())])
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let second = make_result_table_from_rows(
            schema,
            ["c"]
                .into_iter()
                .map(|value| vec![Some(value.to_string())])
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let chunked = ResultTable::concat_same_schema(vec![first, second]);

        for table in [single, chunked] {
            let mut rows = table.rows::<(String,)>().unwrap();
            assert_eq!(rows.len(), 3);
            rows.next().unwrap().unwrap();
            assert_eq!(rows.len(), 2);
            rows.next().unwrap().unwrap();
            assert_eq!(rows.len(), 1);
        }
    }

    #[test]
    fn cell_at_offset_matches_checked_cell_path() {
        let schema = make_schema(vec![
            (
                "ID".to_string(),
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
                false,
            ),
            ("NAME".to_string(), ColumnType::Text { length: None }, true),
        ]);
        let table = make_result_table_from_rows(
            schema,
            vec![vec![Some("1".to_string()), Some("alice".to_string())]],
        )
        .unwrap();

        let block = match table.storage() {
            ResultTableStorage::Single(block) => block.as_ref(),
            ResultTableStorage::Chunks(_) => panic!("expected single block"),
        };
        let row = RowRef {
            table: &table,
            block,
            global_row: 0,
            local_row: 0,
        };

        for (offset, column) in table.schema().columns().iter().enumerate() {
            let direct = row.cell_at_offset(column, offset);
            let checked = row.cell_at(column.index()).unwrap();
            assert_eq!(direct.raw(), checked.raw());
            assert_eq!(direct.column().name(), checked.column().name());
            assert_eq!(direct.row_index(), checked.row_index());
        }
    }
}
