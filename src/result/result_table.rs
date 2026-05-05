use std::sync::Arc;

use bytes::Bytes;

use crate::{
    Result,
    error::RowsetParseError,
    result::{
        DynamicRow, FromRow,
        cell::{Cell, CellBlock, RawSpan, StringArenaBuilder},
        row::Rows,
        schema::Schema,
    },
};

const CAPACITY_RESERVE_CELL_LIMIT: usize = 64 * 1024 * 1024;

/// Materialized batch of rows sharing a single `Schema` and cell storage.
#[derive(Debug)]
pub struct ResultTable {
    schema: Arc<Schema>,
    query_id: Arc<str>,
    storage: ResultTableStorage,
    row_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) enum ResultTableStorage {
    Single(Arc<CellBlock>),
    Chunks(Arc<[Arc<CellBlock>]>),
}

impl ResultTable {
    pub(crate) fn from_block(schema: Arc<Schema>, query_id: Arc<str>, block: CellBlock) -> Self {
        let row_count = block.row_count;
        Self {
            schema,
            query_id,
            storage: ResultTableStorage::Single(Arc::new(block)),
            row_count,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Create a typed row iterator for this table.
    ///
    /// # Errors
    ///
    /// Returns any error produced by [`FromRow::build_plan`] for `T` when
    /// preparing to iterate this table.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` when this table's schema does not match `T`;
    /// inspect the detail via [`crate::Error::as_schema_error`].
    pub fn rows<T: FromRow>(&self) -> Result<Rows<'_, T>> {
        Rows::new(self)
    }

    /// Create a dynamic row iterator for this table.
    ///
    /// This uses the built-in [`DynamicRow`] plan, which currently reuses the
    /// table schema directly and therefore cannot fail.
    pub fn dynamic_rows(&self) -> Result<Rows<'_, DynamicRow>> {
        self.rows::<DynamicRow>()
    }

    pub(crate) fn storage(&self) -> &ResultTableStorage {
        &self.storage
    }

    pub(crate) fn schema_arc(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Concatenate tables that share the same `Arc<Schema>`. Order is preserved.
    ///
    /// Caller must guarantee `tables` is non-empty and all tables share the same
    /// `Arc<Schema>` (i.e. they originate from the same partitioned query). These
    /// invariants are enforced via `debug_assert!` rather than runtime errors —
    /// a violation indicates a connector-internal bug, not a user-recoverable
    /// condition.
    pub(crate) fn concat_same_schema(tables: Vec<ResultTable>) -> ResultTable {
        let mut iter = tables.into_iter();
        let first = iter
            .next()
            .expect("concat_same_schema requires at least one table");
        let schema = Arc::clone(&first.schema);
        let query_id = Arc::clone(&first.query_id);
        let mut blocks = Vec::new();
        let mut row_count = 0usize;
        push_storage(&first, &mut blocks, &mut row_count);

        for t in iter {
            debug_assert!(
                Arc::ptr_eq(&t.schema, &schema),
                "concat_same_schema requires all tables to share the same Arc<Schema>"
            );
            debug_assert_eq!(
                t.query_id.as_ref(),
                query_id.as_ref(),
                "concat_same_schema is expected to join tables from the same query result"
            );
            push_storage(&t, &mut blocks, &mut row_count);
        }

        let storage = if blocks.len() == 1 {
            ResultTableStorage::Single(blocks.pop().unwrap())
        } else {
            ResultTableStorage::Chunks(blocks.into())
        };

        ResultTable {
            schema,
            query_id,
            storage,
            row_count,
        }
    }

    pub(crate) fn empty(schema: Arc<Schema>, query_id: Arc<str>) -> Self {
        let column_count = schema.len();
        let block = CellBlock {
            row_count: 0,
            column_count,
            cells: Box::new([]),
            wire: Bytes::new(),
            decoded: StringArenaBuilder::new().finish(),
        };
        Self::from_block(schema, query_id, block)
    }
}

fn push_storage(t: &ResultTable, blocks: &mut Vec<Arc<CellBlock>>, row_count: &mut usize) {
    match &t.storage {
        ResultTableStorage::Single(b) => {
            *row_count += b.row_count;
            blocks.push(Arc::clone(b));
        }
        ResultTableStorage::Chunks(bs) => {
            for b in bs.iter() {
                *row_count += b.row_count;
                blocks.push(Arc::clone(b));
            }
        }
    }
}

/// Builder used by parsers and adapters to assemble a `ResultTable`.
pub(crate) struct ResultTableBuilder {
    schema: Arc<Schema>,
    query_id: Arc<str>,
    cells: Vec<Cell>,
    decoded: StringArenaBuilder,
    wire: Bytes,
    row_count: usize,
    current_row_cells: usize,
    column_count: usize,
}

impl ResultTableBuilder {
    pub(crate) fn new(
        schema: Arc<Schema>,
        query_id: Arc<str>,
        wire: Option<Bytes>,
        row_count_hint: Option<usize>,
    ) -> Result<Self> {
        let column_count = schema.len();
        let cap = match row_count_hint {
            Some(rows) => column_count
                .checked_mul(rows)
                .ok_or(RowsetParseError::CapacityOverflow)?
                .min(CAPACITY_RESERVE_CELL_LIMIT),
            None => 0,
        };

        Ok(Self {
            schema,
            query_id,
            cells: Vec::with_capacity(cap),
            decoded: StringArenaBuilder::new(),
            wire: wire.unwrap_or_default(),
            row_count: 0,
            current_row_cells: 0,
            column_count,
        })
    }

    pub(crate) fn push_null(&mut self) {
        self.cells.push(Cell::Null);
        self.current_row_cells += 1;
    }

    pub(crate) fn push_raw_text(&mut self, span: RawSpan) {
        self.cells.push(Cell::RawText(span));
        self.current_row_cells += 1;
    }

    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn push_owned_text(&mut self, s: String) {
        let span = self.decoded.append(&s).expect("decoded arena overflow");
        self.cells.push(Cell::DecodedText(span));
        self.current_row_cells += 1;
    }

    pub(crate) fn push_decoded_with(
        &mut self,
        f: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let span = self.decoded.write_with(f)?;
        self.cells.push(Cell::DecodedText(span));
        self.current_row_cells += 1;
        Ok(())
    }

    pub(crate) fn finish_row(&mut self) -> Result<()> {
        if self.current_row_cells != self.column_count {
            return Err(RowsetParseError::RowLengthMismatch {
                row: self.row_count,
                expected: self.column_count,
                actual: self.current_row_cells,
            }
            .into());
        }

        self.row_count += 1;
        self.current_row_cells = 0;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ResultTable> {
        let ResultTableBuilder {
            schema,
            query_id,
            cells,
            decoded,
            wire,
            row_count,
            column_count,
            current_row_cells,
        } = self;
        if current_row_cells != 0 {
            return Err(RowsetParseError::RowLengthMismatch {
                row: row_count,
                expected: column_count,
                actual: current_row_cells,
            }
            .into());
        }

        let expected = row_count
            .checked_mul(column_count)
            .ok_or(RowsetParseError::CapacityOverflow)?;
        if expected != cells.len() {
            return Err(RowsetParseError::RowLengthMismatch {
                row: row_count,
                expected: column_count,
                actual: cells.len() % column_count.max(1),
            }
            .into());
        }

        let block = CellBlock {
            row_count,
            column_count,
            cells: cells.into_boxed_slice(),
            wire,
            decoded: decoded.finish(),
        };
        Ok(ResultTable::from_block(schema, query_id, block))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::schema::{Column, ColumnType};

    fn dummy_schema(n: usize) -> Arc<Schema> {
        let cols = (0..n)
            .map(|i| {
                Column::new(
                    format!("C{i}"),
                    i as u32,
                    true,
                    ColumnType::Text { length: None },
                )
            })
            .collect();
        Arc::new(Schema::from_columns(cols).unwrap())
    }

    #[test]
    fn builder_finish_row_validates_column_count() {
        let schema = dummy_schema(2);
        let mut b = ResultTableBuilder::new(schema, Arc::from("test"), None, Some(2)).unwrap();
        b.push_owned_text("a".into());
        let err = b.finish_row().unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::RowLengthMismatch { .. })
        ));
    }

    #[test]
    fn builder_assembles_table() {
        let schema = dummy_schema(2);
        let mut b = ResultTableBuilder::new(schema, Arc::from("test"), None, Some(2)).unwrap();
        b.push_owned_text("a".into());
        b.push_null();
        b.finish_row().unwrap();
        b.push_owned_text("b".into());
        b.push_owned_text("c".into());
        b.finish_row().unwrap();
        let t = b.finish().unwrap();
        assert_eq!(t.row_count(), 2);
        assert!(!t.is_empty());
    }

    #[test]
    fn builder_rejects_overflowing_capacity_hint() {
        let schema = dummy_schema(2);
        match ResultTableBuilder::new(schema, Arc::from("test"), None, Some(usize::MAX)) {
            Err(err)
                if matches!(
                    err.as_rowset_parse_error(),
                    Some(RowsetParseError::CapacityOverflow)
                ) => {}
            Err(err) => panic!("expected CapacityOverflow, got {err:?}"),
            Ok(_) => panic!("expected CapacityOverflow"),
        }
    }
}
