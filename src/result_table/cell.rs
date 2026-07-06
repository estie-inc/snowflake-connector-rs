use std::result::Result as StdResult;

use bytes::Bytes;

use crate::{
    error::RowsetParseError,
    result_table::schema::Column,
    result_table::{CellConversionError, CellDecodeResult},
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawSpan {
    pub(crate) start: u32,
    pub(crate) len: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TextSpan {
    pub(crate) start: u32,
    pub(crate) len: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Cell {
    Null,
    RawText(RawSpan),
    DecodedText(TextSpan),
}

#[derive(Default)]
pub(crate) struct StringArenaBuilder {
    bytes: Vec<u8>,
}

impl StringArenaBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn append(&mut self, s: &str) -> StdResult<TextSpan, RowsetParseError> {
        let start = self.bytes.len();
        let len = s.len();
        self.check_capacity(len)?;
        self.bytes.extend_from_slice(s.as_bytes());

        Ok(TextSpan {
            start: start as u32,
            len: len as u32,
        })
    }

    pub(crate) fn write_with(
        &mut self,
        f: impl FnOnce(&mut Vec<u8>) -> StdResult<(), RowsetParseError>,
    ) -> StdResult<TextSpan, RowsetParseError> {
        let start = self.bytes.len();
        if let Err(err) = f(&mut self.bytes) {
            self.bytes.truncate(start);
            return Err(err);
        }

        let end = self.bytes.len();
        let len = end - start;
        if (start as u64).saturating_add(len as u64) > u32::MAX as u64 {
            self.bytes.truncate(start);
            return Err(RowsetParseError::SpanOverflow {
                limit: u32::MAX as u64,
                actual: end as u64,
                scope: "string arena",
            });
        }

        Ok(TextSpan {
            start: start as u32,
            len: len as u32,
        })
    }

    #[cfg(any(test, feature = "bench-internals"))]
    fn check_capacity(&self, additional: usize) -> StdResult<(), RowsetParseError> {
        let new_len = self
            .bytes
            .len()
            .checked_add(additional)
            .ok_or(RowsetParseError::CapacityOverflow)?;

        if new_len > u32::MAX as usize {
            return Err(RowsetParseError::SpanOverflow {
                limit: u32::MAX as u64,
                actual: new_len as u64,
                scope: "string arena",
            });
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> StringArena {
        StringArena {
            bytes: self.bytes.into_boxed_slice(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StringArena {
    bytes: Box<[u8]>,
}

impl StringArena {
    pub(crate) fn get(&self, span: TextSpan) -> &str {
        let start = span.start as usize;
        let end = start + span.len as usize;
        std::str::from_utf8(&self.bytes[start..end]).expect("string arena always holds valid UTF-8")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CellBlock {
    pub(crate) row_count: usize,
    pub(crate) column_count: usize,
    pub(crate) cells: Box<[Cell]>,
    pub(crate) wire: Bytes,
    pub(crate) decoded: StringArena,
}

impl CellBlock {
    pub(crate) fn cell(&self, row: usize, col: usize) -> Cell {
        self.cells[row * self.column_count + col]
    }

    pub(crate) fn cell_text(&self, cell: Cell) -> Option<&str> {
        match cell {
            Cell::Null => None,
            Cell::RawText(span) => {
                let start = span.start as usize;
                let end = start + span.len as usize;
                Some(
                    std::str::from_utf8(&self.wire[start..end])
                        .expect("RawText spans must point at valid UTF-8"),
                )
            }
            Cell::DecodedText(span) => Some(self.decoded.get(span)),
        }
    }
}

/// Borrowed view of a single cell.
///
/// Contains the cell's raw text (or NULL), column metadata, and row index.
/// `CellRef` is copyable and does not own the underlying result data.
#[derive(Clone, Copy, Debug)]
pub struct CellRef<'a> {
    pub(crate) row_index: usize,
    pub(crate) column: &'a Column,
    pub(crate) raw: Option<&'a str>,
}

impl<'a> CellRef<'a> {
    /// Returns `true` when the cell is SQL `NULL`.
    pub fn is_null(self) -> bool {
        self.raw.is_none()
    }

    /// Borrows the cell's raw text as Snowflake delivered it.
    ///
    /// Returns `None` for SQL `NULL`. The format is column-type specific (e.g. `"123"` for integers, an epoch fragment for timestamps);
    /// usually you'll let a [`FromCell`](crate::FromCell) implementation parse it rather than reading the raw text directly.
    pub fn raw(self) -> Option<&'a str> {
        self.raw
    }

    /// Borrows the column metadata for this cell.
    pub fn column(self) -> &'a Column {
        self.column
    }

    /// Zero-based index of the row this cell came from.
    pub fn row_index(self) -> usize {
        self.row_index
    }

    /// Returns the raw text when the cell is not SQL `NULL`.
    ///
    /// `NULL` is reported as a [`CellConversionError`](crate::decode::CellConversionError).
    pub fn required_raw(self) -> CellDecodeResult<&'a str> {
        match self.raw {
            Some(s) => Ok(s),
            None => Err(CellConversionError::builder("value is NULL").build()),
        }
    }
}
