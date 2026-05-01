use std::borrow::Cow;

use bytes::Bytes;

use crate::{
    result::schema::{Column, ColumnType},
    {DecodeError, Error, ParseError, Result},
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
    pub(crate) fn append(&mut self, s: &str) -> Result<TextSpan> {
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
        f: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<TextSpan> {
        let start = self.bytes.len();
        if let Err(err) = f(&mut self.bytes) {
            self.bytes.truncate(start);
            return Err(err);
        }

        let end = self.bytes.len();
        let len = end - start;
        if (start as u64).saturating_add(len as u64) > u32::MAX as u64 {
            self.bytes.truncate(start);
            return Err(Error::Parse(ParseError::SpanOverflow {
                limit: u32::MAX as u64,
                actual: end as u64,
                scope: "string arena",
            }));
        }

        Ok(TextSpan {
            start: start as u32,
            len: len as u32,
        })
    }

    #[cfg(any(test, feature = "bench-internals"))]
    fn check_capacity(&self, additional: usize) -> Result<()> {
        let new_len = self
            .bytes
            .len()
            .checked_add(additional)
            .ok_or(Error::Parse(ParseError::CapacityOverflow))?;

        if new_len > u32::MAX as usize {
            return Err(Error::Parse(ParseError::SpanOverflow {
                limit: u32::MAX as u64,
                actual: new_len as u64,
                scope: "string arena",
            }));
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

/// Lightweight view into a single cell.
#[derive(Clone, Copy, Debug)]
pub struct CellRef<'a> {
    pub(crate) row: usize,
    pub(crate) column: &'a Column,
    pub(crate) raw: Option<&'a str>,
}

impl<'a> CellRef<'a> {
    pub fn is_null(self) -> bool {
        self.raw.is_none()
    }

    pub fn raw(self) -> Option<&'a str> {
        self.raw
    }

    pub fn column(self) -> &'a Column {
        self.column
    }

    pub fn column_type(self) -> &'a ColumnType {
        self.column.ty()
    }

    pub fn row(self) -> usize {
        self.row
    }

    /// Convert NULL into a `DecodeError`. Otherwise return the raw text.
    pub fn required_raw(self, expected: impl Into<Cow<'static, str>>) -> Result<&'a str> {
        match self.raw {
            Some(s) => Ok(s),
            None => Err(Error::Decode(DecodeError::new(
                self.row,
                self.column.index(),
                self.column.name(),
                expected,
                self.column.ty().clone(),
                None,
                "value is NULL",
            ))),
        }
    }

    /// Build a context-aware decode error for the underlying cell.
    pub fn decode_error(
        self,
        expected: impl Into<Cow<'static, str>>,
        reason: impl Into<Box<str>>,
    ) -> Error {
        Error::Decode(DecodeError::new(
            self.row,
            self.column.index(),
            self.column.name(),
            expected,
            self.column.ty().clone(),
            self.raw,
            reason,
        ))
    }
}
