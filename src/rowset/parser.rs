//! Snowflake response body / chunk parser.
//!
//! Implements a span-based JSON scanner that converts Snowflake rowset and
//! chunk bytes directly into a [`ResultTable`] without deserializing into
//! `Vec<Vec<Option<String>>>`. Strings without escapes are kept as byte
//! spans into the original `Bytes` buffer; strings with escapes are
//! unescaped once into a per-table arena.

use std::sync::Arc;

use bytes::Bytes;

use super::{
    ParseWorkload, decode_gzip_chunk,
    json_string::{self, JsonStringFragment, JsonStringScanError},
    workload::{ParseWorkError, execute_parse_work},
};
use crate::{
    error::{
        InternalError, QueryScopedError, QueryScopedRepr, QueryScopedResult, RowsetParseError,
    },
    result_table::{RawSpan, ResultTable, ResultTableBuilder, Schema},
    runtime::BlockingParseLimiter,
};

#[derive(Clone, Copy, Debug)]
enum RowsetShape {
    InlineArray,
    ChunkFragmentSequence,
}

type ParseResult<T> = std::result::Result<T, RowsetParseError>;

fn parse_table_with_shape(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
    shape: RowsetShape,
    row_count_hint: Option<usize>,
) -> ParseResult<ResultTable> {
    let mut builder =
        ResultTableBuilder::new(schema, query_id, Some(body.clone()), row_count_hint)?;
    let mut scanner = JsonScanner::new(&body);

    scanner.skip_ws();
    match shape {
        RowsetShape::ChunkFragmentSequence => {
            // First row may or may not exist (empty chunk).
            scanner.skip_ws();
            if scanner.is_eof() {
                return if body.is_empty() {
                    builder.finish()
                } else {
                    Err(scanner.err_unexpected("'['"))
                };
            }
            scanner.parse_row(&mut builder)?;
            loop {
                scanner.skip_ws();
                if scanner.is_eof() {
                    break;
                }
                scanner.expect(b',')?;
                scanner.skip_ws();
                scanner.parse_row(&mut builder)?;
            }
        }
        RowsetShape::InlineArray => {
            scanner.expect(b'[')?;
            scanner.skip_ws();
            if scanner.peek() == Some(b']') {
                scanner.advance();
            } else {
                scanner.parse_row(&mut builder)?;
                loop {
                    scanner.skip_ws();
                    match scanner.peek() {
                        Some(b',') => {
                            scanner.advance();
                            scanner.skip_ws();
                            scanner.parse_row(&mut builder)?;
                        }
                        Some(b']') => {
                            scanner.advance();
                            break;
                        }
                        _ => {
                            return Err(scanner.err_unexpected("',' or ']'"));
                        }
                    }
                }
            }
            scanner.skip_ws();
            if !scanner.is_eof() {
                return Err(scanner.err_unexpected("end of input"));
            }
        }
    }

    builder.finish()
}

fn checked_row_count_hint(row_count: Option<u64>) -> ParseResult<Option<usize>> {
    row_count
        .map(|rows| usize::try_from(rows).map_err(|_| RowsetParseError::CapacityOverflow))
        .transpose()
}

/// Determine whether an extracted inline rowset array contains any rows.
///
/// This treats JSON-insignificant whitespace around `[` / `]` as equivalent,
/// so `[ ]` and `[]` are both empty arrays.
pub(crate) fn inline_rowset_has_rows_inner(body: &[u8]) -> ParseResult<bool> {
    let mut scanner = JsonScanner::new(body);
    scanner.skip_ws();
    scanner.expect(b'[')?;
    scanner.skip_ws();

    if scanner.peek() == Some(b']') {
        scanner.advance();
        scanner.skip_ws();
        if !scanner.is_eof() {
            return Err(scanner.err_unexpected("end of input"));
        }
        return Ok(false);
    }

    if scanner.is_eof() {
        return Err(scanner.err_unexpected("row or ']'"));
    }

    Ok(true)
}

#[cfg(any(test, feature = "bench-internals"))]
pub(crate) fn inline_rows_to_result_table_inner(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    rows: Vec<Vec<Option<String>>>,
) -> ParseResult<ResultTable> {
    let mut builder = ResultTableBuilder::new(schema, query_id, None, Some(rows.len()))?;
    for row in rows {
        for cell in row {
            match cell {
                Some(s) => builder.push_owned_text(s),
                None => builder.push_null(),
            }
        }
        builder.finish_row()?;
    }
    builder.finish()
}

struct JsonScanner<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> JsonScanner<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: utf8_bom_prefix_len(bytes),
        }
    }

    fn skip_ws(&mut self) {
        while let Some(&b) = self.bytes.get(self.offset) {
            if matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
                self.offset += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.offset += 1;
        Some(b)
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    fn expect(&mut self, b: u8) -> ParseResult<()> {
        match self.peek() {
            Some(c) if c == b => {
                self.advance();
                Ok(())
            }
            _ => Err(self.err_unexpected(byte_label(b))),
        }
    }

    fn err_unexpected(&self, expected: &'static str) -> RowsetParseError {
        RowsetParseError::UnexpectedToken {
            offset: self.offset,
            expected,
        }
    }

    fn err_string(&self, reason: impl Into<Box<str>>) -> RowsetParseError {
        RowsetParseError::InvalidString {
            offset: self.offset,
            reason: reason.into(),
        }
    }

    /// Parse a row: `[` cell (`,` cell)* `]`.
    fn parse_row(&mut self, builder: &mut ResultTableBuilder) -> ParseResult<()> {
        self.expect(b'[')?;
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.advance();
            builder.finish_row()?;
            return Ok(());
        }
        self.parse_cell(builder)?;
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.advance();
                    self.skip_ws();
                    self.parse_cell(builder)?;
                }
                Some(b']') => {
                    self.advance();
                    break;
                }
                _ => return Err(self.err_unexpected("',' or ']'")),
            }
        }
        builder.finish_row()
    }

    /// Parse a single JSON value as a cell. Snowflake's driver-API rowset
    /// emits each cell as either `null` or a JSON string.
    fn parse_cell(&mut self, builder: &mut ResultTableBuilder) -> ParseResult<()> {
        match self.peek() {
            Some(b'n') => {
                self.expect_keyword(b"null")?;
                builder.push_null();
                Ok(())
            }
            Some(b'"') => self.parse_string_into_cell(builder),
            _ => Err(self.err_unexpected("string or null")),
        }
    }

    fn expect_keyword(&mut self, kw: &[u8]) -> ParseResult<()> {
        let start = self.offset;
        if self.bytes.len() - self.offset < kw.len() {
            return Err(self.err_unexpected(keyword_label(kw)));
        }
        if &self.bytes[start..start + kw.len()] != kw {
            return Err(self.err_unexpected(keyword_label(kw)));
        }
        self.offset += kw.len();
        Ok(())
    }

    /// Parse a JSON string. Records a `RawSpan` if no escapes; otherwise
    /// unescapes once into the builder's arena.
    fn parse_string_into_cell(&mut self, builder: &mut ResultTableBuilder) -> ParseResult<()> {
        debug_assert_eq!(self.peek(), Some(b'"'));
        self.advance(); // consume "
        let content_start = self.offset;
        let mut saw_non_ascii = false;
        // Fast path: scan until escape, control char, or closing quote.
        loop {
            let Some(b) = self.peek() else {
                return Err(self.err_string("unterminated string"));
            };
            match b {
                b'"' => {
                    let len = self.offset - content_start;
                    let slice = &self.bytes[content_start..content_start + len];
                    if saw_non_ascii {
                        validate_utf8(slice, content_start)?;
                    }
                    if (content_start as u64).saturating_add(len as u64) > u32::MAX as u64 {
                        return Err(RowsetParseError::SpanOverflow {
                            limit: u32::MAX as u64,
                            actual: (content_start + len) as u64,
                            scope: "wire span",
                        });
                    }
                    builder.push_raw_text(RawSpan {
                        start: content_start as u32,
                        len: len as u32,
                    });
                    self.advance(); // consume closing "
                    return Ok(());
                }
                b'\\' => {
                    // Switch to slow path: copy prefix and decode escapes.
                    return self.parse_string_with_escapes(content_start, saw_non_ascii, builder);
                }
                0x00..=0x1f => {
                    return Err(self.err_string("control character in string"));
                }
                _ => {
                    saw_non_ascii |= b & 0x80 != 0;
                    self.offset += 1;
                }
            }
        }
    }

    /// Slow path: at least one escape was seen in the current string.
    fn parse_string_with_escapes(
        &mut self,
        prefix_start: usize,
        prefix_has_non_ascii: bool,
        builder: &mut ResultTableBuilder,
    ) -> ParseResult<()> {
        let prefix = &self.bytes[prefix_start..self.offset];
        if prefix_has_non_ascii {
            validate_utf8(prefix, prefix_start)?;
        }

        let bytes = self.bytes;
        // We can't borrow self.bytes mutably; capture an outer offset and
        // update it after the closure runs.
        let mut local_offset = self.offset;
        let result = builder.push_decoded_with(|buf: &mut Vec<u8>| {
            buf.extend_from_slice(prefix);
            json_string::scan_json_string_body(
                bytes,
                &mut local_offset,
                |fragment| -> ParseResult<()> {
                    match fragment {
                        JsonStringFragment::Raw {
                            bytes,
                            start_offset,
                        } => {
                            if !bytes.is_ascii() {
                                validate_utf8(bytes, start_offset)?;
                            }
                            buf.extend_from_slice(bytes);
                        }
                        JsonStringFragment::DecodedByte(byte) => buf.push(byte),
                        JsonStringFragment::Codepoint(codepoint) => {
                            json_string::push_utf8(buf, codepoint);
                        }
                    }
                    Ok(())
                },
            )
            .map_err(map_json_string_scan_error)
        });
        self.offset = local_offset;
        result
    }
}

fn utf8_bom_prefix_len(bytes: &[u8]) -> usize {
    if bytes.starts_with(b"\xEF\xBB\xBF") {
        3
    } else {
        0
    }
}

fn map_json_string_scan_error(err: JsonStringScanError<RowsetParseError>) -> RowsetParseError {
    match err {
        JsonStringScanError::UnterminatedString { offset } => RowsetParseError::InvalidString {
            offset,
            reason: Box::from("unterminated string"),
        },
        JsonStringScanError::TrailingBackslash { offset } => RowsetParseError::InvalidString {
            offset,
            reason: Box::from("trailing backslash"),
        },
        JsonStringScanError::UnknownEscape { offset, escape } => RowsetParseError::InvalidString {
            offset,
            reason: format!("unknown escape: \\{}", escape as char).into_boxed_str(),
        },
        JsonStringScanError::ControlCharacter { offset } => RowsetParseError::InvalidString {
            offset,
            reason: Box::from("control character in string"),
        },
        JsonStringScanError::InvalidUnicodeEscape { offset }
        | JsonStringScanError::InvalidUnicodeSurrogatePair { offset } => {
            RowsetParseError::InvalidUnicodeEscape { offset }
        }
        JsonStringScanError::Visitor(err) => err,
    }
}

fn validate_utf8(bytes: &[u8], start_offset: usize) -> ParseResult<()> {
    std::str::from_utf8(bytes).map_err(|err| RowsetParseError::InvalidString {
        offset: start_offset + err.valid_up_to(),
        reason: Box::from("invalid UTF-8 in string"),
    })?;
    Ok(())
}

fn byte_label(b: u8) -> &'static str {
    match b {
        b'[' => "'['",
        b']' => "']'",
        b'{' => "'{'",
        b'}' => "'}'",
        b',' => "','",
        b'"' => "'\"'",
        b':' => "':'",
        _ => "expected token",
    }
}

fn keyword_label(kw: &[u8]) -> &'static str {
    match kw {
        b"null" => "null",
        b"true" => "true",
        b"false" => "false",
        _ => "keyword",
    }
}

#[cfg(any(test, feature = "bench-internals"))]
pub(crate) fn parse_inline_result_table(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
) -> crate::Result<ResultTable> {
    parse_table_with_shape(schema, query_id, body, RowsetShape::InlineArray, None)
        .map_err(crate::Error::from)
}

/// Parse the inline rowset bytes (already extracted from `data.rowset`)
/// using the span scanner. Bytes must be a `[..]` array.
pub(crate) async fn parse_inline_result_table_async(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
    row_count: Option<u64>,
    blocking_parse_limiter: Option<BlockingParseLimiter>,
) -> QueryScopedResult<ResultTable> {
    let workload = ParseWorkload::inline_rowset(body.len(), row_count, schema.len());
    let row_count_hint = match checked_row_count_hint(row_count) {
        Ok(hint) => hint,
        Err(err) => {
            return Err(QueryScopedError::new(query_id, err));
        }
    };
    let query_id_for_work = Arc::clone(&query_id);

    let parse_work_result = execute_parse_work(workload, blocking_parse_limiter, move || {
        parse_table_with_shape(
            schema,
            query_id_for_work,
            body,
            RowsetShape::InlineArray,
            row_count_hint,
        )
    })
    .await;

    let table = match parse_work_result {
        Ok((_, table)) => table,
        Err(ParseWorkError::Join(error)) => {
            return Err(QueryScopedError::new(
                query_id,
                InternalError::future_join(error),
            ));
        }
        Err(ParseWorkError::Work(error)) => return Err(QueryScopedError::new(query_id, error)),
    };

    Ok(table)
}

#[cfg(any(test, feature = "bench-internals"))]
pub(crate) fn parse_remote_chunk_result_table(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
) -> crate::Result<ResultTable> {
    parse_table_with_shape(
        schema,
        query_id,
        body,
        RowsetShape::ChunkFragmentSequence,
        None,
    )
    .map_err(crate::Error::from)
}

pub(crate) async fn parse_remote_chunk_result_table_async(
    schema: Arc<Schema>,
    query_id: Arc<str>,
    body: Bytes,
    workload: ParseWorkload,
    blocking_parse_limiter: Option<BlockingParseLimiter>,
) -> QueryScopedResult<ResultTable> {
    let row_count_hint = match checked_row_count_hint(workload.row_count) {
        Ok(hint) => hint,
        Err(err) => {
            return Err(QueryScopedError::new(query_id, err));
        }
    };
    let query_id_for_work = Arc::clone(&query_id);

    let parse_work_result = execute_parse_work(workload, blocking_parse_limiter, move || {
        let bytes = decode_gzip_chunk(body).map_err(QueryScopedRepr::from)?;
        parse_table_with_shape(
            schema,
            query_id_for_work,
            bytes,
            RowsetShape::ChunkFragmentSequence,
            row_count_hint,
        )
        .map_err(QueryScopedRepr::from)
    })
    .await;

    let table = match parse_work_result {
        Ok((_, table)) => table,
        Err(ParseWorkError::Join(error)) => {
            return Err(QueryScopedError::new(
                query_id,
                InternalError::future_join(error),
            ));
        }
        Err(ParseWorkError::Work(error)) => return Err(QueryScopedError::new(query_id, error)),
    };

    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;
    use crate::{
        result_table::{ColumnType, test_data::make_schema},
        rowset::workload::{
            BLOCKING_GZIP_COMPRESSED_BYTES, BLOCKING_PARSE_BYTES, BLOCKING_PARSE_CELLS,
            ParseExecution, should_spawn_blocking_parse,
        },
    };

    fn inline_rowset_has_rows(body: &[u8]) -> crate::Result<bool> {
        inline_rowset_has_rows_inner(body).map_err(crate::Error::from)
    }

    fn schema(n: usize) -> Arc<Schema> {
        let cols = (0..n)
            .map(|i| (format!("C{i}"), ColumnType::Text { length: None }, true))
            .collect();
        make_schema(cols)
    }

    fn qid() -> Arc<str> {
        Arc::from("test")
    }

    #[derive(Default)]
    struct BlockingParseProbe {
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl BlockingParseProbe {
        fn enter(&self) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let mut observed = self.max_active.load(Ordering::SeqCst);
            while active > observed {
                match self.max_active.compare_exchange(
                    observed,
                    active,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(current) => observed = current,
                }
            }
        }

        fn exit(&self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }

        fn max_active(&self) -> usize {
            self.max_active.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn parse_inline_result_table_accepts_top_level_array() {
        let body = Bytes::from(r#"[["1","alice"],["2",null]]"#);
        let s = schema(2);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 2);
        let rs = t
            .rows::<(String, Option<String>)>()
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        let r0 = &rs[0];
        let r1 = &rs[1];
        assert_eq!(r0.0, "1");
        assert_eq!(r0.1.as_deref(), Some("alice"));
        assert_eq!(r1.0, "2");
        assert_eq!(r1.1, None);
    }

    #[test]
    fn parse_remote_chunk_fragment_table_accepts_row_sequence() {
        let body = Bytes::from(r#"["a","b"],["c","d"]"#);
        let s = schema(2);
        let t = parse_remote_chunk_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 2);
    }

    #[tokio::test]
    async fn parse_remote_chunk_fragment_table_async_accepts_row_sequence() {
        let body = Bytes::from(r#"["a","b"],["c","d"]"#);
        let s = schema(2);
        let workload = ParseWorkload::inline_rowset(body.len(), None, s.len());
        let t = parse_remote_chunk_result_table_async(s, qid(), body, workload, None)
            .await
            .unwrap();
        assert_eq!(t.row_count(), 2);
    }

    #[test]
    fn parse_inline_result_table_accepts_empty_array() {
        let body = Bytes::from("[]");
        let s = schema(2);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 0);
    }

    #[test]
    fn parse_inline_result_table_allows_trailing_whitespace() {
        let body = Bytes::from("[[\"x\"]]\n \t");
        let s = schema(1);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 1);
    }

    #[test]
    fn parse_inline_result_table_allows_leading_utf8_bom() {
        let body = Bytes::from_static(b"\xEF\xBB\xBF[[\"x\"]]");
        let s = schema(1);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 1);
    }

    #[test]
    fn parse_inline_result_table_rejects_trailing_garbage() {
        let body = Bytes::from("[[\"x\"]] trailing");
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::UnexpectedToken { expected, .. }) if *expected == "end of input"
        ));
    }

    #[test]
    fn parse_escapes_into_arena() {
        let body = Bytes::from(r#"[["hi","line\nbreak"]]"#);
        let s = schema(2);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        let r0 = t
            .rows::<(String, String)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(r0.0, "hi");
        assert_eq!(r0.1, "line\nbreak");
    }

    #[test]
    fn parse_raw_utf8_without_escapes() {
        let body = Bytes::from_static(b"[[\"\xe3\x81\x82\"]]");
        let s = schema(1);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        let r0 = t.rows::<(String,)>().unwrap().next().unwrap().unwrap();
        assert_eq!(r0.0, "あ");
    }

    #[test]
    fn parse_escapes_with_valid_utf8_suffix() {
        let body = Bytes::from_static(b"[[\"line\\n\xe3\x81\x82\"]]");
        let s = schema(1);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        let r0 = t.rows::<(String,)>().unwrap().next().unwrap().unwrap();
        assert_eq!(r0.0, "line\nあ");
    }

    #[test]
    fn parse_unicode_surrogate_pair() {
        let body = Bytes::from(r#"[["\uD83D\uDE80"]]"#);
        let s = schema(1);
        let t = parse_inline_result_table(s, qid(), body).unwrap();
        let r0 = t.rows::<(String,)>().unwrap().next().unwrap().unwrap();
        assert_eq!(r0.0, "🚀");
    }

    #[test]
    fn parse_rejects_control_char() {
        let body = Bytes::from("[[\"bad\x01\"]]");
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::InvalidString { .. })
        ));
    }

    #[test]
    fn parse_rejects_invalid_raw_utf8() {
        let body = Bytes::from_static(b"[[\"\xff\"]]");
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::InvalidString { .. })
        ));
    }

    #[test]
    fn parse_rejects_invalid_utf8_after_escape() {
        let body = Bytes::from_static(b"[[\"line\\n\xff\"]]");
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::InvalidString { .. })
        ));
    }

    #[test]
    fn parse_row_length_mismatch() {
        // 2-column schema, but row has 1 cell.
        let body = Bytes::from(r#"[["only"]]"#);
        let s = schema(2);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::RowLengthMismatch { .. })
        ));
    }

    #[test]
    fn parse_unterminated_string() {
        let body = Bytes::from(r#"[["no end"#);
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::InvalidString { .. })
        ));
    }

    #[test]
    fn parse_remote_chunk_fragment_table_allows_leading_utf8_bom() {
        let body = Bytes::from_static(b"\xEF\xBB\xBF[\"a\"],[\"b\"]");
        let s = schema(1);
        let t = parse_remote_chunk_result_table(s, qid(), body).unwrap();
        assert_eq!(t.row_count(), 2);
    }

    #[test]
    fn parse_inline_result_table_rejects_remote_chunk_row_sequence() {
        let body = Bytes::from(r#"["1"],["2"]"#);
        let s = schema(1);
        let err = parse_inline_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn parse_remote_chunk_fragment_table_rejects_top_level_array() {
        let body = Bytes::from(r#"[["1"],["2"]]"#);
        let s = schema(1);
        let err = parse_remote_chunk_result_table(s, qid(), body).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn parse_remote_chunk_fragment_table_allows_empty_body() {
        let s = schema(2);
        let t = parse_remote_chunk_result_table(s, qid(), Bytes::new()).unwrap();
        assert_eq!(t.row_count(), 0);
    }

    #[test]
    fn parse_remote_chunk_fragment_table_rejects_whitespace_only_body() {
        let s = schema(1);
        let err =
            parse_remote_chunk_result_table(s, qid(), Bytes::from_static(b" \n\t")).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::UnexpectedToken { expected, .. }) if *expected == "'['"
        ));
    }

    #[tokio::test]
    async fn parse_remote_chunk_result_table_allows_empty_body() {
        let s = schema(2);
        let workload = ParseWorkload::remote_chunk(0, 0, s.len(), false, 0, 0);
        let t = parse_remote_chunk_result_table_async(s, qid(), Bytes::new(), workload, None)
            .await
            .unwrap();
        assert_eq!(t.row_count(), 0);
    }

    fn assert_invalid_cell_token_rejected<F>(parse: F, body: String)
    where
        F: Fn(Bytes) -> crate::Result<ResultTable>,
    {
        let err = parse(Bytes::from(body)).unwrap_err();
        assert!(matches!(
            err.as_rowset_parse_error(),
            Some(RowsetParseError::UnexpectedToken { expected, .. }) if *expected == "string or null"
        ));
    }

    #[test]
    fn parse_inline_result_table_rejects_non_string_non_null_cells() {
        let s = schema(1);
        for token in ["true", "false", "123", "{}", "[]"] {
            assert_invalid_cell_token_rejected(
                |body| parse_inline_result_table(Arc::clone(&s), qid(), body),
                format!("[[{token}]]"),
            );
        }
    }

    #[test]
    fn parse_remote_chunk_fragment_table_rejects_non_string_non_null_cells() {
        let s = schema(1);
        for token in ["true", "false", "123", "{}", "[]"] {
            assert_invalid_cell_token_rejected(
                |body| parse_remote_chunk_result_table(Arc::clone(&s), qid(), body),
                format!("[{token}]"),
            );
        }
    }

    #[test]
    fn inline_rowset_has_rows_treats_whitespace_empty_arrays_as_empty() {
        assert!(!inline_rowset_has_rows(b"[ ]").unwrap());
        assert!(!inline_rowset_has_rows(b"[\n]").unwrap());
    }

    #[test]
    fn inline_rowset_has_rows_detects_non_empty_arrays() {
        assert!(inline_rowset_has_rows(br#"[["x"]]"#).unwrap());
    }

    #[test]
    fn inline_rowset_has_rows_allows_leading_utf8_bom() {
        assert!(!inline_rowset_has_rows(b"\xEF\xBB\xBF[ ]").unwrap());
        assert!(inline_rowset_has_rows(b"\xEF\xBB\xBF[[\"x\"]]").unwrap());
    }

    #[test]
    fn blocking_parse_threshold_ignores_small_inputs() {
        let workload = ParseWorkload {
            input_bytes: BLOCKING_PARSE_BYTES - 1,
            row_count: Some((BLOCKING_PARSE_CELLS - 1) / 4),
            column_count: 4,
            gzip_encoded: false,
            compressed_bytes: None,
            uncompressed_bytes: None,
        };
        assert!(!should_spawn_blocking_parse(&workload));
    }

    #[test]
    fn blocking_parse_threshold_triggers_on_bytes() {
        let workload = ParseWorkload::inline_rowset(BLOCKING_PARSE_BYTES, None, 2);
        assert!(should_spawn_blocking_parse(&workload));
    }

    #[test]
    fn blocking_parse_threshold_triggers_on_cells() {
        let workload = ParseWorkload {
            input_bytes: 1024,
            row_count: Some(BLOCKING_PARSE_CELLS / 5),
            column_count: 5,
            gzip_encoded: false,
            compressed_bytes: None,
            uncompressed_bytes: None,
        };
        assert!(should_spawn_blocking_parse(&workload));
    }

    #[test]
    fn blocking_parse_threshold_triggers_on_inline_row_count() {
        let workload = ParseWorkload::inline_rowset(1024, Some(BLOCKING_PARSE_CELLS / 5), 5);
        assert!(should_spawn_blocking_parse(&workload));
    }

    #[test]
    fn blocking_parse_threshold_triggers_on_gzip_size() {
        let workload = ParseWorkload {
            input_bytes: 1024,
            row_count: None,
            column_count: 2,
            gzip_encoded: true,
            compressed_bytes: Some(BLOCKING_GZIP_COMPRESSED_BYTES),
            uncompressed_bytes: None,
        };
        assert!(should_spawn_blocking_parse(&workload));
    }

    #[tokio::test]
    async fn execute_parse_work_small_input_stays_inline() {
        let workload = ParseWorkload::inline_rowset(1024, None, 2);
        let (path, value) = execute_parse_work(workload, None, || Ok::<_, crate::Error>(7usize))
            .await
            .unwrap();
        assert_eq!(path, ParseExecution::Inline);
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn execute_parse_work_large_input_uses_spawn_blocking() {
        let workload = ParseWorkload::inline_rowset(BLOCKING_PARSE_BYTES, None, 2);
        let (path, value) = execute_parse_work(workload, None, || Ok::<_, crate::Error>(11usize))
            .await
            .unwrap();
        assert_eq!(path, ParseExecution::SpawnBlocking);
        assert_eq!(value, 11);
    }

    #[tokio::test]
    async fn execute_parse_work_respects_shared_blocking_parse_limiter() {
        let workload = ParseWorkload::inline_rowset(BLOCKING_PARSE_BYTES, None, 2);
        let limiter = BlockingParseLimiter::new(NonZeroUsize::new(1).unwrap());
        let probe = Arc::new(BlockingParseProbe::default());

        let first_probe = Arc::clone(&probe);
        let first = execute_parse_work(workload.clone(), Some(limiter.clone()), move || {
            first_probe.enter();
            std::thread::sleep(Duration::from_millis(50));
            first_probe.exit();
            Ok::<_, crate::Error>(())
        });

        let second_probe = Arc::clone(&probe);
        let second = execute_parse_work(workload, Some(limiter), move || {
            second_probe.enter();
            std::thread::sleep(Duration::from_millis(50));
            second_probe.exit();
            Ok::<_, crate::Error>(())
        });

        tokio::try_join!(first, second).unwrap();
        assert_eq!(probe.max_active(), 1);
    }

    #[tokio::test]
    async fn execute_parse_work_maps_join_error() {
        let workload = ParseWorkload::inline_rowset(BLOCKING_PARSE_BYTES, None, 2);
        let err = execute_parse_work(workload, None, || -> crate::Result<()> {
            panic!("boom");
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ParseWorkError::Join(_)));
    }
}
