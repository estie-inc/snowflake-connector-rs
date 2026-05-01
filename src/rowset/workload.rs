use crate::{Error, Result, runtime::BlockingParseLimiter};

// These thresholds intentionally allow modest inline parsing so callers avoid
// paying `spawn_blocking` overhead for medium-sized rowsets, while still
// offloading work large enough to monopolize an async runtime worker for tens
// of milliseconds. They are aligned to the current rowset parser and gzip
// benchmarks rather than a strict byte-for-byte equivalence.
pub(crate) const BLOCKING_PARSE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const BLOCKING_PARSE_CELLS: u64 = 1_000_000;
pub(crate) const BLOCKING_GZIP_COMPRESSED_BYTES: usize = 3 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct ParseWorkload {
    pub(crate) input_bytes: usize,
    pub(crate) row_count: Option<u64>,
    pub(crate) column_count: usize,
    pub(crate) gzip_encoded: bool,
    pub(crate) compressed_bytes: Option<usize>,
    pub(crate) uncompressed_bytes: Option<usize>,
}

impl ParseWorkload {
    pub(crate) fn inline_rowset(
        input_bytes: usize,
        row_count: Option<u64>,
        column_count: usize,
    ) -> Self {
        Self {
            input_bytes,
            row_count,
            column_count,
            gzip_encoded: false,
            compressed_bytes: None,
            uncompressed_bytes: None,
        }
    }

    pub(crate) fn remote_chunk(
        input_bytes: usize,
        row_count: i64,
        column_count: usize,
        gzip_encoded: bool,
        compressed_size: i64,
        uncompressed_size: i64,
    ) -> Self {
        Self {
            input_bytes,
            row_count: u64::try_from(row_count).ok(),
            column_count,
            gzip_encoded,
            compressed_bytes: usize::try_from(compressed_size).ok(),
            uncompressed_bytes: usize::try_from(uncompressed_size).ok(),
        }
    }
}

pub(crate) fn should_spawn_blocking_parse(workload: &ParseWorkload) -> bool {
    let bytes = workload.uncompressed_bytes.unwrap_or(workload.input_bytes);
    let cells = workload
        .row_count
        .map(|rows| rows.saturating_mul(workload.column_count as u64))
        .unwrap_or(0);

    bytes >= BLOCKING_PARSE_BYTES
        || cells >= BLOCKING_PARSE_CELLS
        || (workload.gzip_encoded
            && workload.compressed_bytes.unwrap_or(0) >= BLOCKING_GZIP_COMPRESSED_BYTES)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParseExecution {
    Inline,
    SpawnBlocking,
}

pub(crate) async fn execute_parse_work<T, F>(
    workload: ParseWorkload,
    blocking_parse_limiter: Option<BlockingParseLimiter>,
    work: F,
) -> Result<(ParseExecution, T)>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    if should_spawn_blocking_parse(&workload) {
        let permit = match blocking_parse_limiter {
            Some(limiter) => Some(limiter.acquire_owned().await),
            None => None,
        };

        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(Error::FutureJoin)?;

        Ok((ParseExecution::SpawnBlocking, result?))
    } else {
        Ok((ParseExecution::Inline, work()?))
    }
}
