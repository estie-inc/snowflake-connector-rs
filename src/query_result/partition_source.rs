use std::sync::Arc;

use crate::{
    chunk::ChunkDownloader,
    error::QueryScopedResult,
    result::{ResultTable, Schema},
    runtime::BlockingParseLimiter,
};

use super::{lease::ResolvedLease, partition::PartitionSpec, snapshot::ResultSnapshot};

#[derive(Clone)]
pub(crate) struct FetchContext {
    pub(crate) ordinal: usize,
    pub(crate) query_id: Arc<str>,
    pub(crate) row_count: i64,
    pub(crate) compressed_size: i64,
    pub(crate) uncompressed_size: i64,
    pub(crate) blocking_parse_limiter: Option<BlockingParseLimiter>,
}

pub(crate) enum PartitionSource {
    Static(StaticPartitionSource),
    #[cfg(test)]
    Fake(tests::FakePartitionSource),
}

impl PartitionSource {
    pub(crate) async fn fetch_table(
        &self,
        ctx: FetchContext,
        schema: Arc<Schema>,
    ) -> QueryScopedResult<ResultTable> {
        match self {
            Self::Static(s) => s.fetch(ctx, schema).await,
            #[cfg(test)]
            Self::Fake(f) => f.fetch(ctx, schema).await,
        }
    }
}

pub(crate) struct StaticPartitionSource {
    lease: ResolvedLease,
    downloader: ChunkDownloader,
}

impl StaticPartitionSource {
    pub(crate) fn new(lease: ResolvedLease, downloader: ChunkDownloader) -> Self {
        Self { lease, downloader }
    }

    async fn fetch(
        &self,
        ctx: FetchContext,
        schema: Arc<Schema>,
    ) -> QueryScopedResult<ResultTable> {
        let locator = self
            .lease
            .locators
            .get(&ctx.ordinal)
            .expect("locator must exist for every Remote partition ordinal");

        self.downloader
            .download_table(&locator.url, &locator.headers, schema, ctx)
            .await
    }
}

pub(crate) fn remote_fetch_context(
    snapshot: &ResultSnapshot,
    ordinal: usize,
    blocking_parse_limiter: Option<BlockingParseLimiter>,
) -> FetchContext {
    match snapshot.partitions[ordinal] {
        PartitionSpec::Remote {
            row_count,
            compressed_size,
            uncompressed_size,
        } => FetchContext {
            ordinal,
            query_id: Arc::clone(&snapshot.identity.query_id),
            row_count,
            compressed_size,
            uncompressed_size,
            blocking_parse_limiter,
        },
        PartitionSpec::Inline => {
            panic!("fetch contexts may only be constructed for remote partitions")
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::time::sleep;

    use super::*;
    use crate::{
        error::{QueryScopedError, QueryScopedRepr},
        rowset::parser::inline_rows_to_result_table_inner,
    };

    #[derive(Default)]
    pub(crate) struct BlockingFetchProbe {
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl BlockingFetchProbe {
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

        pub(crate) fn max_active(&self) -> usize {
            self.max_active.load(Ordering::SeqCst)
        }
    }

    pub(crate) enum FakeResponse {
        Rows(std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr>),
        BlockingRows {
            rows: std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr>,
            probe: Arc<BlockingFetchProbe>,
            delay: Duration,
        },
    }

    impl From<std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr>> for FakeResponse {
        fn from(rows: std::result::Result<Vec<Vec<Option<String>>>, QueryScopedRepr>) -> Self {
            Self::Rows(rows)
        }
    }

    /// Pre-configured queue of fake partition fetches keyed by ordinal.
    pub(crate) type FakeResponses = HashMap<usize, VecDeque<FakeResponse>>;

    /// Test-only partition source that returns pre-configured raw rows
    /// (then converted to a ResultTable) keyed by partition ordinal.
    pub(crate) struct FakePartitionSource {
        responses: Mutex<FakeResponses>,
        fetch_count: Arc<AtomicUsize>,
    }

    impl FakePartitionSource {
        pub(crate) fn new(responses: FakeResponses) -> Self {
            Self {
                responses: Mutex::new(responses),
                fetch_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub(crate) fn fetch_count(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.fetch_count)
        }

        pub(crate) async fn fetch(
            &self,
            ctx: FetchContext,
            schema: Arc<Schema>,
        ) -> QueryScopedResult<ResultTable> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);

            let response = {
                let mut map = self
                    .responses
                    .lock()
                    .expect("FakePartitionSource: poisoned");
                let queue = map
                    .get_mut(&ctx.ordinal)
                    .expect("FakePartitionSource: no responses configured for ordinal");
                queue
                    .pop_front()
                    .expect("FakePartitionSource: response queue exhausted for ordinal")
            };

            let raw = match response {
                FakeResponse::Rows(rows) => {
                    rows.map_err(|error| QueryScopedError::new(Arc::clone(&ctx.query_id), error))?
                }
                FakeResponse::BlockingRows { rows, probe, delay } => {
                    let _permit = match ctx.blocking_parse_limiter {
                        Some(limiter) => Some(limiter.acquire_owned().await),
                        None => None,
                    };
                    probe.enter();
                    sleep(delay).await;
                    probe.exit();
                    rows.map_err(|error| QueryScopedError::new(Arc::clone(&ctx.query_id), error))?
                }
            };

            inline_rows_to_result_table_inner(schema, Arc::clone(&ctx.query_id), raw)
                .map_err(|error| QueryScopedError::new(Arc::clone(&ctx.query_id), error))
        }
    }
}
