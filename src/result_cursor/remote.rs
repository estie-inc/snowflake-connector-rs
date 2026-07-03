mod downloader;

use std::{collections::HashMap, sync::Arc};

use http::HeaderMap;

use crate::{
    error::QueryScopedResult,
    result_table::{ResultTable, Schema},
    runtime::BlockingParseLimiter,
};

use super::model::{PartitionSpec, ResultSnapshot};

use downloader::{DownloadRequest, RemotePartitionDownloader};

pub(crate) struct DownloadLocator {
    pub(crate) url: String,
    pub(crate) headers: Arc<HeaderMap>,
}

pub(crate) struct ResolvedLease {
    pub(crate) locators: HashMap<usize, DownloadLocator>,
}

/// Remote partition materialization facade used by the cursor and collection paths.
pub(crate) enum PartitionSource {
    Remote(RemotePartitionSource),
    #[cfg(test)]
    Fake(tests::FakePartitionSource),
}

impl PartitionSource {
    pub(crate) async fn fetch_table(
        &self,
        snapshot: &ResultSnapshot,
        ordinal: usize,
        schema: Arc<Schema>,
        blocking_parse_limiter: Option<BlockingParseLimiter>,
    ) -> QueryScopedResult<ResultTable> {
        match self {
            Self::Remote(s) => {
                s.fetch_table(snapshot, ordinal, schema, blocking_parse_limiter)
                    .await
            }
            #[cfg(test)]
            Self::Fake(f) => {
                f.fetch(snapshot, ordinal, schema, blocking_parse_limiter)
                    .await
            }
        }
    }
}

pub(crate) struct RemotePartitionSource {
    lease: ResolvedLease,
    downloader: RemotePartitionDownloader,
}

impl RemotePartitionSource {
    pub(crate) fn new(lease: ResolvedLease, client: reqwest::Client) -> Self {
        Self {
            lease,
            downloader: RemotePartitionDownloader::new(client),
        }
    }

    pub(crate) async fn fetch_table(
        &self,
        snapshot: &ResultSnapshot,
        ordinal: usize,
        schema: Arc<Schema>,
        blocking_parse_limiter: Option<BlockingParseLimiter>,
    ) -> QueryScopedResult<ResultTable> {
        let locator = self
            .lease
            .locators
            .get(&ordinal)
            .expect("locator must exist for every Remote partition ordinal");
        let request = download_request(snapshot, ordinal, locator, blocking_parse_limiter);
        self.downloader.download_table(request, schema).await
    }
}

fn download_request(
    snapshot: &ResultSnapshot,
    ordinal: usize,
    locator: &DownloadLocator,
    blocking_parse_limiter: Option<BlockingParseLimiter>,
) -> DownloadRequest {
    match snapshot.partitions[ordinal] {
        PartitionSpec::Remote {
            row_count,
            compressed_size,
            uncompressed_size,
        } => DownloadRequest {
            url: locator.url.clone(),
            headers: Arc::clone(&locator.headers),
            query_id: Arc::clone(&snapshot.identity.query_id),
            row_count,
            compressed_size,
            uncompressed_size,
            blocking_parse_limiter,
        },
        PartitionSpec::Inline => {
            panic!("download requests may only be constructed for remote partitions")
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

    pub(crate) type FakeResponses = HashMap<usize, VecDeque<FakeResponse>>;

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
            snapshot: &ResultSnapshot,
            ordinal: usize,
            schema: Arc<Schema>,
            blocking_parse_limiter: Option<BlockingParseLimiter>,
        ) -> QueryScopedResult<ResultTable> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            let query_id = Arc::clone(&snapshot.identity.query_id);

            let response = {
                let mut map = self
                    .responses
                    .lock()
                    .expect("FakePartitionSource: poisoned");
                let queue = map
                    .get_mut(&ordinal)
                    .expect("FakePartitionSource: no responses configured for ordinal");
                queue
                    .pop_front()
                    .expect("FakePartitionSource: response queue exhausted for ordinal")
            };

            let raw = match response {
                FakeResponse::Rows(rows) => {
                    rows.map_err(|error| QueryScopedError::new(Arc::clone(&query_id), error))?
                }
                FakeResponse::BlockingRows { rows, probe, delay } => {
                    let _permit = match blocking_parse_limiter {
                        Some(limiter) => Some(limiter.acquire_owned().await),
                        None => None,
                    };
                    probe.enter();
                    sleep(delay).await;
                    probe.exit();
                    rows.map_err(|error| QueryScopedError::new(Arc::clone(&query_id), error))?
                }
            };

            inline_rows_to_result_table_inner(schema, Arc::clone(&query_id), raw)
                .map_err(|error| QueryScopedError::new(Arc::clone(&query_id), error))
        }
    }
}
