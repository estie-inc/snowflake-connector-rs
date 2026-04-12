use crate::Result;

use super::chunk_downloader::ChunkDownloader;
use super::snapshot::{RawPartitionRows, ResolvedLease};

pub(crate) struct FetchContext {
    pub(crate) ordinal: usize,
}

pub(crate) enum PartitionSource {
    Static(StaticPartitionSource),
    #[cfg(test)]
    Fake(tests::FakePartitionSource),
}

impl PartitionSource {
    pub(crate) async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
        match self {
            Self::Static(s) => s.fetch(ctx).await,
            #[cfg(test)]
            Self::Fake(f) => f.fetch(ctx).await,
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

    async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
        let locator = self
            .lease
            .locators
            .get(&ctx.ordinal)
            .expect("locator must exist for every Remote partition ordinal");

        self.downloader
            .download(&locator.url, &locator.headers)
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use super::*;

    /// Test-only partition source that returns pre-configured results
    /// keyed by partition ordinal. Supports multiple calls per ordinal
    /// (e.g. first call fails, second succeeds).
    pub(crate) struct FakePartitionSource {
        responses: Mutex<HashMap<usize, VecDeque<Result<RawPartitionRows>>>>,
    }

    impl FakePartitionSource {
        pub(crate) fn new(responses: HashMap<usize, VecDeque<Result<RawPartitionRows>>>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }

        pub(crate) async fn fetch(&self, ctx: FetchContext) -> Result<RawPartitionRows> {
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
        }
    }
}
