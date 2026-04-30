use std::{num::NonZeroUsize, sync::Arc, thread};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_BLOCKING_PARSE_CONCURRENCY: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct QueryRuntime {
    blocking_parse_limiter: BlockingParseLimiter,
}

impl QueryRuntime {
    pub(crate) fn new() -> Self {
        Self {
            blocking_parse_limiter: BlockingParseLimiter::new(blocking_parse_concurrency_limit()),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_blocking_parse_concurrency(concurrency: NonZeroUsize) -> Self {
        Self {
            blocking_parse_limiter: BlockingParseLimiter::new(concurrency),
        }
    }

    pub(crate) fn blocking_parse_limiter(&self) -> BlockingParseLimiter {
        self.blocking_parse_limiter.clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BlockingParseLimiter {
    semaphore: Arc<Semaphore>,
}

impl BlockingParseLimiter {
    pub(crate) fn new(concurrency: NonZeroUsize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency.get())),
        }
    }

    pub(crate) async fn acquire_owned(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("blocking parse semaphore must remain open")
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.semaphore, &other.semaphore)
    }
}

pub(crate) fn blocking_parse_concurrency_limit() -> NonZeroUsize {
    let available = thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(MAX_BLOCKING_PARSE_CONCURRENCY);
    NonZeroUsize::new(available.min(MAX_BLOCKING_PARSE_CONCURRENCY))
        .expect("blocking parse concurrency must be non-zero")
}
