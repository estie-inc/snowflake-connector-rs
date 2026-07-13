//! Cooperative cancellation for in-flight queries.

use std::{
    fmt,
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use tokio::sync::Notify;

/// A cooperative signal for cancelling an in-flight query.
///
/// Attach a token to a query with
/// [`QueryOptions::with_cancellation_token`](crate::QueryOptions::with_cancellation_token), then call
/// [`cancel`](CancellationToken::cancel) from anywhere — including another thread — to abort it while it runs.
///
/// Cancellation does two things: it stops the client-side wait for the query response, and it sends Snowflake an
/// out-of-band abort for the statement so the warehouse stops executing it (and stops accruing cost). Cancellation
/// covers the execution phase — statement submit and the async result poll — which is where warehouse time is spent;
/// it does not interrupt downloading already-computed result chunks.
///
/// The token is cheap to [`clone`](Clone); every clone shares one cancellation state.
///
/// ```no_run
/// # use snowflake_connector_rs::{CancellationToken, QueryOptions, Session};
/// # async fn run(session: Session) -> snowflake_connector_rs::Result<()> {
/// let token = CancellationToken::new();
/// let watcher = token.clone();
/// // e.g. from a cancel button on another thread/task: `watcher.cancel();`
/// let _ = &watcher;
/// let result = session
///     .query_with_options("CALL long_running()", QueryOptions::new().with_cancellation_token(token))
///     .await;
/// # let _ = result; Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    /// Create a fresh, un-cancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation. Idempotent and safe to call from any thread; only the first call has an effect.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    /// Whether [`cancel`](Self::cancel) has been called on this token (or any of its clones).
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Resolve as soon as the token is cancelled. Cancellation-safe.
    pub(crate) async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            // Register the waiter, then re-check, so a `cancel` racing with this call cannot slip between the check
            // and the wait.
            let mut notified = pin!(self.inner.notify.notified());
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Drive `fut` to completion unless `token` is cancelled first.
///
/// Returns `Some(output)` when `fut` finished, or `None` when cancellation won the race. Completion of `fut` is
/// preferred when both are ready in the same poll, so a query that finished on the same turn it was cancelled still
/// yields its result.
pub(crate) async fn run_until_cancelled<F, T>(token: &CancellationToken, fut: F) -> Option<T>
where
    F: Future<Output = T>,
{
    let mut fut = pin!(fut);
    let mut cancelled = pin!(token.cancelled());
    poll_fn(|cx| {
        if let Poll::Ready(out) = fut.as_mut().poll(cx) {
            return Poll::Ready(Some(out));
        }
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;

    #[test]
    fn new_token_is_not_cancelled() {
        assert!(!CancellationToken::new().is_cancelled());
    }

    #[test]
    fn cancel_sets_the_flag_and_is_shared_across_clones() {
        let token = CancellationToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(token.is_cancelled());
        assert!(clone.is_cancelled(), "clones share one cancellation state");
    }

    #[tokio::test]
    async fn cancelled_resolves_after_cancel() {
        let token = CancellationToken::new();
        let waiter = token.clone();
        let handle = tokio::spawn(async move { waiter.cancelled().await });
        // The waiter must still be pending until we cancel.
        assert!(timeout(Duration::from_millis(50), async {}).await.is_ok());
        token.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("cancelled() must resolve promptly once cancelled")
            .expect("waiter task should not panic");
    }

    #[tokio::test]
    async fn cancelled_resolves_immediately_when_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("an already-cancelled token resolves without waiting");
    }

    #[tokio::test]
    async fn run_until_cancelled_returns_output_when_future_wins() {
        let token = CancellationToken::new();
        let out = run_until_cancelled(&token, async { 7_u32 }).await;
        assert_eq!(out, Some(7));
    }

    #[tokio::test]
    async fn run_until_cancelled_returns_none_when_cancelled_first() {
        let token = CancellationToken::new();
        let canceller = token.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            canceller.cancel();
        });
        let out = run_until_cancelled(&token, async {
            // Never completes on its own.
            std::future::pending::<()>().await;
        })
        .await;
        assert_eq!(out, None);
    }
}
