use std::time::{Duration, Instant};

use crate::error::TimeoutError;

const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// Resolves `timeout` into an absolute deadline.
///
/// Falls back to a finite far-future instant when `Instant::checked_add` would overflow, so callers never observe a
/// deadline that has silently wrapped.
pub(super) fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout)
        .or_else(|| now.checked_add(FAR_FUTURE))
        .unwrap_or(now)
}

#[derive(Clone, Copy)]
pub(crate) struct QueryResponseDeadline {
    deadline: Instant,
}

impl QueryResponseDeadline {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            deadline: deadline_after(timeout),
        }
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub(crate) fn remaining_or_timeout(&self) -> std::result::Result<Duration, TimeoutError> {
        let remaining = self.remaining();
        if remaining.is_zero() {
            Err(TimeoutError::query())
        } else {
            Ok(remaining)
        }
    }
}
