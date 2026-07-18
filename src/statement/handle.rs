use std::{fmt, sync::Arc, time::Duration};

use crate::{
    Result,
    result_cursor::{ResultCursor, TypedResultCursor},
    result_table::{FromRow, RowPlanContext},
};

use super::{
    StatementExecutor, StatementParts,
    cancel::{CancelDecision, QueryControl},
    client::StatementApiClient,
};

/// What a successful [`QueryCanceller::cancel`] call achieved.
///
/// This is a report, not a control-flow discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueryCancelStatus {
    /// Cancellation won before the query submit request began; neither the query nor an abort request was sent.
    NotSubmitted,
    /// Snowflake accepted the abort request; query terminality is not implied.
    Accepted,
    /// The connector already processed a terminal query response; no abort request was sent.
    AlreadyFinished,
}

/// An owned statement execution that can be explicitly cancelled through a [`QueryCanceller`].
///
/// Cancellation runs only through [`QueryCanceller::cancel`] on a canceller retained from [`Self::canceller`];
/// dropping the execution future never cancels the remote query.
pub struct QueryHandle {
    statement: StatementParts,
    executor: StatementExecutor,
    control: Arc<QueryControl>,
}

/// A cloneable controller that cancels the query of the [`QueryHandle`] it was obtained from.
///
/// It remains usable from any task, even after the handle has been consumed by execution or the execution future
/// has been dropped.
#[derive(Clone)]
pub struct QueryCanceller {
    api: StatementApiClient,
    control: Arc<QueryControl>,
    request_timeout: Duration,
}

impl QueryHandle {
    pub(crate) fn new(
        statement: StatementParts,
        executor: StatementExecutor,
        control: Arc<QueryControl>,
    ) -> Self {
        Self {
            statement,
            executor,
            control,
        }
    }

    /// Returns the client-generated query request ID used by the submit and abort requests.
    pub fn request_id(&self) -> &str {
        self.control.query_request_id()
    }

    /// Returns a cloneable cancellation controller for this query.
    pub fn canceller(&self) -> QueryCanceller {
        QueryCanceller {
            api: self.executor.api_client(),
            control: Arc::clone(&self.control),
            request_timeout: self.executor.cancel_request_timeout(),
        }
    }

    /// Submits the statement and returns a streaming result cursor.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`](crate::Session::query). If cancellation wins before submission,
    /// this returns an [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled) error without sending either the query
    /// or abort request.
    pub async fn execute(self) -> Result<ResultCursor> {
        let Self {
            statement,
            executor,
            control,
        } = self;
        executor.execute(statement, control).await
    }

    /// Submits the statement and builds a typed streaming result cursor.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::execute`]. After the statement succeeds, this also propagates plan-time
    /// decode failures from [`FromRow::build_plan`] for `T`.
    pub async fn execute_as<T>(self) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
    {
        let result = self.execute().await?;
        let plan = T::build_plan(RowPlanContext::new(result.shared_schema()))?;
        Ok(TypedResultCursor::new(result, plan))
    }
}

impl fmt::Debug for QueryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryHandle")
            .field("request_id", &self.request_id())
            .field("phase", &self.control.execution_phase())
            .finish_non_exhaustive()
    }
}

impl QueryCanceller {
    /// Returns the client-generated query request ID targeted by this canceller.
    pub fn request_id(&self) -> &str {
        self.control.query_request_id()
    }

    /// Requests remote cancellation and waits for the abort response.
    ///
    /// On success, reports what the cancellation achieved as a [`QueryCancelStatus`].
    ///
    /// This method never returns an [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled) error; that error is what
    /// [`QueryHandle::execute`] returns once the cancellation takes effect.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::Network`](crate::ErrorKind::Network), [`ErrorKind::Timeout`](crate::ErrorKind::Timeout),
    /// [`ErrorKind::Server`](crate::ErrorKind::Server),
    /// [`ErrorKind::SessionExpired`](crate::ErrorKind::SessionExpired), or
    /// [`ErrorKind::Protocol`](crate::ErrorKind::Protocol). Transient transport failures are retried within the
    /// configured cancellation deadline, and a failed call never caches a successful status, so a later call can
    /// retry the cancellation.
    pub async fn cancel(&self) -> Result<QueryCancelStatus> {
        let _gate = self.control.lock_cancel_gate().await;

        match self.control.begin_cancel_attempt() {
            CancelDecision::Return(outcome) => Ok(outcome),
            CancelDecision::SendAbort => {
                self.api
                    .abort_query(self.control.query_request_id(), self.request_timeout)
                    .await?;
                self.control.record_abort_accepted();
                Ok(QueryCancelStatus::Accepted)
            }
        }
    }
}

impl fmt::Debug for QueryCanceller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryCanceller")
            .field("request_id", &self.request_id())
            .field("phase", &self.control.execution_phase())
            .finish_non_exhaustive()
    }
}

const _: fn() = || {
    fn assert_send_sync_static<T: Send + Sync + 'static>() {}
    assert_send_sync_static::<QueryHandle>();
    assert_send_sync_static::<QueryCanceller>();
};
