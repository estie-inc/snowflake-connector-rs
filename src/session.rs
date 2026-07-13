use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    CancellationToken, ClientShared, IntoStatement, Result,
    result_cursor::{ResultCursor, TypedResultCursor},
    result_table::{FromRow, RowPlanContext},
    statement::{StatementExecutor, builder::into_statement_parts},
};

pub struct Session {
    pub(crate) shared: Arc<ClientShared>,
    pub(crate) auth: Arc<SessionAuth>,
}

/// Per-query overrides for a single [`Session::query_with_options`] /
/// [`Session::query_as_with_options`] call.
///
/// Unset options inherit the defaults from [`QueryConfig`](crate::QueryConfig). These options sit between the
/// client-wide query defaults and per-collect [`CollectOptions`](crate::CollectOptions).
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub(crate) query_response_timeout: Option<Duration>,
    pub(crate) collect_prefetch_concurrency: Option<NonZeroUsize>,
    pub(crate) cancellation_token: Option<CancellationToken>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides [`QueryConfig::with_query_response_timeout`](crate::QueryConfig::with_query_response_timeout) for
    /// this query.
    pub fn with_query_response_timeout(mut self, timeout: Duration) -> Self {
        self.query_response_timeout = Some(timeout);
        self
    }

    /// Overrides [`QueryConfig::with_collect_prefetch_concurrency`](crate::QueryConfig::with_collect_prefetch_concurrency)
    /// for the cursor returned by this query.
    ///
    /// This sets the default collect policy embedded in the returned [`ResultCursor`]; a later
    /// [`CollectOptions::with_prefetch_concurrency`](crate::CollectOptions::with_prefetch_concurrency) still has the
    /// final say for a specific collection call.
    pub fn with_collect_prefetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.collect_prefetch_concurrency = Some(concurrency);
        self
    }

    /// Attach a [`CancellationToken`] so this query can be aborted while it runs.
    ///
    /// Cancelling the token stops the client-side wait for the query response and sends Snowflake a best-effort abort
    /// for the statement, so the warehouse stops executing it. See [`CancellationToken`] for the exact semantics.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }
}

/// Per-session authentication state.
pub(crate) struct SessionAuth {
    pub(crate) session_token: String,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The session token authenticates every request; never print it.
        f.debug_struct("Session")
            .field("base_url", &self.shared.base_url)
            .field("session_token", &"<redacted>")
            .field("query", &self.shared.query)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl SessionAuth {
    /// Build a per-session auth handle for unit tests.
    pub(crate) fn for_test(session_token: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            session_token: session_token.into(),
        })
    }
}

impl Session {
    /// Submit a statement and return a `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::BindEncode`, `ErrorKind::Network`, `ErrorKind::Server`, `ErrorKind::SessionExpired`,
    /// `ErrorKind::Timeout`, or `ErrorKind::Protocol`.
    pub async fn query<S: IntoStatement>(&self, statement: S) -> Result<ResultCursor> {
        self.query_with_options(statement, QueryOptions::default())
            .await
    }

    /// Submit a statement with per-query [`QueryOptions`] and return a `ResultCursor` for streaming partition access.
    ///
    /// Unset options inherit the configured [`QueryConfig`](crate::QueryConfig) defaults.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`].
    pub async fn query_with_options<S>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> Result<ResultCursor>
    where
        S: IntoStatement,
    {
        let cancellation = options.cancellation_token.clone();
        let settings = self.shared.query.resolve_options(options);
        let executor = StatementExecutor::new(self, settings).with_cancellation(cancellation);
        executor.execute(into_statement_parts(statement)).await
    }

    /// Submit a statement and return a typed `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`], including `ErrorKind::BindEncode` when bind values cannot be encoded or
    /// validated before the request is sent. After the statement succeeds, this also propagates any error returned by
    /// [`FromRow::build_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use `ErrorKind::Decode` when the result schema does not match `T`;
    /// inspect the detail via [`Error::as_schema_error`](crate::Error::as_schema_error). For custom plan-time validation
    /// from hand-written decoders, inspect [`Error::as_custom_plan_error`](crate::Error::as_custom_plan_error).
    pub async fn query_as<T, S>(&self, statement: S) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        self.query_as_with_options(statement, QueryOptions::default())
            .await
    }

    /// Submit a statement with per-query [`QueryOptions`] and return a typed `ResultCursor`.
    ///
    /// Unset options inherit the configured [`QueryConfig`](crate::QueryConfig) defaults.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`]. After the statement succeeds, this also propagates plan-time
    /// decode failures from [`FromRow::build_plan`] for `T`: use [`Error::as_schema_error`](crate::Error::as_schema_error)
    /// for schema mismatches and [`Error::as_custom_plan_error`](crate::Error::as_custom_plan_error) for custom
    /// hand-written decoder validation.
    pub async fn query_as_with_options<T, S>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        let result = self.query_with_options(statement, options).await?;
        let plan = T::build_plan(RowPlanContext::new(result.shared_schema()))?;
        Ok(TypedResultCursor::new(result, plan))
    }
}
