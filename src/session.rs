use std::fmt;

use url::Url;

use crate::{
    IntoStatement, Result,
    config::QueryConfig,
    query_result::{ResultCursor, TypedResultCursor},
    result::{FromRow, RowPlanContext},
    runtime::QueryRuntime,
    statement::{StatementExecutor, builder::into_statement_parts},
};

pub struct Session {
    pub(super) http: reqwest::Client,
    pub(super) base_url: Url,
    pub(super) session_token: String,
    pub(super) query: QueryConfig,
    pub(super) runtime: QueryRuntime,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The session token authenticates every request; never print it.
        f.debug_struct("Session")
            .field("base_url", &self.base_url)
            .field("session_token", &"<redacted>")
            .field("query", &self.query)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Submit a statement and return a `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::BindEncode`, `ErrorKind::Network`,
    /// `ErrorKind::Server`, `ErrorKind::SessionExpired`,
    /// `ErrorKind::Timeout`, or `ErrorKind::Protocol`.
    pub async fn query<S: IntoStatement>(&self, statement: S) -> Result<ResultCursor> {
        let executor = StatementExecutor::new(self);
        executor.execute(into_statement_parts(statement)).await
    }

    /// Submit a statement and return a typed `ResultCursor` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Session::query`], including
    /// `ErrorKind::BindEncode` when bind values cannot be encoded or validated
    /// before the request is sent. After the statement succeeds, this also
    /// propagates any error returned by [`FromRow::build_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` when the result schema does not match `T`;
    /// inspect the detail via [`Error::as_schema_error`](crate::Error::as_schema_error).
    pub async fn query_as<T, S>(&self, statement: S) -> Result<TypedResultCursor<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        let result = self.query(statement).await?;
        let plan = T::build_plan(RowPlanContext::new(result.shared_schema()))?;
        Ok(TypedResultCursor::new(result, plan))
    }
}
