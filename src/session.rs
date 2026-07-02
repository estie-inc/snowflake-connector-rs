use std::{fmt, sync::Arc};

use crate::{
    ClientShared, IntoStatement, Result,
    query_result::{ResultCursor, TypedResultCursor},
    result::{FromRow, RowPlanContext},
    statement::{StatementExecutor, builder::into_statement_parts},
};

pub struct Session {
    pub(crate) shared: Arc<ClientShared>,
    pub(crate) auth: Arc<SessionAuth>,
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
