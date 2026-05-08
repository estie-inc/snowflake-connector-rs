use url::Url;

use crate::{
    IntoStatement, Result,
    config::SnowflakeQueryConfig,
    query_result::{ResultSet, TypedResultSet},
    result::{FromRow, RowPlanContext},
    runtime::QueryRuntime,
    statement::{StatementExecutor, builder::into_statement_parts},
};

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) base_url: Url,
    pub(super) session_token: String,
    pub(super) query: SnowflakeQueryConfig,
    pub(super) runtime: QueryRuntime,
}

impl SnowflakeSession {
    /// Submit a statement and return a `ResultSet` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::BindEncode`, `ErrorKind::Network`,
    /// `ErrorKind::Server`, `ErrorKind::SessionExpired`,
    /// `ErrorKind::Timeout`, or `ErrorKind::Protocol`.
    pub async fn query<S: IntoStatement>(&self, statement: S) -> Result<ResultSet> {
        let executor = StatementExecutor::new(self);
        executor.execute(into_statement_parts(statement)).await
    }

    /// Submit a statement and return a typed `ResultSet` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`SnowflakeSession::query`], including
    /// `ErrorKind::BindEncode` when bind values cannot be encoded or validated
    /// before the request is sent. After the statement succeeds, this also
    /// propagates any error returned by [`FromRow::build_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` when the result schema does not match `T`;
    /// inspect the detail via [`Error::as_schema_error`](crate::Error::as_schema_error).
    pub async fn query_as<T, S>(&self, statement: S) -> Result<TypedResultSet<T>>
    where
        T: FromRow,
        S: IntoStatement,
    {
        let result = self.query(statement).await?;
        let plan = T::build_plan(RowPlanContext::new(result.schema_arc()))?;
        Ok(TypedResultSet::new(result, plan))
    }
}
