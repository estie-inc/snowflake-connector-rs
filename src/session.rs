use url::Url;

use crate::{
    Result,
    config::SnowflakeQueryConfig,
    query::QueryRequest,
    query_result::{ResultSet, TypedResultSet},
    result::{FromRow, RowPlanContext},
    runtime::QueryRuntime,
    statement::StatementExecutor,
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
    /// Returns `ErrorKind::Network`, `ErrorKind::Server`, `ErrorKind::SessionExpired`,
    /// `ErrorKind::Timeout`, or `ErrorKind::Protocol`.
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<ResultSet> {
        let executor = StatementExecutor::new(self);
        executor.execute(request.into()).await
    }

    /// Submit a statement and return a typed `ResultSet` for streaming partition access.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`SnowflakeSession::query`]. After the
    /// statement succeeds, this also propagates any error returned by
    /// [`FromRow::build_plan`] for `T`.
    ///
    /// Built-in and derive-based row types typically use
    /// `ErrorKind::Decode` when the result schema does not match `T`;
    /// inspect the detail via [`crate::Error::as_schema_error`].
    pub async fn query_as<T, Q>(&self, request: Q) -> Result<TypedResultSet<T>>
    where
        T: FromRow,
        Q: Into<QueryRequest>,
    {
        let result = self.query(request).await?;
        let plan = T::build_plan(RowPlanContext::new(result.schema_arc()))?;
        Ok(TypedResultSet::new(result, plan))
    }
}
