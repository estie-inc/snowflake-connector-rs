use std::time::Duration;

use crate::{
    Result, SnowflakeRow,
    query::{QueryExecutor, QueryRequest},
};

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
    pub(super) timeout: Option<Duration>,
}

impl SnowflakeSession {
    /// Run a query while capping concurrent chunk downloads.
    ///
    /// The `max_concurrency` field on the request limits how many result chunks are fetched at
    /// once. Values below `1` are treated as `1`.
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let request: QueryRequest = request.into();
        let max_concurrency = request.max_concurrency.unwrap_or(usize::MAX);

        let executor = QueryExecutor::create(self, request).await?;
        executor.fetch_all_with_limit(max_concurrency).await
    }

    pub async fn execute<Q: Into<QueryRequest>>(&self, request: Q) -> Result<QueryExecutor> {
        QueryExecutor::create(self, request).await
    }
}
