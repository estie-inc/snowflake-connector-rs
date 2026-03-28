use url::Url;

use crate::{
    Result, SnowflakeRow,
    config::SnowflakeQueryConfig,
    query::{QueryExecutor, QueryRequest},
};

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) base_url: Url,
    pub(super) session_token: String,
    pub(super) query: SnowflakeQueryConfig,
}

impl SnowflakeSession {
    /// Run a query and fetch all results.
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let executor = QueryExecutor::create(self, request).await?;
        executor.fetch_all().await
    }

    pub async fn execute<Q: Into<QueryRequest>>(&self, request: Q) -> Result<QueryExecutor> {
        QueryExecutor::create(self, request).await
    }
}
