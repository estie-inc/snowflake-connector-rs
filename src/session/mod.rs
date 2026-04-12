use url::Url;

use crate::{
    Result, SnowflakeRow,
    config::SnowflakeQueryConfig,
    query::{QueryRequest, ResultSet, StatementExecutor},
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
        let result = self.execute(request).await?;
        result.collect_all().await
    }

    pub async fn execute<Q: Into<QueryRequest>>(&self, request: Q) -> Result<ResultSet> {
        let executor = StatementExecutor::new(self);
        executor.execute(request.into()).await
    }
}
