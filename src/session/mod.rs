use std::time::Duration;

use crate::{
    Result, SnowflakeRow,
    query::{QueryExecutor, QueryRequest},
};

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) host: Option<String>,
    pub(super) port: Option<u16>,
    pub(super) protocol: Option<String>,
    pub(super) account: String,
    pub(super) session_token: String,
    pub(super) timeout: Option<Duration>,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let executor = QueryExecutor::create(self, request).await?;
        executor.fetch_all().await
    }

    pub async fn execute<Q: Into<QueryRequest>>(&self, request: Q) -> Result<QueryExecutor> {
        QueryExecutor::create(self, request).await
    }
}
