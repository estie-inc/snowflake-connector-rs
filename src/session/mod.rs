use std::time::Duration;

use crate::{
    query::{QueryExecutor, QueryRequest},
    Result, SnowflakeRow,
};

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
    pub(super) timeout: Option<Duration>,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let mut executor = QueryExecutor::create(self, request).await?;
        executor.fetch_all().await.map(|v| v.unwrap_or(vec![]))
    }
}
