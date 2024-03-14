use std::time::Duration;

use crate::{
    query::{query, QueryRequest},
    Result, SnowflakeRow,
};

const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
    pub(super) timeout: Option<Duration>,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let rows = query(
            &self.http,
            &self.account,
            request,
            &self.session_token,
            self.timeout
                .unwrap_or(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS)),
        )
        .await?;
        Ok(rows)
    }
}
