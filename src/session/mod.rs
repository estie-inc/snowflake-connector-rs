use crate::{
    query::{query, QueryRequest},
    Result, SnowflakeRow,
};
pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
    pub(super) polling_interval: Option<std::time::Duration>,
    pub(super) max_polling_attempts: Option<usize>
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let rows = query(&self.http, &self.account, request, &self.session_token, self.polling_interval, self.max_polling_attempts).await?;
        Ok(rows)
    }
}
