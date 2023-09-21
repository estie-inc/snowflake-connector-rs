use crate::{
    query::{query, QueryRequest},
    Result, SnowflakeRow,
};
pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(&self, request: Q) -> Result<Vec<SnowflakeRow>> {
        let rows = query(&self.http, &self.account, request, &self.session_token).await?;
        Ok(rows)
    }
}
