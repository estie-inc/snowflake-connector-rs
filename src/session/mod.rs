use crate::{
    query::{query, QueryRequest},
    ResponseRowType, Result,
};
pub struct SnowflakeSession {
    pub(super) http: reqwest::Client,
    pub(super) account: String,
    pub(super) session_token: String,
}

impl SnowflakeSession {
    pub async fn query<Q: Into<QueryRequest>>(
        &self,
        request: Q,
    ) -> Result<(Vec<ResponseRowType>, Vec<Vec<Option<String>>>)> {
        let (row_types, row_set) =
            query(&self.http, &self.account, request, &self.session_token).await?;
        Ok((row_types, row_set))
    }
}
