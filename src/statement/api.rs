mod abort;
mod deadline;
mod poll;
mod submit;

#[cfg(test)]
mod test_support;

use std::sync::Arc;

use crate::{ApiContext, Result, session::SessionAuth};

pub(crate) use deadline::QueryResponseDeadline;

#[derive(Clone)]
pub(crate) struct QueryApiClient {
    api: Arc<ApiContext>,
    auth: Arc<SessionAuth>,
}

impl QueryApiClient {
    pub(crate) fn new(api: Arc<ApiContext>, auth: Arc<SessionAuth>) -> Self {
        Self { api, auth }
    }

    /// The current session token and the `Authorization` header value that carries it.
    fn authorization(&self) -> (Arc<str>, String) {
        let token = self.auth.session_token();
        let header = format!(r#"Snowflake Token="{token}""#);
        (token, header)
    }

    /// Renew `expired` after Snowflake rejected a request made with it; see [`SessionAuth::renew`].
    pub(crate) async fn renew_session(&self, expired: &str) -> Result<()> {
        self.auth.renew(&self.api, expired).await
    }
}
