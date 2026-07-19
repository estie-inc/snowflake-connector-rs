mod abort;
mod deadline;
mod poll;
mod submit;

#[cfg(test)]
mod test_support;

use std::sync::Arc;

use crate::{ApiContext, session::SessionAuth};

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
}
