use std::sync::Arc;

use reqwest::Client;

use crate::{ClientShared, session::SessionAuth};

mod abort;
mod deadline;
mod poll;
mod submit;

#[cfg(test)]
mod test_support;

pub(crate) use deadline::QueryResponseDeadline;

#[derive(Clone)]
pub(crate) struct QueryApiClient {
    shared: Arc<ClientShared>,
    auth: Arc<SessionAuth>,
}

impl QueryApiClient {
    pub(crate) fn new(shared: Arc<ClientShared>, auth: Arc<SessionAuth>) -> Self {
        Self { shared, auth }
    }

    pub(crate) fn http_client(&self) -> Client {
        self.shared.http.clone()
    }
}
