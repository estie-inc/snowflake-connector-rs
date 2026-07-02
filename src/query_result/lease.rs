use std::{collections::HashMap, sync::Arc};

use http::HeaderMap;

pub(crate) struct DownloadLocator {
    pub(crate) url: String,
    pub(crate) headers: Arc<HeaderMap>,
}

pub(crate) struct ResolvedLease {
    pub(crate) locators: HashMap<usize, DownloadLocator>,
}
