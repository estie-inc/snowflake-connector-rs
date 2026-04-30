use std::{collections::HashMap, sync::Arc};

use http::HeaderMap;

use crate::result::Schema;

pub(crate) struct ResultIdentity {
    pub(crate) query_id: Arc<str>,
}

pub(crate) struct ResultSnapshot {
    pub(crate) identity: ResultIdentity,
    pub(crate) schema: Arc<Schema>,
    pub(crate) partitions: Vec<PartitionSpec>,
}

#[derive(Clone, Copy)]
pub(crate) enum PartitionSpec {
    Inline,
    Remote {
        row_count: i64,
        compressed_size: i64,
        uncompressed_size: i64,
    },
}

pub(crate) struct PartitionCursor {
    pub(crate) next_ordinal: usize,
    total: usize,
}

impl PartitionCursor {
    pub(crate) fn new(total: usize) -> Self {
        Self {
            next_ordinal: 0,
            total,
        }
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.next_ordinal >= self.total
    }

    pub(crate) fn advance(&mut self) {
        self.next_ordinal += 1;
    }
}

pub(crate) struct DownloadLocator {
    pub(crate) url: String,
    pub(crate) headers: Arc<HeaderMap>,
}

pub(crate) struct ResolvedLease {
    pub(crate) locators: HashMap<usize, DownloadLocator>,
}
