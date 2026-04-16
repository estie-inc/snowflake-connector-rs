use std::collections::HashMap;
use std::sync::Arc;

use http::HeaderMap;

use crate::SnowflakeColumn;

pub(crate) type RawRow = Vec<Option<String>>;
pub(crate) type RawPartitionRows = Vec<RawRow>;

pub(crate) struct ResultIdentity {
    pub(crate) query_id: String,
}

pub(crate) struct ResultSnapshot {
    pub(crate) identity: ResultIdentity,
    pub(crate) columns: Arc<[SnowflakeColumn]>,
    pub(crate) column_indices: Arc<HashMap<String, usize>>,
    pub(crate) partitions: Vec<PartitionSpec>,
}

pub(crate) enum PartitionSpec {
    Inline,
    Remote {
        /// Number of rows in this partition as reported by the server.
        /// Retained for byte-budget / weighted scheduling in the future.
        #[allow(dead_code)]
        row_count: i64,
        /// Compressed byte size as reported by the server.
        /// Retained for byte-budget / weighted scheduling in the future.
        #[allow(dead_code)]
        compressed_size: i64,
        /// Uncompressed byte size as reported by the server.
        /// Retained for byte-budget / weighted scheduling in the future.
        #[allow(dead_code)]
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
