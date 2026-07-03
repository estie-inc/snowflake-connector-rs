use std::sync::Arc;

use bytes::Bytes;

use crate::result_table::Schema;

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

pub(crate) struct InlineRowset {
    pub(crate) bytes: Bytes,
    pub(crate) row_count_hint: Option<u64>,
}
