use std::sync::Arc;

use crate::{query_result::partition::PartitionSpec, result::Schema};

pub(crate) struct ResultIdentity {
    pub(crate) query_id: Arc<str>,
}

pub(crate) struct ResultSnapshot {
    pub(crate) identity: ResultIdentity,
    pub(crate) schema: Arc<Schema>,
    pub(crate) partitions: Vec<PartitionSpec>,
}
