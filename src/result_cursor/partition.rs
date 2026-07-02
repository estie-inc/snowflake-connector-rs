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
