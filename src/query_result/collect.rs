use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

use tokio::task::JoinSet;

use crate::{Error, Result, result::ResultTable, runtime::BlockingParseLimiter};

use super::{
    partition_source::{PartitionSource, remote_fetch_context},
    snapshot::ResultSnapshot,
};

#[derive(Clone, Copy)]
pub(crate) struct CollectPolicy {
    pub(crate) prefetch_concurrency: NonZeroUsize,
}

impl CollectPolicy {
    pub(crate) fn new(prefetch_concurrency: NonZeroUsize) -> Self {
        Self {
            prefetch_concurrency,
        }
    }
}

pub(crate) struct CollectWindow {
    next_spawn_ordinal: usize,
    next_commit_ordinal: usize,
    total: usize,
    max_in_flight: usize,
    tasks: JoinSet<(usize, Result<ResultTable>)>,
    buffered: BTreeMap<usize, ResultTable>,
}

impl CollectWindow {
    pub(crate) fn new(start_ordinal: usize, total: usize, max_in_flight: usize) -> Self {
        Self {
            next_spawn_ordinal: start_ordinal,
            next_commit_ordinal: start_ordinal,
            total,
            max_in_flight,
            tasks: JoinSet::new(),
            buffered: BTreeMap::new(),
        }
    }

    pub(crate) fn fill(
        &mut self,
        source: &Arc<PartitionSource>,
        snapshot: &Arc<ResultSnapshot>,
        blocking_parse_limiter: &BlockingParseLimiter,
    ) {
        while self.tasks.len() < self.max_in_flight && self.next_spawn_ordinal < self.total {
            let ordinal = self.next_spawn_ordinal;
            let source = Arc::clone(source);
            let schema = Arc::clone(&snapshot.schema);
            let ctx = remote_fetch_context(
                snapshot.as_ref(),
                ordinal,
                Some(blocking_parse_limiter.clone()),
            );
            self.tasks.spawn(async move {
                let result = source.fetch_table(ctx, schema).await;
                (ordinal, result)
            });
            self.next_spawn_ordinal += 1;
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<Result<(usize, ResultTable)>> {
        let join_result = self.tasks.join_next().await?;
        Some(
            join_result
                .map_err(Error::FutureJoin)
                .and_then(|(ordinal, r)| r.map(|t| (ordinal, t))),
        )
    }

    pub(crate) fn commit(
        &mut self,
        ordinal: usize,
        table: ResultTable,
        out: &mut Vec<ResultTable>,
    ) {
        self.buffered.insert(ordinal, table);
        while let Some(table) = self.buffered.remove(&self.next_commit_ordinal) {
            out.push(table);
            self.next_commit_ordinal += 1;
        }
    }
}
