/// Declares whether a result-producing backend adapter can refresh lease
/// locators after the initial response, and under which invariant refresh is
/// safe.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendResultCapability {
    StaticOnly,
    Refreshing { invariant: RefreshInvariant },
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefreshInvariant {
    /// The total partition layout (count, inline/remote boundaries, row counts)
    /// is stable across refreshes; only remote locators change.
    StableRemainingPartitions,
}
