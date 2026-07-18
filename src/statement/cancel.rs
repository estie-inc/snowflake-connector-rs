use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};

use tokio::sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard};

use super::handle::QueryCancelStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionPhase {
    Prepared,
    InFlight,
    OutcomeUnknown,
    Finished,
    CancelledBeforeSubmit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SuccessfulCancel {
    NotSubmitted,
    Accepted,
}

#[derive(Debug)]
struct ControlState {
    execution: ExecutionPhase,
    query_id: Option<Arc<str>>,
    cancel_intent: bool,
    successful_cancel: Option<SuccessfulCancel>,
}

#[derive(Debug)]
pub(crate) struct QueryRequestIdentity {
    query_request_id: Arc<str>,
}

impl QueryRequestIdentity {
    pub(crate) fn new(query_request_id: Arc<str>) -> Self {
        Self { query_request_id }
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.query_request_id
    }
}

#[derive(Debug)]
pub(crate) struct QueryControl {
    identity: QueryRequestIdentity,
    state: StdMutex<ControlState>,
    cancel_gate: TokioMutex<()>,
}

pub(crate) enum SubmissionDecision {
    Start(ExecutionGuard),
    CancelledBeforeSubmit,
    AlreadyStarted,
}

pub(crate) enum CancelDecision {
    Return(QueryCancelStatus),
    SendAbort,
}

impl QueryControl {
    pub(crate) fn new(query_request_id: Arc<str>) -> Arc<Self> {
        Arc::new(Self {
            identity: QueryRequestIdentity::new(query_request_id),
            state: StdMutex::new(ControlState {
                execution: ExecutionPhase::Prepared,
                query_id: None,
                cancel_intent: false,
                successful_cancel: None,
            }),
            cancel_gate: TokioMutex::new(()),
        })
    }

    pub(crate) fn query_request_id(&self) -> &str {
        self.identity.request_id()
    }

    pub(crate) async fn lock_cancel_gate(&self) -> TokioMutexGuard<'_, ()> {
        self.cancel_gate.lock().await
    }

    pub(crate) fn begin_submission(self: &Arc<Self>) -> SubmissionDecision {
        let mut state = self.lock_state();
        match state.execution {
            ExecutionPhase::Prepared => {
                state.execution = ExecutionPhase::InFlight;
                SubmissionDecision::Start(ExecutionGuard {
                    control: Arc::clone(self),
                    terminal: false,
                })
            }
            ExecutionPhase::CancelledBeforeSubmit => SubmissionDecision::CancelledBeforeSubmit,
            ExecutionPhase::InFlight
            | ExecutionPhase::OutcomeUnknown
            | ExecutionPhase::Finished => SubmissionDecision::AlreadyStarted,
        }
    }

    pub(crate) fn begin_cancel_attempt(&self) -> CancelDecision {
        let mut state = self.lock_state();

        if let Some(outcome) = state.successful_cancel {
            return CancelDecision::Return(match outcome {
                SuccessfulCancel::NotSubmitted => QueryCancelStatus::NotSubmitted,
                SuccessfulCancel::Accepted => QueryCancelStatus::Accepted,
            });
        }

        match state.execution {
            ExecutionPhase::Prepared => {
                state.execution = ExecutionPhase::CancelledBeforeSubmit;
                state.successful_cancel = Some(SuccessfulCancel::NotSubmitted);
                CancelDecision::Return(QueryCancelStatus::NotSubmitted)
            }
            ExecutionPhase::CancelledBeforeSubmit => {
                state.successful_cancel = Some(SuccessfulCancel::NotSubmitted);
                CancelDecision::Return(QueryCancelStatus::NotSubmitted)
            }
            ExecutionPhase::Finished => CancelDecision::Return(QueryCancelStatus::AlreadyFinished),
            ExecutionPhase::InFlight | ExecutionPhase::OutcomeUnknown => {
                state.cancel_intent = true;
                CancelDecision::SendAbort
            }
        }
    }

    pub(crate) fn record_abort_accepted(&self) {
        let mut state = self.lock_state();
        state.successful_cancel = Some(SuccessfulCancel::Accepted);
    }

    pub(crate) fn mark_outcome_unknown(&self) {
        let mut state = self.lock_state();
        if state.execution == ExecutionPhase::InFlight {
            state.execution = ExecutionPhase::OutcomeUnknown;
        }
    }

    pub(crate) fn mark_terminal(&self) {
        let mut state = self.lock_state();
        if matches!(
            state.execution,
            ExecutionPhase::InFlight | ExecutionPhase::OutcomeUnknown
        ) {
            state.execution = ExecutionPhase::Finished;
        }
    }

    pub(crate) fn record_query_id(&self, query_id: Arc<str>) {
        self.lock_state().query_id = Some(query_id);
    }

    pub(crate) fn query_id(&self) -> Option<Arc<str>> {
        self.lock_state().query_id.clone()
    }

    pub(crate) fn cancel_intent(&self) -> bool {
        self.lock_state().cancel_intent
    }

    pub(crate) fn execution_phase(&self) -> ExecutionPhase {
        self.lock_state().execution
    }

    fn lock_state(&self) -> StdMutexGuard<'_, ControlState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(crate) struct ExecutionGuard {
    control: Arc<QueryControl>,
    terminal: bool,
}

impl ExecutionGuard {
    pub(crate) fn record_query_id(&self, query_id: Arc<str>) {
        self.control.record_query_id(query_id);
    }

    pub(crate) fn mark_terminal(&mut self) {
        self.control.mark_terminal();
        self.terminal = true;
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if !self.terminal {
            self.control.mark_outcome_unknown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control() -> Arc<QueryControl> {
        QueryControl::new(Arc::from("query-request-id"))
    }

    #[test]
    fn cancel_before_submission_prevents_start_and_is_cached() {
        let control = control();

        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::Return(QueryCancelStatus::NotSubmitted)
        ));
        assert_eq!(
            control.execution_phase(),
            ExecutionPhase::CancelledBeforeSubmit
        );
        assert!(matches!(
            control.begin_submission(),
            SubmissionDecision::CancelledBeforeSubmit
        ));
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::Return(QueryCancelStatus::NotSubmitted)
        ));
    }

    #[test]
    fn submission_wins_and_guard_drop_marks_outcome_unknown() {
        let control = control();
        let guard = match control.begin_submission() {
            SubmissionDecision::Start(guard) => guard,
            _ => panic!("submission should start"),
        };

        assert_eq!(control.execution_phase(), ExecutionPhase::InFlight);
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::SendAbort
        ));
        assert!(control.cancel_intent());
        drop(guard);
        assert_eq!(control.execution_phase(), ExecutionPhase::OutcomeUnknown);
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::SendAbort
        ));
    }

    #[test]
    fn terminal_execution_returns_already_finished() {
        let control = control();
        let mut guard = match control.begin_submission() {
            SubmissionDecision::Start(guard) => guard,
            _ => panic!("submission should start"),
        };
        guard.mark_terminal();

        assert_eq!(control.execution_phase(), ExecutionPhase::Finished);
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::Return(QueryCancelStatus::AlreadyFinished)
        ));
    }

    #[test]
    fn accepted_cancel_is_cached_but_failed_attempt_can_retry() {
        let control = control();
        let _guard = match control.begin_submission() {
            SubmissionDecision::Start(guard) => guard,
            _ => panic!("submission should start"),
        };

        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::SendAbort
        ));
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::SendAbort
        ));

        control.record_abort_accepted();
        assert!(matches!(
            control.begin_cancel_attempt(),
            CancelDecision::Return(QueryCancelStatus::Accepted)
        ));
    }

    #[test]
    fn query_id_is_retained_for_diagnostics() {
        let control = control();
        control.record_query_id(Arc::from("query-id"));
        assert_eq!(control.query_id().as_deref(), Some("query-id"));
    }
}
