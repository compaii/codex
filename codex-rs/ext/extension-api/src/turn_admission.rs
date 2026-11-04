//! Lets Core turn-input submissions participate in a host's shutdown drain.

/// A host-provided gate checked before Core starts a turn-input submission.
///
/// Implementations return a permit for work admitted before shutdown and
/// Core retains it through submission. `None` skips the start without consuming
/// pending input. Steering an existing turn does not acquire a new permit.
/// Mailbox wakeups that start work remain gated. Parent-delegated subagent
/// input may use the separate delegated gate to finish existing work.
pub trait TurnStartAdmission: std::fmt::Debug + Send + Sync {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>>;

    /// Reserve an internally generated goal continuation. A maintenance host may
    /// retain one revalidation callback per thread until its bounded request is
    /// abandoned. Explicit shutdown never resumes withheld work.
    fn admit_goal_continuation(
        &self,
        _thread_id: codex_protocol::ThreadId,
        _on_deferred: Box<dyn FnOnce() + Send>,
    ) -> Option<Box<dyn Send>> {
        // Core still checks ordinary admission at actual submission. Hosts
        // without maintenance support must not acquire an extra quota permit.
        Some(Box::new(()))
    }

    /// Admission for explicit parent-delegated child work. Ungated hosts retain
    /// the historical exception; a draining host can require a live parent task.
    fn admit_delegated_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    /// Tracks an already-admitted task through terminal persistence and abort
    /// cleanup, independently of asynchronous host lifecycle event delivery.
    /// Core retains shared ownership in both the task future and RunningTask.
    fn track_running_turn(&self) -> std::sync::Arc<dyn Send + Sync> {
        std::sync::Arc::new(())
    }
}
