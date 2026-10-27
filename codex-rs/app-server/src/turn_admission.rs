//! Tracks client runtime changes and turn starts admitted before a shutdown drain.

use codex_app_server_protocol::JSONRPCErrorError;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnAbortInput;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnStartInput;
use codex_extension_api::TurnStopInput;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

use crate::error_code::server_draining_error;

#[derive(Debug, Default)]
struct AdmissionState {
    closed: bool,
    active: usize,
    running: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct TurnAdmission {
    state: Arc<Mutex<AdmissionState>>,
    active_tx: watch::Sender<usize>,
}

impl Default for TurnAdmission {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(AdmissionState::default())),
            active_tx: watch::channel(0).0,
        }
    }
}

pub(crate) struct TurnPermit(TurnAdmission);

impl TurnAdmission {
    pub(crate) fn begin_drain(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
    }

    // Core's synchronous lifecycle guard bridges submission to actual task
    // completion, independent of delayed TurnStarted/TurnCompleted consumption.
    // The live host count additionally protects work still finishing in the host.
    pub(crate) fn begin_idle_drain(&self, running: &watch::Receiver<usize>) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active != 0 || state.running != 0 || *running.borrow() != 0 {
            return false;
        }
        state.closed = true;
        true
    }

    pub(crate) fn cancel_drain(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = false;
    }

    fn track_started_turn(&self) -> RunningTurnPermit {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // This task was already admitted. Explicit stop may close admission
        // between submission and registration; it must still wait for this task.
        state.running += 1;
        self.active_tx.send_replace(state.active);
        RunningTurnPermit(self.clone())
    }

    pub(crate) fn subscribe_active(&self) -> watch::Receiver<usize> {
        self.active_tx.subscribe()
    }

    // Admit and close take the same short lock. The permit keeps shutdown from
    // finishing while an earlier request is still preparing or submitting work.
    pub(crate) fn admit(&self) -> Result<TurnPermit, JSONRPCErrorError> {
        self.try_admit().ok_or_else(server_draining_error)
    }

    fn try_admit(&self) -> Option<TurnPermit> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return None;
        }
        state.active += 1;
        self.active_tx.send_replace(state.active);
        Some(TurnPermit(self.clone()))
    }
}

impl TurnStartAdmission for TurnAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        self.try_admit()
            .map(|permit| Box::new(permit) as Box<dyn Send>)
    }
}

// The turn store owns the guard until stop/abort (or teardown). No event
// consumer acknowledgement is needed to keep maintenance from claiming idle.
struct RunningTurnPermit(TurnAdmission);

impl Drop for RunningTurnPermit {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.running -= 1;
        // Wake pending maintenance even when the request-admission count is unchanged.
        self.0.active_tx.send_replace(state.active);
    }
}

impl TurnLifecycleContributor for TurnAdmission {
    fn on_turn_start<'a>(&'a self, input: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            input.turn_store.insert(self.track_started_turn());
        })
    }

    fn on_turn_stop<'a>(&'a self, input: TurnStopInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            input.turn_store.remove::<RunningTurnPermit>();
        })
    }

    fn on_turn_abort<'a>(&'a self, input: TurnAbortInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            input.turn_store.remove::<RunningTurnPermit>();
        })
    }
}

impl Drop for TurnPermit {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active -= 1;
        self.0.active_tx.send_replace(state.active);
    }
}

#[cfg(test)]
#[path = "turn_admission_tests.rs"]
mod tests;
