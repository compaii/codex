//! Tracks client runtime changes and turn starts admitted before a shutdown drain.

use codex_app_server_protocol::JSONRPCErrorError;
use codex_extension_api::TurnStartAdmission;
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

    // Core's task-lifetime guard bridges submission to actual task
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

    fn admit_delegated_turn_start(&self) -> Option<Box<dyn Send>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A closed maintenance gate has no live tasks and cannot be bypassed.
        // Explicit draining may still finish work delegated by an existing task.
        if state.closed && state.running == 0 {
            return None;
        }
        state.active += 1;
        self.active_tx.send_replace(state.active);
        Some(Box::new(TurnPermit(self.clone())))
    }

    fn track_running_turn(&self) -> Arc<dyn Send + Sync> {
        Arc::new(self.track_started_turn())
    }
}

// Shared ownership spans the actual task future and its abort owner, including
// terminal persistence. Delayed host events cannot make a live task appear idle.
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
