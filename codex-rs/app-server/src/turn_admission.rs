//! Tracks client runtime changes and turn starts admitted before a shutdown drain.

use codex_app_server_protocol::JSONRPCErrorError;
use codex_extension_api::TurnStartAdmission;
use codex_protocol::ThreadId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

use crate::error_code::invalid_request;
use crate::error_code::server_draining_error;

#[derive(Default)]
struct AdmissionState {
    closed: bool,
    paused: bool,
    active: usize,
    running: usize,
    maintenance_pending: bool,
    // At most one callback per live goal runtime, retained only for the existing
    // bounded maintenance request. Callbacks capture weak runtime references.
    withheld_goals: BTreeMap<String, Box<dyn FnOnce() + Send>>,
}

#[derive(Clone)]
pub(crate) struct TurnAdmission {
    state: Arc<Mutex<AdmissionState>>,
    active_tx: watch::Sender<usize>,
}

impl std::fmt::Debug for TurnAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnAdmission").finish_non_exhaustive()
    }
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
pub(crate) struct TurnAdmissionPause(TurnAdmission);

impl TurnAdmission {
    pub(crate) async fn pause(&self) -> Result<TurnAdmissionPause, JSONRPCErrorError> {
        let mut active = self.subscribe_active();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed || state.paused {
                return Err(invalid_request("Runtime changes are already paused"));
            }
            state.paused = true;
        }
        let pause = TurnAdmissionPause(self.clone());
        tokio::time::timeout(
            std::time::Duration::from_secs(/*secs*/ 10),
            active.wait_for(|active| *active == 0),
        )
        .await
        .map_err(|_| invalid_request("Work is still starting; retry the workspace switch"))?
        .map_err(|_| invalid_request("Turn admission is unavailable"))?;
        Ok(pause)
    }

    pub(crate) fn begin_drain(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.maintenance_pending = false;
        state.withheld_goals.clear();
    }

    pub(crate) fn defer_goal_continuations(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .maintenance_pending = true;
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
        let withheld = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = false;
            state.maintenance_pending = false;
            std::mem::take(&mut state.withheld_goals)
        };
        // Never invoke runtime code while holding the admission lock.
        for resume in withheld.into_values() {
            resume();
        }
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
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(server_draining_error());
        }
        if state.paused {
            return Err(invalid_request(
                "Workspace change in progress; retry when it completes",
            ));
        }
        state.active += 1;
        self.active_tx.send_replace(state.active);
        Ok(TurnPermit(self.clone()))
    }
}

impl Drop for TurnAdmissionPause {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused = false;
    }
}

impl TurnStartAdmission for TurnAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        self.admit()
            .ok()
            .map(|permit| Box::new(permit) as Box<dyn Send>)
    }

    fn admit_goal_continuation(
        &self,
        thread_id: ThreadId,
        on_deferred: Box<dyn FnOnce() + Send>,
    ) -> Option<Box<dyn Send>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.maintenance_pending {
            // Replace an older idle observation for this same runtime. Later
            // human work/goal changes must not be resumed using stale context.
            state
                .withheld_goals
                .insert(thread_id.to_string(), on_deferred);
            return None;
        }
        if state.closed || state.paused {
            return None;
        }
        state.active += 1;
        self.active_tx.send_replace(state.active);
        Some(Box::new(TurnPermit(self.clone())))
    }

    fn admit_delegated_turn_start(&self) -> Option<Box<dyn Send>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A closed maintenance gate has no live tasks and cannot be bypassed.
        // Explicit draining may still finish work delegated by an existing task.
        if state.paused || (state.closed && state.running == 0) {
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
