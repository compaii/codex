use super::TurnAdmission;
use crate::error_code::INVALID_REQUEST_ERROR_CODE;
use codex_extension_api::TurnStartAdmission;
use pretty_assertions::assert_eq;

#[test]
fn drain_rejects_new_work_without_waiting_for_admitted_work() {
    let admission = TurnAdmission::default();
    let active = admission.subscribe_active();
    let in_flight = admission.admit().expect("initial request admitted");
    let automatic = admission
        .admit_turn_start()
        .expect("automatic start admitted");
    admission.begin_drain();
    assert_eq!(*active.borrow(), 2);
    assert!(admission.admit_turn_start().is_none());

    let error = admission.admit().err().expect("new request rejected");
    assert_eq!(error.code, INVALID_REQUEST_ERROR_CODE);
    assert_eq!(
        error.data,
        Some(serde_json::json!({ "reason": "serverShuttingDown" }))
    );
    drop(in_flight);
    assert_eq!(*active.borrow(), 1);
    drop(automatic);
    assert_eq!(*active.borrow(), 0);
}

#[test]
fn idle_maintenance_checks_live_turns_and_preparing_starts_before_closing() {
    let admission = TurnAdmission::default();
    let (running_tx, running) = tokio::sync::watch::channel(0);
    let permit = admission.admit().unwrap();
    assert!(!admission.begin_idle_drain(&running));
    // Publication happens before the start permit is released.
    running_tx.send_replace(1);
    drop(permit);
    assert!(!admission.begin_idle_drain(&running));
    assert!(admission.admit_turn_start().is_some());
    running_tx.send_replace(0);
    assert!(admission.begin_idle_drain(&running));
    assert!(admission.admit_turn_start().is_none());
    admission.cancel_drain();
    assert!(admission.admit_turn_start().is_some());
}

#[test]
fn idle_maintenance_waits_for_core_lifetime_when_host_events_are_delayed() {
    let admission = TurnAdmission::default();
    let (_, running) = tokio::sync::watch::channel(0);
    let submission = admission.admit().unwrap();
    let lifetime = admission.track_started_turn();
    drop(submission);
    // The host has not consumed TurnStarted, but Core already owns the task.
    assert!(!admission.begin_idle_drain(&running));
    assert!(admission.admit().is_ok());
    drop(lifetime);
    assert!(admission.begin_idle_drain(&running));
}

#[test]
fn core_lifetime_waits_for_both_task_future_and_abort_owner() {
    let admission = TurnAdmission::default();
    let (_, running) = tokio::sync::watch::channel(0);
    let future = admission.track_running_turn();
    let abort_owner = std::sync::Arc::clone(&future);
    drop(future);
    assert!(!admission.begin_idle_drain(&running));
    drop(abort_owner);
    assert!(admission.begin_idle_drain(&running));
}

#[test]
fn explicit_drain_finishes_delegated_work_but_idle_maintenance_cannot_be_bypassed() {
    let admission = TurnAdmission::default();
    let parent = admission.track_running_turn();
    admission.begin_drain();
    assert!(admission.admit_turn_start().is_none());
    let child = admission
        .admit_delegated_turn_start()
        .expect("finish existing delegation");
    drop(parent);
    assert!(admission.admit_delegated_turn_start().is_none());
    drop(child);
    admission.cancel_drain();
    let (_, running) = tokio::sync::watch::channel(0);
    assert!(admission.begin_idle_drain(&running));
    assert!(admission.admit_delegated_turn_start().is_none());
}
