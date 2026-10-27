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
