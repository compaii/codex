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

#[test]
fn maintenance_withholds_only_goals_and_resumes_latest_observation_once() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    let admission = TurnAdmission::default();
    let thread = codex_protocol::ThreadId::new();
    let resumed = Arc::new(AtomicUsize::new(0));
    let admitted_before = admission
        .admit_goal_continuation(thread, Box::new(|| {}))
        .unwrap();
    admission.defer_goal_continuations();
    for value in [1, 10] {
        let resumed = Arc::clone(&resumed);
        assert!(
            admission
                .admit_goal_continuation(
                    thread,
                    Box::new(move || {
                        resumed.fetch_add(value, Ordering::SeqCst);
                    })
                )
                .is_none()
        );
    }
    assert!(admission.admit().is_ok(), "human starts remain available");
    let parent = admission.track_running_turn();
    assert!(admission.admit_delegated_turn_start().is_some());
    let (_, running) = tokio::sync::watch::channel(0);
    drop(parent);
    assert!(
        !admission.begin_idle_drain(&running),
        "pre-admitted goal still preparing"
    );
    drop(admitted_before);
    assert!(admission.begin_idle_drain(&running));
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    admission.cancel_drain();
    admission.cancel_drain();
    assert_eq!(resumed.load(Ordering::SeqCst), 10);
    assert!(
        admission
            .admit_goal_continuation(thread, Box::new(|| {}))
            .is_some()
    );
}

#[test]
fn explicit_stop_discards_maintenance_retries() {
    let admission = TurnAdmission::default();
    admission.defer_goal_continuations();
    assert!(
        admission
            .admit_goal_continuation(
                codex_protocol::ThreadId::new(),
                Box::new(|| panic!("explicit stop must not restart withheld work"))
            )
            .is_none()
    );
    admission.begin_drain();
    admission.cancel_drain();
}

#[test]
fn maintenance_and_goal_reservation_have_one_atomic_order() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    // Check both possible scheduler orders repeatedly, without sleeps.
    for _ in 0..64 {
        let admission = TurnAdmission::default();
        let barrier = Arc::new(Barrier::new(2));
        let resumed = Arc::new(AtomicBool::new(false));
        let worker = {
            let admission = admission.clone();
            let barrier = Arc::clone(&barrier);
            let resumed = Arc::clone(&resumed);
            std::thread::spawn(move || {
                barrier.wait();
                admission.admit_goal_continuation(
                    codex_protocol::ThreadId::new(),
                    Box::new(move || {
                        resumed.store(true, Ordering::SeqCst);
                    }),
                )
            })
        };
        barrier.wait();
        admission.defer_goal_continuations();
        let permit = worker.join().unwrap();
        let (_, running) = tokio::sync::watch::channel(0);
        assert_eq!(admission.begin_idle_drain(&running), permit.is_none());
        admission.cancel_drain();
        assert_eq!(resumed.load(Ordering::SeqCst), permit.is_none());
    }
}

#[test]
fn legacy_hosts_do_not_acquire_an_extra_goal_quota_permit() {
    #[derive(Debug, Default)]
    struct Legacy(std::sync::atomic::AtomicUsize);
    impl TurnStartAdmission for Legacy {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(Box::new(()))
        }
    }
    let host = Legacy::default();
    let _reservation =
        host.admit_goal_continuation(codex_protocol::ThreadId::new(), Box::new(|| {}));
    assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    let _submission = host.admit_turn_start();
    assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 1);
}
