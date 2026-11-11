#![cfg(test)]

use super::LogFormat;
use super::ShutdownAction;
use super::ShutdownSignal;
use super::ShutdownState;
#[cfg(debug_assertions)]
use super::loader_overrides_with_test_user_config_file;
use super::turn_admission::TurnAdmission;
#[cfg(debug_assertions)]
use codex_config::LoaderOverrides;
use codex_extension_api::TurnStartAdmission;
#[cfg(debug_assertions)]
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::watch;

#[test]
fn shutdown_waits_for_admitted_requests_but_force_remains_available() {
    let admission = TurnAdmission::default();
    let active = admission.subscribe_active();
    let in_flight = admission.admit().expect("request admitted");
    let mut shutdown = ShutdownState::default();
    let connection_count = 1;
    let running_turn_count = 0;
    let (_, running) = watch::channel(running_turn_count);
    shutdown.on_signal(
        ShutdownSignal::Forceable,
        connection_count,
        running_turn_count,
        &admission,
    );
    assert!(matches!(
        shutdown.update(&running, *active.borrow(), connection_count, &admission),
        ShutdownAction::Noop
    ));
    shutdown.on_signal(
        ShutdownSignal::Forceable,
        connection_count,
        running_turn_count,
        &admission,
    );
    assert!(shutdown.forced());
    assert!(matches!(
        shutdown.update(&running, *active.borrow(), connection_count, &admission),
        ShutdownAction::Finish
    ));
    drop(in_flight);
}

#[cfg(unix)]
#[test]
fn maintenance_waits_without_closing_admission_then_commits_only_at_idle() {
    let admission = TurnAdmission::default();
    let (running_tx, running) = watch::channel(1);
    let active = admission.subscribe_active();
    let mut shutdown = ShutdownState::default();
    for _ in 0..2 {
        shutdown.on_signal(ShutdownSignal::GracefulOnly, 2, 1, &admission);
        assert!(matches!(
            shutdown.update(&running, *active.borrow(), 2, &admission),
            ShutdownAction::Noop
        ));
        assert!(!shutdown.draining);
        assert!(!shutdown.forced());
        drop(admission.admit().expect("other clients remain admitted"));
    }
    // A start still preparing work must also keep maintenance out.
    let start = admission.admit().expect("request admitted before idle");
    running_tx.send_replace(0);
    assert!(matches!(
        shutdown.update(&running, *active.borrow(), 2, &admission),
        ShutdownAction::Noop
    ));
    drop(start);
    assert!(matches!(
        shutdown.update(&running, *active.borrow(), 2, &admission),
        ShutdownAction::Finish
    ));
    assert!(admission.admit().is_err());
    assert!(shutdown.maintenance_deadline.is_some());
}

#[cfg(unix)]
#[test]
fn maintenance_stalled_snapshot_reopens_admission_and_cancels_handoff() {
    let admission = TurnAdmission::default();
    let (_, running) = watch::channel(0);
    let mut shutdown = ShutdownState::default();
    shutdown.on_signal(ShutdownSignal::GracefulOnly, 1, 0, &admission);
    assert!(matches!(
        shutdown.update(&running, 0, 1, &admission),
        ShutdownAction::Finish
    ));
    assert!(admission.admit().is_err());
    // A stalled snapshot must never leave the shared server closed.
    let resumed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&resumed);
    assert!(
        admission
            .admit_goal_continuation(
                codex_protocol::ThreadId::new(),
                Box::new(move || {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                })
            )
            .is_none()
    );
    shutdown.maintenance_deadline = Some(tokio::time::Instant::now());
    assert!(matches!(
        shutdown.update(&running, 0, 1, &admission),
        ShutdownAction::ResumeServing
    ));
    assert!(!shutdown.requested());
    assert!(admission.admit().is_ok());
    assert!(resumed.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn maintenance_does_not_rearm_after_queue_expiry_or_lost_observer() {
    let admission = TurnAdmission::default();
    for abandoned in [false, true] {
        let mut shutdown = ShutdownState::default();
        let (reply, observer) = oneshot::channel();
        let (_flushed, flush_complete) = oneshot::channel();
        let expires = if abandoned {
            drop(observer);
            tokio::time::Instant::now() + std::time::Duration::from_secs(120)
        } else {
            tokio::time::Instant::now()
        };
        shutdown.on_maintenance_request(expires, reply, flush_complete, &admission);
        assert!(!shutdown.requested());
        assert!(admission.admit().is_ok());
    }
    let mut shutdown = ShutdownState::default();
    let (reply, _observer) = oneshot::channel();
    let (_flushed, flush_complete) = oneshot::channel();
    let received_expiry = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    shutdown.on_maintenance_request(received_expiry, reply, flush_complete, &admission);
    assert_eq!(shutdown.maintenance_deadline, Some(received_expiry));
    assert!(
        admission
            .admit_goal_continuation(codex_protocol::ThreadId::new(), Box::new(|| {}))
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn maintenance_pending_request_expires_without_ever_closing_admission() {
    let admission = TurnAdmission::default();
    let (_, running) = watch::channel(1);
    let mut shutdown = ShutdownState::default();
    shutdown.on_signal(ShutdownSignal::GracefulOnly, 1, 1, &admission);
    assert!(!shutdown.draining);
    let resumed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&resumed);
    assert!(
        admission
            .admit_goal_continuation(
                codex_protocol::ThreadId::new(),
                Box::new(move || {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                })
            )
            .is_none()
    );
    shutdown.maintenance_deadline = Some(tokio::time::Instant::now());
    assert!(matches!(
        shutdown.update(&running, 0, 1, &admission),
        ShutdownAction::ResumeServing
    ));
    assert!(!shutdown.requested());
    assert!(admission.admit().is_ok());
    assert!(resumed.load(std::sync::atomic::Ordering::SeqCst));
}

#[cfg(unix)]
#[test]
fn maintenance_request_is_not_the_first_of_two_forceable_stop_signals() {
    let admission = TurnAdmission::default();
    let mut shutdown = ShutdownState::default();
    shutdown.on_signal(ShutdownSignal::GracefulOnly, 1, 1, &admission);
    shutdown.on_signal(ShutdownSignal::Forceable, 1, 1, &admission);
    assert!(!shutdown.forced());
    assert!(shutdown.draining);
    assert!(!shutdown.maintenance);
    assert!(admission.admit().is_err());
    shutdown.on_signal(ShutdownSignal::Forceable, 1, 1, &admission);
    assert!(shutdown.forced());
}

#[test]
fn log_format_from_env_value_matches_json_values_case_insensitively() {
    assert_eq!(LogFormat::from_env_value(Some("json")), LogFormat::Json);
    assert_eq!(LogFormat::from_env_value(Some("JSON")), LogFormat::Json);
    assert_eq!(LogFormat::from_env_value(Some("  Json  ")), LogFormat::Json);
}

#[test]
fn log_format_from_env_value_defaults_for_non_json_values() {
    assert_eq!(
        LogFormat::from_env_value(/*value*/ None),
        LogFormat::Default
    );
    assert_eq!(LogFormat::from_env_value(Some("")), LogFormat::Default);
    assert_eq!(LogFormat::from_env_value(Some("text")), LogFormat::Default);
    assert_eq!(LogFormat::from_env_value(Some("jsonl")), LogFormat::Default);
}

#[cfg(debug_assertions)]
#[test]
fn debug_test_user_config_file_overrides_loader_path() {
    let path = std::env::temp_dir().join("codex-app-server-test-config.toml");
    let loader_overrides =
        loader_overrides_with_test_user_config_file(LoaderOverrides::default(), Some(path.clone()))
            .expect("test config path should be valid");

    assert_eq!(
        loader_overrides.user_config_path,
        Some(AbsolutePathBuf::from_absolute_path(path).expect("absolute test path"))
    );
}

#[path = "stderr_logging_tests.rs"]
mod stderr_logging_tests;
