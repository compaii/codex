use super::*;

#[test]
fn running_daemon_replacement_prompt() {
    let request = InstallRequest {
        source: "/cli/package".into(),
        version: "0.0.0".into(),
        destination: "/home/packages/app-server-daemon".into(),
        installed_version: Some("0.152.0".into()),
        restart_required: true,
    };
    insta::assert_snapshot!(describe_install(&request), @r"
    Replace installed daemon version 0.152.0 with CLI version 0.0.0 from /cli/package.
    The daemon package will be installed in /home/packages/app-server-daemon.
    The selected package will be pinned. Run `codex app-server daemon update` to return to production updates.
    The running daemon will restart; active or queued work may be interrupted.
    ");
}

#[test]
fn stopped_only_confirmation_cannot_override_a_running_receiver_with_yes() {
    let mut request = InstallRequest {
        source: "/cli/package".into(),
        version: "0.0.0".into(),
        destination: "/home/packages/app-server-daemon".into(),
        installed_version: Some("0.152.0".into()),
        restart_required: true,
    };
    assert!(confirm_install_if_stopped(&request, true).is_err());
    request.restart_required = false;
    assert!(confirm_install_if_stopped(&request, true).unwrap());
}
