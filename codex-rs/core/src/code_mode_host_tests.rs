//! Exercises the process-owned gRPC transport against the standalone host.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_code_mode::CodeModeProcessMetricsSample;
use codex_code_mode::CodeModeSession;
use codex_code_mode::CodeModeSessionProvider;
use codex_code_mode::ExecuteRequest;
use codex_code_mode::FunctionCallOutputContentItem;
use codex_code_mode::NoopCodeModeSessionDelegate;
use codex_code_mode::RuntimeResponse;
#[cfg(unix)]
use core_test_support::code_mode_host::CodeModeHostRecorder;
use pretty_assertions::assert_eq;
use tokio::time::timeout;

use super::HostConnection;
use super::ProcessOwnedGrpcCodeModeSessionProvider;

const TEST_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 20);

#[cfg(unix)]
#[path = "code_mode_host_lifecycle_tests.rs"]
mod lifecycle;

async fn execute(
    session: &Arc<dyn CodeModeSession>,
    source: &str,
) -> Result<Vec<FunctionCallOutputContentItem>> {
    let response = timeout(TEST_TIMEOUT, async {
        session
            .execute(
                ExecuteRequest {
                    tool_call_id: "call-1".to_string(),
                    enabled_tools: Vec::new(),
                    source: source.to_string(),
                    yield_time_ms: Some(/*value*/ 5_000),
                    max_output_tokens: Some(/*value*/ 1_000),
                },
                Arc::new(NoopCodeModeSessionDelegate),
                /*preempt*/ None,
            )
            .await?
            .initial_response()
            .await
    })
    .await
    .context("timed out executing a stdio gRPC cell")?
    .map_err(anyhow::Error::msg)?;
    match response {
        RuntimeResponse::Result {
            content_items,
            error_text: None,
            ..
        } => Ok(content_items),
        other => anyhow::bail!("unexpected code-mode response: {other:?}"),
    }
}

#[tokio::test]
async fn grpc_stdio_sessions_share_a_host_and_isolate_state() -> Result<()> {
    #[cfg(unix)]
    let recorder = CodeModeHostRecorder::new()?;
    #[cfg(unix)]
    let host_program = recorder.program();
    #[cfg(not(unix))]
    let host_program = codex_utils_cargo_bin::cargo_bin("codex-code-mode-host")?;
    let provider = ProcessOwnedGrpcCodeModeSessionProvider::with_host_program(host_program);
    assert_eq!(
        provider
            .sample_process_metrics()
            .await
            .map_err(anyhow::Error::msg)?,
        CodeModeProcessMetricsSample::NotRunning
    );
    #[cfg(unix)]
    assert!(recorder.invocations()?.is_empty());
    let (first, second) = tokio::try_join!(provider.create_session(), provider.create_session())
        .map_err(anyhow::Error::msg)?;
    assert_eq!(
        provider
            .sample_process_metrics()
            .await
            .map_err(anyhow::Error::msg)?,
        CodeModeProcessMetricsSample::Unsupported
    );
    assert_eq!(
        execute(&first, "store(\"stored\", 42); text(load(\"stored\"));").await?,
        vec![FunctionCallOutputContentItem::InputText {
            text: "42".to_string()
        }]
    );
    assert_eq!(
        execute(&second, "text(typeof load(\"stored\"));").await?,
        vec![FunctionCallOutputContentItem::InputText {
            text: "undefined".to_string()
        }]
    );
    first.shutdown().await.map_err(anyhow::Error::msg)?;
    assert_eq!(
        execute(&second, "text(\"still open\");").await?,
        vec![FunctionCallOutputContentItem::InputText {
            text: "still open".to_string()
        }]
    );
    second.shutdown().await.map_err(anyhow::Error::msg)?;
    #[cfg(unix)]
    assert_eq!(
        recorder.invocations()?,
        vec!["--listen grpc+stdio://".to_string()],
        "both sessions must share one gRPC host process"
    );
    drop(first);
    drop(second);
    drop(provider);
    #[cfg(unix)]
    {
        let pid = recorder.pid()?;
        timeout(TEST_TIMEOUT, async {
            while unsafe {
                libc::kill(pid as libc::pid_t, /*sig*/ 0)
            } == 0
            {
                tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
            }
        })
        .await
        .context("metrics retained the host after provider teardown")?;
    }
    Ok(())
}

#[tokio::test]
async fn grpc_stdio_host_exits_when_stdin_closes() -> Result<()> {
    let mut connection = HostConnection::spawn(
        &codex_utils_cargo_bin::cargo_bin("codex-code-mode-host")?,
        None,
        None,
        None,
    )
    .await?;
    let mut child = connection.child.take().context("host process")?;
    drop(connection);
    let status = timeout(TEST_TIMEOUT, child.wait())
        .await
        .context("host did not exit on stdin EOF")??;
    assert!(status.success());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn hung_grpc_stdio_host_is_reaped_and_the_session_recovers() -> Result<()> {
    let recorder = CodeModeHostRecorder::new()?;
    let provider = ProcessOwnedGrpcCodeModeSessionProvider::with_host_program(recorder.program());
    let session = provider
        .create_session()
        .await
        .map_err(anyhow::Error::msg)?;
    let pid = recorder.pid()?;
    // Suspended processes keep their pipes open: EOF cannot detect this failure.
    if unsafe { libc::kill(pid as libc::pid_t, libc::SIGSTOP) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // A metrics deadline cancels only its stream. It must neither reap the
    // suspended child nor advance the independent 30s/10s keepalive deadline.
    assert!(provider.sample_process_metrics().await.is_err());
    assert_eq!(
        unsafe {
            libc::kill(pid as libc::pid_t, /*sig*/ 0)
        },
        0
    );
    assert_eq!(recorder.invocations()?.len(), 1);
    timeout(Duration::from_secs(/*secs*/ 55), async {
        while unsafe {
            libc::kill(pid as libc::pid_t, /*sig*/ 0)
        } == 0
        {
            tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
        }
    })
    .await
    .context("unresponsive host was not killed and reaped")?;
    assert_eq!(
        provider
            .sample_process_metrics()
            .await
            .map_err(anyhow::Error::msg)?,
        CodeModeProcessMetricsSample::NotRunning
    );
    assert_eq!(
        recorder.invocations()?.len(),
        1,
        "metrics must not restart a dead host"
    );
    let reset = execute(&session, r#"store("recovery", "must not run");"#)
        .await
        .unwrap_err();
    assert!(
        reset
            .to_string()
            .contains("Stored values and running cells were lost")
    );
    assert_eq!(
        execute(&session, r#"text(load("recovery") ?? "recovered");"#).await?,
        vec![FunctionCallOutputContentItem::InputText {
            text: "recovered".to_string()
        }]
    );
    assert_eq!(
        recorder.invocations()?,
        vec!["--listen grpc+stdio://".to_string(); 2],
        "the session must reconnect to a fresh gRPC host"
    );
    session.shutdown().await.map_err(anyhow::Error::msg)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires an explicitly selected local user-manager resource qualification"]
async fn resource_owned_grpc_host_keeps_sandbox_and_stops_with_group() -> Result<()> {
    use codex_protocol::permissions::FileSystemAccessMode;
    use codex_protocol::permissions::FileSystemSandboxEntry;
    use codex_protocol::permissions::FileSystemSandboxPolicy;
    use codex_protocol::permissions::NetworkSandboxPolicy;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use codex_utils_pty::linux_resources::LinuxProcessLimits;
    use codex_utils_pty::linux_resources::LinuxProcessResourceGroup;
    let recorder = CodeModeHostRecorder::new()?;
    recorder.enable_inheritable_descriptor_probe().await?;
    let program = recorder.program();
    let root = program.parent().context("recorder directory")?;
    let alias_dir = tempfile::tempdir()?;
    let alias = alias_dir.path().join("codex-linux-sandbox");
    std::os::unix::fs::symlink(std::env::current_exe()?, &alias)?;
    let mut policy = FileSystemSandboxPolicy::read_only();
    for (path, mode) in [
        (root.to_path_buf(), FileSystemAccessMode::Write),
        (root.join("blocked credential"), FileSystemAccessMode::Deny),
        (
            std::path::PathBuf::from("/sys/fs/cgroup"),
            FileSystemAccessMode::Deny,
        ),
        (
            std::path::PathBuf::from(format!("/run/user/{}", unsafe { libc::geteuid() })),
            FileSystemAccessMode::Deny,
        ),
    ] {
        policy.entries.push(FileSystemSandboxEntry::new(
            AbsolutePathBuf::from_absolute_path(path)?.into(),
            mode,
        ));
    }
    let group = Arc::new(
        LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: 512 * 1024 * 1024,
                cpu_percent: 50,
                tasks: 128,
            },
            Duration::from_secs(30),
        )
        .await?,
    );
    let guarded_program = root.join("resource guard wrapper");
    codex_utils_cargo_bin::write_executable(
        &guarded_program,
        &format!(
            r#"#!/bin/sh
/usr/bin/python3 - <<'PY_RESOURCE_GUARD'
import errno, os, socket
try:
    os.listdir('/sys/fs/cgroup')
except OSError as e:
    assert e.errno in (errno.EPERM, errno.EACCES, errno.ENOENT), e
else:
    raise AssertionError('resource cgroup path exposed')
s = None
try:
    s = socket.socket(socket.AF_UNIX)
    s.connect('/run/user/{uid}/bus')
except OSError as e:
    assert e.errno in (errno.EPERM, errno.EACCES, errno.ENOENT), e
else:
    raise AssertionError('resource manager bus connected')
finally:
    if s is not None: s.close()
PY_RESOURCE_GUARD
[ "$?" -eq 0 ] || exit 1
exec {recorder_program} "$@"
"#,
            uid = unsafe { libc::geteuid() },
            recorder_program = shlex::try_quote(program.to_str().context("host path")?)?
        ),
    )?;
    let provider = ProcessOwnedGrpcCodeModeSessionProvider::with_host_program(guarded_program);
    let session = provider
        .create_isolated_session_with_options(
            codex_code_mode::CodeModeSessionOptions {
                cell_execution_limits: codex_code_mode::CodeModeSessionCellExecutionLimits {
                    max_yield_time_ms: Some(1000),
                    max_heap_size_bytes: Some(32 * 1024 * 1024),
                },
                ..Default::default()
            },
            codex_code_mode::CodeModeHostOptions {
                environment: Some(std::collections::HashMap::from([
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    (
                        "CODEX_TEST_CODE_MODE_RECORDER".into(),
                        root.to_string_lossy().into_owned(),
                    ),
                ])),
                sandbox: Some(codex_code_mode::CodeModeHostSandbox {
                    permissions:
                        codex_protocol::models::PermissionProfile::from_runtime_permissions(
                            &policy,
                            NetworkSandboxPolicy::Restricted,
                        ),
                    cwd: root.to_path_buf(),
                    linux_sandbox_exe: alias,
                }),
                process_launcher: Some(group.clone()),
            },
        )
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(
        execute(&session, "text('resource-owned');").await?,
        vec![FunctionCallOutputContentItem::InputText {
            text: "resource-owned".into()
        }]
    );
    assert_eq!(
        std::fs::read_to_string(root.join("guard result"))?,
        "guarded"
    );
    let pid = recorder.pid()?;
    let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    assert!(membership.trim().ends_with(group.identity().0));
    group.close().await?;
    assert!(
        execute(&session, "text('must not restart');")
            .await
            .is_err()
    );
    assert_eq!(recorder.invocations()?.len(), 1);
    Ok(())
}
