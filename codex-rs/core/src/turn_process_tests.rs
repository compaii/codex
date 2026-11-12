use super::*;
use anyhow::Context;
use codex_exec_server::ExecParams;
use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ProcessId;
use codex_protocol::config_types::ShellEnvironmentPolicy;
use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::FileSystemAccessMode;
use codex_protocol::permissions::FileSystemSandboxEntry;
use codex_protocol::permissions::FileSystemSandboxPolicy;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_protocol::turn_input::TurnCodeModeLimits;
use codex_utils_absolute_path::AbsolutePathBuf;
use core_test_support::code_mode_host::CodeModeHostRecorder;
use std::time::Duration;

#[test]
#[ignore = "requires the standalone executor/V8 binaries and an explicitly selected own user manager"]
fn bounded_turn_shares_actual_executors_and_closes_retained_steps() -> anyhow::Result<()> {
    core_test_support::test_codex::run_test_with_large_stack("owned-turn", || async {
        let binary = std::fs::canonicalize(std::env::var("CODEX_TEST_EXEC_SERVER_BIN")?)?;
        let package = tempfile::tempdir()?;
        std::os::unix::fs::symlink(binary, package.path().join("codex-exec-server"))?;
        let native_home = tempfile::tempdir()?;
        let credential = native_home.path().join("fake-provider-credential");
        std::fs::write(&credential, "test-only-provider-secret")?;
        let recorder = CodeModeHostRecorder::new()?;
        let program = recorder.program();
        let workspace = program.parent().context("recorder workspace")?;
        let (session, _, _events) =
            crate::session::tests::make_session_and_context_with_auth_config_home_and_rx(
                codex_login::CodexAuth::from_api_key("Test API Key"),
                vec![],
                native_home.path(),
                |config| {
                    config.cwd =
                        AbsolutePathBuf::from_absolute_path(workspace).expect("absolute workspace");
                    config.codex_self_exe = Some(package.path().join("codex"));
                    config.codex_linux_sandbox_exe =
                        Some(std::env::current_exe().expect("sandbox helper"));
                    config
                        .permissions
                        .set_permission_profile(PermissionProfile::Disabled)
                        .expect("fixture authority");
                },
            )
            .await;
        let mut filesystem = FileSystemSandboxPolicy::read_only();
        filesystem.entries.push(FileSystemSandboxEntry::new(
            AbsolutePathBuf::from_absolute_path(workspace)?.into(),
            FileSystemAccessMode::Write,
        ));
        filesystem.entries.push(FileSystemSandboxEntry::new(
            AbsolutePathBuf::from_absolute_path(workspace.join("blocked credential"))?.into(),
            FileSystemAccessMode::Deny,
        ));
        let (turn, _) = session
            .new_turn_with_sub_id(
                "bounded-process-owner".into(),
                Default::default(),
                crate::session::turn_context::NewTurnContextOptions {
                    process_limits: Some(TurnProcessLimits {
                        memory_bytes: 512 * 1024 * 1024,
                        cpu_percent: 50,
                        tasks: 128,
                    }),
                    code_mode_limits: Some(TurnCodeModeLimits {
                        max_heap_size_bytes: 32 * 1024 * 1024,
                        max_yield_time_ms: 1000,
                    }),
                    permission_ceiling: Some(PermissionProfile::from_runtime_permissions(
                        &filesystem,
                        NetworkSandboxPolicy::Restricted,
                    )),
                    command_environment_policy: Some(ShellEnvironmentPolicy {
                        inherit: ShellEnvironmentPolicyInherit::None,
                        r#set: HashMap::from([
                            ("PATH".into(), "/usr/bin:/bin".into()),
                            (
                                "CODEX_TEST_CODE_MODE_RECORDER".into(),
                                workspace.to_string_lossy().into_owned(),
                            ),
                        ]),
                        ..Default::default()
                    }),
                    tool_ceiling: Some(vec![]),
                    ..Default::default()
                },
            )
            .await?;
        let allowance = Arc::new(codex_api::InferenceAllowance::new(
            1,
            4096,
            tokio::time::Instant::now() + Duration::from_secs(60),
        )?);
        allowance.claim_attempt()?;
        turn.extension_data.insert(allowance.clone());
        let cancel = CancellationToken::new();
        // Exercise the real step preparation route, not a test-only replacement.
        let step = session.capture_step_context(turn.clone(), &cancel).await?;
        let selected = step
            .environments
            .turn_environments()
            .next()
            .context("owned selected environment")?;
        assert!(selected.environment.is_owned_local());
        let home = selected
            .user_home_dir
            .as_ref()
            .context("private home")?
            .to_abs_path()?;
        assert!(home.as_path().exists());
        let uid = unsafe { libc::geteuid() };
        let script = format!(
            r#"import os, socket, time
assert os.environ['HOME'] == os.environ['CODEX_HOME']
for path in [os.environ['FAKE_PROVIDER'], '/sys/fs/cgroup/cgroup.procs']:
    try: open(path).read()
    except OSError: pass
    else: raise AssertionError('host authority readable')
try:
    s = socket.socket(socket.AF_UNIX)
    s.connect('/run/user/{uid}/bus')
except OSError: pass
else: raise AssertionError('host manager reachable')
finally: s.close()
with open('effect', 'a') as f: f.write('once\n')
print('owned-turn-ready', flush=True)
time.sleep(120)
"#
        );
        let parameters = |id: &str| ExecParams {
            process_id: ProcessId::from(id),
            metadata: Default::default(),
            argv: vec!["/usr/bin/python3".into(), "-c".into(), script.clone()],
            cwd: selected.cwd().clone(),
            env_policy: Some(codex_exec_server::ExecEnvPolicy {
                inherit: ShellEnvironmentPolicyInherit::None,
                ignore_default_excludes: false,
                exclude: vec![],
                include_only: vec![],
                set: crate::exec_env::create_env(selected.shell_environment_policy(), None),
            }),
            env: HashMap::from([(
                "FAKE_PROVIDER".into(),
                credential.to_string_lossy().into_owned(),
            )]),
            shell_snapshot: None,
            tty: false,
            pipe_stdin: false,
            arg0: None,
            sandbox: Some(
                selected
                    .sandbox_context_with_permissions(None)
                    .expect("execution ceiling"),
            ),
            enforce_managed_network: false,
            managed_network: None,
            network_proxy: None,
        };
        let backend = selected.environment.get_exec_backend();
        let raw = backend.start(parameters("first")).await?;
        let mut events = raw.process.subscribe_events();
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut text = Vec::new();
            loop {
                match events.recv().await? {
                    ExecProcessEvent::Output(chunk) => {
                        text.extend_from_slice(&chunk.chunk.0);
                        if String::from_utf8_lossy(&text).contains("owned-turn-ready") {
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                    event => anyhow::bail!("raw turn stopped early: {event:?}; {text:?}"),
                }
            }
        })
        .await??;
        let service = crate::tools::code_mode::CodeModeService::new(
            session.thread_id,
            Arc::new(
                crate::code_mode_host::ProcessOwnedGrpcCodeModeSessionProvider::with_host_program(
                    program.clone(),
                ),
            ),
            &crate::config::CodeModeConfig::default(),
            Default::default(),
        );
        let cell = service
            .execute(
                codex_code_mode::ExecuteRequest {
                    tool_call_id: "owned-v8".into(),
                    enabled_tools: vec![],
                    source: "text('owned-v8');".into(),
                    yield_time_ms: Some(1000),
                    max_output_tokens: Some(500),
                },
                step.clone(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
        let response = cell.initial_response().await.map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            format!("{response:?}").contains("owned-v8"),
            "V8 did not return useful output"
        );
        let state = turn
            .extension_data
            .get::<ProcessResources>()
            .context("captured process owner")?;
        let owner = state
            .started
            .get()
            .context("started owner")?
            .borrow()
            .clone()
            .context("owner result")?
            .map_err(anyhow::Error::msg)?;
        let membership = std::fs::read_to_string(format!("/proc/{}/cgroup", recorder.pid()?))?;
        anyhow::ensure!(
            membership.trim().ends_with(owner.group.identity().0),
            "V8 used a different aggregate"
        );
        let shown = tokio::process::Command::new("/usr/bin/systemctl")
            .args([
                "--user",
                "show",
                owner.group.identity().0,
                "--property=ControlGroup",
                "--value",
            ])
            .output()
            .await?;
        anyhow::ensure!(shown.status.success(), "own group inspection failed");
        let cgroup = std::path::Path::new("/sys/fs/cgroup").join(
            String::from_utf8(shown.stdout)?
                .trim()
                .trim_start_matches('/'),
        );
        let mut commands = Vec::new();
        for pid in std::fs::read_to_string(cgroup.join("cgroup.procs"))?.split_whitespace() {
            commands.push(std::fs::read_to_string(format!("/proc/{pid}/comm"))?);
        }
        anyhow::ensure!(
            commands.iter().any(|s| s.contains("codex-exec-serv"))
                && commands.iter().any(|s| s.contains("python3")),
            "raw executor/tool outside aggregate"
        );
        let again = session.capture_step_context(turn.clone(), &cancel).await?;
        assert!(Arc::ptr_eq(
            &selected.environment,
            &again
                .environments
                .turn_environments()
                .next()
                .context("second step")?
                .environment
        ));
        allowance.close();
        let mut cleanup = owner.cleanup.clone();
        let result =
            tokio::time::timeout(Duration::from_secs(10), cleanup.wait_for(Option::is_some))
                .await??
                .clone()
                .context("cleanup result")?;
        result.map_err(anyhow::Error::msg)?;
        assert!(!home.as_path().exists());
        assert!(!cgroup.exists());
        assert!(backend.start(parameters("after-finish")).await.is_err());
        assert_eq!(std::fs::read_to_string(workspace.join("effect"))?, "once\n");
        let (ordinary, _) = session
            .new_turn_with_sub_id(
                "ordinary-after-process-owner".into(),
                Default::default(),
                Default::default(),
            )
            .await?;
        assert!(ordinary.extension_data.get::<ProcessResources>().is_none());
        assert!(
            !ordinary
                .initial_environments
                .turn_environments()
                .next()
                .context("ordinary environment")?
                .environment
                .is_owned_local()
        );
        Ok(())
    })
}
