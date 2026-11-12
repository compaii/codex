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
    bounded_turn_executor_journey(false, false)
}

#[test]
#[ignore = "requires the standalone executor/V8 binaries and an explicitly selected own user manager"]
fn bounded_turn_bootstraps_packaged_executor_under_denied_native_home() -> anyhow::Result<()> {
    bounded_turn_executor_journey(true, false)
}

#[test]
#[ignore = "requires the standalone executor/V8 binaries, system bwrap, cc and an explicitly selected own user manager"]
fn bounded_turn_bootstraps_denied_runtime_with_legacy_bwrap_mounts() -> anyhow::Result<()> {
    bounded_turn_executor_journey(true, true)
}

fn bounded_turn_executor_journey(
    runtime_under_private_home: bool,
    legacy_bwrap: bool,
) -> anyhow::Result<()> {
    core_test_support::test_codex::run_test_with_large_stack("owned-turn", move || async move {
        let binary = std::fs::canonicalize(std::env::var("CODEX_TEST_EXEC_SERVER_BIN")?)?;
        let native_home = tempfile::tempdir()?;
        let package = if runtime_under_private_home {
            tempfile::tempdir_in(native_home.path())?
        } else {
            tempfile::tempdir()?
        };
        let runner = std::env::current_exe()?;
        anyhow::ensure!(
            std::fs::metadata(&runner)?.len() + std::fs::metadata(&binary)?.len()
                <= 768 * 1024 * 1024,
            "test runtime package exceeds temporary copy bound"
        );
        // Both layouts use one real package, not symlinks to unrelated binaries.
        // Same-filesystem hard links cost no executable copy. EXDEV copies live
        // only for this fixture and are bounded together above.
        for (source, name) in [(&binary, "codex-exec-server"), (&runner, "codex")] {
            let destination = package.path().join(name);
            if let Err(error) = std::fs::hard_link(source, &destination) {
                if error.raw_os_error() != Some(libc::EXDEV) {
                    return Err(error.into());
                }
                std::fs::copy(source, destination)?;
            }
        }
        let credential = native_home.path().join("fake-provider-credential");
        std::fs::write(&credential, "test-only-provider-secret")?;
        let native_alias = native_home.path().join("codex-linux-sandbox");
        std::os::unix::fs::symlink(package.path().join("codex"), &native_alias)?;
        let recorder = CodeModeHostRecorder::new()?;
        let program = recorder.program();
        let workspace = program.parent().context("recorder workspace")?;
        let path = if legacy_bwrap {
            // Pre-sandbox executable discovery correctly rejects actor-writable
            // PATH entries. Keep this host-owned test wrapper in the denied
            // runtime package, outside the actor cwd and writable workspace.
            let wrapper = package.path().join("bwrap");
            let marker = workspace.join("legacy-bwrap-used");
            let actual_bwrap = std::fs::canonicalize("/usr/bin/bwrap")?;
            let source = package.path().join("legacy-bwrap.c");
            std::fs::write(
                &source,
                format!(
                    r#"#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
int main(int argc, char **argv) {{
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {{
        puts("--perms");
        return 0;
    }}
    int marker = open({marker:?}, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (marker < 0 || write(marker, "used\n", 5) != 5) return 110;
    close(marker);
    argv[0] = {actual_bwrap:?};
    execv(argv[0], argv);
    return 111;
}}
"#,
                    marker = marker.to_string_lossy(),
                    actual_bwrap = actual_bwrap.to_string_lossy(),
                ),
            )?;
            // The trusted launcher pins bwrap by descriptor. A shebang wrapper
            // cannot survive its close-on-exec launcher FD; use a tiny native
            // fixture without changing production descriptor ownership.
            let compile = std::process::Command::new("/usr/bin/cc")
                .args([
                    source.as_os_str(),
                    std::ffi::OsStr::new("-o"),
                    wrapper.as_os_str(),
                ])
                .output()?;
            anyhow::ensure!(
                compile.status.success(),
                "legacy bwrap fixture compile failed"
            );
            format!("{}:/usr/bin:/bin", package.path().display())
        } else {
            "/usr/bin:/bin".to_string()
        };
        let (session, _, _events) =
            crate::session::tests::make_session_and_context_with_auth_config_home_and_rx(
                codex_login::CodexAuth::from_api_key("Test API Key"),
                vec![],
                native_home.path(),
                |config| {
                    config.cwd =
                        AbsolutePathBuf::from_absolute_path(workspace).expect("absolute workspace");
                    config.codex_self_exe = Some(package.path().join("codex"));
                    config.codex_linux_sandbox_exe = Some(native_alias.clone());
                    config
                        .permissions
                        .set_permission_profile(PermissionProfile::Disabled)
                        .expect("fixture authority");
                },
            )
            .await;
        let mut filesystem = FileSystemSandboxPolicy::restricted(
            ["/usr", "/bin", "/lib", "/lib64", "/etc", "/proc"]
                .into_iter()
                .map(|path| {
                    Ok(FileSystemSandboxEntry::new(
                        AbsolutePathBuf::from_absolute_path(path)?.into(),
                        FileSystemAccessMode::Read,
                    ))
                })
                .collect::<std::io::Result<Vec<_>>>()?,
        );
        for executable in [
            std::env::current_exe()?,
            std::fs::canonicalize(std::env::var("CODEX_TEST_EXEC_SERVER_BIN")?)?,
            codex_utils_cargo_bin::cargo_bin("codex-code-mode-host")?,
        ] {
            filesystem.entries.push(FileSystemSandboxEntry::new(
                AbsolutePathBuf::from_absolute_path(executable)?.into(),
                FileSystemAccessMode::Read,
            ));
        }
        filesystem.entries.push(FileSystemSandboxEntry::new(
            AbsolutePathBuf::from_absolute_path("/tmp")?.into(),
            FileSystemAccessMode::Write,
        ));
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
                            ("PATH".into(), path.clone()),
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
        let environment = turn
            .command_environment_policy()
            .context("owned environment")?;
        let control_home = std::path::PathBuf::from(
            environment
                .r#set
                .get("CODEX_HOME")
                .context("executor control home")?,
        );
        assert!(control_home.starts_with(native_home.path()));
        assert!(control_home.exists());
        let uid = unsafe { libc::geteuid() };
        let script = format!(
            r#"import os, socket, subprocess, time
assert os.environ['HOME'] != os.environ['CODEX_HOME']
runtime = '/dev/codex-runtime'
assert os.listdir(runtime) == ['codex-linux-sandbox']
for action in [
    lambda: os.open(runtime + '/codex-linux-sandbox', os.O_WRONLY),
    lambda: os.unlink(runtime + '/codex-linux-sandbox'),
    lambda: os.rename(runtime, runtime + '-moved'),
    lambda: os.chmod(runtime, 0o777),
]:
    try: action()
    except OSError: pass
    else: raise AssertionError('runtime bootstrap mount mutable')
for descriptor in os.listdir('/proc/self/fd'):
    if int(descriptor) <= 2: continue
    try: target = os.readlink('/proc/self/fd/' + descriptor)
    except OSError: continue
    assert not target.endswith(('/codex', '/codex-exec-server', '/codex-linux-sandbox')), target
with open(os.path.join(os.environ['HOME'], 'actor-cache'), 'x') as cache: cache.write('owned')
try: os.listdir(os.environ['CODEX_HOME'])
except OSError: pass
else: raise AssertionError('executor control home readable')
native_home = os.path.dirname(os.environ['FAKE_PROVIDER'])
try:
    subprocess.run([os.path.join(native_home, 'codex-linux-sandbox'), '--version'],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
except OSError: pass
else: raise AssertionError('raw actor executed private runtime alias')
try: os.unlink(os.path.join(native_home, 'codex-linux-sandbox'))
except OSError: pass
else: raise AssertionError('trusted helper alias replaced')
try: os.rename(native_home, native_home + '-moved')
except OSError: pass
else: raise AssertionError('trusted helper directory renamed')
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
        assert!(!control_home.exists());
        assert!(!cgroup.exists());
        assert!(backend.start(parameters("after-finish")).await.is_err());
        assert_eq!(std::fs::read_to_string(workspace.join("effect"))?, "once\n");
        if legacy_bwrap {
            assert!(
                std::fs::read_to_string(workspace.join("legacy-bwrap-used"))
                    .context("legacy bubblewrap wrapper must have been selected")?
                    .contains("used")
            );
        }
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
