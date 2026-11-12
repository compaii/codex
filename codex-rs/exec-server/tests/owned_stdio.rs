#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_exec_server::Environment;
use codex_exec_server::ExecEnvPolicy;
use codex_exec_server::ExecParams;
use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ExecServerRuntimeOptions;
use codex_exec_server::FileSystemSandboxContext;
use codex_exec_server::ProcessId;
use codex_exec_server::StdioExecServerCommand;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::FileSystemAccessMode;
use codex_protocol::permissions::FileSystemSandboxEntry;
use codex_protocol::permissions::FileSystemSandboxPolicy;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use codex_utils_pty::linux_resources::LinuxProcessLimits;
use codex_utils_pty::linux_resources::LinuxProcessResourceGroup;

#[ctor::ctor]
fn initialize_spawn_helper() {
    codex_utils_pty::init_spawn_helper(std::env::args_os());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CODEX_TEST_EXEC_SERVER_BIN and an explicitly selected local user manager"]
async fn owned_stdio_limits_actual_executor_and_child_without_reconnecting() -> Result<()> {
    let binary = std::fs::canonicalize(std::env::var("CODEX_TEST_EXEC_SERVER_BIN")?)?;
    let root = tempfile::tempdir()?;
    let home = root.path().join("empty-home");
    std::fs::create_dir(&home)?;
    let denied = root.path().join("fake-credential");
    std::fs::write(&denied, "test-only-secret")?;
    let inherited = std::fs::File::open(&denied)?;
    let descriptor = inherited.as_raw_fd();
    // Only this test-owned descriptor. The admitted executor must close it
    // before exec, independently of the target's subsequent filesystem policy.
    assert_ne!(unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) }, -1);
    let group = Arc::new(
        LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: 512 * 1024 * 1024,
                cpu_percent: 50,
                tasks: 128,
            },
            Duration::from_secs(60),
        )
        .await?,
    );
    let environment = Environment::owned_stdio(
        StdioExecServerCommand {
            program: binary.to_string_lossy().into_owned(),
            args: vec!["--listen".into(), "stdio://".into()],
            cwd: Some(root.path().to_path_buf()),
            env: HashMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), home.to_string_lossy().into_owned()),
                ("CODEX_HOME".into(), home.to_string_lossy().into_owned()),
            ]),
        },
        group.clone(),
        ExecServerRuntimeOptions::new(binary, None)?,
        HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
    )?;
    let uid = unsafe { libc::geteuid() };
    let cwd = PathUri::from_host_native_path(root.path())?;
    let mut policy = FileSystemSandboxPolicy::read_only();
    for (path, mode) in [
        (root.path().to_path_buf(), FileSystemAccessMode::Write),
        (denied, FileSystemAccessMode::Deny),
        ("/sys/fs/cgroup".into(), FileSystemAccessMode::Deny),
        (
            format!("/run/user/{uid}").into(),
            FileSystemAccessMode::Deny,
        ),
    ] {
        policy.entries.push(FileSystemSandboxEntry::new(
            AbsolutePathBuf::from_absolute_path(path)?.into(),
            mode,
        ));
    }
    let params = |id: &str| ExecParams {
        process_id: ProcessId::from(id),
        metadata: Default::default(),
        argv: vec![
            "/usr/bin/python3".into(),
            "-c".into(),
            format!(
                r#"import os, socket, time
assert 'OPENAI_API_KEY' not in os.environ
assert 'DBUS_SESSION_BUS_ADDRESS' not in os.environ
assert not any(os.path.realpath('/proc/self/fd/' + n).endswith('/fake-credential') for n in os.listdir('/proc/self/fd'))
for path in ['fake-credential', '/sys/fs/cgroup/cgroup.procs']:
    try:
        open(path).read()
    except OSError:
        pass
    else:
        raise AssertionError('protected path readable: ' + path)
try:
    s = socket.socket(socket.AF_UNIX)
    s.connect('/run/user/{uid}/bus')
except OSError:
    pass
else:
    raise AssertionError('resource manager reachable')
finally:
    s.close()
with open('effect', 'a') as f: f.write('once\n')
print('owned-raw-ready', flush=True)
time.sleep(120)
"#
            ),
        ],
        cwd: cwd.clone(),
        env_policy: Some(ExecEnvPolicy {
            inherit: ShellEnvironmentPolicyInherit::None,
            ignore_default_excludes: false,
            exclude: Vec::new(),
            set: HashMap::new(),
            include_only: Vec::new(),
        }),
        shell_snapshot: None,
        env: HashMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        tty: false,
        pipe_stdin: false,
        arg0: None,
        sandbox: Some(FileSystemSandboxContext::from_permission_profile(
            PermissionProfile::from_runtime_permissions(&policy, NetworkSandboxPolicy::Restricted),
            cwd.clone(),
        )),
        enforce_managed_network: false,
        managed_network: None,
        network_proxy: None,
    };
    let backend = environment.get_exec_backend();
    let started =
        tokio::time::timeout(Duration::from_secs(20), backend.start(params("first"))).await??;
    let mut events = started.process.subscribe_events();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut output = Vec::new();
        loop {
            match events.recv().await? {
                ExecProcessEvent::Output(chunk) => {
                    output.extend_from_slice(&chunk.chunk.0);
                    if String::from_utf8_lossy(&output).contains("owned-raw-ready") {
                        return Ok::<_, anyhow::Error>(());
                    }
                }
                event => anyhow::bail!(
                    "raw process stopped before readiness: {event:?}; output={output:?}"
                ),
            }
        }
    })
    .await??;
    // Inspect only the fresh test-owned group, never other users' processes.
    let shown = tokio::process::Command::new("/usr/bin/systemctl")
        .args([
            "--user",
            "show",
            group.identity().0,
            "--property=ControlGroup",
            "--value",
        ])
        .output()
        .await?;
    anyhow::ensure!(shown.status.success(), "own cgroup lookup failed");
    let path = std::path::Path::new("/sys/fs/cgroup").join(
        String::from_utf8(shown.stdout)?
            .trim()
            .trim_start_matches('/'),
    );
    let pids = std::fs::read_to_string(path.join("cgroup.procs"))?;
    let mut commands = Vec::new();
    for pid in pids.split_whitespace() {
        commands.push(std::fs::read_to_string(format!("/proc/{pid}/comm"))?);
    }
    anyhow::ensure!(
        commands.iter().any(|c| c.contains("codex-exec-serv")),
        "executor not in own cgroup"
    );
    anyhow::ensure!(
        commands.iter().any(|c| c.contains("python3")),
        "actual tool child not in own cgroup"
    );
    group.close().await?;
    match std::fs::read_to_string(path.join("cgroup.procs")) {
        Ok(pids) => anyhow::ensure!(pids.trim().is_empty(), "owned group still has processes"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let retained = tokio::process::Command::new("/usr/bin/systemctl")
        .args([
            "--user",
            "list-units",
            "--all",
            "--plain",
            "--no-legend",
            group.identity().0,
        ])
        .output()
        .await?;
    anyhow::ensure!(
        retained.status.success() && retained.stdout.is_empty(),
        "finished transient unit was not collected"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Ok(ExecProcessEvent::Output(_)) => {}
                _ => return,
            }
        }
    })
    .await
    .context("group close did not terminate the tool")?;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            backend.start(params("distinct-after-close"))
        )
        .await?
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("effect"))?,
        "once\n"
    );
    Ok(())
}
