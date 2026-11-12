//! Records real code-mode hosts without replacing their transport. The optional
//! bounded-profile probe checks the inherited OS sandbox before execing V8.

use std::fs;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use tempfile::TempDir;

pub struct CodeModeHostRecorder {
    _directory: TempDir,
    program: PathBuf,
    #[cfg(target_os = "linux")]
    host_program: PathBuf,
    invocations: PathBuf,
    pid: PathBuf,
    #[cfg(unix)]
    _listener: std::os::unix::net::UnixListener,
    #[cfg(target_os = "linux")]
    descriptor: fs::File,
}

impl CodeModeHostRecorder {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let program = directory.path().join("code mode host wrapper");
        let invocations = directory.path().join("host invocations");
        let pid = directory.path().join("host pid");
        let credential = directory.path().join("blocked credential");
        let socket = directory.path().join("owner socket");
        let result = directory.path().join("guard result");
        fs::write(&invocations, "")?;
        fs::write(&credential, "private fixture")?;
        #[cfg(unix)]
        let listener = std::os::unix::net::UnixListener::bind(&socket)?;
        #[cfg(unix)]
        let socket_descriptor_number = {
            use std::os::fd::AsRawFd;
            listener.as_raw_fd()
        };
        #[cfg(not(unix))]
        let socket_descriptor_number = -1;
        #[cfg(target_os = "linux")]
        let descriptor = fs::File::open(&credential)?;
        #[cfg(target_os = "linux")]
        let descriptor_number = {
            use std::os::fd::AsRawFd;
            descriptor.as_raw_fd()
        };
        #[cfg(not(target_os = "linux"))]
        let descriptor_number = -1;
        let host_program = codex_utils_cargo_bin::cargo_bin("codex-code-mode-host")?;
        let quoted_host =
            shlex::try_quote(host_program.to_str().context("host path is not UTF-8")?)?;
        let quoted_log = shlex::try_quote(invocations.to_str().context("log path is not UTF-8")?)?;
        let quoted_pid = shlex::try_quote(pid.to_str().context("PID path is not UTF-8")?)?;
        let quoted_directory =
            shlex::try_quote(directory.path().to_str().context("fixture path")?)?;
        let quoted_credential = shlex::try_quote(credential.to_str().context("fixture path")?)?;
        let quoted_socket = shlex::try_quote(socket.to_str().context("fixture path")?)?;
        let quoted_result = shlex::try_quote(result.to_str().context("fixture path")?)?;
        codex_utils_cargo_bin::write_executable(
            &program,
            &format!(
                r#"#!/bin/sh
if [ "${{CODEX_TEST_CODE_MODE_RECORDER-}}" = {quoted_directory} ]; then
/usr/bin/python3 - {quoted_credential} {quoted_socket} {quoted_result} 2>{quoted_result} <<'PY'
import errno, os, socket, sys
# Opening the path and borrowing an already-open parent descriptor are
# different routes. A sandbox may reuse this number for its own pipe, so
# pread avoids blocking and checks the exact fixture bytes, not just EBADF.
try:
    inherited = os.pread({descriptor_number}, 15, 0)
except OSError:
    pass
else:
    assert inherited != b'private fixture', 'private descriptor inherited'
try:
    borrowed = socket.socket(fileno=os.dup({socket_descriptor_number}))
except OSError:
    pass
else:
    try:
        assert borrowed.getsockname() != sys.argv[2], 'owner socket descriptor inherited'
    finally:
        borrowed.close()
try:
    open(sys.argv[1], 'rb').read(1)
except OSError as e:
    assert e.errno in (errno.EPERM, errno.EACCES, errno.ENOENT), e.errno
else:
    raise AssertionError('private credential readable')
s = socket.socket(socket.AF_UNIX)
try:
    s.connect(sys.argv[2])
except OSError as e:
    assert e.errno in (errno.EPERM, errno.EACCES), e.errno
else:
    raise AssertionError('owner IPC connected')
finally:
    s.close()
open(sys.argv[3], 'w').write('guarded')
PY
probe=$?
[ "$probe" -eq 0 ] || exit "$probe"
fi
printf '%s\n' "$*" >> {quoted_log}
printf '%s\n' "$$" > {quoted_pid}
exec {quoted_host} "$@"
"#
            ),
        )?;
        Ok(Self {
            _directory: directory,
            program,
            #[cfg(target_os = "linux")]
            host_program: fs::canonicalize(host_program)?,
            invocations,
            pid,
            #[cfg(unix)]
            _listener: listener,
            #[cfg(target_os = "linux")]
            descriptor,
        })
    }

    /// Establish that the controlled file and socket are actually inheritable
    /// before the sandbox probe. No ambient descriptor or credential is read.
    #[cfg(target_os = "linux")]
    pub async fn enable_inheritable_descriptor_probe(&self) -> Result<()> {
        use std::os::fd::AsRawFd;
        let fd = self.descriptor.as_raw_fd();
        let socket_fd = self._listener.as_raw_fd();
        for fd in [fd, socket_fd] {
            // SAFETY: this fixture owns both live descriptors through every
            // child launch; only its harmless file/socket become inheritable.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            anyhow::ensure!(flags >= 0, "fixture descriptor flags unavailable");
            let changed = unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
            anyhow::ensure!(changed == 0, "fixture descriptor not inheritable");
        }
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::process::Command::new("/usr/bin/python3")
                .args([
                    "-c",
                    "import os,socket,sys; assert os.pread(int(sys.argv[1]),15,0)==b'private fixture'; s=socket.socket(fileno=os.dup(int(sys.argv[2]))); assert s.getsockname()==sys.argv[3]",
                    &fd.to_string(),
                    &socket_fd.to_string(),
                    self._directory.path().join("owner socket").to_str().context("socket path")?,
                ])
                .env_clear()
                .kill_on_drop(true)
                .status(),
        )
        .await??;
        anyhow::ensure!(status.success(), "ordinary descriptor control failed");
        Ok(())
    }

    pub fn program(&self) -> PathBuf {
        self.program.clone()
    }

    pub fn invocations(&self) -> Result<Vec<String>> {
        Ok(fs::read_to_string(&self.invocations)?
            .lines()
            .map(str::to_string)
            .collect())
    }

    pub fn pid(&self) -> Result<u32> {
        let reported = fs::read_to_string(&self.pid)?.trim().parse()?;
        #[cfg(target_os = "linux")]
        {
            if fs::read_link(format!("/proc/{reported}/exe")).ok().as_ref()
                == Some(&self.host_program)
            {
                return Ok(reported);
            }
            // A private PID namespace reports its inner PID. Resolve only this
            // test process's descendants, matching executable, inner PID and our
            // explicit fixture marker; never infer that PID1 is the host's init.
            use std::collections::BTreeSet;
            use std::collections::VecDeque;
            use std::io::Read;
            let marker = format!(
                "CODEX_TEST_CODE_MODE_RECORDER={}",
                self.program.parent().context("fixture parent")?.display()
            );
            let mut queue = VecDeque::from([std::process::id()]);
            let mut visited = BTreeSet::new();
            let mut candidates = Vec::new();
            let mut tasks = 0;
            while let Some(pid) = queue.pop_front() {
                if !visited.insert(pid) {
                    continue;
                }
                anyhow::ensure!(
                    visited.len() <= 512,
                    "fixture process observation exceeded512 descendants"
                );
                if fs::read_link(format!("/proc/{pid}/exe")).ok().as_ref()
                    == Some(&self.host_program)
                {
                    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
                    let inner = status
                        .lines()
                        .find_map(|line| line.strip_prefix("NSpid:"))
                        .and_then(|line| line.split_whitespace().last())
                        .and_then(|value| value.parse::<u32>().ok());
                    if inner == Some(reported) {
                        let mut raw = Vec::new();
                        fs::File::open(format!("/proc/{pid}/environ"))?
                            .take(128 * 1024)
                            .read_to_end(&mut raw)?;
                        if raw
                            .split(|byte| *byte == 0)
                            .any(|entry| entry == marker.as_bytes())
                        {
                            candidates.push(pid);
                        }
                    }
                }
                let Ok(entries) = fs::read_dir(format!("/proc/{pid}/task")) else {
                    continue;
                };
                for entry in entries {
                    tasks += 1;
                    anyhow::ensure!(
                        tasks <= 4096,
                        "fixture process observation exceeded4096 tasks"
                    );
                    let entry = entry?;
                    let Ok(children) = fs::read_to_string(entry.path().join("children")) else {
                        continue;
                    };
                    queue.extend(
                        children
                            .split_whitespace()
                            .filter_map(|child| child.parse::<u32>().ok()),
                    );
                }
            }
            anyhow::ensure!(
                candidates.len() == 1,
                "fixture could not uniquely resolve namespaced host: {candidates:?}"
            );
            Ok(candidates[0])
        }
        #[cfg(not(target_os = "linux"))]
        Ok(reported)
    }
}
