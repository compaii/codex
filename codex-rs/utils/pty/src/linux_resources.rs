//! An execution-owned resource group through the existing Linux user manager.
//! This supplies no grant: the trusted caller selects its actual task bounds.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tokio::process::Command;

/// Explicit aggregate limits, not defaults or a resource authorization.
#[derive(Clone, Copy, Debug)]
pub struct LinuxProcessLimits {
    pub memory_bytes: u64,
    pub cpu_percent: u32,
    pub tasks: u32,
}

/// Owns only one fresh finite transient service. Closing its stdin ends the
/// holder and systemd terminates the whole cgroup. Kernel expiry also survives
/// loss of the interpreting task or process. This never controls the parent unit.
pub struct LinuxProcessResourceGroup {
    unit: String,
    invocation: String,
    cgroup: PathBuf,
    limits: LinuxProcessLimits,
    deadline: Instant,
    holder: Child,
}

const MANAGER_TIMEOUT: Duration = Duration::from_secs(4);

fn manager_command(program: &str) -> Command {
    let mut command = Command::new(program);
    // Management tools are trusted, local and separate from the target's env.
    // Never forward target loader settings or select a remote user bus.
    let runtime = format!("/run/user/{}", unsafe { libc::geteuid() });
    command
        .env_clear()
        .env("XDG_RUNTIME_DIR", &runtime)
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={runtime}/bus"),
        )
        .stdin(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn manager_output(command: &mut Command) -> io::Result<String> {
    let output = tokio::time::timeout(MANAGER_TIMEOUT, command.output())
        .await
        .map_err(io::Error::other)??;
    if !output.status.success() {
        // Unit/manager errors may contain local paths. Keep diagnostics at the
        // trusted caller; do not include environment or target arguments.
        return Err(io::Error::other(format!(
            "resource manager failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout).map_err(io::Error::other)
}

impl LinuxProcessResourceGroup {
    /// Start the existing own-work route once. Bounds are required and narrowed
    /// by the operator's supported maxima (1GiB, one CPU,128tasks,10minutes).
    /// Timeout/unknown startup never retries: dropping stdin and the finite
    /// kernel deadline bound any service that was accepted without a reply.
    pub async fn start(limits: LinuxProcessLimits, lifetime: Duration) -> io::Result<Self> {
        let seconds = lifetime.as_secs();
        if limits.memory_bytes == 0
            || limits.memory_bytes > 1024 * 1024 * 1024
            || limits.cpu_percent == 0
            || limits.cpu_percent > 100
            || limits.tasks == 0
            || limits.tasks > 128
            || seconds == 0
            || lifetime > Duration::from_secs(600)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid own-work resource bounds",
            ));
        }
        let uuid = std::fs::read_to_string("/proc/sys/kernel/random/uuid")?;
        let uuid = uuid.trim().replace('-', "");
        if uuid.len() != 32 || !uuid.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(io::Error::other("invalid kernel resource identity"));
        }
        let unit = format!("matrix-work-{uuid}.service");
        let deadline = Instant::now() + lifetime;
        let mut command = manager_command("/usr/bin/systemd-run");
        command
            .args([
                "--user",
                "--quiet",
                "--pipe",
                "--wait",
                "--service-type=exec",
            ])
            .arg(format!("--unit={unit}"))
            .arg(format!("--property=MemoryMax={}", limits.memory_bytes))
            .arg(format!("--property=CPUQuota={}%", limits.cpu_percent))
            .arg(format!("--property=TasksMax={}", limits.tasks))
            .arg(format!("--property=RuntimeMaxSec={seconds}"))
            .args([
                "--property=TimeoutStopSec=2",
                "--property=KillMode=control-group",
                "--property=NoNewPrivileges=yes",
                "--property=Restart=no",
                "--property=Delegate=yes",
                "/usr/bin/cat",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            // EOF is the cleanup signal; killing the relay before delivering EOF
            // is unnecessary. Tokio retains the child's ordinary reaping path.
            .kill_on_drop(false);
        let holder = command.spawn()?;
        let mut owner = Self {
            unit,
            invocation: String::new(),
            cgroup: PathBuf::new(),
            limits,
            deadline,
            holder,
        };
        // The fixed holder echoes a private readiness marker. No poll loop or
        // inferred readiness from a merely running relay process is needed.
        let ready = async {
            owner
                .holder
                .stdin
                .as_mut()
                .ok_or_else(|| io::Error::other("holder stdin unavailable"))?
                .write_all(b"ready\n")
                .await?;
            let mut marker = [0; 6];
            owner
                .holder
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("holder stdout unavailable"))?
                .read_exact(&mut marker)
                .await?;
            if &marker != b"ready\n" {
                return Err(io::Error::other("invalid holder readiness"));
            }
            let props = owner.properties().await?;
            owner.invocation = props.get("InvocationID").cloned().unwrap_or_default();
            let group = props.get("ControlGroup").cloned().unwrap_or_default();
            if props.get("ActiveState").map(String::as_str) != Some("active")
                || owner.invocation.len() != 32
                || !group.ends_with(&owner.unit)
                || !group.starts_with("/user.slice/")
            {
                return Err(io::Error::other("invalid resource unit identity"));
            }
            owner.cgroup = PathBuf::from("/sys/fs/cgroup").join(group.trim_start_matches('/'));
            owner.verify_kernel()
        };
        tokio::time::timeout(MANAGER_TIMEOUT, ready)
            .await
            .map_err(io::Error::other)??;
        Ok(owner)
    }

    async fn properties(&self) -> io::Result<HashMap<String, String>> {
        let text = manager_output(manager_command("/usr/bin/systemctl").args([
            "--user",
            "show",
            &self.unit,
            "-p",
            "ActiveState",
            "-p",
            "ControlGroup",
            "-p",
            "InvocationID",
        ]))
        .await?;
        Ok(text
            .lines()
            .filter_map(|line| {
                line.split_once('=')
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
            })
            .collect())
    }

    fn verify_kernel(&self) -> io::Result<()> {
        let read = |name: &str| std::fs::read_to_string(self.cgroup.join(name));
        let memory = read("memory.max")?
            .trim()
            .parse::<u64>()
            .map_err(io::Error::other)?;
        let tasks = read("pids.max")?
            .trim()
            .parse::<u32>()
            .map_err(io::Error::other)?;
        let cpu = read("cpu.max")?;
        let cpu = cpu
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?;
        if memory != self.limits.memory_bytes
            || tasks != self.limits.tasks
            || cpu.len() != 2
            || cpu[1] == 0
            || u128::from(cpu[0]) * 100 != u128::from(cpu[1]) * u128::from(self.limits.cpu_percent)
        {
            return Err(io::Error::other(
                "kernel resource limits differ from selected bounds",
            ));
        }
        Ok(())
    }

    /// Admit only this caller's owned helper; no arbitrary PID control API.
    pub async fn spawn(&self, command: crate::Command) -> io::Result<crate::Child> {
        command
            .spawn_with_admission(|pid| async move {
                if Instant::now() >= self.deadline {
                    return Err(io::Error::other("resource group expired"));
                }
                let props = self.properties().await?;
                if props.get("ActiveState").map(String::as_str) != Some("active")
                    || props.get("InvocationID") != Some(&self.invocation)
                {
                    return Err(io::Error::other("resource group identity changed"));
                }
                self.verify_kernel()?;
                manager_output(manager_command("/usr/bin/busctl").args([
                    "--user",
                    "call",
                    "org.freedesktop.systemd1",
                    "/org/freedesktop/systemd1",
                    "org.freedesktop.systemd1.Manager",
                    "AttachProcessesToUnit",
                    "ssau",
                    &self.unit,
                    "",
                    "1",
                    &pid.to_string(),
                ]))
                .await?;
                let props = self.properties().await?;
                if props.get("InvocationID") != Some(&self.invocation) {
                    return Err(io::Error::other("resource group changed during admission"));
                }
                self.verify_kernel()?;
                let actual = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
                let expected = format!(
                    "0::/{}\n",
                    self.cgroup
                        .strip_prefix("/sys/fs/cgroup")
                        .map_err(io::Error::other)?
                        .display()
                );
                if actual != expected || Instant::now() >= self.deadline {
                    return Err(io::Error::other(
                        "resource membership unavailable or expired",
                    ));
                }
                Ok(())
            })
            .await
    }

    /// Close this exact group's holder. Failure is retained, never retried as a
    /// different stop operation; kernel expiry remains the last cleanup bound.
    pub async fn close(mut self) -> io::Result<()> {
        self.holder.stdin.take();
        let exit = tokio::time::timeout(MANAGER_TIMEOUT, self.holder.wait())
            .await
            .map_err(io::Error::other)??;
        if !exit.success() || self.cgroup.exists() {
            return Err(io::Error::other("resource group close not confirmed"));
        }
        Ok(())
    }
}

impl Drop for LinuxProcessResourceGroup {
    fn drop(&mut self) {
        self.holder.stdin.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;

    fn workload() -> crate::Command {
        let mut command = crate::Command::new("/bin/sh");
        command
            .args([
                "-c",
                "/bin/cat /proc/self/cgroup; /bin/sleep 15 & echo $!; wait",
            ])
            .process_mode(crate::ProcessMode::NewSession)
            .descriptor_policy(crate::DescriptorPolicy::Explicit);
        command
    }

    #[tokio::test]
    #[ignore = "requires an explicitly selected own Linux user-manager resource qualification"]
    async fn owned_group_shares_limits_and_closes_all_children() -> anyhow::Result<()> {
        let parent = std::fs::read_to_string("/proc/self/cgroup")?;
        let group = LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: 128 * 1024 * 1024,
                cpu_percent: 25,
                tasks: 16,
            },
            Duration::from_secs(20),
        )
        .await?;
        let path = group.cgroup.clone();
        let mut first = group.spawn(workload()).await?;
        let mut second = group.spawn(workload()).await?;
        let mut descendants = Vec::new();
        for child in [&mut first, &mut second] {
            let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
            let observed = tokio::time::timeout(MANAGER_TIMEOUT, lines.next_line())
                .await??
                .unwrap();
            assert_eq!(
                observed,
                format!("0::/{}", path.strip_prefix("/sys/fs/cgroup")?.display())
            );
            let pid = tokio::time::timeout(MANAGER_TIMEOUT, lines.next_line())
                .await??
                .unwrap()
                .parse::<u32>()?;
            assert_eq!(
                std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?.trim(),
                observed
            );
            descendants.push(crate::spawn_helper_tests::open_pidfd(pid as _)?);
        }
        assert_eq!(std::fs::read_to_string("/proc/self/cgroup")?, parent);
        group.close().await?;
        for child in [&mut first, &mut second] {
            assert!(
                !tokio::time::timeout(MANAGER_TIMEOUT, child.wait())
                    .await??
                    .success()
            );
        }
        for fd in descendants {
            let child = tokio::io::unix::AsyncFd::new(fd)?;
            let _ready = tokio::time::timeout(MANAGER_TIMEOUT, child.readable()).await??;
        }
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an explicitly selected own Linux user-manager resource qualification"]
    async fn owned_group_kernel_deadline_ends_child() -> anyhow::Result<()> {
        let group = LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: 128 * 1024 * 1024,
                cpu_percent: 25,
                tasks: 16,
            },
            Duration::from_secs(1),
        )
        .await?;
        let path = group.cgroup.clone();
        let mut child = group.spawn(workload()).await?;
        assert!(
            !tokio::time::timeout(MANAGER_TIMEOUT, child.wait())
                .await??
                .success()
        );
        let mut command = crate::Command::new("/bin/true");
        command
            .process_mode(crate::ProcessMode::NewSession)
            .descriptor_policy(crate::DescriptorPolicy::Explicit);
        assert!(group.spawn(command).await.is_err());
        drop(group);
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an explicitly selected own Linux user-manager resource qualification"]
    async fn owned_group_drop_closes_live_children() -> anyhow::Result<()> {
        let group = LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: 128 * 1024 * 1024,
                cpu_percent: 25,
                tasks: 16,
            },
            Duration::from_secs(20),
        )
        .await?;
        let path = group.cgroup.clone();
        let relay = tokio::io::unix::AsyncFd::new(crate::spawn_helper_tests::open_pidfd(
            group.holder.id().unwrap() as _,
        )?)?;
        let mut child = group.spawn(workload()).await?;
        drop(group);
        assert!(
            !tokio::time::timeout(MANAGER_TIMEOUT, child.wait())
                .await??
                .success()
        );
        let _ready = tokio::time::timeout(MANAGER_TIMEOUT, relay.readable()).await??;
        assert!(!path.exists());
        Ok(())
    }
}
