//! Owns Core's local code-mode output rollout and gRPC host. Sessions share a lazy
//! HTTP/2 connection. Only session admission can replace a dead host; metrics
//! use the current generation and cannot reconnect. Dropping its I/O reaps the child.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use codex_code_mode::CodeModeHostOptions;
use codex_code_mode::CodeModeHostSandbox;
use codex_code_mode::CodeModeOutputTruncation;
use codex_code_mode::CodeModeProcessMetricsSample;
use codex_code_mode::CodeModeSessionCellExecutionLimits;
use codex_code_mode::CodeModeSessionOptions;
use codex_code_mode::CodeModeSessionProvider;
use codex_code_mode::CodeModeSessionProviderFuture;
use codex_code_mode::CodeModeSessionResultFuture;
use codex_code_mode::GrpcCodeModeSessionProvider;
use codex_code_mode::ProcessOwnedCodeModeSessionProvider;
use codex_protocol::shell_environment::scrub_non_inheritable_env_vars;
use codex_utils_process::background_command;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::process::Child;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tonic::Status;
use tonic::body::Body;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tower::ServiceExt;
use tower::service_fn;

/// Only wraps Core's built-in providers; injected providers own their session settings.
pub(crate) struct LocalServerOutputProvider {
    pub(crate) inner: Arc<dyn CodeModeSessionProvider>,
}

impl CodeModeSessionProvider for LocalServerOutputProvider {
    fn availability(&self) -> Result<(), String> {
        self.inner.availability()
    }

    fn sample_process_metrics(
        &self,
    ) -> CodeModeSessionResultFuture<'_, CodeModeProcessMetricsSample> {
        self.inner.sample_process_metrics()
    }

    fn create_session(&self) -> CodeModeSessionProviderFuture<'_> {
        self.create_session_with_limits(CodeModeSessionCellExecutionLimits::default())
    }

    fn create_isolated_session_with_options<'a>(
        &'a self,
        options: CodeModeSessionOptions,
        host_options: CodeModeHostOptions,
    ) -> CodeModeSessionProviderFuture<'a> {
        self.inner
            .create_isolated_session_with_options(options, host_options)
    }

    fn create_session_with_limits<'a>(
        &'a self,
        limits: CodeModeSessionCellExecutionLimits,
    ) -> CodeModeSessionProviderFuture<'a> {
        self.inner
            .create_session_with_options(CodeModeSessionOptions {
                cell_execution_limits: limits,
                output_truncation: CodeModeOutputTruncation::Server,
            })
    }

    fn create_session_with_options<'a>(
        &'a self,
        options: CodeModeSessionOptions,
    ) -> CodeModeSessionProviderFuture<'a> {
        self.inner.create_session_with_options(options)
    }
}

pub struct ProcessOwnedGrpcCodeModeSessionProvider {
    host_program: PathBuf,
    host_environment: Option<Arc<HashMap<String, String>>>,
    host_sandbox: Option<Arc<CodeModeHostSandbox>>,
    provider: OnceLock<GrpcCodeModeSessionProvider>,
    closed: Arc<AtomicBool>,
    current: Arc<Mutex<Option<HostChannel>>>,
}

impl ProcessOwnedGrpcCodeModeSessionProvider {
    pub fn with_host_program(host_program: PathBuf) -> Self {
        Self {
            host_program,
            host_environment: None,
            host_sandbox: None,
            provider: OnceLock::new(),
            closed: Arc::new(AtomicBool::new(false)),
            current: Arc::new(Mutex::new(None)),
        }
    }
}

impl CodeModeSessionProvider for ProcessOwnedGrpcCodeModeSessionProvider {
    fn availability(&self) -> Result<(), String> {
        ProcessOwnedCodeModeSessionProvider::with_host_program(self.host_program.clone())
            .availability()
    }

    fn sample_process_metrics(
        &self,
    ) -> CodeModeSessionResultFuture<'_, CodeModeProcessMetricsSample> {
        // Absence is local lifecycle state, not a meaning assigned to any remote
        // gRPC error. A child dying after this observation produces a failed sample.
        if !self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|host| host.alive.load(Ordering::Acquire))
        {
            return Box::pin(async { Ok(CodeModeProcessMetricsSample::NotRunning) });
        }
        match self.provider.get() {
            Some(provider) => provider.sample_process_metrics(),
            None => Box::pin(async { Ok(CodeModeProcessMetricsSample::NotRunning) }),
        }
    }

    fn create_session(&self) -> CodeModeSessionProviderFuture<'_> {
        self.create_session_with_options(CodeModeSessionOptions::default())
    }

    fn create_isolated_session_with_options<'a>(
        &'a self,
        options: CodeModeSessionOptions,
        host_options: CodeModeHostOptions,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(async move {
            let mut owner = Self::with_host_program(self.host_program.clone());
            owner.host_environment = host_options.environment.map(Arc::new);
            owner.host_sandbox = host_options.sandbox.map(Arc::new);
            let owner = Arc::new(owner);
            let inner = owner.create_session_with_options(options).await?;
            Ok(Arc::new(IsolatedSession { inner, owner })
                as Arc<dyn codex_code_mode::CodeModeSession>)
        })
    }

    fn create_session_with_limits<'a>(
        &'a self,
        limits: CodeModeSessionCellExecutionLimits,
    ) -> CodeModeSessionProviderFuture<'a> {
        self.create_session_with_options(CodeModeSessionOptions {
            cell_execution_limits: limits,
            ..CodeModeSessionOptions::default()
        })
    }

    fn create_session_with_options<'a>(
        &'a self,
        options: CodeModeSessionOptions,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(async move {
            self.availability()?;
            let provider = self.provider.get_or_init(|| {
                let transport = Arc::new(HostTransport {
                    host_program: self.host_program.clone(),
                    host_environment: self.host_environment.clone(),
                    host_sandbox: self.host_sandbox.clone(),
                    current: self.current.clone(),
                    closed: self.closed.clone(),
                });
                GrpcCodeModeSessionProvider::with_transport(service_fn(move |request| {
                    let transport = transport.clone();
                    async move { transport.send(request).await }
                }))
            });
            provider.create_session_with_options(options).await
        })
    }
}

struct IsolatedSession {
    inner: Arc<dyn codex_code_mode::CodeModeSession>,
    owner: Arc<ProcessOwnedGrpcCodeModeSessionProvider>,
}
struct ClosePrivateHost<'a>(&'a ProcessOwnedGrpcCodeModeSessionProvider);
impl Drop for ClosePrivateHost<'_> {
    fn drop(&mut self) {
        let mut current = self
            .0
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.0.closed.store(true, Ordering::Release);
        current.take();
    }
}
impl Drop for IsolatedSession {
    fn drop(&mut self) {
        drop(ClosePrivateHost(&self.owner));
    }
}
impl codex_code_mode::CodeModeSession for IsolatedSession {
    fn output_truncation(&self) -> CodeModeOutputTruncation {
        self.inner.output_truncation()
    }
    fn execute<'a>(
        &'a self,
        request: codex_code_mode::ExecuteRequest,
        delegate: Arc<dyn codex_code_mode::CodeModeSessionDelegate>,
        preempt: Option<tokio_util::sync::CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, codex_code_mode::StartedCell> {
        self.inner.execute(request, delegate, preempt)
    }
    fn wait<'a>(
        &'a self,
        request: codex_code_mode::WaitRequest,
        preempt: Option<tokio_util::sync::CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, codex_code_mode::WaitOutcome> {
        self.inner.wait(request, preempt)
    }
    fn terminate<'a>(
        &'a self,
        request: codex_code_mode::TerminateRequest,
    ) -> CodeModeSessionResultFuture<'a, codex_code_mode::WaitOutcome> {
        self.inner.terminate(request)
    }
    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()> {
        Box::pin(async move {
            let _close = ClosePrivateHost(&self.owner);
            self.inner.shutdown().await
        })
    }
}

// Every channel gets exactly one child. Tonic's internal reconnect loop cannot
// spawn a process, including if the child dies between a metrics liveness check
// and dispatch. Session admission selects a new generation during recovery.
struct HostTransport {
    host_program: PathBuf,
    host_environment: Option<Arc<HashMap<String, String>>>,
    host_sandbox: Option<Arc<CodeModeHostSandbox>>,
    current: Arc<Mutex<Option<HostChannel>>>,
    closed: Arc<AtomicBool>,
}

struct HostChannel {
    channel: Channel,
    alive: Arc<AtomicBool>,
}

impl HostTransport {
    async fn send(&self, request: http::Request<Body>) -> Result<http::Response<Body>, Status> {
        self.channel(request.uri().path())?
            .oneshot(request)
            .await
            .map_err(|error| {
                // Channel errors are transport failures, not host RPC statuses.
                // In particular a child exiting during handshake can surface as
                // BrokenPipe, which generic tonic conversion calls Unknown and
                // would prevent the existing bounded OpenSession retry policy.
                let mut status = Status::unavailable("code-mode host transport failed");
                status.set_source(Arc::new(error));
                status
            })
    }

    fn channel(&self, rpc_path: &str) -> Result<Channel, Status> {
        Ok({
            let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
            if self.closed.load(Ordering::Acquire) {
                return Err(Status::unavailable("private code-mode host is closed"));
            }
            if let Some(host) = current
                .as_ref()
                .filter(|host| host.alive.load(Ordering::Acquire))
            {
                host.channel.clone()
            } else {
                if rpc_path != "/codex.code_mode.v1.CodeModeHost/OpenSession" {
                    return Err(Status::unavailable("code-mode host is not running"));
                }
                let connection = HostConnection::spawn(
                    &self.host_program,
                    self.host_environment.as_deref(),
                    self.host_sandbox.as_deref(),
                )
                .map_err(|error| Status::unavailable(error.to_string()))?;
                let alive = connection.alive.clone();
                let connection = Arc::new(Mutex::new(Some(connection)));
                // Preserve execution liveness detection independently of metrics cadence.
                let channel = Endpoint::from_static("http://code-mode-host")
                    .http2_keep_alive_interval(Duration::from_secs(/*secs*/ 30))
                    .keep_alive_timeout(Duration::from_secs(/*secs*/ 10))
                    .keep_alive_while_idle(/*enabled*/ true)
                    .connect_with_connector_lazy(service_fn(move |_| {
                        let connection = connection
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take();
                        async move {
                            connection.map(TokioIo::new).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotConnected,
                                    "code-mode host connection ended",
                                )
                            })
                        }
                    }));
                *current = Some(HostChannel {
                    channel: channel.clone(),
                    alive,
                });
                channel
            }
        })
    }
}

fn sandboxed_host_command(
    host_program: &Path,
    environment: Option<&HashMap<String, String>>,
    sandbox: &CodeModeHostSandbox,
) -> io::Result<Command> {
    use codex_protocol::config_types::WindowsSandboxLevel;
    use codex_protocol::models::PermissionProfile;
    use codex_sandboxing::SandboxCommand;
    use codex_sandboxing::SandboxManager;
    use codex_sandboxing::SandboxTransformRequest;
    use codex_sandboxing::SandboxType;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use codex_utils_path_uri::PathUri;
    if !cfg!(target_os = "linux")
        || !matches!(sandbox.permissions, PermissionProfile::Managed { .. })
        || sandbox
            .permissions
            .file_system_sandbox_policy()
            .has_full_disk_write_access()
        || sandbox.permissions.network_sandbox_policy().is_enabled()
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process-isolated code-mode sandbox requires qualified restrictive offline Linux permissions",
        ));
    }
    let cwd = PathUri::from_abs_path(&AbsolutePathBuf::from_absolute_path(&sandbox.cwd)?);
    let request = SandboxManager::new()
        .transform(SandboxTransformRequest {
            command: SandboxCommand {
                program: host_program.as_os_str().to_owned(),
                args: vec!["--listen".into(), "grpc+stdio://".into()],
                cwd: cwd.clone(),
                env: environment
                    .cloned()
                    .unwrap_or_else(|| std::env::vars().collect()),
                managed_network: None,
                additional_permissions: None,
            },
            permissions: &sandbox.permissions,
            sandbox: SandboxType::LinuxSeccomp,
            enforce_managed_network: false,
            environment_id: None,
            network: None,
            sandbox_policy_cwd: &cwd,
            sandbox_exe: Some(&sandbox.linux_sandbox_exe),
            use_legacy_landlock: false,
            windows_sandbox_level: WindowsSandboxLevel::Disabled,
        })
        .map_err(io::Error::other)?;
    let (program, args) = request
        .command
        .split_first()
        .ok_or_else(|| io::Error::other("empty sandbox host command"))?;
    let mut command = Command::from(background_command(program));
    command
        .args(args)
        .current_dir(request.cwd.to_abs_path()?.as_path())
        .env_clear()
        .envs(&request.env);
    #[cfg(unix)]
    if let Some(arg0) = request.arg0 {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().arg0(arg0);
    }
    Ok(command)
}

struct HostConnection {
    io: tokio::io::Join<ChildStdout, ChildStdin>,
    child: Option<Child>,
    alive: Arc<AtomicBool>,
}

impl HostConnection {
    fn spawn(
        host_program: &Path,
        host_environment: Option<&HashMap<String, String>>,
        host_sandbox: Option<&CodeModeHostSandbox>,
    ) -> io::Result<Self> {
        let mut command = if let Some(sandbox) = host_sandbox {
            sandboxed_host_command(host_program, host_environment, sandbox)?
        } else {
            let mut command = Command::from(background_command(host_program));
            command.args(["--listen", "grpc+stdio://"]);
            if let Some(environment) = host_environment {
                command.env_clear().envs(environment);
            }
            command
        };
        #[cfg(unix)]
        command.process_group(/*pgroup*/ 0);
        #[cfg(target_os = "linux")]
        if host_sandbox.is_some() {
            // SAFETY: the existing strict cleanup uses only a fork-safe syscall.
            // Marking CLOEXEC preserves Rust's spawn-error pipe until exec;
            // failure refuses the private host rather than inheriting authority.
            unsafe {
                command.pre_exec(codex_utils_pty::require_explicit_descriptors);
            }
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(/*kill_on_drop*/ true);
        scrub_non_inheritable_env_vars(command.as_std_mut());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("code-mode host has no stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("code-mode host has no stdout"))?;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!("code-mode host stderr: {line}");
                }
            });
        }
        Ok(Self {
            io: tokio::io::join(stdout, stdin),
            child: Some(child),
            alive: Arc::new(AtomicBool::new(true)),
        })
    }
}

impl AsyncRead for HostConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for HostConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl Drop for HostConnection {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

#[cfg(test)]
#[path = "code_mode_host_tests.rs"]
mod tests;
