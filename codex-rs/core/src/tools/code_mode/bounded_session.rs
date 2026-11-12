//! Host-selected execution resources, isolated from the thread's human session.
use std::sync::Arc;
use std::time::Duration;

use codex_api::InferenceAllowance;
use codex_code_mode::CellId;
use codex_code_mode::CodeModeNestedToolCall;
use codex_code_mode::CodeModeOutputTruncation;
use codex_code_mode::CodeModeSession;
use codex_code_mode::CodeModeSessionDelegate;
use codex_code_mode::CodeModeSessionOptions;
use codex_code_mode::CodeModeSessionResultFuture;
use codex_code_mode::ExecuteRequest;
use codex_code_mode::NotificationFuture;
use codex_code_mode::RuntimeResponse;
use codex_code_mode::StartedCell;
use codex_code_mode::TerminateRequest;
use codex_code_mode::ToolInvocationFuture;
use codex_code_mode::WaitOutcome;
use codex_code_mode::WaitRequest;
use codex_extension_api::ExtensionData;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::CodeModeService;
use crate::session::turn_context::TurnContext;

/// Trusted embedding-host policy carried by the existing ExtensionDataInit.
/// This is not serialized input, a model tool or a change to human defaults.
#[derive(Clone, Debug)]
pub struct BoundedCodeModeOptions(pub CodeModeSessionOptions);

pub(super) struct BoundedSessionState {
    options: CodeModeSessionOptions,
    pub(super) session: OnceCell<Arc<dyn CodeModeSession>>,
    closed: CancellationToken,
}

impl Drop for BoundedSessionState {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}

pub(crate) fn capture_options(data: &ExtensionData) {
    if let Some(options) = data.get::<BoundedCodeModeOptions>() {
        data.insert(BoundedSessionState {
            options: options.0.clone(),
            session: OnceCell::new(),
            closed: CancellationToken::new(),
        });
    }
}

/// Legacy allowance-only attention has no host isolation policy. Preserve its
/// existing session until the owner selects a complete restricted profile.
pub(crate) fn uses_isolated_session(turn: &TurnContext) -> bool {
    crate::turn_inference::allowance(turn).is_some()
        && (turn.has_tool_ceiling()
            || turn.permission_ceiling().is_some()
            || turn.extension_data.get::<BoundedSessionState>().is_some())
}

impl CodeModeService {
    pub(super) async fn session_for_cell(
        &self,
        turn: &TurnContext,
        id: &CellId,
    ) -> Result<Arc<dyn CodeModeSession>, String> {
        let Some(allowance) = crate::turn_inference::allowance(turn) else {
            return self.session().await;
        };
        allowance
            .check_execution_open()
            .map_err(|e| e.to_string())?;
        if !uses_isolated_session(turn) {
            return self.session().await;
        }
        if !id
            .as_str()
            .starts_with(&format!("bounded:{}:", turn.sub_id))
        {
            return Err("bounded code mode cannot access another execution's cell".into());
        }
        turn.extension_data
            .get::<BoundedSessionState>()
            .and_then(|state| state.session.get().cloned())
            .ok_or_else(|| "bounded code mode has no initialized cell session".into())
    }

    pub(super) async fn session_for_turn(
        &self,
        step: &crate::session::step_context::StepContext,
    ) -> Result<Arc<dyn CodeModeSession>, String> {
        let turn = &step.turn;
        let Some(allowance) = crate::turn_inference::allowance(turn) else {
            return self.session().await;
        };
        allowance
            .check_execution_open()
            .map_err(|e| e.to_string())?;
        if !uses_isolated_session(turn) {
            return self.session().await;
        }
        if self.shutdown_token.is_cancelled() {
            return Err("code mode session is shutting down".into());
        }
        let state = turn
            .extension_data
            .get::<BoundedSessionState>()
            .ok_or_else(|| {
                "bounded code mode requires host-selected session resource limits".to_string()
            })?;
        let mut options = state.options.clone();
        let limits = &mut options.cell_execution_limits;
        if limits.max_heap_size_bytes.is_none_or(|n| n == 0)
            || limits.max_yield_time_ms.is_none_or(|n| n == 0)
        {
            return Err("bounded code mode requires positive heap and observation limits".into());
        }
        let remaining = u64::try_from(
            allowance
                .deadline()
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
        .max(1);
        limits.max_yield_time_ms = limits.max_yield_time_ms.map(|n| n.min(remaining));
        let host_environment = turn
            .command_environment_policy()
            .map(|policy| crate::exec_env::create_env(&policy, None));
        let sandbox = if turn.permission_ceiling().is_some() {
            let environment = step
                .environments
                .turn_environments()
                .find(|environment| !environment.environment.is_remote())
                .ok_or_else(|| {
                    "process-isolated code mode has no qualified local environment".to_string()
                })?;
            let permissions = environment
                .sandbox_context_with_permissions(
                    turn.granted_permissions(&environment.selection.environment_id),
                )
                .map_err(|error| error.to_string())?
                .permissions;
            Some(codex_code_mode::CodeModeHostSandbox {
                permissions: codex_protocol::models::PermissionProfile::from_runtime_permissions_with_enforcement(
                    permissions.enforcement(), &permissions.file_system_sandbox_policy(),
                    codex_protocol::permissions::NetworkSandboxPolicy::Restricted,
                ),
                cwd: environment.cwd().to_abs_path().map_err(|error| error.to_string())?.to_path_buf(),
                linux_sandbox_exe: turn.config.codex_linux_sandbox_exe.clone()
                    .ok_or_else(|| "process-isolated code mode requires a qualified sandbox helper".to_string())?,
            })
        } else {
            None
        };
        let host_options = codex_code_mode::CodeModeHostOptions {
            environment: host_environment,
            sandbox,
            process_launcher: None,
        };
        state.session.get_or_try_init(|| async {
            let inner = tokio::select! {
                biased;
                _ = allowance.execution_finished() => return Err("bounded code mode execution ended".into()),
                _ = self.shutdown_token.cancelled() => return Err("code mode session is shutting down".into()),
                result = self.session_provider.create_isolated_session_with_options(options, host_options) => result?,
            };
            let closed = state.closed.clone();
            let shutdown = self.shutdown_token.clone();
            let lifetime = Arc::clone(&allowance);
            let owned = Arc::clone(&inner);
            // Cleanup survives the interpreting future and a closure racing open.
            // No retry or new session is created by this finite task.
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = lifetime.execution_finished() => {},
                    _ = closed.cancelled() => {},
                    _ = shutdown.cancelled() => {},
                }
                if !matches!(tokio::time::timeout(Duration::from_secs(2), owned.shutdown()).await, Ok(Ok(()))) {
                    tracing::warn!("bounded code mode session shutdown unconfirmed");
                }
            });
            allowance.check_execution_open().map_err(|e| e.to_string())?;
            Ok(Arc::new(ScopedSession {
                inner,
                namespace: format!("bounded:{}:", turn.sub_id),
                allowance: Arc::clone(&allowance),
                closed: state.closed.clone(),
                shutdown: self.shutdown_token.clone(),
            }) as Arc<dyn CodeModeSession>)
        }).await.map(Arc::clone)
    }
}

struct ScopedSession {
    inner: Arc<dyn CodeModeSession>,
    namespace: String,
    allowance: Arc<InferenceAllowance>,
    closed: CancellationToken,
    shutdown: CancellationToken,
}

fn expose_id(namespace: &str, id: &CellId) -> CellId {
    CellId::new(format!("{namespace}{id}"))
}

fn expose_response(namespace: &str, mut response: RuntimeResponse) -> RuntimeResponse {
    let (RuntimeResponse::Yielded { cell_id, .. }
    | RuntimeResponse::Terminated { cell_id, .. }
    | RuntimeResponse::Result { cell_id, .. }) = &mut response;
    *cell_id = expose_id(namespace, cell_id);
    response
}

impl ScopedSession {
    fn check_open(&self) -> Result<(), String> {
        if self.closed.is_cancelled() || self.shutdown.is_cancelled() {
            return Err("bounded code mode session ended".into());
        }
        self.allowance
            .check_execution_open()
            .map_err(|e| e.to_string())
    }
    fn resolve_id(&self, id: &CellId) -> Result<CellId, String> {
        self.check_open()?;
        id.as_str()
            .strip_prefix(&self.namespace)
            .filter(|s| !s.is_empty())
            .map(|s| CellId::new(s.to_owned()))
            .ok_or_else(|| "bounded code mode cannot access another execution's cell".into())
    }
    fn expose_outcome(&self, outcome: WaitOutcome) -> WaitOutcome {
        match outcome {
            WaitOutcome::LiveCell(response) => {
                WaitOutcome::LiveCell(expose_response(&self.namespace, response))
            }
            WaitOutcome::MissingCell(response) => {
                WaitOutcome::MissingCell(expose_response(&self.namespace, response))
            }
        }
    }
}

impl CodeModeSession for ScopedSession {
    fn output_truncation(&self) -> CodeModeOutputTruncation {
        self.inner.output_truncation()
    }
    fn execute<'a>(
        &'a self,
        request: ExecuteRequest,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        preempt: Option<CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, StartedCell> {
        Box::pin(async move {
            self.check_open()?;
            let delegate = Arc::new(ScopedDelegate {
                inner: delegate,
                namespace: self.namespace.clone(),
                allowance: Arc::clone(&self.allowance),
                closed: self.closed.clone(),
                shutdown: self.shutdown.clone(),
            });
            let started = tokio::select! {
                biased;
                _ = finished(&self.allowance, &self.closed, &self.shutdown) => return Err("bounded code mode execution ended".into()),
                result = self.inner.execute(request, delegate, preempt) => result?,
            };
            let id = expose_id(&self.namespace, &started.cell_id);
            let namespace = self.namespace.clone();
            let allowance = Arc::clone(&self.allowance);
            let closed = self.closed.clone();
            let shutdown = self.shutdown.clone();
            Ok(StartedCell::from_future(id, async move {
                tokio::select! {
                    biased;
                    _ = finished(&allowance, &closed, &shutdown) => Err("bounded code mode execution ended".into()),
                    result = started.initial_response() => result.map(|r| expose_response(&namespace, r)),
                }
            }))
        })
    }
    fn wait<'a>(
        &'a self,
        mut request: WaitRequest,
        preempt: Option<CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(async move {
            request.cell_id = self.resolve_id(&request.cell_id)?;
            let result = tokio::select! {
                biased;
                _ = finished(&self.allowance, &self.closed, &self.shutdown) => return Err("bounded code mode execution ended".into()),
                result = self.inner.wait(request, preempt) => result?,
            };
            Ok(self.expose_outcome(result))
        })
    }
    fn terminate<'a>(
        &'a self,
        mut request: TerminateRequest,
    ) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(async move {
            request.cell_id = self.resolve_id(&request.cell_id)?;
            let result = tokio::select! {
                biased;
                _ = finished(&self.allowance, &self.closed, &self.shutdown) => return Err("bounded code mode execution ended".into()),
                result = self.inner.terminate(request) => result?,
            };
            Ok(self.expose_outcome(result))
        })
    }
    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()> {
        self.closed.cancel();
        self.inner.shutdown()
    }
}

async fn finished(
    allowance: &InferenceAllowance,
    closed: &CancellationToken,
    shutdown: &CancellationToken,
) {
    tokio::select! {
        biased;
        _ = allowance.execution_finished() => {},
        _ = closed.cancelled() => {},
        _ = shutdown.cancelled() => {},
    }
}

struct ScopedDelegate {
    inner: Arc<dyn CodeModeSessionDelegate>,
    namespace: String,
    allowance: Arc<InferenceAllowance>,
    closed: CancellationToken,
    shutdown: CancellationToken,
}
impl CodeModeSessionDelegate for ScopedDelegate {
    fn invoke_tool<'a>(
        &'a self,
        mut invocation: CodeModeNestedToolCall,
        cancellation: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        Box::pin(async move {
            if self.closed.is_cancelled()
                || self.shutdown.is_cancelled()
                || self.allowance.check_execution_open().is_err()
            {
                cancellation.cancel();
                return Err("bounded code mode execution ended".into());
            }
            invocation.cell_id = expose_id(&self.namespace, &invocation.cell_id);
            self.inner.invoke_tool(invocation, cancellation).await
        })
    }
    fn notify<'a>(
        &'a self,
        call_id: String,
        cell_id: CellId,
        text: String,
        cancellation: CancellationToken,
    ) -> NotificationFuture<'a> {
        Box::pin(async move {
            if self.closed.is_cancelled()
                || self.shutdown.is_cancelled()
                || self.allowance.check_execution_open().is_err()
            {
                cancellation.cancel();
                return Err("bounded code mode execution ended".into());
            }
            self.inner
                .notify(
                    call_id,
                    expose_id(&self.namespace, &cell_id),
                    text,
                    cancellation,
                )
                .await
        })
    }
    fn cell_closed(&self, cell_id: &CellId) {
        self.inner.cell_closed(&expose_id(&self.namespace, cell_id));
    }
}
