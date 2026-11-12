//! One turn-local process owner shared by the private executor and V8 host.
//! Selection is host input, never persisted thread state or a resource grant.

use std::collections::HashMap;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::OnceLock;

use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use codex_protocol::turn_input::TurnProcessLimits;
use tokio_util::sync::CancellationToken;

use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::session::turn_context::TurnContext;

#[cfg(target_os = "linux")]
type OwnerResult = std::result::Result<Arc<Owner>, String>;

pub(crate) struct ProcessResources {
    #[cfg(target_os = "linux")]
    limits: TurnProcessLimits,
    closed: CancellationToken,
    #[cfg(target_os = "linux")]
    started: OnceLock<tokio::sync::watch::Receiver<Option<OwnerResult>>>,
}

impl ProcessResources {
    pub(crate) fn new(limits: TurnProcessLimits) -> Self {
        #[cfg(not(target_os = "linux"))]
        let _ = limits;
        Self {
            #[cfg(target_os = "linux")]
            limits,
            closed: CancellationToken::new(),
            #[cfg(target_os = "linux")]
            started: OnceLock::new(),
        }
    }
}

impl Drop for ProcessResources {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}

#[cfg(target_os = "linux")]
struct Owner {
    group: Arc<codex_utils_pty::linux_resources::LinuxProcessResourceGroup>,
    environments: TurnEnvironmentSnapshot,
    host_environment: HashMap<String, String>,
    command_policy: Arc<codex_protocol::config_types::ShellEnvironmentPolicy>,
    cleanup: tokio::sync::watch::Receiver<Option<std::result::Result<(), String>>>,
}

/// Freeze one restricted snapshot before tool discovery or sampling. Initialization
/// runs once in a finite task so cancelling an observer cannot repeat an uncertain
/// allocation. A failure is retained for this turn, not retried on its next step.
pub(crate) async fn environments(
    turn: &Arc<TurnContext>,
    snapshot: TurnEnvironmentSnapshot,
    cancellation: &CancellationToken,
) -> Result<TurnEnvironmentSnapshot> {
    let Some(state) = turn.extension_data.get::<ProcessResources>() else {
        return Ok(snapshot);
    };
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (state, cancellation);
        Err(CodexErr::InvalidRequest("owned turn requires Linux".into()))
    }
    #[cfg(target_os = "linux")]
    {
        let allowance = crate::turn_inference::allowance(turn).ok_or_else(|| {
            CodexErr::InvalidRequest("owned process resources require a finite allowance".into())
        })?;
        allowance
            .check_execution_open()
            .map_err(|e| CodexErr::InvalidRequest(e.to_string()))?;
        let mut observation = state
            .started
            .get_or_init(|| {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                let limits = state.limits.clone();
                let closed = state.closed.clone();
                let cancelled = cancellation.clone();
                let config = Arc::clone(&turn.config);
                let policy = turn.command_environment_policy();
                let lifetime = Arc::clone(&allowance);
                tokio::spawn(async move {
                    let result = tokio::select! {
                        biased;
                        _ = lifetime.execution_finished() => Err("owned turn ended during process allocation".into()),
                        _ = closed.cancelled() => Err("owned turn closed during process allocation".into()),
                        _ = cancelled.cancelled() => Err("owned turn cancelled during process allocation".into()),
                        result = create_owner(config, snapshot, policy, limits, Arc::clone(&lifetime), closed.clone(), cancelled.clone()) => result,
                    };
                    sender.send_replace(Some(result));
                });
                receiver
            })
            .clone();
        loop {
            if let Some(result) = observation.borrow().clone() {
                allowance
                    .check_execution_open()
                    .map_err(|e| CodexErr::InvalidRequest(e.to_string()))?;
                if cancellation.is_cancelled() || state.closed.is_cancelled() {
                    return Err(CodexErr::TurnAborted);
                }
                return result
                    .map(|owner| owner.environments.clone())
                    .map_err(CodexErr::InvalidRequest);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(CodexErr::TurnAborted),
                _ = allowance.execution_finished() => return Err(CodexErr::InvalidRequest("owned turn ended".into())),
                changed = observation.changed() => changed.map_err(|_| CodexErr::InvalidRequest("owned process allocation ended without a result".into()))?,
            }
        }
    }
}

/// Both consumers receive the exact same owner and sanitized explicit environment.
type HostSelection = (
    HashMap<String, String>,
    Arc<dyn codex_utils_pty::ProcessLauncher>,
);

pub(crate) fn host_selection(
    turn: &TurnContext,
) -> std::result::Result<Option<HostSelection>, String> {
    let Some(state) = turn.extension_data.get::<ProcessResources>() else {
        return Ok(None);
    };
    #[cfg(not(target_os = "linux"))]
    {
        let _ = state;
        Err("owned turn requires Linux".into())
    }
    #[cfg(target_os = "linux")]
    {
        let owner = state
            .started
            .get()
            .and_then(|receiver| receiver.borrow().clone())
            .ok_or_else(|| "owned process resources are not initialized".to_string())??;
        if owner.cleanup.borrow().is_some() {
            return Err("owned process resources have closed".into());
        }
        Ok(Some((owner.host_environment.clone(), owner.group.clone())))
    }
}

/// Preparation and dispatch must use the same sanitized environment even when a
/// tool resolves its environment again just before execution.
pub(crate) fn command_policy(
    turn: &TurnContext,
) -> Option<Arc<codex_protocol::config_types::ShellEnvironmentPolicy>> {
    #[cfg(target_os = "linux")]
    {
        let state = turn.extension_data.get::<ProcessResources>()?;
        let result = state.started.get()?.borrow().clone()?;
        result.ok().map(|owner| owner.command_policy.clone())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = turn;
        None
    }
}

pub(crate) fn enforce_environment(
    turn: &TurnContext,
    environment: &mut crate::session::turn_context::TurnEnvironment,
) -> Result<()> {
    let Some(state) = turn.extension_data.get::<ProcessResources>() else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        let owner = state
            .started
            .get()
            .and_then(|receiver| receiver.borrow().clone())
            .ok_or_else(|| CodexErr::InvalidRequest("owned executor is not ready".into()))?
            .map_err(CodexErr::InvalidRequest)?;
        let selected = owner
            .environments
            .turn_environments()
            .find(|selected| {
                selected.selection.environment_id == environment.selection.environment_id
                    && selected.cwd() == environment.cwd()
                    && selected.workspace_roots() == environment.workspace_roots()
            })
            .ok_or_else(|| {
                CodexErr::InvalidRequest(
                    "tool selected an environment outside this execution owner".into(),
                )
            })?;
        *environment = selected.clone();
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (state, environment);
        Err(CodexErr::InvalidRequest("owned turn requires Linux".into()))
    }
}

#[cfg(target_os = "linux")]
async fn create_owner(
    config: Arc<crate::config::Config>,
    mut snapshot: TurnEnvironmentSnapshot,
    policy: Option<Arc<codex_protocol::config_types::ShellEnvironmentPolicy>>,
    limits: TurnProcessLimits,
    allowance: Arc<codex_api::InferenceAllowance>,
    closed: CancellationToken,
    cancelled: CancellationToken,
) -> OwnerResult {
    use crate::environment_selection::TurnEnvironmentState;
    use codex_exec_server::Environment;
    use codex_exec_server::ExecServerRuntimeOptions;
    use codex_exec_server::StdioExecServerCommand;
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::permissions::FileSystemAccessMode;
    use codex_protocol::permissions::FileSystemSandboxEntry;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use codex_utils_path_uri::PathUri;
    use codex_utils_pty::linux_resources::LinuxProcessLimits;
    use codex_utils_pty::linux_resources::LinuxProcessResourceGroup;

    let policy =
        policy.ok_or_else(|| "owned turn has no explicit environment policy".to_string())?;
    if snapshot
        .turn_environments()
        .any(|environment| environment.environment.is_remote())
    {
        return Err("owned process profile requires qualified local environments".into());
    }
    // Actor processes cannot use the host manager to escape their aggregate, or
    // recover the native provider login. Reapply these immutable denies through
    // the same permission ceiling used by filesystem and raw command consumers.
    for state in &mut snapshot.environments {
        if let TurnEnvironmentState::Ready(selected) = state {
            let authority = selected.permission_profile().clone();
            let mut filesystem = authority.file_system_sandbox_policy();
            for path in [
                std::path::PathBuf::from("/sys/fs/cgroup"),
                std::path::PathBuf::from(format!("/run/user/{}", unsafe { libc::geteuid() })),
                config.codex_home.to_path_buf(),
            ] {
                filesystem.entries.push(FileSystemSandboxEntry::new(
                    AbsolutePathBuf::from_absolute_path(path)
                        .map_err(|e| e.to_string())?
                        .into(),
                    FileSystemAccessMode::Deny,
                ));
            }
            let narrowed = PermissionProfile::from_runtime_permissions_with_enforcement(
                authority.enforcement(),
                &filesystem,
                authority.network_sandbox_policy(),
            );
            selected
                .restrict_permissions(&narrowed, Some(&policy))
                .map_err(|e| e.to_string())?;
        }
    }
    let cwd = snapshot
        .turn_environments()
        .next()
        .ok_or_else(|| "owned turn has no ready local environment".to_string())?
        .cwd()
        .to_abs_path()
        .map_err(|e| e.to_string())?
        .to_path_buf();
    // The complete native package owns this sibling. Never PATH-search, use a
    // human-configured remote executor, or substitute an unowned command.
    let binary = config
        .codex_self_exe
        .as_ref()
        .and_then(|exe| exe.parent())
        .ok_or_else(|| "owned turn has no native package executable path".to_string())?
        .join("codex-exec-server");
    let binary =
        std::fs::canonicalize(binary).map_err(|e| format!("owned executor unavailable: {e}"))?;
    let home = tempfile::Builder::new()
        .prefix("codex-owned-exec-")
        .tempdir()
        .map_err(|e| e.to_string())?;
    let home_uri = PathUri::from_host_native_path(home.path()).map_err(|e| e.to_string())?;
    // The executor creates trusted arg0 helpers beneath CODEX_HOME. Keep that
    // control state under the native home's immutable denial, even when the
    // actor may write its separate HOME or the surrounding temporary directory.
    let control_home = tempfile::Builder::new()
        .prefix("codex-exec-control-")
        .tempdir_in(config.codex_home.as_path())
        .map_err(|error| error.to_string())?;
    let mut environment = crate::exec_env::create_env(&policy, None);
    environment.insert("HOME".into(), home.path().to_string_lossy().into_owned());
    environment.insert(
        "CODEX_HOME".into(),
        control_home.path().to_string_lossy().into_owned(),
    );
    let mut command_policy = (*policy).clone();
    command_policy.r#set = environment.clone();
    let command_policy = Arc::new(command_policy);
    let remaining = allowance
        .deadline()
        .saturating_duration_since(tokio::time::Instant::now());
    let group = Arc::new(
        LinuxProcessResourceGroup::start(
            LinuxProcessLimits {
                memory_bytes: limits.memory_bytes,
                cpu_percent: limits.cpu_percent,
                tasks: limits.tasks,
            },
            remaining,
        )
        .await
        .map_err(|e| e.to_string())?,
    );
    let executor = Arc::new(
        Environment::owned_stdio(
            StdioExecServerCommand {
                program: binary.to_string_lossy().into_owned(),
                args: vec!["--listen".into(), "stdio://".into()],
                env: environment.clone(),
                cwd: Some(cwd),
            },
            group.clone(),
            ExecServerRuntimeOptions::new(binary, config.codex_linux_sandbox_exe.clone())
                .map_err(|e| e.to_string())?,
            config.http_client_factory(),
        )
        .map_err(|e| e.to_string())?,
    );
    for state in &mut snapshot.environments {
        if let TurnEnvironmentState::Ready(selected) = state {
            selected.environment = executor.clone();
            selected.user_home_dir = Some(home_uri.clone());
            selected.temporary_directories = Some(vec![home_uri.clone()]);
            if let codex_protocol::protocol::EnvironmentConfigState::Ready(config) =
                &mut selected.selection.config
            {
                config.shell_environment_policy = (*command_policy).clone();
                config.allow_login_shell = false;
            }
        }
    }
    let (cleanup, cleanup_result) = tokio::sync::watch::channel(None);
    let owned = Arc::new(Owner {
        group: group.clone(),
        environments: snapshot,
        host_environment: environment,
        command_policy,
        cleanup: cleanup_result,
    });
    // This finite cleanup survives observers and retained step/session references.
    // The kernel deadline also bounds loss of the interpreting runtime.
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = allowance.execution_finished() => {},
            _ = closed.cancelled() => {},
            _ = cancelled.cancelled() => {},
        }
        let result = group.close().await.map_err(|error| error.to_string());
        if let Err(error) = &result {
            tracing::warn!("owned turn resource close unconfirmed: {error}");
        }
        let home_result = home.close().map_err(|error| error.to_string());
        if let Err(error) = &home_result {
            tracing::warn!("owned turn private home cleanup unconfirmed: {error}");
        }
        let control_result = control_home.close().map_err(|error| error.to_string());
        if let Err(error) = &control_result {
            tracing::warn!("owned executor control cleanup unconfirmed: {error}");
        }
        cleanup.send_replace(Some(result.and(home_result).and(control_result)));
    });
    Ok(owned)
}

#[cfg(all(test, target_os = "linux"))]
#[path = "turn_process_tests.rs"]
mod tests;
