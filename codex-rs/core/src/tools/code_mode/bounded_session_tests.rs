use super::bounded_session::*;
use super::*;
use crate::session::session::SessionSettingsUpdate;
use anyhow::Context as _;
use codex_code_mode::CodeModeSessionCellExecutionLimits;
use codex_code_mode::CodeModeSessionOptions;
#[cfg(target_os = "linux")]
use codex_code_mode::FunctionCallOutputContentItem;
#[cfg(target_os = "linux")]
use codex_code_mode::WaitOutcome;
use codex_extension_api::ExtensionDataInit;
#[cfg(target_os = "linux")]
use core_test_support::code_mode_host::CodeModeHostRecorder;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

async fn sandbox_session() -> (
    Arc<Session>,
    Arc<TurnContext>,
    async_channel::Receiver<codex_protocol::protocol::Event>,
) {
    crate::session::tests::make_session_and_context_with_auth_and_config_and_rx(
        codex_login::CodexAuth::from_api_key("Test API Key"),
        vec![],
        |config| {
            let executable = std::env::current_exe().expect("test executable");
            config.codex_self_exe = Some(executable.clone());
            config.codex_linux_sandbox_exe = Some(executable);
            config
                .permissions
                .set_permission_profile(codex_protocol::models::PermissionProfile::Disabled)
                .expect("full-access fixture baseline");
        },
    )
    .await
}

async fn context(
    session: &Arc<Session>,
    name: &str,
    duration: Duration,
    recording_directory: Option<&std::path::Path>,
) -> anyhow::Result<(Arc<TurnContext>, Arc<codex_api::InferenceAllowance>)> {
    let mut file_system = codex_protocol::permissions::FileSystemSandboxPolicy::read_only();
    let mut environment = std::collections::HashMap::from([
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("BOUNDED_HOST_MARKER".into(), "selected-fixture".into()),
    ]);
    if let Some(directory) = recording_directory {
        use codex_protocol::permissions::FileSystemAccessMode;
        use codex_protocol::permissions::FileSystemSandboxEntry;
        use codex_utils_absolute_path::AbsolutePathBuf;
        let root = AbsolutePathBuf::from_absolute_path(directory)?;
        file_system.entries.push(FileSystemSandboxEntry::new(
            root.clone().into(),
            FileSystemAccessMode::Write,
        ));
        file_system.entries.push(FileSystemSandboxEntry::new(
            root.join("blocked credential").into(),
            FileSystemAccessMode::Deny,
        ));
        environment.insert(
            "CODEX_TEST_CODE_MODE_RECORDER".into(),
            directory.to_string_lossy().into_owned(),
        );
    }
    let mut init = ExtensionDataInit::new();
    init.insert(BoundedCodeModeOptions(CodeModeSessionOptions {
        cell_execution_limits: CodeModeSessionCellExecutionLimits {
            max_yield_time_ms: Some(1000),
            max_heap_size_bytes: Some(32 * 1024 * 1024),
        },
        output_truncation: CodeModeOutputTruncation::Server,
    }));
    let (turn, _) = session
        .new_turn_with_sub_id(
            name.into(),
            SessionSettingsUpdate {
                turn_extension_init: Some(init),
                ..Default::default()
            },
            crate::session::turn_context::NewTurnContextOptions {
                permission_ceiling: Some(
                    codex_protocol::models::PermissionProfile::from_runtime_permissions(
                        &file_system,
                        codex_protocol::permissions::NetworkSandboxPolicy::Restricted,
                    ),
                ),
                tool_ceiling: Some(vec![]),
                command_environment_policy: Some(
                    codex_protocol::config_types::ShellEnvironmentPolicy {
                        inherit: codex_protocol::config_types::ShellEnvironmentPolicyInherit::None,
                        r#set: environment,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        )
        .await?;
    let allowance = Arc::new(codex_api::InferenceAllowance::new(
        1,
        4096,
        tokio::time::Instant::now() + duration,
    )?);
    allowance.claim_attempt()?;
    turn.extension_data.insert(Arc::clone(&allowance));
    // A later public attachment change cannot replace the private captured policy.
    turn.extension_data
        .insert(BoundedCodeModeOptions(CodeModeSessionOptions::default()));
    Ok((turn, allowance))
}

#[cfg(target_os = "linux")]
async fn run(
    service: &CodeModeService,
    turn: &Arc<TurnContext>,
    source: &str,
) -> anyhow::Result<(CellId, RuntimeResponse)> {
    let started = service
        .execute(
            codex_code_mode::ExecuteRequest {
                tool_call_id: "test-code-mode".into(),
                enabled_tools: vec![],
                source: source.into(),
                yield_time_ms: Some(1),
                max_output_tokens: Some(500),
            },
            StepContext::for_test(Arc::clone(turn)),
        )
        .await
        .map_err(anyhow::Error::msg)?;
    let id = started.cell_id.clone();
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = started.initial_response().await?;
        while matches!(response, RuntimeResponse::Yielded { .. }) && !source.contains("setTimeout")
        {
            response = service
                .wait(
                    codex_code_mode::WaitRequest {
                        cell_id: id.clone(),
                        yield_time_ms: 500,
                        max_output_tokens: Some(500),
                    },
                    None,
                    turn,
                )
                .await?
                .into();
        }
        Ok::<_, String>(response)
    })
    .await?
    .map_err(anyhow::Error::msg)?;
    Ok((id, response))
}

#[cfg(target_os = "linux")]
fn result_text(response: &RuntimeResponse) -> String {
    match response {
        RuntimeResponse::Result {
            content_items,
            error_text: None,
            ..
        } => content_items
            .iter()
            .filter_map(|item| match item {
                FunctionCallOutputContentItem::InputText { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        other => panic!("expected a successful result, got {other:?}"),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn bounded_code_mode_native_sessions_isolate_heap_names_state_and_lifetime() -> anyhow::Result<()> {
    core_test_support::test_codex::run_test_with_large_stack("bounded-code-mode", || async {
        let recorder = CodeModeHostRecorder::new()?;
        recorder.enable_inheritable_descriptor_probe().await?;
        let recorder_program = recorder.program();
        let recording_directory = recorder_program.parent().expect("recording directory");
        let (session, human, _events) = sandbox_session().await;
        let service = CodeModeService::new(
            session.thread_id,
            Arc::new(
                crate::code_mode_host::ProcessOwnedGrpcCodeModeSessionProvider::with_host_program(
                    recorder.program(),
                ),
            ),
            &CodeModeConfig::default(),
            Default::default(),
        );
        let (_, response) =
            run(&service, &human, "store('human',42); text(load('human'));").await?;
        assert_eq!(result_text(&response), "42");
        let ambient_key = ["HOME", "USER", "LANG"]
            .into_iter()
            .find(|key| std::env::var_os(key).is_some())
            .expect("native host fixture has an ambient environment");
        let has_env = |pid: u32, key: &str, value: Option<&str>| -> anyhow::Result<bool> {
            let raw = std::fs::read(format!("/proc/{pid}/environ"))?;
            let prefix = format!("{key}=");
            Ok(raw.split(|byte| *byte == 0).any(|entry| {
                entry.strip_prefix(prefix.as_bytes()).is_some_and(|actual| {
                    value.is_none_or(|expected| actual == expected.as_bytes())
                })
            }))
        };
        let human_pid = recorder.pid()?;
        assert!(has_env(human_pid, ambient_key, None)?);
        assert!(!has_env(human_pid, "BOUNDED_HOST_MARKER", None)?);
        let human_session = service.session().await.map_err(anyhow::Error::msg)?;
        let (legacy, _) = session
            .new_turn_with_sub_id(
                "legacy-allowance-only".into(),
                SessionSettingsUpdate {
                    turn_extension_init: Some(ExtensionDataInit::new()),
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        let legacy_allowance = Arc::new(codex_api::InferenceAllowance::new(
            1,
            4096,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )?);
        legacy.extension_data.insert(Arc::clone(&legacy_allowance));
        assert!(Arc::ptr_eq(
            &human_session,
            &service
                .session_for_turn(StepContext::for_test(Arc::clone(&legacy)).as_ref())
                .await
                .map_err(anyhow::Error::msg)?
        ));
        let (_, response) = run(&service, &legacy, "text(load('human'));").await?;
        assert_eq!(result_text(&response), "42");
        legacy_allowance.close();
        let (human_cell, response) = run(
            &service,
            &human,
            "await new Promise(r => setTimeout(r,60000)); text('human later');",
        )
        .await?;
        assert!(matches!(response, RuntimeResponse::Yielded { .. }));
        for mode in ["close", "deadline", "heap"] {
            let (turn, allowance) = context(
                &session,
                &format!("bounded-{mode}"),
                if mode == "deadline" {
                    Duration::from_secs(5)
                } else {
                    Duration::from_secs(20)
                },
                Some(recording_directory),
            )
            .await?;
            let (owned_id, response) = run(
                &service,
                &turn,
                "store('own',7); text(typeof load('human'));",
            )
            .await
            .with_context(|| {
                std::fs::read_to_string(recording_directory.join("guard result"))
                    .unwrap_or_else(|error| format!("fixture probe unavailable: {error}"))
            })?;
            assert_eq!(result_text(&response), "undefined");
            assert!(
                owned_id
                    .as_str()
                    .starts_with(&format!("bounded:{}:", turn.sub_id))
            );
            assert_eq!(
                service.output_truncation(&turn),
                CodeModeOutputTruncation::Server
            );
            assert_eq!(
                service.output_truncation(&human),
                CodeModeOutputTruncation::Client
            );
            let private_pid = recorder.pid()?;
            assert_eq!(
                std::fs::read_to_string(recording_directory.join("guard result"))?,
                "guarded"
            );
            assert_ne!(private_pid, human_pid);
            assert!(has_env(
                private_pid,
                "BOUNDED_HOST_MARKER",
                Some("selected-fixture")
            )?);
            assert!(!has_env(private_pid, ambient_key, None)?);
            assert!(has_env(human_pid, ambient_key, None)?);
            assert!(!has_env(human_pid, "BOUNDED_HOST_MARKER", None)?);
            let refused = service
                .wait(
                    codex_code_mode::WaitRequest {
                        cell_id: human_cell.clone(),
                        yield_time_ms: 1,
                        max_output_tokens: None,
                    },
                    None,
                    &turn,
                )
                .await;
            assert!(refused.unwrap_err().contains("another execution"));
            assert!(
                service
                    .terminate(
                        codex_code_mode::TerminateRequest {
                            cell_id: human_cell.clone(),
                            max_output_tokens: None
                        },
                        &turn
                    )
                    .await
                    .unwrap_err()
                    .contains("another execution")
            );
            assert!(matches!(
                human_session
                    .wait(
                        codex_code_mode::WaitRequest {
                            cell_id: human_cell.clone(),
                            yield_time_ms: 1,
                            max_output_tokens: None
                        },
                        None
                    )
                    .await
                    .map_err(anyhow::Error::msg)?,
                WaitOutcome::LiveCell(RuntimeResponse::Yielded { .. })
            ));
            if mode == "heap" {
                // A finite allocation exceeds32MiB but fits the ordinary default heap.
                // V8 OOM must kill this private child, never the human host.
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    run(
                        &service,
                        &turn,
                        "const a=[]; for(let i=0;i<512;i++) a.push(new Array(65536).fill(1)); text('bounded allocation unexpectedly fit');",
                    ),
                )
                .await?;
                assert!(
                    result.is_err(),
                    "heap exhaustion must fail the private execution"
                );
                // Prove failure killed the child before our cleanup or deadline,
                // rather than mistaking a polling timeout for heap enforcement.
                assert!(allowance.check_execution_open().is_ok());
                tokio::time::timeout(Duration::from_secs(2), async {
                    while std::path::Path::new(&format!("/proc/{private_pid}")).exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
                allowance.close();
            } else {
                let (_, response) = run(
                    &service,
                    &turn,
                    "await new Promise(r => setTimeout(r,15000)); text('late');",
                )
                .await?;
                assert!(matches!(response, RuntimeResponse::Yielded { .. }));
                if mode == "close" {
                    allowance.close();
                }
            }
            tokio::time::timeout(Duration::from_secs(7), async {
                while std::path::Path::new(&format!("/proc/{private_pid}")).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert!(run(&service, &turn, "text('must not run')").await.is_err());
            assert_eq!(allowance.attempted()?, 1);
            assert!(std::path::Path::new(&format!("/proc/{human_pid}")).exists());
            let (_, response) = run(&service, &human, "text(load('human'));").await?;
            assert_eq!(result_text(&response), "42");
            assert!(Arc::ptr_eq(
                &human_session,
                &service.session().await.map_err(anyhow::Error::msg)?
            ));
        }
        assert_eq!(
            recorder.invocations()?.len(),
            4,
            "one human host and one private host per bounded turn; no replay or respawn"
        );
        human_session
            .terminate(human_cell.into())
            .await
            .map_err(anyhow::Error::msg)?;
        service.shutdown().await.map_err(anyhow::Error::msg)?;
        Ok(())
    })
}

#[tokio::test]
async fn bounded_code_mode_refuses_unqualified_provider_without_default_session_fallback()
-> anyhow::Result<()> {
    struct Legacy(Arc<AtomicBool>);
    impl codex_code_mode::CodeModeSessionProvider for Legacy {
        fn create_session(&self) -> codex_code_mode::CodeModeSessionProviderFuture<'_> {
            self.0.store(true, Ordering::Release);
            Box::pin(async { Err("legacy create must not be called".into()) })
        }
    }
    let (session, _, _events) = sandbox_session().await;
    let called = Arc::new(AtomicBool::new(false));
    let service = CodeModeService::new(
        session.thread_id,
        Arc::new(Legacy(Arc::clone(&called))),
        &CodeModeConfig::default(),
        Default::default(),
    );
    let (turn, allowance) = context(
        &session,
        "unqualified-provider",
        Duration::from_secs(10),
        None,
    )
    .await?;
    assert!(
        service
            .wait(
                codex_code_mode::WaitRequest {
                    cell_id: CellId::new("human-cell".into()),
                    yield_time_ms: 1,
                    max_output_tokens: None
                },
                None,
                &turn
            )
            .await
            .unwrap_err()
            .contains("another execution")
    );
    let result = service
        .session_for_turn(StepContext::for_test(Arc::clone(&turn)).as_ref())
        .await;
    assert!(matches!(result, Err(error) if error.contains("process-isolated")));
    assert!(!called.load(Ordering::Acquire));
    assert!(service.session.get().is_none());
    allowance.close();
    Ok(())
}

#[tokio::test]
async fn bounded_code_mode_refuses_missing_sandbox_helper_without_default_host_fallback()
-> anyhow::Result<()> {
    struct DefaultHost(Arc<AtomicBool>);
    impl codex_code_mode::CodeModeSessionProvider for DefaultHost {
        fn create_session(&self) -> codex_code_mode::CodeModeSessionProviderFuture<'_> {
            self.0.store(true, Ordering::Release);
            Box::pin(async { Err("default host must not be created".into()) })
        }
    }
    let (session, _, _events) = crate::session::tests::make_session_and_context_with_rx().await;
    let called = Arc::new(AtomicBool::new(false));
    let service = CodeModeService::new(
        session.thread_id,
        Arc::new(DefaultHost(Arc::clone(&called))),
        &CodeModeConfig::default(),
        Default::default(),
    );
    let (turn, allowance) = context(
        &session,
        "missing-sandbox-helper",
        Duration::from_secs(10),
        None,
    )
    .await?;
    let result = service
        .session_for_turn(StepContext::for_test(Arc::clone(&turn)).as_ref())
        .await;
    assert!(matches!(result, Err(error) if error.contains("sandbox helper")));
    assert!(!called.load(Ordering::Acquire));
    assert!(service.session.get().is_none());
    allowance.close();
    Ok(())
}
