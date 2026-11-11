use super::bounded_session::*;
use super::*;
use crate::session::session::SessionSettingsUpdate;
use crate::session::tests::make_session_and_context_with_rx;
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

async fn context(
    session: &Arc<Session>,
    name: &str,
    duration: Duration,
) -> anyhow::Result<(Arc<TurnContext>, Arc<codex_api::InferenceAllowance>)> {
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
            Default::default(),
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
        let (session, human, _events) = make_session_and_context_with_rx().await;
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
        let human_pid = recorder.pid()?;
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
                .session_for_turn(&legacy)
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
            )
            .await?;
            let (owned_id, response) = run(
                &service,
                &turn,
                "store('own',7); text(typeof load('human'));",
            )
            .await?;
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
            assert_ne!(private_pid, human_pid);
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
    let (session, _, _events) = make_session_and_context_with_rx().await;
    let called = Arc::new(AtomicBool::new(false));
    let service = CodeModeService::new(
        session.thread_id,
        Arc::new(Legacy(Arc::clone(&called))),
        &CodeModeConfig::default(),
        Default::default(),
    );
    let (turn, allowance) =
        context(&session, "unqualified-provider", Duration::from_secs(10)).await?;
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
    let result = service.session_for_turn(&turn).await;
    assert!(matches!(result, Err(error) if error.contains("process-isolated")));
    assert!(!called.load(Ordering::Acquire));
    assert!(service.session.get().is_none());
    allowance.close();
    Ok(())
}
