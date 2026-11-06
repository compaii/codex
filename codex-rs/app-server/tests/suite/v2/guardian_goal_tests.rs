//! Goal API edits reach model history and existing workers' Guardian authorization.

use super::*;
use codex_app_server_protocol::ThreadGoalSetResponse;
use pretty_assertions::assert_eq;
use test_case::test_case;

/// A goal set after spawning a worker must reach its reviewer only with user provenance.
#[test_case("user"; "user goal")]
#[test_case("automatic"; "automatic goal is not authorization")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_guardian_sees_goal_set_after_spawn(origin: &str) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let state = Arc::new(MockResponsesState {
        root_worker: true,
        mcp_server_name: Some(TEST_SERVER_NAME),
        ..Default::default()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let responses_url = format!("http://{}", listener.local_addr()?);
    let router = Router::new()
        .route("/v1/responses", post(parent_response))
        .with_state(Arc::clone(&state));
    let responses_server = tokio::spawn(async move { axum::serve(listener, router).await });
    let (mcp_url, mcp_server) = start_mcp_server(/*sensitive_action*/ None).await?;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&responses_url)
        .with_provider_config("supports_websockets = false")
        .with_approval_policy("on-request")
        .with_root_config("approvals_reviewer = \"auto_review\"")
        .with_extra_config(&format!(
            "[mcp_servers.{TEST_SERVER_NAME}]\nurl = \"{mcp_url}/mcp\"\ndefault_tools_approval_mode = \"prompt\""
        ))
        .enable_feature(Feature::GuardianApproval)
        .enable_feature(Feature::Collab)
        .enable_feature(Feature::MultiAgentV2)
        .enable_feature(Feature::Goals)
        .disable_feature(Feature::GuardianV2)
        .disable_feature(Feature::EnableRequestCompression)
        .write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let root = app.start_thread(ThreadStartParams::default()).await?.thread;
    state.allow_guardian_review.notify_one();
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: root.id.clone(),
            input: vec![UserInput::Text {
                text: USER_CONTEXT.to_owned(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(id).await?;
    let review: ItemGuardianApprovalReviewStartedNotification = timeout(
        TIMEOUT,
        app.read_notification("item/autoApprovalReview/started"),
    )
    .await??;
    assert_ne!(review.thread_id, root.id);
    loop {
        let completed: TurnCompletedNotification =
            timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
        if completed.thread_id == review.thread_id {
            assert_eq!(completed.turn.status, TurnStatus::Completed);
            break;
        }
    }
    wait_for_guardian_reviews(&state, /*expected*/ 2).await?;

    // The worker's fork and first review predate the goal. Keep the goal paused so
    // only the explicit follow-up below starts work, without an autonomous goal turn.
    let objective = "Publish the approved report using the configured MCP tool.";
    let id = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({
                "threadId": root.id, "objective": objective, "status": "paused", "origin": origin,
            })),
        )
        .await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: root.id,
            input: vec![UserInput::Text {
                text: "Ask the worker to check again.".to_owned(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(id).await?;
    wait_for_guardian_reviews(&state, /*expected*/ 3).await?;
    {
        let requests = state.guardian_requests.lock().expect("Guardian requests");
        assert!(!requests[0].to_string().contains(objective));
        let text = requests[2]["input"].to_string();
        assert_eq!(text.contains(objective), origin == "user", "{text}");
        if origin == "user" {
            let (_, root_context) = text
                .split_once(">>> ROOT CONVERSATION START")
                .expect("root authorization context");
            let (root_context, _) = root_context
                .split_once(">>> ROOT CONVERSATION END")
                .expect("root authorization context end");
            assert!(
                root_context.contains("User set the goal:"),
                "{root_context}"
            );
            assert!(root_context.contains(objective), "{root_context}");
        }
    }
    app.shutdown_gracefully().await?;
    mcp_server.abort();
    responses_server.abort();
    Ok(())
}

#[test_case(false; "loaded")]
#[test_case(true; "unloaded with path resume")]
#[tokio::test]
async fn user_goal_updates_survive_resume_and_clear(unloaded: bool) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![responses::ev_completed("done")]),
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .disable_feature(Feature::EnableRequestCompression)
        .write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread_id = if unloaded {
        create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "",
            Some("mock_provider"),
            /*git_info*/ None,
        )?
    } else {
        app.start_thread(ThreadStartParams::default())
            .await?
            .thread
            .id
    };
    // Automatic lifecycle calls mutate goal state without creating user authorization.
    let id = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({
                "threadId": thread_id, "objective": "Automatically generated goal.",
                "status": "paused", "origin": "automatic",
            })),
        )
        .await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    let mut resume_request = None;
    for objective in [
        "Send the approved report.",
        "Draft the report, but do not send it.",
    ] {
        if unloaded && objective.starts_with("Draft") {
            let path = codex_rollout::find_thread_path_by_id_str(
                codex_home.path(),
                &thread_id,
                /*state_db_ctx*/ None,
            )
            .await?
            .expect("unloaded rollout");
            resume_request = Some(
                app.send_thread_resume_request(ThreadResumeParams {
                    thread_id: String::new(),
                    path: Some(path),
                    ..Default::default()
                })
                .await?,
            );
        }
        let id = app
            .send_raw_request(
                "thread/goal/set",
                Some(json!({
                    "threadId": thread_id, "objective": objective, "status": "paused", "origin": "user",
                })),
            )
            .await?;
        let _: ThreadGoalSetResponse = app.read_response(id).await?;
    }
    if let Some(id) = resume_request {
        let _: ThreadResumeResponse = app.read_response(id).await?;
    }
    let id = app
        .send_raw_request(
            "thread/goal/clear",
            Some(json!({"threadId": thread_id, "origin": "user"})),
        )
        .await?;
    let _: codex_app_server_protocol::ThreadGoalClearResponse = app.read_response(id).await?;
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id,
            input: vec![UserInput::Text {
                text: "What is the status?".to_owned(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(id).await?;
    let completed: TurnCompletedNotification =
        timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    let history = mock.single_request().message_input_texts("user").join("\n");
    assert!(
        !history.contains("Automatically generated goal."),
        "{history}"
    );
    let old = history
        .find("User set the goal: \"Send the approved report.\"")
        .expect("original user goal");
    let new = history
        .find("User set the goal: \"Draft the report, but do not send it.\"")
        .expect("updated user goal");
    assert!(old < new, "{history}");
    assert!(
        history.contains("User set goal status: \"paused\"."),
        "{history}"
    );
    assert!(history.contains("User cleared the goal."), "{history}");
    app.shutdown_gracefully().await?;
    Ok(())
}

#[test_case(false; "conditional loaded")]
#[test_case(true; "conditional unloaded")]
#[tokio::test]
async fn conditional_goal_controls_reject_stale_selection_without_recording(
    unloaded: bool,
) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread_id = if unloaded {
        create_fake_rollout(
            home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "",
            Some("mock_provider"),
            None,
        )?
    } else {
        app.start_thread(ThreadStartParams::default())
            .await?
            .thread
            .id
    };
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread_id})))
        .await?;
    let initial: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    assert!(initial.goal.is_none());
    let id = app.send_raw_request("thread/goal/conditionalSet",Some(json!({
        "threadId":thread_id,"expectedRevision":initial.revision,"objective":"selected original","status":"paused","origin":"user"
    }))).await?;
    let selected: codex_app_server_protocol::ThreadGoalConditionalSetResponse =
        app.read_response(id).await?;
    let id = app.send_raw_request("thread/goal/set",Some(json!({"threadId":thread_id,"objective":"later human definition","status":"paused","origin":"user"}))).await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    for (method, extra) in [
        (
            "thread/goal/conditionalSet",
            json!({"objective":"stale-mutation-must-not-be-recorded","status":"complete"}),
        ),
        ("thread/goal/conditionalClear", json!({})),
    ] {
        let mut params =
            json!({"threadId":thread_id,"expectedRevision":selected.revision,"origin":"user"});
        params
            .as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        let id = app.send_raw_request(method, Some(params)).await?;
        let error = app
            .read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(id))
            .await?;
        assert!(error.error.message.contains("goal changed"), "{error:?}");
    }
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread_id})))
        .await?;
    let current: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    assert_eq!(
        current.goal.expect("retained goal").objective,
        "later human definition"
    );
    let id = app
        .send_raw_request(
            "thread/goal/conditionalClear",
            Some(json!({"threadId":thread_id,"expectedRevision":current.revision,"origin":"user"})),
        )
        .await?;
    let cleared: codex_app_server_protocol::ThreadGoalConditionalClearResponse =
        app.read_response(id).await?;
    assert!(cleared.cleared);
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread_id})))
        .await?;
    let final_snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    assert!(final_snapshot.goal.is_none());
    assert_eq!(cleared.revision, final_snapshot.revision);
    assert_ne!(initial.revision, final_snapshot.revision);
    let path = codex_rollout::find_thread_path_by_id_str(home.path(), &thread_id, None)
        .await?
        .expect("retained history");
    let history = std::fs::read_to_string(path)?;
    assert!(!history.contains("stale-mutation-must-not-be-recorded"));
    app.shutdown_gracefully().await?;
    Ok(())
}

#[tokio::test]
async fn conditional_activation_delivers_context_to_first_useful_turn() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("activate"),
                responses::ev_function_call(
                    "finish-goal",
                    "update_goal",
                    r#"{"status":"complete"}"#,
                ),
                responses::ev_completed("activate"),
            ]),
            responses::sse(vec![
                responses::ev_assistant_message("final", "Finished useful work."),
                responses::ev_completed("finished"),
            ]),
        ],
    )
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .disable_feature(Feature::EnableRequestCompression)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread.id})))
        .await?;
    let snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    let id=app.send_raw_request("thread/goal/conditionalActivate",Some(json!({
        "threadId":thread.id,"expectedRevision":snapshot.revision,"objective":"Perform the approved useful work, then complete the goal.","origin":"user",
        "additionalContext":{"interaction":{"value":"FRESH-CONTROLLER-LEASE-ACTIVATION","kind":"untrusted"}}
    }))).await?;
    let activated: codex_app_server_protocol::ThreadGoalConditionalActivateResponse =
        app.read_response(id).await?;
    let actual_turn = match activated.activation {
        codex_app_server_protocol::ThreadGoalActivationDisposition::Started { turn_id } => turn_id,
        other => panic!("Expected first useful native turn: {other:?}"),
    };
    let completed: TurnCompletedNotification =
        timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(actual_turn, completed.turn.id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    let requests = mock.requests();
    assert_eq!(
        requests.len(),
        2,
        "one useful turn with a completion tool, no priming turn"
    );
    assert!(
        requests[0]
            .body_json()
            .to_string()
            .contains("FRESH-CONTROLLER-LEASE-ACTIVATION")
    );
    let id=app.send_raw_request("thread/goal/conditionalActivate",Some(json!({
        "threadId":thread.id,"expectedRevision":snapshot.revision,"objective":"STALE-ACTIVATION-MUST-NOT-RUN","origin":"user",
        "additionalContext":{"interaction":{"value":"STALE-CONTEXT-MUST-NOT-APPEAR","kind":"untrusted"}}
    }))).await?;
    let error = app
        .read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(id))
        .await?;
    assert!(error.error.message.contains("goal changed"), "{error:?}");
    assert_eq!(mock.requests().len(), 2);
    let path = codex_rollout::find_thread_path_by_id_str(home.path(), &thread.id, None)
        .await?
        .expect("retained rollout");
    let history = std::fs::read_to_string(path)?;
    assert!(!history.contains("STALE-ACTIVATION-MUST-NOT-RUN"));
    assert!(!history.contains("STALE-CONTEXT-MUST-NOT-APPEAR"));
    app.shutdown_gracefully().await?;
    Ok(())
}

#[tokio::test]
async fn conditional_activation_unloaded_rejects_before_mutating_goal() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread = create_fake_rollout(
        home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "",
        Some("mock_provider"),
        None,
    )?;
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread})))
        .await?;
    let before: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    let id=app.send_raw_request("thread/goal/conditionalActivate",Some(json!({"threadId":thread,"expectedRevision":before.revision,"objective":"DO-NOT-CREATE-UNLOADED","origin":"user","additionalContext":{"interaction":{"value":"NO-UNLOADED-INJECTION","kind":"untrusted"}}}))).await?;
    let error = app
        .read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(id))
        .await?;
    assert!(
        error.error.message.contains("loaded goal runtime"),
        "{error:?}"
    );
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread})))
        .await?;
    let after: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    assert_eq!(before.revision, after.revision);
    assert!(after.goal.is_none());
    assert!(
        server
            .received_requests()
            .await
            .expect("mock requests")
            .is_empty()
    );
    let path = codex_rollout::find_thread_path_by_id_str(home.path(), &thread, None)
        .await?
        .expect("rollout");
    let history = std::fs::read_to_string(path)?;
    assert!(!history.contains("DO-NOT-CREATE-UNLOADED"));
    assert!(!history.contains("NO-UNLOADED-INJECTION"));
    app.shutdown_gracefully().await?;
    Ok(())
}

#[tokio::test]
async fn conditional_activation_steers_the_existing_useful_turn() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(responses::sse(vec![
                    responses::ev_response_created("busy"),
                    responses::ev_function_call(
                        "complete-busy-goal",
                        "update_goal",
                        r#"{"status":"complete"}"#,
                    ),
                    responses::ev_completed("busy"),
                ]))
                .set_delay(Duration::from_secs(5)),
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(responses::sse(vec![
                    responses::ev_assistant_message("final", "Finished current work."),
                    responses::ev_completed("finished"),
                ])),
        ],
    )
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .disable_feature(Feature::EnableRequestCompression)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let id=app.send_raw_request("thread/goal/set",Some(json!({"threadId":thread.id,"objective":"Existing goal","status":"paused","origin":"user"}))).await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread.id})))
        .await?;
    let snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "Continue this authorized work.".into(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let started: TurnStartResponse = app.read_response(id).await?;
    timeout(TIMEOUT, async {
        while mock.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let id=app.send_raw_request("thread/goal/conditionalActivate",Some(json!({"threadId":thread.id,"expectedRevision":snapshot.revision,"origin":"user","additionalContext":{"interaction":{"value":"FRESH-CONTEXT-WHILE-BUSY","kind":"untrusted"}}}))).await?;
    let activation: codex_app_server_protocol::ThreadGoalConditionalActivateResponse =
        app.read_response(id).await?;
    assert_eq!(
        activation.activation,
        codex_app_server_protocol::ThreadGoalActivationDisposition::Steered {
            turn_id: started.turn.id.clone()
        }
    );
    let completed: TurnCompletedNotification =
        timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.id, started.turn.id);
    let requests = mock.requests();
    assert_eq!(requests.len(), 2, "no additional physical turn");
    assert!(
        !requests[0]
            .body_json()
            .to_string()
            .contains("FRESH-CONTEXT-WHILE-BUSY")
    );
    assert!(
        requests[1]
            .body_json()
            .to_string()
            .contains("FRESH-CONTEXT-WHILE-BUSY")
    );
    app.shutdown_gracefully().await?;
    Ok(())
}

/// A bounded attention turn is not the authority for an independent persisted goal.
#[test_case(false; "legacy agent tools")]
#[test_case(true; "v2 agent tools")]
#[tokio::test]
async fn bounded_attention_preserves_human_goal_and_independent_continuation(
    v2: bool,
) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let event_stream = |events| {
        wiremock::ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(responses::sse(events))
    };
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            event_stream(vec![
                responses::ev_response_created("attention-tools"),
                responses::ev_function_call(
                    "forbidden-update",
                    "update_goal",
                    r#"{"status":"complete"}"#,
                ),
                responses::ev_function_call(
                    "forbidden-create",
                    "create_goal",
                    r#"{"objective":"Unauthorized replacement"}"#,
                ),
                responses::ev_function_call("allowed-read", "get_goal", "{}"),
                responses::ev_function_call_with_namespace(
                    "unreserved-spawn",
                    if v2 {
                        "collaboration"
                    } else {
                        "multi_agent_v1"
                    },
                    "spawn_agent",
                    r#"{"task_name":"unreserved","message":"Do not start this unreserved work"}"#,
                ),
                responses::ev_function_call_with_namespace(
                    "unreserved-followup",
                    if v2 {
                        "collaboration"
                    } else {
                        "multi_agent_v1"
                    },
                    if v2 { "followup_task" } else { "send_input" },
                    r#"{"target":"unreserved","message":"Do not wake this target"}"#,
                ),
                responses::ev_completed_with_tokens("attention-tools", 123),
            ]),
            event_stream(vec![
                responses::ev_assistant_message("attention-final", "Review finished."),
                responses::ev_completed_with_tokens("attention-finished", 456),
            ])
            .set_delay(Duration::from_secs(5)),
            event_stream(vec![
                responses::ev_response_created("goal-work"),
                responses::ev_function_call("goal-progress-read", "get_goal", "{}"),
                responses::ev_completed_with_tokens("goal-work", 10),
            ]),
            // Account useful progress in the preceding response. Give this
            // completion frame no usage: its tool and token callbacks can race,
            // and existing terminal accounting deliberately excludes late usage.
            // The nonzero independent response still proves goal accounting,
            // while both nonzero bounded responses must remain excluded.
            event_stream(vec![
                responses::ev_response_created("goal-completion"),
                responses::ev_function_call(
                    "finish-independent-goal",
                    "update_goal",
                    r#"{"status":"complete"}"#,
                ),
                responses::ev_completed_with_tokens("goal-completion", 0),
            ]),
            event_stream(vec![
                responses::ev_assistant_message("goal-final", "Authorized work finished."),
                responses::ev_completed_with_tokens("goal-finished", 10),
            ]),
        ],
    )
    .await;
    let home = TempDir::new()?;
    let config = MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::Goals)
        .enable_feature(Feature::Collab)
        .disable_feature(Feature::EnableRequestCompression);
    let config = if v2 {
        config.enable_feature(Feature::MultiAgentV2)
    } else {
        config.disable_feature(Feature::MultiAgentV2)
    };
    config.write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized_with_timeout(TIMEOUT)
        .await?;
    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let id = app.send_raw_request("thread/goal/set", Some(json!({
        "threadId": thread.id, "objective": "Preserved authorized goal", "status": "paused", "origin": "user"
    }))).await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    let turn = TurnStartParams {
        thread_id: thread.id.clone(),
        input: vec![],
        tool_output: Some(Box::new(codex_app_server_protocol::TurnToolOutput {
            name: "heartbeat_attention".into(),
            namespace: None,
            retain: true,
            output: codex_protocol::models::FunctionCallOutputBody::Text("Review only.".into()),
        })),
        ..Default::default()
    };
    let id = app
        .send_raw_request(
            "turn/startBoundedIfIdle",
            Some(json!({
                "turn": turn, "allowance": {"maxProviderAttempts": 2, "maxRequestBytes": 1024*1024,
                "deadlineUnixMs": chrono::Utc::now().timestamp_millis()+30_000}
            })),
        )
        .await?;
    let started: TurnStartResponse = app.read_response(id).await?;
    timeout(TIMEOUT, async {
        while mock.requests().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let second_request = mock.requests()[1].body_json();
    let second_input = second_request.to_string();
    assert_eq!(
        second_input
            .matches("Bounded attention cannot create or modify goals")
            .count(),
        2
    );
    assert_eq!(
        second_input
            .matches("Bounded attention requires separately reserved agent admission")
            .count(),
        2
    );
    let read_result = second_request["input"]
        .as_array()
        .expect("model input")
        .iter()
        .find(|item| item["type"] == "function_call_output" && item["call_id"] == "allowed-read")
        .expect("get_goal result remains available");
    assert!(
        read_result["output"]
            .to_string()
            .contains("Preserved authorized goal")
    );
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread.id})))
        .await?;
    let snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    let goal = snapshot.goal.expect("original goal retained");
    assert_eq!(goal.objective, "Preserved authorized goal");
    assert_eq!(
        goal.status,
        codex_app_server_protocol::ThreadGoalStatus::Paused
    );
    assert_eq!(goal.tokens_used, 0);
    // Human authority may resume the existing goal while bounded attention is busy.
    let id = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({
                "threadId":thread.id, "status":"active", "origin":"user"
            })),
        )
        .await?;
    let _: ThreadGoalSetResponse = app.read_response(id).await?;
    let completed: TurnCompletedNotification =
        timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.id, started.turn.id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    let completed: TurnCompletedNotification =
        timeout(TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_ne!(completed.turn.id, started.turn.id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    assert_eq!(
        mock.requests().len(),
        5,
        "independent continuation has its own inference authority"
    );
    let id = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread.id})))
        .await?;
    let snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(id).await?;
    let goal = snapshot.goal.expect("completed original goal");
    assert_eq!(goal.objective, "Preserved authorized goal");
    assert_eq!(
        goal.status,
        codex_app_server_protocol::ThreadGoalStatus::Complete
    );
    assert_eq!(
        goal.tokens_used, 10,
        "only independent goal inference is accounted"
    );
    app.shutdown_gracefully().await?;
    Ok(())
}
