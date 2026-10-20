//! Goal API edits and clears enter model history across loaded and cold-resume paths.

use super::*;
use codex_app_server_protocol::ThreadGoalSetResponse;
use pretty_assertions::assert_eq;
use test_case::test_case;

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
