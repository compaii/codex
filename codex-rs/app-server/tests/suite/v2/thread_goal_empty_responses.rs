//! Exercises the goal extension's no-activity breaker through the public API.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_sequence;
use codex_app_server_protocol::ConfigBatchWriteParams;
use codex_app_server_protocol::ConfigEdit;
use codex_app_server_protocol::ConfigWriteResponse;
use codex_app_server_protocol::HookEventName;
use codex_app_server_protocol::HookSource;
use codex_app_server_protocol::HooksListParams;
use codex_app_server_protocol::HooksListResponse;
use codex_app_server_protocol::MergeStrategy;
use codex_app_server_protocol::ThreadGoalSetResponse;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadGoalUpdatedNotification;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_features::Feature;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[test_case::test_case(None; "empty")]
#[test_case::test_case(Some("final_answer"); "draft")]
#[test_case::test_case(Some("commentary"); "commentary")]
#[test_case::test_case(Some("tool"); "tool")]
#[test_case::test_case(Some("interrupt"); "interrupt")]
#[tokio::test]
async fn empty_goal_continuations_block_after_three_without_activity(
    recovery: Option<&str>,
) -> Result<()> {
    let turns = match recovery {
        Some("interrupt") => 4,
        Some(_) => 6,
        None => 3,
    };
    let mut scripts = Vec::new();
    for turn in 1..=turns {
        let mut id = format!("response-{turn}");
        let mut events = vec![responses::ev_response_created(&id)];
        if recovery == Some("interrupt") {
            events.push(responses::ev_assistant_message(
                "progress",
                "Ready for the next step",
            ));
            events.push(responses::ev_completed_with_tokens(
                &id, /*total_tokens*/ 330_000,
            ));
            scripts.push(responses::sse(events));
            break;
        }
        if turn == 3
            && let Some(recovery) = recovery
        {
            if recovery == "tool" {
                events.push(responses::ev_function_call("get-goal", "get_goal", "{}"));
                events.push(responses::ev_completed(&id));
                scripts.push(responses::sse(events));
                id.push_str("-after-tool");
                events = vec![responses::ev_response_created(&id)];
            } else {
                let mut added = responses::ev_assistant_message("progress", "");
                added["type"] = json!("response.output_item.added");
                events.push(added);
                events.push(responses::ev_output_text_delta("Useful progress"));
                let mut event = responses::ev_assistant_message("progress", "Useful progress");
                event["item"]["phase"] = json!(recovery);
                events.push(event);
            }
        }
        let mut empty_final = responses::ev_assistant_message(&format!("empty-final-{turn}"), "");
        empty_final["item"]["phase"] = json!("final_answer");
        events.push(empty_final);
        events.push(responses::ev_completed(&id));
        scripts.push(responses::sse(events));
    }
    let server = create_mock_responses_server_sequence(scripts).await;
    let codex_home = TempDir::new()?;
    let mut config = MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .enable_feature(Feature::Goals);
    if recovery == Some("interrupt") {
        config = config
            .enable_feature(Feature::CodexHooks)
            .with_root_config(
                "model_auto_compact_token_limit = 200000\nmodel_post_turn_compact_threshold_percent = 0",
            )
            .with_extra_config(
                r#"
[[hooks.PreCompact]]
matcher = "auto"
[[hooks.PreCompact.hooks]]
type = "command"
command = """printf '%s' '{"continue":false}'"""
command_windows = """Write-Output '{"continue":false}'"""
"#,
            );
    }
    config.write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    if recovery == Some("interrupt") {
        let list = mcp
            .send_hooks_list_request(HooksListParams {
                cwds: vec![codex_home.path().to_path_buf()],
                ..Default::default()
            })
            .await?;
        let HooksListResponse { data } = mcp.read_response(list).await?;
        let hook = data[0]
            .hooks
            .iter()
            .find(|hook| {
                hook.source == HookSource::User && hook.event_name == HookEventName::PreCompact
            })
            .expect("user config should register the pre-compact hook");
        let trust = mcp
            .send_config_batch_write_request(ConfigBatchWriteParams {
                edits: vec![ConfigEdit {
                    key_path: "hooks.state".to_string(),
                    value: json!({hook.key.clone(): {"trusted_hash": hook.current_hash}}),
                    merge_strategy: MergeStrategy::Upsert,
                }],
                file_path: None,
                expected_version: None,
                reload_user_config: true,
            })
            .await?;
        let _: ConfigWriteResponse = mcp.read_response(trust).await?;
    }
    let request = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = mcp.read_response(request).await?;
    let request = mcp
        .send_raw_request(
            "thread/goal/set",
            Some(json!({"threadId": thread.id, "objective": "Finish the work"})),
        )
        .await?;
    let _: ThreadGoalSetResponse = mcp.read_response(request).await?;

    for turn in 1..=turns {
        if turn == 3 && matches!(recovery, Some("final_answer" | "commentary")) {
            let delta: serde_json::Value = timeout(
                Duration::from_secs(30),
                mcp.read_notification("item/agentMessage/delta"),
            )
            .await??;
            assert_eq!(json!("Useful progress"), delta["delta"]);
        }
        let completed: TurnCompletedNotification = timeout(
            Duration::from_secs(30),
            mcp.read_notification("turn/completed"),
        )
        .await??;
        let expected = if recovery == Some("interrupt") && turn > 1 {
            TurnStatus::Interrupted
        } else {
            TurnStatus::Completed
        };
        assert_eq!(expected, completed.turn.status);
        assert_eq!(None, completed.turn.error);
    }
    timeout(Duration::from_secs(30), async {
        loop {
            let notification: ThreadGoalUpdatedNotification =
                mcp.read_notification("thread/goal/updated").await?;
            if notification.goal.status == ThreadGoalStatus::Blocked {
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    server.verify().await;
    Ok(())
}

/// A configured checkpoint leaves the native thread available without changing the goal.
/// A direct human turn supersedes the waiting continuation; a later pause cancels its effect.
#[test_case::test_case(false; "automatic_after_checkpoint")]
#[test_case::test_case(true; "human_input_supersedes_checkpoint")]
#[tokio::test]
async fn goal_checkpoint_yields_without_pausing_or_blocking_human_input(human: bool) -> Result<()> {
    let scripts = (1..=2)
        .map(|i| {
            responses::sse(vec![
                responses::ev_response_created(&format!("checkpoint-{i}")),
                responses::ev_assistant_message(&format!("progress-{i}"), "Useful progress saved"),
                responses::ev_completed(&format!("checkpoint-{i}")),
            ])
        })
        .collect();
    let server = create_mock_responses_server_sequence(scripts).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .enable_feature(Feature::Goals)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let request = app
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = app.read_response(request).await?;
    app.shutdown_gracefully().await?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .enable_feature(Feature::Goals)
        .with_extra_config(&format!(
            "[goals.continuation_idle_seconds]\n\"{}\" = 3\n",
            thread.id
        ))
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let request = app
        .send_raw_request("thread/resume", Some(json!({"threadId":thread.id})))
        .await?;
    let _: serde_json::Value = app.read_response(request).await?;
    let request=app.send_raw_request("thread/goal/set",Some(json!({"threadId":thread.id,"objective":"Preserve the goal while yielding","tokenBudget":100000}))).await?;
    let before: ThreadGoalSetResponse = app.read_response(request).await?;
    let _: TurnCompletedNotification = timeout(
        Duration::from_secs(30),
        app.read_notification("turn/completed"),
    )
    .await??;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("mock requests")
            .len(),
        1
    );
    if human {
        let request=app.send_raw_request("turn/start",Some(json!({"threadId":thread.id,"input":[{"type":"text","text":"Direct human steering","text_elements":[]}]}))).await?;
        let _: serde_json::Value =
            timeout(Duration::from_secs(1), app.read_response(request)).await??;
    }
    let _: TurnCompletedNotification = timeout(
        Duration::from_secs(30),
        app.read_notification("turn/completed"),
    )
    .await??;
    let request = app
        .send_raw_request("thread/goal/snapshot", Some(json!({"threadId":thread.id})))
        .await?;
    let snapshot: codex_app_server_protocol::ThreadGoalSnapshotResponse =
        app.read_response(request).await?;
    let goal = snapshot.goal.expect("goal retained");
    assert_eq!(goal.status, ThreadGoalStatus::Active);
    assert_eq!(goal.objective, before.goal.objective);
    assert_eq!(goal.token_budget, before.goal.token_budget);
    assert_eq!(goal.created_at, before.goal.created_at);
    let request = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({"threadId":thread.id,"status":"paused","origin":"user"})),
        )
        .await?;
    let _: ThreadGoalSetResponse = app.read_response(request).await?;
    tokio::time::sleep(Duration::from_millis(3300)).await;
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("mock requests")
            .len(),
        2
    );
    app.shutdown_gracefully().await?;
    Ok(())
}
