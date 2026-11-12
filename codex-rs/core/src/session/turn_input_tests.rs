use super::*;
use crate::config::Constrained;
use crate::session::step_settings::StepSettingsUpdate;
use crate::session::tests::make_session_and_context;
use crate::session::tests::make_session_and_context_with_rx;
use crate::session::turn_context::TurnContext;
use crate::state::TaskKind;
use crate::tasks::SessionTask;
use crate::tasks::SessionTaskResult;
use codex_features::Feature;
use codex_protocol::AgentPath;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::config_types::Settings;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::turn_input::TurnInput as SubmittedTurnInput;
use codex_protocol::user_input::UserInput;
use codex_utils_absolute_path::AbsolutePathBuf;
use core_test_support::test_codex::local_selections;
use pretty_assertions::assert_eq;
use test_case::test_case;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
struct NeverEndingTask {
    kind: TaskKind,
    listen_to_cancellation_token: bool,
}

impl SessionTask for NeverEndingTask {
    fn kind(&self) -> TaskKind {
        self.kind
    }

    fn span_name(&self) -> &'static str {
        "session_task.turn_input_test"
    }

    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        if self.listen_to_cancellation_token {
            cancellation_token.cancelled().await;
            return Ok(None);
        }
        loop {
            sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        status: None,
        encrypted_content: None,
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

async fn submit_start_only(
    session: &Arc<Session>,
    input: SubmittedTurnInput,
) -> TurnInputSubmission {
    handle(
        session,
        TurnInputRequest::new(input),
        TurnInputMode::StartIfIdle,
        "test-submission".to_string(),
    )
    .await
    .expect("start-only submission should be valid")
}

async fn submit_steer_only(
    session: &Arc<Session>,
    input: Vec<UserInput>,
    expected_turn_id: &str,
) -> TurnInputSubmission {
    handle(
        session,
        TurnInputRequest::new(SubmittedTurnInput::UserInput {
            content: input,
            client_id: None,
        }),
        TurnInputMode::Steer {
            expected_turn_id: expected_turn_id.to_string(),
        },
        "test-submission".to_string(),
    )
    .await
    .expect("steer-only submission should be valid")
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "simulate an in-flight realtime append while checking input admission"
)]
async fn steering_does_not_wait_for_realtime_history() {
    let (mut session, turn_context) = make_session_and_context().await;
    session.realtime_history = Some(tokio::sync::Mutex::new(Default::default()));
    let session = Arc::new(session);
    let turn_context = Arc::new(turn_context);
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let history = session
        .realtime_history
        .as_ref()
        .expect("realtime history")
        .lock()
        .await;
    for mode in [
        TurnInputMode::StartOrSteer,
        TurnInputMode::Steer {
            expected_turn_id: turn_context.sub_id.clone(),
        },
    ] {
        let submission = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle(
                &session,
                TurnInputRequest::user_input(vec![UserInput::Text {
                    text: "steer without waiting for persistence".to_string(),
                    text_elements: Vec::new(),
                }]),
                mode,
                "steer-submission".to_string(),
            ),
        )
        .await
        .expect("steering must not wait for the realtime recorder")
        .expect("steering should succeed");
        assert_eq!(
            submission,
            TurnInputSubmission::Steered {
                root_turn_id: turn_context.sub_id.clone(),
                turn_id: turn_context.sub_id.clone()
            }
        );
    }
    drop(history);
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn accepted_input_applies_thread_settings() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    let config = session.get_config().await;
    handle(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".to_string(),
            text_elements: Vec::new(),
        }])
        .with_thread_settings(ThreadSettingsOverrides {
            environments: Some(local_selections(config.cwd.clone()).into_requests()),
            approval_policy: Some(config.permissions.approval_policy.value()),
            approvals_reviewer: Some(codex_config::types::ApprovalsReviewer::AutoReview),
            sandbox_policy: Some(config.legacy_sandbox_policy()),
            summary: config.model_reasoning_summary,
            personality: config.personality,
            collaboration_mode: Some(CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: turn_context.model_info().slug.clone(),
                    reasoning_effort: config.model_reasoning_effort.clone(),
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        }),
        TurnInputMode::StartOrSteer,
        "sub-1".to_string(),
    )
    .await
    .expect("submit user turn");

    let state = session.state.lock().await;
    assert_eq!(
        state.session_configuration.step_settings.approvals_reviewer,
        codex_config::types::ApprovalsReviewer::AutoReview
    );
    assert!(
        session.mcp_refresh.is_pending(),
        "server elicitation authority changes must refresh MCP state"
    );
}

#[tokio::test]
async fn start_only_rejects_active_turn_without_injecting() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let input = SubmittedTurnInput::ResponseItem(user_message("synthetic idle input"));
    let submission = submit_start_only(&session, input).await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        },
        submission
    );
    assert_eq!(
        Vec::<TurnInput>::new(),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
            .0
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn recovery_rejects_active_turn_without_injecting_or_applying_settings() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    let original_approval_policy = session
        .get_config()
        .await
        .permissions
        .approval_policy
        .value();
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let submission = handle_recovery(
        &session,
        ThreadSettingsOverrides {
            approval_policy: Some(AskForApproval::Never),
            ..Default::default()
        },
        TurnStartOptions::default(),
        "recovered-turn".to_string(),
    )
    .await
    .expect("recovery should return a typed rejection");

    assert_eq!(
        submission,
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        }
    );
    assert_eq!(
        session
            .get_config()
            .await
            .permissions
            .approval_policy
            .value(),
        original_approval_policy
    );
    assert_eq!(
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
            .0,
        Vec::<TurnInput>::new()
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_rejects_current_plan_before_validating_settings() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let default_mode = session.collaboration_mode().await;
    {
        let mut state = session.state.lock().await;
        let settings = Arc::make_mut(&mut state.session_configuration.step_settings);
        settings.collaboration_mode.mode = ModeKind::Plan;
        settings.approval_policy = Constrained::allow_only(AskForApproval::OnRequest);
    }
    let desired_settings = session.thread_settings_snapshot().await;
    let invalid_override = ThreadSettingsOverrides {
        collaboration_mode: Some(default_mode.clone()),
        approval_policy: Some(AskForApproval::Never),
        ..Default::default()
    };

    // Current Plan takes precedence even when the request would leave Plan or
    // fail settings validation. Nothing has been reserved or applied yet.
    let submission = handle(
        &session,
        TurnInputRequest::new(SubmittedTurnInput::ResponseItem(user_message(
            "synthetic idle input",
        )))
        .with_thread_settings(invalid_override.clone()),
        TurnInputMode::StartIfIdle,
        "automatic-plan-submission".to_string(),
    )
    .await
    .expect("current Plan must reject before settings validation");
    assert_eq!(
        submission,
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PlanMode,
        }
    );
    assert_eq!(session.thread_settings_snapshot().await, desired_settings);
    assert!(session.active_turn.lock().await.is_none());

    session
        .update_settings(SessionSettingsUpdate {
            step_settings: StepSettingsUpdate {
                collaboration_mode: Some(default_mode),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("explicit settings may leave Plan mode");
    let desired_settings = session.thread_settings_snapshot().await;
    let result = handle(
        &session,
        TurnInputRequest::new(SubmittedTurnInput::ResponseItem(user_message(
            "invalid automatic input",
        )))
        .with_thread_settings(invalid_override),
        TurnInputMode::StartIfIdle,
        "invalid-automatic-submission".to_string(),
    )
    .await;
    let error = result.expect_err("invalid automatic settings must be rejected");
    assert!(matches!(
        error.details(),
        CodexErrorDetails::InvalidRequest(_)
    ));
    assert_eq!(session.thread_settings_snapshot().await, desired_settings);
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        Vec::<TurnInput>::new(),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
            .0
    );
}

#[tokio::test]
async fn prepared_user_updates_merge_with_settings_at_turn_start() {
    for (requested, intervening) in [
        (
            ThreadSettingsOverrides {
                effort: Some(Some(ReasoningEffort::High)),
                ..Default::default()
            },
            ThreadSettingsOverrides {
                model: Some("gpt-5.2".to_string()),
                ..Default::default()
            },
        ),
        (
            ThreadSettingsOverrides {
                model: Some("gpt-5.2".to_string()),
                ..Default::default()
            },
            ThreadSettingsOverrides {
                effort: Some(Some(ReasoningEffort::High)),
                ..Default::default()
            },
        ),
    ] {
        let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
        let initial = CollaborationMode {
            mode: ModeKind::Default,
            settings: Settings {
                model: "gpt-5.4".to_string(),
                reasoning_effort: Some(ReasoningEffort::Low),
                developer_instructions: None,
            },
        };
        session
            .update_settings(thread_settings::prepare_update(
                ThreadSettingsOverrides {
                    collaboration_mode: Some(initial.clone()),
                    ..Default::default()
                },
                &session.services.selected_capability_roots,
            ))
            .await
            .expect("set initial model and effort");
        let prepared =
            PreparedTurnInputSettings::prepare(&session, requested, TurnStartOptions::default())
                .await
                .expect("prepare partial protocol update");
        session
            .update_settings(thread_settings::prepare_update(
                intervening,
                &session.services.selected_capability_roots,
            ))
            .await
            .expect("commit intervening settings");

        let turn_context = prepared
            .apply_started(
                &session,
                "sparse-user-start".to_string(),
                TurnStartKind::User,
            )
            .await
            .expect("apply prepared settings")
            .expect("user start is permitted");
        let expected = initial.with_updates(
            Some("gpt-5.2".to_string()),
            Some(Some(ReasoningEffort::High)),
            /*developer_instructions*/ None,
        );
        assert_eq!(session.collaboration_mode().await, expected);
        assert_eq!(turn_context.collaboration_mode(), expected);
        assert_eq!(
            turn_context.initial_settings.selected_collaboration_mode(),
            &expected
        );
        assert!(Arc::ptr_eq(
            &turn_context.initial_settings.model_info,
            turn_context.model_info(),
        ));
    }
}

#[tokio::test]
async fn automatic_admission_uses_current_candidate_after_plan_preview() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let default_mode = session.collaboration_mode().await;
    let mut plan_mode = default_mode.clone();
    plan_mode.mode = ModeKind::Plan;
    session
        .update_settings(thread_settings::prepare_update(
            ThreadSettingsOverrides {
                collaboration_mode: Some(plan_mode),
                ..Default::default()
            },
            &session.services.selected_capability_roots,
        ))
        .await
        .expect("enter Plan after the initial admission check");
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides {
            effort: Some(Some(ReasoningEffort::High)),
            ..Default::default()
        },
        TurnStartOptions::default(),
    )
    .await
    .expect("validate the patch while the preview is Plan");
    session
        .update_settings(thread_settings::prepare_update(
            ThreadSettingsOverrides {
                collaboration_mode: Some(default_mode.clone()),
                ..Default::default()
            },
            &session.services.selected_capability_roots,
        ))
        .await
        .expect("leave Plan before atomic admission");

    let turn_context = prepared
        .apply_started(
            &session,
            "automatic-after-plan-preview".to_string(),
            TurnStartKind::Automatic,
        )
        .await
        .expect("automatic admission should succeed")
        .expect("current and proposed modes are both Default");
    let expected = default_mode.with_updates(
        /*model*/ None,
        Some(Some(ReasoningEffort::High)),
        /*developer_instructions*/ None,
    );
    assert_eq!(session.collaboration_mode().await, expected);
    assert_eq!(turn_context.collaboration_mode(), expected);
    assert_eq!(
        turn_context.initial_settings.selected_collaboration_mode(),
        &expected
    );
}

#[tokio::test]
async fn automatic_admission_rechecks_plan_mode_without_committing_sparse_settings() {
    struct ConfigRecorder(Arc<std::sync::Mutex<Vec<(AskForApproval, ApprovalsReviewer)>>>);

    impl codex_extension_api::ConfigContributor<crate::config::Config> for ConfigRecorder {
        fn on_config_changed(
            &self,
            _session_store: &codex_extension_api::ExtensionData,
            _thread_store: &codex_extension_api::ExtensionData,
            _previous_config: &crate::config::Config,
            new_config: &crate::config::Config,
        ) {
            self.0.lock().expect("config records lock").push((
                new_config.permissions.approval_policy.value(),
                new_config.approvals_reviewer,
            ));
        }
    }

    let (mut session, _turn_context, rx) = make_session_and_context_with_rx().await;
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut extensions =
        codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    extensions.config_contributor(Arc::new(ConfigRecorder(Arc::clone(&records))));
    Arc::get_mut(&mut session)
        .expect("unique test session")
        .services
        .extensions = Arc::new(extensions.build());
    {
        let mut state = session.state.lock().await;
        let settings = Arc::make_mut(&mut state.session_configuration.step_settings);
        settings.approval_policy = Constrained::allow_any(AskForApproval::OnRequest);
        settings.approvals_reviewer = ApprovalsReviewer::User;
    }
    let original_environments = session.services.turn_environments.selections();
    let workspace = tempfile::tempdir().expect("create proposed workspace");
    let proposed_environments = local_selections(
        AbsolutePathBuf::try_from(workspace.path()).expect("absolute workspace path"),
    );
    assert_ne!(original_environments, proposed_environments.environments);
    let default_mode = session.collaboration_mode().await;
    let overrides = ThreadSettingsOverrides {
        model: Some("automatic-model-must-not-be-applied".to_string()),
        service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
        environments: Some(proposed_environments.clone().into_requests()),
        approval_policy: Some(AskForApproval::Never),
        approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
        ..Default::default()
    };
    let default_override = ThreadSettingsOverrides {
        collaboration_mode: Some(default_mode.clone()),
        ..overrides.clone()
    };
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        overrides.clone(),
        TurnStartOptions::default(),
    )
    .await
    .expect("sparse settings should preview successfully");
    let prepared_default =
        PreparedTurnInputSettings::prepare(&session, default_override, TurnStartOptions::default())
            .await
            .expect("Default replacement should preview successfully");

    // Another settings writer changes the effective mode after preview. The
    // sparse patch must not commit in Plan, and a full Default replacement
    // must not let automatic work escape the now-current Plan configuration.
    let mut collaboration_mode = default_mode;
    collaboration_mode.mode = ModeKind::Plan;
    session
        .update_settings(SessionSettingsUpdate {
            step_settings: StepSettingsUpdate {
                collaboration_mode: Some(collaboration_mode),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("Plan mode should be allowed for explicit settings updates");
    let desired_settings = session.thread_settings_snapshot().await;
    records.lock().expect("config records lock").clear();
    // Keep the existing refresh worker from consuming invalidation while the
    // rejection and its positive control inspect MCP's dirty state.
    let _mcp_refresh = session
        .mcp_refresh
        .acquire()
        .await
        .expect("acquire MCP refresh gate");
    session.mcp_refresh.claim();
    assert!(!session.mcp_refresh.is_pending());

    for (submission_id, prepared) in [
        ("automatic-after-preview", prepared),
        ("automatic-default-after-preview", prepared_default),
    ] {
        let outcome = prepared
            .apply_started(
                &session,
                submission_id.to_string(),
                TurnStartKind::Automatic,
            )
            .await
            .expect("automatic admission should return a typed rejection");
        assert!(outcome.is_none());
        assert_eq!(session.thread_settings_snapshot().await, desired_settings);
        assert!(session.active_turn.lock().await.is_none());
        assert_eq!(
            session.services.turn_environments.selections(),
            original_environments
        );
        assert!(!session.mcp_refresh.is_pending());
        assert_eq!(*records.lock().expect("config records lock"), Vec::new());
        while let Ok(event) = rx.try_recv() {
            assert_ne!(event.id, submission_id);
        }
    }

    assert_eq!(
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
            .0,
        Vec::<TurnInput>::new()
    );

    // The rejected candidate is valid and would have real runtime effects if
    // accepted by an ordinary settings update.
    session
        .update_settings(thread_settings::prepare_update(
            overrides,
            &session.services.selected_capability_roots,
        ))
        .await
        .expect("explicit settings update accepts the same patch");
    assert_eq!(
        session.configured_environment_selections().await,
        proposed_environments.environments
    );
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(
        *records.lock().expect("config records lock"),
        vec![(AskForApproval::Never, ApprovalsReviewer::AutoReview)]
    );
}

#[test_case(TurnStartKind::User; "ordinary constructor")]
#[test_case(TurnStartKind::Automatic; "conditional constructor")]
#[tokio::test]
async fn admission_revalidates_constraints_before_committing(kind: TurnStartKind) {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    {
        let mut state = session.state.lock().await;
        Arc::make_mut(&mut state.session_configuration.step_settings).approval_policy =
            Constrained::allow_any(AskForApproval::OnRequest);
    }
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides {
            approval_policy: Some(AskForApproval::Never),
            service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
            ..Default::default()
        },
        TurnStartOptions::default(),
    )
    .await
    .expect("approval-policy edit should initially be valid");

    let approval_policy = Constrained::allow_only(AskForApproval::OnRequest);
    let expected_message = CodexErr::InvalidRequest(
        approval_policy
            .can_set(&AskForApproval::Never)
            .expect_err("new constraint must reject the prepared edit")
            .to_string(),
    )
    .to_string();
    {
        let mut state = session.state.lock().await;
        Arc::make_mut(&mut state.session_configuration.step_settings).approval_policy =
            approval_policy;
    }
    let desired_settings = session.thread_settings_snapshot().await;
    let submission_id = "constraints-after-preview";
    let result = prepared
        .apply_started(&session, submission_id.to_string(), kind)
        .await;
    let Err(error) = result else {
        panic!("commit-time constraint failure must return InvalidRequest");
    };
    let CodexErrorDetails::InvalidRequest(message) = error.details() else {
        panic!("unexpected commit-time error: {error}");
    };
    assert_eq!(message, &expected_message);
    let errors: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .filter(|event| event.id == submission_id)
        .map(|event| match event.msg {
            EventMsg::Error(error) => error,
            other => panic!("unexpected rejected-turn event: {other:?}"),
        })
        .collect();
    assert_eq!(
        errors,
        vec![ErrorEvent {
            misalignment: None,
            message: expected_message,
            codex_error_info: Some(CodexErrorInfo::BadRequest),
        }]
    );
    assert_eq!(session.thread_settings_snapshot().await, desired_settings);
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
            .0,
        Vec::<TurnInput>::new()
    );
}

#[tokio::test]
async fn start_only_accepts_user_input_in_plan_mode() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        Arc::make_mut(&mut state.session_configuration.step_settings).collaboration_mode =
            collaboration_mode;
        state.merge_connector_selection(["calendar".to_string()]);
    }

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "queued user input".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: Some("queued-user-message".to_string()),
        },
    )
    .await;
    assert!(matches!(submission, TurnInputSubmission::Started { .. }));
    assert!(
        session
            .state
            .lock()
            .await
            .get_connector_selection()
            .is_empty()
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_rejects_empty_user_input_in_plan_mode() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        Arc::make_mut(&mut state.session_configuration.step_settings).collaboration_mode =
            collaboration_mode;
    }

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::UserInput {
            content: Vec::new(),
            client_id: Some("empty-queued-user-message".to_string()),
        },
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PlanMode,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
}

#[tokio::test]
async fn start_only_rejects_pending_trigger_turn_without_injecting() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    {
        let mut state = session.state.lock().await;
        Arc::make_mut(&mut state.session_configuration.step_settings)
            .collaboration_mode
            .mode = ModeKind::Plan;
    }
    session
        .input_queue
        .enqueue_mailbox_communication(
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "pending trigger".to_string(),
                /*trigger_turn*/ true,
            )
            .into(),
            Default::default(),
        )
        .await;

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PendingTriggerTurn,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    assert_eq!(session.collaboration_mode().await.mode, ModeKind::Plan);
}

/// Queue-only agent mail respects stop, Plan mode, and inherited child configuration.
#[test_case(SessionSource::Exec, true, ModeKind::Default; "interrupted_root")]
#[test_case(SessionSource::Exec, false, ModeKind::Plan; "plan_root")]
#[test_case(SessionSource::SubAgent(SubAgentSource::Review), false, ModeKind::Default; "inherited_child_flag")]
#[tokio::test]
async fn agent_message_wake_reservation_preserves_existing_boundaries(
    source: SessionSource,
    interrupted: bool,
    mode: ModeKind,
) {
    let (mut session, _turn_context) = make_session_and_context().await;
    session
        .features
        .enable(Feature::WakeOnAgentMessages)
        .expect("enable agent-message wakes");
    {
        let mut state = session.state.lock().await;
        state.session_configuration.session_source = source;
        Arc::make_mut(&mut state.session_configuration.step_settings)
            .collaboration_mode
            .mode = mode;
    }
    if interrupted {
        session.mark_interrupted();
    }
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/worker").expect("valid worker path"),
        AgentPath::root(),
        Vec::new(),
        "late worker update".to_string(),
        /*trigger_turn*/ false,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(communication.clone().into(), Default::default())
        .await;

    assert!(session.reserve_pending_work_turn().await.is_none());
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(communication.into())]
    );
}

#[tokio::test]
async fn steer_only_requires_active_turn() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let submission = submit_steer_only(
        &session,
        vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }],
        "missing-turn-id",
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NoActiveTurn,
        },
        submission
    );
}

#[tokio::test]
async fn steer_only_enforces_expected_turn_id() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    turn_context
        .turn_metadata_state
        .set_turn_trigger("composer".to_string());
    session
        .spawn_task(
            Arc::clone(&turn_context),
            vec![TurnInput::UserInput {
                metadata: Default::default(),
                content: vec![UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: Vec::new(),
                }],
                client_id: None,
            }],
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;

    let submission = submit_steer_only(
        &session,
        vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }],
        "different-turn-id",
    )
    .await;
    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::ExpectedTurnMismatch {
                expected: "different-turn-id".to_string(),
                actual: turn_context.sub_id.clone(),
            },
        },
        submission
    );

    let output: ResponseItem = serde_json::from_value(serde_json::json!({
        "type": "function_call_output",
        "name": "send_message_to_thread",
        "output": "delegated work",
    }))
    .expect("valid standalone output");

    let submission = handle(
        &session,
        TurnInputRequest::new(SubmittedTurnInput::ResponseItem(output)).on_start(
            TurnStartOptions {
                turn_trigger: Some("automation_cron_scheduled".to_string()),
                ..Default::default()
            },
        ),
        TurnInputMode::StartOrSteer,
        "test-submission".to_string(),
    )
    .await
    .expect("standalone output should steer the active turn");

    assert_eq!(
        submission,
        TurnInputSubmission::Steered {
            root_turn_id: turn_context.sub_id.clone(),
            turn_id: turn_context.sub_id.clone()
        }
    );
    assert_eq!(
        turn_context
            .turn_metadata_state
            .current_turn_trigger()
            .as_deref(),
        Some("composer")
    );
    let turn_state = session
        .input_queue
        .turn_state_for_sub_id(&session.active_turn, &turn_context.sub_id)
        .await
        .expect("active turn state");
    assert_eq!(
        session
            .input_queue
            .subscribe_activity(Some(turn_state.as_ref()))
            .await
            .1,
        Some(crate::session::input_queue::InputQueueActivity::Steer)
    );
}

#[tokio::test]
async fn rejects_non_regular_turns() {
    for (task_kind, turn_kind) in [
        (TaskKind::Review, NonSteerableTurnKind::Review),
        (TaskKind::Compact, NonSteerableTurnKind::Compact),
    ] {
        let (session, incoming_turn_context, _rx) = make_session_and_context_with_rx().await;
        incoming_turn_context
            .turn_metadata_state
            .set_root_turn_id("incoming-root".to_string());
        let turn_context = session
            .new_turn_with_default_settings("turn".to_string(), Default::default())
            .await
            .expect("build turn context");
        turn_context
            .turn_metadata_state
            .set_root_turn_id("active-root".to_string());
        session
            .spawn_task(
                Arc::clone(&turn_context),
                vec![TurnInput::UserInput {
                    metadata: Default::default(),
                    content: vec![UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
                NeverEndingTask {
                    kind: task_kind,
                    listen_to_cancellation_token: true,
                },
            )
            .await;

        let steer_input = vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }];
        let steer_submission = submit_steer_only(&session, steer_input.clone(), "turn").await;
        assert_eq!(
            TurnInputSubmission::NotSubmitted {
                reason: NotSubmittedReason::ActiveTurnNotSteerable { turn_kind },
            },
            steer_submission
        );
        let start_or_steer_submission = handle(
            &session,
            TurnInputRequest::user_input(steer_input),
            TurnInputMode::StartOrSteer,
            "test-submission".to_string(),
        )
        .await
        .expect("start-or-steer submission should be valid");
        assert_eq!(
            TurnInputSubmission::NotSubmitted {
                reason: NotSubmittedReason::ActiveTurnNotSteerable { turn_kind },
            },
            start_or_steer_submission
        );
        assert_eq!(
            turn_context.turn_metadata_state.root_turn_id().as_deref(),
            Some("active-root")
        );

        session.abort_all_tasks(TurnAbortReason::Interrupted).await;
    }
}

#[test_case("automation_heartbeat_scheduled", None, UserInputOrigin::User; "human")]
#[test_case("composer", Some("automation_heartbeat_scheduled"), UserInputOrigin::Heartbeat; "scheduled")]
#[tokio::test]
async fn steer_preserves_request_origin(
    active_trigger: &str,
    request_trigger: Option<&str>,
    origin: UserInputOrigin,
) {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    turn_context
        .turn_metadata_state
        .set_turn_trigger(active_trigger.to_owned());
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    let content = vec![UserInput::Text {
        text: "Create the worktree now.".to_owned(),
        text_elements: Vec::new(),
    }];
    handle(
        &session,
        TurnInputRequest::user_input(content.clone()).on_start(TurnStartOptions {
            turn_trigger: request_trigger.map(str::to_owned),
            ..Default::default()
        }),
        TurnInputMode::Steer {
            expected_turn_id: turn_context.sub_id.clone(),
        },
        "steer-submission".to_owned(),
    )
    .await
    .unwrap();
    let pending = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await
        .0;
    assert_eq!(
        pending,
        vec![TurnInput::UserInput {
            content,
            client_id: None,
            metadata: crate::session::UserInputMetadata {
                acceptance_order: Some(0),
                origin,
                ..Default::default()
            },
        }]
    );
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn bounded_admission_attaches_one_shared_account_without_sticky_thread_limits() {
    let (session, _) = make_session_and_context().await;
    let session = Arc::new(session);
    let spec = codex_protocol::turn_input::TurnInferenceAllowance {
        max_provider_attempts: 2,
        max_request_bytes: 1024,
        deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
    };
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            inference_allowance: Some(spec),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ctx = prepared
        .apply_started(&session, "bounded-test".into(), TurnStartKind::User)
        .await
        .unwrap()
        .unwrap();
    let first = crate::turn_inference::allowance(&ctx).unwrap();
    let second = crate::turn_inference::allowance(&ctx).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    first.claim_attempt().unwrap();
    assert_eq!(second.attempted().unwrap(), 1);
    let ordinary = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions::default(),
    )
    .await
    .unwrap()
    .apply_started(
        &session,
        "ordinary-after-bounded".into(),
        TurnStartKind::User,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(crate::turn_inference::allowance(&ordinary).is_none());
}

#[tokio::test]
async fn bounded_recovery_refuses_instead_of_replenishing_spent_attempts() {
    let (session, _) = make_session_and_context().await;
    let session = Arc::new(session);
    let spec = codex_protocol::turn_input::TurnInferenceAllowance {
        max_provider_attempts: 2,
        max_request_bytes: 128,
        deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
    };
    assert!(
        handle_recovery(
            &session,
            ThreadSettingsOverrides::default(),
            TurnStartOptions {
                inference_allowance: Some(spec),
                ..Default::default()
            },
            "retained-bounded-turn".into()
        )
        .await
        .is_err()
    );
    assert!(session.active_turn.lock().await.is_none());
}

#[tokio::test]
async fn bounded_predecessor_refuses_an_unreserved_direct_continuation() {
    let (session, _) = make_session_and_context().await;
    let session = Arc::new(session);
    session.state.lock().await.turn_attribution = Some(codex_history::TurnAttribution {
        inference_allowance: Some(Box::new(
            codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 128,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            },
        )),
        turn_id: "bounded-predecessor".into(),
        turn_trigger: Some("heartbeat".into()),
        parent_turn_id: None,
        initiating_agent_path: None,
        root_turn_id: None,
    });
    session.state.lock().await.last_started_turn_id = Some("bounded-predecessor".into());
    let input = TurnInputRequest::new(SubmittedTurnInput::ResponseItem(
        ResponseItem::FunctionCallOutput {
            id: None,
            call_id: None,
            name: Some("automatic_continuation".into()),
            namespace: None,
            output: codex_protocol::models::FunctionCallOutputPayload {
                body: codex_protocol::models::FunctionCallOutputBody::Text("continue".into()),
                success: None,
            },
            internal_chat_message_metadata_passthrough: None,
        },
    ));
    assert!(
        handle(
            &session,
            input,
            TurnInputMode::ContinueIfIdle {
                expected_previous_turn_id: "bounded-predecessor".into(),
            },
            "unreserved-successor".into()
        )
        .await
        .is_err()
    );
    assert!(session.active_turn.lock().await.is_none());
}

#[tokio::test]
async fn scored_prompt_keeps_its_submission_item_id() {
    let (session, turn, events) = make_session_and_context_with_rx().await;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    // A legacy steer can present the same UI client ID as an admitted steer.
    for id in [Some(first), None, Some(second)] {
        let mut request = TurnInputRequest::new(SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "identical input".into(),
                text_elements: vec![],
            }],
            client_id: Some("same-client-id".into()),
        });
        request.user_item_uuid = id;
        handle(
            &session,
            request,
            TurnInputMode::Steer {
                expected_turn_id: turn.sub_id.clone(),
            },
            Uuid::now_v7().to_string(),
        )
        .await
        .unwrap();
    }
    let (pending, _) = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await;
    for input in pending {
        let TurnInput::UserInput {
            content,
            client_id,
            metadata,
        } = input
        else {
            panic!("expected admitted user input");
        };
        session
            .record_user_prompt_and_emit_turn_item(
                &turn,
                turn.model_info(),
                &content,
                client_id,
                metadata,
                codex_thread_store::PersistContext::Standard,
            )
            .await;
    }
    let history = session.clone_history().await;
    let ids: Vec<_> = history
        .raw_items()
        .filter_map(|item| {
            if let ResponseItem::Message { role, id, .. } = item
                && role == "user"
            {
                id.as_ref().map(ToString::to_string)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], format!("msg_input_{first}"));
    assert!(!ids[1].starts_with("msg_input_"));
    assert_eq!(ids[2], format!("msg_input_{second}"));
    let ui_ids = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event.msg {
            codex_protocol::protocol::EventMsg::ItemCompleted(event) => {
                let item = codex_app_server_protocol::ThreadItemPayload::from(event.item);
                if let codex_app_server_protocol::ThreadItemPayload::UserMessage {
                    id,
                    client_id,
                    ..
                } = item
                {
                    assert_eq!(client_id.as_deref(), Some("same-client-id"));
                    return Some(id);
                }
                None
            }
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ui_ids.len(), 3);
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

/// Queue-only message wakes may reuse ordinary context but never replenish a bounded turn.
#[test_case(false; "ordinary_wake")]
#[test_case(true; "bounded_predecessor")]
#[tokio::test]
async fn agent_message_wake_preserves_bounded_predecessor(bounded: bool) {
    let (mut session, _) = make_session_and_context().await;
    session
        .features
        .enable(Feature::WakeOnAgentMessages)
        .expect("enable agent-message wakes");
    session.state.lock().await.turn_attribution = Some(codex_history::TurnAttribution {
        inference_allowance: bounded.then(|| {
            Box::new(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 128,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            })
        }),
        turn_id: "predecessor".into(),
        turn_trigger: Some("heartbeat".into()),
        parent_turn_id: None,
        initiating_agent_path: None,
        root_turn_id: None,
    });
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/worker").expect("valid worker path"),
        AgentPath::root(),
        Vec::new(),
        "late worker update".to_string(),
        /*trigger_turn*/ false,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(communication.clone().into(), Default::default())
        .await;

    let reservation = session.reserve_pending_work_turn().await;
    assert_eq!(reservation.is_none(), bounded);
    assert_eq!(session.active_turn.lock().await.is_none(), bounded);
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(communication.into())]
    );
}

#[derive(Debug, Default)]
struct FundingGateFixture(std::sync::atomic::AtomicUsize);
impl codex_api::ProviderAttemptGate for FundingGateFixture {
    fn reserve(&self, _: codex_api::ProviderAttempt) -> codex_api::ProviderAttemptReservation<'_> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { false })
    }
}

async fn set_funding_gate(session: &Session, gate: Arc<FundingGateFixture>) {
    let gate: Arc<dyn codex_api::ProviderAttemptGate> = gate;
    session
        .state
        .lock()
        .await
        .session_configuration
        .turn_extension_init
        .insert(gate);
}

#[tokio::test]
async fn funding_gate_uses_actual_start_attachment_and_refuses_unbounded_start() {
    let (session, _ctx, _rx) = make_session_and_context_with_rx().await;
    let initial = session
        .state
        .lock()
        .await
        .session_configuration
        .step_settings
        .collaboration_mode
        .clone();
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides {
            model: Some("gpt-5.2".into()),
            ..Default::default()
        },
        TurnStartOptions::default(),
    )
    .await
    .expect("prepare unfunded start");
    let old_gate = Arc::new(FundingGateFixture::default());
    set_funding_gate(&session, Arc::clone(&old_gate)).await;
    assert!(
        prepared
            .apply_started(&session, "unfunded-start".into(), TurnStartKind::User)
            .await
            .expect("admission result")
            .is_none()
    );
    assert_eq!(
        session
            .state
            .lock()
            .await
            .session_configuration
            .step_settings
            .collaboration_mode,
        initial
    );
    let spec = codex_protocol::turn_input::TurnInferenceAllowance {
        max_provider_attempts: 2,
        max_request_bytes: 4096,
        deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
    };
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            inference_allowance: Some(spec),
            ..Default::default()
        },
    )
    .await
    .expect("prepare funded start");
    let current_gate = Arc::new(FundingGateFixture::default());
    set_funding_gate(&session, Arc::clone(&current_gate)).await;
    let ctx = prepared
        .apply_started(&session, "funded-start".into(), TurnStartKind::User)
        .await
        .expect("start result")
        .expect("bounded start");
    let allowance = crate::turn_inference::allowance(&ctx).expect("native allowance");
    assert!(allowance.claim_authorized_attempt().await.is_err());
    assert_eq!(old_gate.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(current_gate.0.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn funding_gate_does_not_block_human_steering_of_an_active_turn() {
    let (session, ctx, _rx) = make_session_and_context_with_rx().await;
    set_funding_gate(&session, Arc::new(FundingGateFixture::default())).await;
    session
        .spawn_task(
            Arc::clone(&ctx),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;
    let result = submit_steer_only(
        &session,
        vec![UserInput::Text {
            text: "human steering remains available".into(),
            text_elements: Vec::new(),
        }],
        &ctx.sub_id,
    )
    .await;
    assert!(matches!(result, TurnInputSubmission::Steered { .. }));
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn per_turn_reservation_is_not_inherited_and_requires_finite_limits() {
    let (session, _ctx, _rx) = make_session_and_context_with_rx().await;
    let gate = Arc::new(FundingGateFixture::default());
    let selected = codex_protocol::provider_attempt::ProviderAttemptGateSelection(gate.clone());
    assert!(
        PreparedTurnInputSettings::prepare(
            &session,
            ThreadSettingsOverrides::default(),
            TurnStartOptions {
                provider_attempt_gate: Some(selected.clone()),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            provider_attempt_gate: Some(selected),
            inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 4096,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ctx = prepared
        .apply_started(&session, "one-funded-turn".into(), TurnStartKind::User)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::turn_inference::allowance(&ctx)
            .unwrap()
            .claim_authorized_attempt()
            .await
            .is_err()
    );
    assert_eq!(gate.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    let next = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions::default(),
    )
    .await
    .unwrap()
    .apply_started(
        &session,
        "independent-human-turn".into(),
        TurnStartKind::User,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(crate::turn_inference::allowance(&next).is_none());
    assert!(
        next.extension_data
            .get::<Arc<dyn codex_api::ProviderAttemptGate>>()
            .is_none()
    );
}

#[tokio::test]
async fn per_turn_caller_cannot_replace_an_existing_host_reservation_policy() {
    let (session, _ctx, _rx) = make_session_and_context_with_rx().await;
    let prepared = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            provider_attempt_gate: Some(
                codex_protocol::provider_attempt::ProviderAttemptGateSelection(Arc::new(
                    FundingGateFixture::default(),
                )),
            ),
            inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 4096,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    set_funding_gate(&session, Arc::new(FundingGateFixture::default())).await;
    assert!(
        prepared
            .apply_started(&session, "conflicting-owner".into(), TurnStartKind::User)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn host_tool_ceiling_intersection_never_widens_either_policy() {
    use codex_extension_api::ToolPolicy;
    use codex_protocol::ToolName;
    let plain = ToolName {
        namespace: None,
        name: "exec".into(),
    };
    let alias = ToolName {
        namespace: Some("functions".into()),
        name: "exec".into(),
    };
    let external = ToolName {
        namespace: Some("remote".into()),
        name: "exec".into(),
    };
    let base = ToolPolicy {
        allowed_tools: Some(vec![plain.clone(), external.clone()]),
        ..Default::default()
    };
    let ceiling = ToolPolicy {
        allowed_tools: Some(vec![alias.clone()]),
        require_managed_sandbox: true,
        require_unified_exec: true,
        expose_additional_permissions: false,
    };
    for policy in [base.intersect(&ceiling), ceiling.intersect(&base)] {
        assert!(policy.allows(&plain));
        assert!(policy.allows(&alias));
        assert!(!policy.allows(&external));
        assert!(policy.require_managed_sandbox);
        assert!(policy.require_unified_exec);
        assert!(!policy.expose_additional_permissions);
        assert!(
            !policy
                .intersect(&ToolPolicy {
                    allowed_tools: Some(vec![]),
                    ..Default::default()
                })
                .allows(&plain)
        );
    }
}

#[tokio::test]
async fn host_tool_ceiling_is_transient_and_requires_finite_allowance() {
    let (session, _, _rx) = make_session_and_context_with_rx().await;
    let tool = codex_protocol::ToolName {
        namespace: None,
        name: "exec".into(),
    };
    assert!(
        PreparedTurnInputSettings::prepare(
            &session,
            ThreadSettingsOverrides::default(),
            TurnStartOptions {
                tool_ceiling: Some(vec![tool.clone()]),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    let bounded = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            tool_ceiling: Some(vec![tool.clone()]),
            inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 2,
                max_request_bytes: 4096,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("prepare bounded tools")
    .apply_started(&session, "restricted-tools".into(), TurnStartKind::User)
    .await
    .expect("admit")
    .expect("context");
    assert!(bounded.has_tool_ceiling());
    let policy = bounded.effective_tool_policy(&session.tool_policy);
    assert!(policy.allows(&tool));
    assert!(!policy.allows(&codex_protocol::ToolName {
        namespace: None,
        name: "other".into()
    }));
    assert!(!policy.expose_additional_permissions);
    let ordinary = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions::default(),
    )
    .await
    .expect("prepare human turn")
    .apply_started(&session, "ordinary-tools".into(), TurnStartKind::User)
    .await
    .expect("admit human")
    .expect("context");
    assert!(!ordinary.has_tool_ceiling());
    assert!(Arc::ptr_eq(
        &ordinary.effective_tool_policy(&session.tool_policy),
        &session.tool_policy
    ));
}

#[tokio::test]
async fn code_mode_limits_require_the_complete_bounded_host_selection() {
    use codex_protocol::config_types::ShellEnvironmentPolicy;
    use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::turn_input::TurnCodeModeLimits;
    let (session, _, _rx) = make_session_and_context_with_rx().await;
    let valid = TurnStartOptions {
        process_limits: cfg!(target_os = "linux").then_some(
            codex_protocol::turn_input::TurnProcessLimits {
                memory_bytes: 512 * 1024 * 1024,
                cpu_percent: 50,
                tasks: 128,
            },
        ),
        code_mode_limits: Some(TurnCodeModeLimits {
            max_heap_size_bytes: 32 * 1024 * 1024,
            max_yield_time_ms: 1000,
        }),
        command_environment_policy: Some(ShellEnvironmentPolicy {
            inherit: ShellEnvironmentPolicyInherit::None,
            ..Default::default()
        }),
        permission_ceiling: Some(PermissionProfile::read_only()),
        tool_ceiling: Some(vec![]),
        inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
            max_provider_attempts: 1,
            max_request_bytes: 4096,
            deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
        }),
        ..Default::default()
    };
    for mutation in 0..if cfg!(target_os = "linux") { 10 } else { 6 } {
        let mut invalid = valid.clone();
        match mutation {
            0 => {
                invalid
                    .code_mode_limits
                    .as_mut()
                    .unwrap()
                    .max_heap_size_bytes = 0
            }
            1 => invalid.code_mode_limits.as_mut().unwrap().max_yield_time_ms = 0,
            2 => invalid.command_environment_policy = None,
            3 => invalid.permission_ceiling = None,
            4 => invalid.tool_ceiling = None,
            5 => invalid.inference_allowance = None,
            6 => invalid.process_limits.as_mut().unwrap().memory_bytes = 0,
            7 => invalid.process_limits.as_mut().unwrap().cpu_percent = 101,
            8 => invalid.process_limits.as_mut().unwrap().tasks = 129,
            _ => invalid.code_mode_limits = None,
        }
        assert!(
            PreparedTurnInputSettings::prepare(
                &session,
                ThreadSettingsOverrides::default(),
                invalid
            )
            .await
            .is_err()
        );
    }
    assert!(
        PreparedTurnInputSettings::prepare(&session, ThreadSettingsOverrides::default(), valid)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn host_tool_ceiling_cannot_be_discarded_by_steering() {
    let (session, _, _rx) = make_session_and_context_with_rx().await;
    for mode in [
        TurnInputMode::StartOrSteer,
        TurnInputMode::Steer {
            expected_turn_id: "restricted-target".into(),
        },
    ] {
        let request = TurnInputRequest::user_input(vec![]).on_start(TurnStartOptions {
            tool_ceiling: Some(vec![]),
            ..Default::default()
        });
        assert!(
            handle(&session, request, mode, "restricted-steering".into())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn host_permission_ceiling_requires_finite_managed_explicit_tool_admission() {
    use codex_protocol::models::PermissionProfile;
    let (session, _, _rx) = make_session_and_context_with_rx().await;
    let allowance = codex_protocol::turn_input::TurnInferenceAllowance {
        max_provider_attempts: 1,
        max_request_bytes: 4096,
        deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
    };
    for options in [
        TurnStartOptions {
            permission_ceiling: Some(PermissionProfile::read_only()),
            ..Default::default()
        },
        TurnStartOptions {
            permission_ceiling: Some(PermissionProfile::read_only()),
            inference_allowance: Some(allowance.clone()),
            ..Default::default()
        },
        TurnStartOptions {
            permission_ceiling: Some(PermissionProfile::Disabled),
            inference_allowance: Some(allowance.clone()),
            tool_ceiling: Some(vec![]),
            ..Default::default()
        },
    ] {
        assert!(
            PreparedTurnInputSettings::prepare(
                &session,
                ThreadSettingsOverrides::default(),
                options
            )
            .await
            .is_err()
        );
    }
}

#[ctor::ctor]
fn dispatch_permission_test_filesystem_helpers() {
    use codex_test_binary_support::TestBinaryDispatchMode;
    use codex_test_binary_support::configure_test_binary_dispatch;
    configure_test_binary_dispatch("codex-core-unit-permissions", |name, argument| {
        if argument == Some(codex_exec_server::CODEX_FS_HELPER_ARG1)
            || name == codex_sandboxing::landlock::CODEX_LINUX_SANDBOX_ARG0
        {
            TestBinaryDispatchMode::DispatchArg0Only
        } else {
            // No aliases, environment changes or helper startup for ordinary tests.
            TestBinaryDispatchMode::Skip
        }
    });
}

#[cfg(unix)]
#[tokio::test]
async fn host_permission_ceiling_enforces_real_files_after_old_and_new_grants_without_changing_human_defaults()
-> anyhow::Result<()> {
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context_with_auth_and_config_and_rx;
    use codex_file_system::EnvironmentAccess;
    use codex_file_system::ReadFileOptions;
    use codex_file_system::WriteFileOptions;
    use codex_protocol::models::AdditionalPermissionProfile;
    use codex_protocol::models::FileSystemPermissions;
    use codex_protocol::models::NetworkPermissions;
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::permissions::FileSystemAccessMode;
    use codex_protocol::permissions::FileSystemSandboxEntry;
    use codex_protocol::permissions::FileSystemSandboxPolicy;
    use codex_protocol::permissions::NetworkSandboxPolicy;
    use codex_utils_path_uri::PathUri;
    let directory = tempfile::tempdir()?;
    let root = AbsolutePathBuf::from_absolute_path(directory.path())?;
    let writable = root.join("work");
    std::fs::create_dir_all(&writable)?;
    let secret = root.join("private.txt");
    std::fs::write(&secret, "private fixture")?;
    let outside = PathUri::from_abs_path(&root.join("outside.txt"));
    let inside = PathUri::from_abs_path(&writable.join("inside.txt"));
    let secret_uri = PathUri::from_abs_path(&secret);
    let selected_root = root.clone();
    let executable = std::env::current_exe()?;
    #[cfg(target_os = "linux")]
    let sandbox_alias = {
        let path = directory.path().join("codex-linux-sandbox");
        std::os::unix::fs::symlink(&executable, &path)?;
        Some(path)
    };
    #[cfg(not(target_os = "linux"))]
    let sandbox_alias = None;
    let (session, original, _rx) = make_session_and_context_with_auth_and_config_and_rx(
        codex_login::CodexAuth::from_api_key("Test API Key"),
        Vec::new(),
        move |config| {
            config.codex_self_exe = Some(executable);
            config.codex_linux_sandbox_exe = sandbox_alias;
            config.cwd = selected_root.clone();
            config.permissions.set_workspace_roots(vec![selected_root]);
            config
                .permissions
                .set_permission_profile(PermissionProfile::Disabled)
                .expect("full-access fixture");
        },
    )
    .await;
    let mut policy = FileSystemSandboxPolicy::read_only();
    policy.entries.push(FileSystemSandboxEntry::new(
        writable.clone().into(),
        FileSystemAccessMode::Write,
    ));
    policy.entries.push(FileSystemSandboxEntry::new(
        secret.clone().into(),
        FileSystemAccessMode::Deny,
    ));
    let ceiling =
        PermissionProfile::from_runtime_permissions(&policy, NetworkSandboxPolicy::Restricted);
    let grant = AdditionalPermissionProfile {
        network: Some(NetworkPermissions {
            enabled: Some(true),
        }),
        file_system: Some(FileSystemPermissions {
            entries: vec![
                FileSystemSandboxEntry::new(root.into(), FileSystemAccessMode::Write),
                FileSystemSandboxEntry::new(secret.clone().into(), FileSystemAccessMode::Read),
            ],
            glob_scan_max_depth: None,
        }),
    };
    let environment_id = original
        .initial_environments
        .primary()
        .expect("original environment")
        .selection
        .environment_id
        .clone();
    session
        .services
        .record_granted_permissions(&environment_id, grant.clone());
    let bounded = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions {
            permission_ceiling: Some(ceiling),
            tool_ceiling: Some(vec![codex_protocol::ToolName::plain("exec_command")]),
            inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 4096,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            }),
            ..Default::default()
        },
    )
    .await?
    .apply_started(
        &session,
        "restricted-file-access".into(),
        TurnStartKind::User,
    )
    .await?
    .expect("admitted context");
    bounded.record_granted_permissions(&environment_id, grant.clone(), false);
    let cancellation = CancellationToken::new();
    let step = session
        .capture_speculative_step_context(Arc::clone(&bounded), &cancellation)
        .await?;
    let environment = step.environments.primary().expect("restricted environment");
    assert_eq!(
        environment
            .sandbox_context_with_permissions(Some(grant))?
            .permissions
            .network_sandbox_policy(),
        NetworkSandboxPolicy::Restricted
    );
    // Use the broader pre-existing environment explicitly: the accessor must
    // still apply its originating turn's immutable ceiling after live grants.
    let files = step.fs_accessor(
        original
            .initial_environments
            .primary()
            .expect("broader environment"),
    )?;
    assert!(
        files
            .read_file(&secret_uri, ReadFileOptions::default())
            .await
            .is_err()
    );
    assert!(
        files
            .write_file(&outside, b"forbidden".to_vec(), WriteFileOptions::default())
            .await
            .is_err()
    );
    files
        .write_file(&inside, b"allowed".to_vec(), WriteFileOptions::default())
        .await?;
    assert_eq!(std::fs::read(writable.join("inside.txt"))?, b"allowed");
    let ordinary = PreparedTurnInputSettings::prepare(
        &session,
        ThreadSettingsOverrides::default(),
        TurnStartOptions::default(),
    )
    .await?
    .apply_started(&session, "ordinary-file-access".into(), TurnStartKind::User)
    .await?
    .expect("ordinary context");
    assert!(ordinary.permission_ceiling().is_none());
    assert_eq!(ordinary.permission_profile(), PermissionProfile::Disabled);
    assert!(
        ordinary.granted_permissions(&environment_id).is_some(),
        "existing grants were not deleted"
    );
    let ordinary_step = StepContext::for_test(Arc::clone(&ordinary));
    let files = ordinary_step.fs_accessor(
        ordinary_step
            .environments
            .primary()
            .expect("ordinary environment"),
    )?;
    assert_eq!(
        files
            .read_file(&secret_uri, ReadFileOptions::default())
            .await?,
        b"private fixture"
    );
    files
        .write_file(&outside, b"human".to_vec(), WriteFileOptions::default())
        .await?;
    assert_eq!(
        files
            .read_file(&outside, ReadFileOptions::default())
            .await?,
        b"human"
    );
    Ok(())
}

#[tokio::test]
async fn host_permission_ceiling_does_not_wait_for_an_unrelated_unavailable_environment()
-> anyhow::Result<()> {
    use crate::environment_selection::TurnEnvironmentState;
    use codex_protocol::models::PermissionProfile;
    let (_, context, _rx) = make_session_and_context_with_rx().await;
    let mut environments = context.initial_environments.clone();
    let mut selection = environments
        .primary()
        .expect("qualified environment")
        .selection
        .clone();
    selection.environment_id = "unavailable-fixture".into();
    environments
        .environments
        .push(TurnEnvironmentState::Failed {
            selection,
            required_skills: Vec::new(),
            error: "fixture unavailable".into(),
        });
    let restricted = environments.restrict_permissions(&PermissionProfile::read_only(), None)?;
    assert_eq!(restricted.turn_environments().count(), 1);
    assert!(
        matches!(&restricted.environments[1], TurnEnvironmentState::Failed { error, .. } if error == "fixture unavailable")
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn host_profile_raw_process_enforces_private_files_offline_ipc_and_empty_inheritance()
-> anyhow::Result<()> {
    core_test_support::test_codex::run_test_with_large_stack("host-profile-process", || async {
        use crate::tools::handlers::unified_exec::preparation::ExecCommandLifetime;
        use crate::tools::handlers::unified_exec::preparation::ParsedExecCommand;
        use crate::unified_exec::ExecCommandRequest;
        use crate::unified_exec::UnifiedExecContext;
        use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
        use codex_protocol::models::PermissionProfile;
        use codex_protocol::permissions::FileSystemAccessMode;
        use codex_protocol::permissions::FileSystemSandboxEntry;
        use codex_protocol::permissions::FileSystemSandboxPolicy;
        use codex_protocol::permissions::NetworkSandboxPolicy;
        async fn launch(
            session: &Arc<Session>,
            turn: Arc<TurnContext>,
            script: &str,
        ) -> anyhow::Result<String> {
            let context = UnifiedExecContext::new(
                Arc::clone(session),
                crate::session::step_context::StepContext::for_test(Arc::clone(&turn)),
                CancellationToken::new(),
                "profile-process-call".into(),
            );
            let arguments = serde_json::json!({
                "cmd": format!("/usr/bin/python3 - <<'PY'\n{script}\nPY"),
                "login": false, "tty": false, "yield_time_ms": 1000,
            })
            .to_string();
            let prepared = ParsedExecCommand::parse(
                &arguments,
                session,
                &context.step_context,
                turn.initial_environments.primary().expect("environment"),
            )?
            .prepare(ExecCommandLifetime::OneShot)?;
            let manager = &session.services.unified_exec_manager;
            let output = Box::pin(manager.exec_command(
                ExecCommandRequest {
                    prepared,
                    process_id: manager.allocate_process_id().await,
                    network: None,
                },
                &context,
            ))
            .await?;
            assert_eq!(
                output.exit_code,
                Some(0),
                "{}",
                output.truncated_output(2000)
            );
            Ok(output.truncated_output(2000))
        }
        let directory = tempfile::tempdir()?;
        let root = AbsolutePathBuf::from_absolute_path(directory.path())?;
        let work = root.join("work");
        std::fs::create_dir(&work)?;
        let secret = root.join("credential.txt");
        std::fs::write(&secret, "private fixture")?;
        let socket = root.join("owner.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
        let executable = std::env::current_exe()?;
        let alias = directory.path().join("codex-linux-sandbox");
        std::os::unix::fs::symlink(&executable, &alias)?;
        let cwd = root.clone();
        let (session, _initial, _events) = Box::pin(
            crate::session::tests::make_session_and_context_with_auth_and_config_and_rx(
                codex_login::CodexAuth::from_api_key("Test API Key"),
                Vec::new(),
                move |config| {
                    config.codex_self_exe = Some(executable);
                    config.codex_linux_sandbox_exe = Some(alias);
                    config.cwd = cwd.clone();
                    config.permissions.set_workspace_roots(vec![cwd]);
                    config
                        .permissions
                        .set_permission_profile(PermissionProfile::Disabled)
                        .expect("fixture permissions");
                    config.permissions.approval_policy =
                        Constrained::allow_any(AskForApproval::Never);
                    config.permissions.shell_environment_policy.inherit =
                        ShellEnvironmentPolicyInherit::None;
                    config
                        .permissions
                        .shell_environment_policy
                        .r#set
                        .insert("PATH".into(), "/usr/bin:/bin".into());
                    config.permissions.shell_environment_policy.r#set.insert(
                        "PROFILE_PRIVATE".into(),
                        "private fixture environment".into(),
                    );
                },
            ),
        )
        .await;
        let mut policy = FileSystemSandboxPolicy::read_only();
        policy.entries.push(FileSystemSandboxEntry::new(
            secret.clone().into(),
            FileSystemAccessMode::Deny,
        ));
        policy.entries.push(FileSystemSandboxEntry::new(
            work.clone().into(),
            FileSystemAccessMode::Write,
        ));
        let ceiling =
            PermissionProfile::from_runtime_permissions(&policy, NetworkSandboxPolicy::Restricted);
        let bounded = PreparedTurnInputSettings::prepare(
            &session,
            ThreadSettingsOverrides::default(),
            TurnStartOptions {
                command_environment_policy: Some(
                    codex_protocol::config_types::ShellEnvironmentPolicy {
                        inherit: ShellEnvironmentPolicyInherit::None,
                        r#set: std::collections::HashMap::from([(
                            "PATH".into(),
                            "/usr/bin:/bin".into(),
                        )]),
                        ..Default::default()
                    },
                ),
                permission_ceiling: Some(ceiling),
                tool_ceiling: Some(vec![codex_protocol::ToolName::plain("exec_command")]),
                inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                    max_provider_attempts: 1,
                    max_request_bytes: 4096,
                    deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
                }),
                ..Default::default()
            },
        )
        .await?
        .apply_started(&session, "profile-process".into(), TurnStartKind::User)
        .await?
        .expect("bounded turn");
        let script = format!(
            r#"import errno, os, socket
assert os.environ['PATH'] == '/usr/bin:/bin'
for key in ('PROFILE_PRIVATE', 'HOME', 'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'CODEX_EXEC_SERVER_NOISE_AUTH_TOKEN'):
    assert key not in os.environ, key
for path in ({secret}, '/proc/{parent_pid}/environ'):
    try:
        open(path, 'rb').read()
    except OSError as e:
        assert e.errno in (errno.EPERM, errno.EACCES, errno.ENOENT), (path, e.errno)
    else:
        raise AssertionError('protected file readable')
s = socket.socket(socket.AF_UNIX)
try:
    s.connect({socket})
except OSError as e:
    assert e.errno in (errno.EPERM, errno.EACCES), e.errno
else:
    raise AssertionError('offline owner IPC connected')
finally:
    s.close()
open({allowed}, 'w').write('useful work')
print('private-files-denied ipc-denied inheritance-empty work-written')"#,
            secret = serde_json::to_string(secret.as_path())?,
            socket = serde_json::to_string(socket.as_path())?,
            parent_pid = std::process::id(),
            allowed = serde_json::to_string(work.join("result.txt").as_path())?
        );
        let output = launch(&session, bounded, &script).await?;
        assert!(output.contains("private-files-denied ipc-denied inheritance-empty work-written"));
        assert_eq!(
            std::fs::read_to_string(work.join("result.txt"))?,
            "useful work"
        );
        let ordinary = PreparedTurnInputSettings::prepare(
            &session,
            ThreadSettingsOverrides::default(),
            TurnStartOptions::default(),
        )
        .await?
        .apply_started(&session, "profile-human".into(), TurnStartKind::User)
        .await?
        .expect("human turn");
        let script = format!(
            "import os, socket\nassert os.environ['PROFILE_PRIVATE'] == 'private fixture environment'\nassert open({secret}).read() == 'private fixture'\ns=socket.socket(socket.AF_UNIX)\ns.connect({socket})\ns.close()\nprint('human-access-preserved')",
            secret = serde_json::to_string(secret.as_path())?,
            socket = serde_json::to_string(socket.as_path())?
        );
        assert!(
            launch(&session, ordinary, &script)
                .await?
                .contains("human-access-preserved")
        );
        Ok(())
    })
}

#[tokio::test]
async fn host_command_environment_requires_explicit_bounded_non_profile_values() {
    use codex_protocol::config_types::ShellEnvironmentPolicy;
    use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
    let (session, _, _rx) = make_session_and_context_with_rx().await;
    let explicit = ShellEnvironmentPolicy {
        inherit: ShellEnvironmentPolicyInherit::None,
        ..Default::default()
    };
    for (policy, permissions) in [
        (ShellEnvironmentPolicy::default(), true),
        (
            ShellEnvironmentPolicy {
                use_profile: true,
                ..explicit.clone()
            },
            true,
        ),
        (
            ShellEnvironmentPolicy {
                r#set: std::collections::HashMap::from([("LARGE".into(), "x".repeat(64 * 1024))]),
                ..explicit.clone()
            },
            true,
        ),
        (explicit.clone(), false),
    ] {
        let options = TurnStartOptions {
            command_environment_policy: Some(policy),
            permission_ceiling: permissions
                .then(codex_protocol::models::PermissionProfile::read_only),
            tool_ceiling: Some(vec![]),
            inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
                max_provider_attempts: 1,
                max_request_bytes: 4096,
                deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            }),
            ..Default::default()
        };
        assert!(
            PreparedTurnInputSettings::prepare(
                &session,
                ThreadSettingsOverrides::default(),
                options
            )
            .await
            .is_err()
        );
    }
    let options = TurnStartOptions {
        command_environment_policy: Some(explicit),
        permission_ceiling: Some(codex_protocol::models::PermissionProfile::read_only()),
        tool_ceiling: Some(vec![]),
        inference_allowance: Some(codex_protocol::turn_input::TurnInferenceAllowance {
            max_provider_attempts: 1,
            max_request_bytes: 4096,
            deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
        }),
        ..Default::default()
    };
    assert!(
        PreparedTurnInputSettings::prepare(&session, ThreadSettingsOverrides::default(), options)
            .await
            .is_ok()
    );
}
