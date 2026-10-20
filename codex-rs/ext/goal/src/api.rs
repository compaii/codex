use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;

use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::ThreadGoalUpdatedEvent;
use codex_protocol::protocol::validate_thread_goal_objective;
use codex_rollout::RolloutItem;

use crate::runtime::GoalRuntimeHandle;
use crate::runtime::PreviousGoalSnapshot;
use crate::tool::fill_empty_thread_preview_if_possible;
use crate::tool::protocol_goal_from_state;
use crate::tool::state_status_from_protocol;
use crate::tool::validate_goal_budget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalServiceError {
    InvalidRequest(String),
    Internal(String),
}

impl fmt::Display for GoalServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for GoalServiceError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoalObjectiveUpdate<'a> {
    Keep,
    Set(&'a str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoalTokenBudgetUpdate {
    Keep,
    Set(Option<i64>),
}

#[derive(Clone, Copy, Debug)]
pub struct GoalSetRequest<'a> {
    pub thread_id: ThreadId,
    pub objective: GoalObjectiveUpdate<'a>,
    pub status: Option<ThreadGoalStatus>,
    pub token_budget: GoalTokenBudgetUpdate,
    pub max_goal_token_budget: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct GoalSetOutcome {
    pub goal: ThreadGoal,
    state_goal: codex_state::ThreadGoal,
    previous_goal: Option<PreviousGoalSnapshot>,
    /// Present for the conditional contract, captured in the mutation transaction.
    pub revision: Option<String>,
    activation_context: Option<BTreeMap<String, codex_protocol::protocol::AdditionalContextEntry>>,
    // Retain the original permit through rollout publication and work admission.
    activation_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    activation_runtime: Option<Weak<GoalRuntimeHandle>>,
}

impl GoalSetOutcome {
    pub fn thread_goal_updated_item(&self) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::ThreadGoalUpdated(ThreadGoalUpdatedEvent {
            thread_id: self.goal.thread_id,
            turn_id: None,
            goal: self.goal.clone(),
        }))
    }

    pub async fn activate_with_context(
        &self,
        service: &GoalService,
    ) -> Result<crate::runtime::GoalActivationDisposition, String> {
        if self.activation_permit.is_none() {
            return Err("Native activation permit unavailable".into());
        }
        let runtime = service
            .runtime_for_thread(self.goal.thread_id)
            .ok_or("Selected runtime unavailable after commitment")?;
        let selected = self
            .activation_runtime
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or("Selected activation runtime retired")?;
        if !Arc::ptr_eq(&runtime, &selected) {
            return Err(
                "Selected runtime changed after metadata commitment; context was not submitted"
                    .into(),
            );
        }

        runtime
            .activate_with_context(
                self.state_goal.clone(),
                self.previous_goal.clone(),
                self.revision
                    .as_deref()
                    .ok_or("Conditional activation revision unavailable")?,
                self.activation_context
                    .clone()
                    .ok_or("Activation context unavailable")?,
            )
            .await
    }

    pub async fn apply_runtime_effects(&self, goal_service: &GoalService) {
        if let Some(runtime) = goal_service.runtime_for_thread(self.goal.thread_id)
            && let Err(err) = runtime
                .apply_external_goal_set(self.state_goal.clone(), self.previous_goal.clone())
                .await
        {
            tracing::warn!("failed to apply external goal status runtime effects: {err}");
        }
    }
}

#[derive(Debug, Default)]
pub struct GoalService {
    runtimes: Mutex<HashMap<String, Weak<GoalRuntimeHandle>>>,
}

impl GoalService {
    pub fn activation_runtime_available(&self, thread_id: ThreadId) -> bool {
        self.runtime_for_thread(thread_id)
            .is_some_and(|runtime| runtime.tools_available())
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Restores persisted goal state into the registered runtime for `thread_id`.
    pub async fn restore_thread_runtime_after_resume(
        &self,
        thread_id: ThreadId,
    ) -> Result<(), GoalServiceError> {
        let runtime = self.runtime_for_thread(thread_id).ok_or_else(|| {
            GoalServiceError::Internal(format!(
                "goal runtime is unavailable for thread {thread_id}"
            ))
        })?;
        runtime
            .restore_after_resume()
            .await
            .map_err(GoalServiceError::Internal)
    }

    /// Flushes any in-flight goal accounting before a fork copies the source goal snapshot.
    pub async fn flush_thread_goal_progress_for_fork(
        &self,
        thread_id: ThreadId,
    ) -> Result<(), GoalServiceError> {
        let Some(runtime) = self.runtime_for_thread(thread_id) else {
            return Ok(());
        };
        let _goal_state_permit = runtime
            .goal_state_permit()
            .await
            .map_err(GoalServiceError::Internal)?;
        runtime
            .prepare_external_goal_mutation()
            .await
            .map_err(GoalServiceError::Internal)
    }

    pub async fn get_thread_goal(
        &self,
        state_db: &codex_state::StateRuntime,
        thread_id: ThreadId,
    ) -> Result<Option<ThreadGoal>, GoalServiceError> {
        state_db
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .map(|goal| goal.map(protocol_goal_from_state))
            .map_err(|err| GoalServiceError::Internal(format!("failed to read thread goal: {err}")))
    }

    /// Records the user's instruction after validation and before changing goal state.
    /// The recording future runs under the runtime's goal mutation permit; failure leaves
    /// the goal unchanged. Recorded user intent remains valid if the database write fails.
    pub async fn set_thread_goal(
        &self,
        state_db: &codex_state::StateRuntime,
        request: GoalSetRequest<'_>,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<GoalSetOutcome, GoalServiceError> {
        self.set_thread_goal_inner(state_db, request, None, None, record_user_instruction)
            .await
    }

    /// Older servers do not expose this conditional contract; callers must never
    /// downgrade a stale-selection-sensitive mutation to the legacy set method.
    pub async fn set_thread_goal_conditionally(
        &self,
        state_db: &codex_state::StateRuntime,
        request: GoalSetRequest<'_>,
        expected_revision: &str,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<GoalSetOutcome, GoalServiceError> {
        self.set_thread_goal_inner(
            state_db,
            request,
            Some(expected_revision),
            None,
            record_user_instruction,
        )
        .await
    }

    /// Mutate and admit context under one native goal-state permit. Automatic
    /// continuation cannot start between commitment and context admission.
    pub async fn activate_thread_goal_conditionally(
        &self,
        state_db: &codex_state::StateRuntime,
        request: GoalSetRequest<'_>,
        expected_revision: &str,
        context: BTreeMap<String, codex_protocol::protocol::AdditionalContextEntry>,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<GoalSetOutcome, GoalServiceError> {
        self.set_thread_goal_inner(
            state_db,
            request,
            Some(expected_revision),
            Some(context),
            record_user_instruction,
        )
        .await
    }

    pub async fn snapshot_thread_goal(
        &self,
        state_db: &codex_state::StateRuntime,
        thread_id: ThreadId,
    ) -> Result<(Option<ThreadGoal>, String), GoalServiceError> {
        let snapshot = state_db
            .thread_goals()
            .snapshot_thread_goal(thread_id)
            .await
            .map_err(|err| {
                GoalServiceError::Internal(format!("failed to read goal snapshot: {err}"))
            })?;
        Ok((
            snapshot.goal.map(protocol_goal_from_state),
            snapshot.revision,
        ))
    }

    async fn set_thread_goal_inner(
        &self,
        state_db: &codex_state::StateRuntime,
        request: GoalSetRequest<'_>,
        expected_revision: Option<&str>,
        activation_context: Option<
            BTreeMap<String, codex_protocol::protocol::AdditionalContextEntry>,
        >,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<GoalSetOutcome, GoalServiceError> {
        let GoalSetRequest {
            thread_id,
            objective,
            status,
            token_budget,
            max_goal_token_budget,
        } = request;
        let status = status.map(state_status_from_protocol);
        let objective = match objective {
            GoalObjectiveUpdate::Keep => None,
            GoalObjectiveUpdate::Set(objective) => Some(objective.trim()),
        };
        let token_budget = match token_budget {
            GoalTokenBudgetUpdate::Keep => None,
            GoalTokenBudgetUpdate::Set(token_budget) => {
                Some(token_budget.or(max_goal_token_budget))
            }
        };

        if let Some(objective) = objective {
            validate_thread_goal_objective(objective).map_err(GoalServiceError::InvalidRequest)?;
        }
        if objective.is_some() || token_budget.is_some() {
            validate_goal_budget(token_budget.flatten(), max_goal_token_budget)
                .map_err(GoalServiceError::InvalidRequest)?;
        }

        let runtime = self.runtime_for_thread(thread_id);
        // Hold this through the prepare/write window so idle continuation cannot
        // launch from goal state that this external mutation is about to change.
        let activation_permit = if activation_context.is_some() {
            let runtime = runtime.as_ref().ok_or_else(|| {
                GoalServiceError::InvalidRequest(
                    "Selected loaded runtime unavailable before activation".into(),
                )
            })?;
            Some(Arc::new(
                runtime
                    .owned_goal_state_permit()
                    .await
                    .map_err(GoalServiceError::Internal)?,
            ))
        } else {
            None
        };
        let _goal_state_permit = match runtime.as_ref().filter(|_| activation_permit.is_none()) {
            Some(runtime) => Some(
                runtime
                    .goal_state_permit()
                    .await
                    .map_err(GoalServiceError::Internal)?,
            ),
            None => None,
        };
        prepare_goal_mutation(state_db, thread_id, runtime.as_deref(), expected_revision).await?;

        let existing_goal = state_db
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .map_err(|err| {
                GoalServiceError::Internal(format!("failed to read thread goal: {err}"))
            })?;
        if objective.is_none() && existing_goal.is_none() {
            return Err(GoalServiceError::InvalidRequest(format!(
                "cannot update goal for thread {thread_id}: no goal exists"
            )));
        }
        if let Some(revision) = expected_revision {
            let outcome = state_db
                .thread_goals()
                .mutate_thread_goal_conditionally(
                    thread_id,
                    revision,
                    codex_state::ConditionalGoalMutation::Set(codex_state::GoalUpdate {
                        objective: objective.map(str::to_string),
                        status,
                        token_budget,
                        expected_goal_id: None,
                    }),
                    record_user_instruction,
                )
                .await
                .map_err(|err| {
                    GoalServiceError::Internal(format!("conditional goal update failed: {err}"))
                })?
                .ok_or_else(|| {
                    GoalServiceError::InvalidRequest(
                        "goal changed; inspect a fresh snapshot before making another selection"
                            .into(),
                    )
                })?;
            let goal = outcome.snapshot.goal.ok_or_else(|| {
                GoalServiceError::Internal("conditional goal update returned no goal".into())
            })?;
            if let Some(runtime) = runtime.as_ref() {
                runtime.clear_pending_turn_start_options().await;
            }
            if objective.is_some() {
                fill_empty_thread_preview_if_possible(state_db, thread_id, &goal).await;
            }
            let previous_goal = outcome.before.as_ref().map(PreviousGoalSnapshot::from);
            return Ok(GoalSetOutcome {
                goal: protocol_goal_from_state(goal.clone()),
                previous_goal,
                activation_runtime: activation_permit
                    .as_ref()
                    .and_then(|_| runtime.as_ref().map(Arc::downgrade)),
                activation_context,
                activation_permit,
                state_goal: goal,
                revision: Some(outcome.snapshot.revision),
            });
        }
        record_user_instruction.await?;

        let (goal, previous_goal) = if let Some(objective) = objective {
            if let Some(existing_goal) = existing_goal.as_ref() {
                let previous_goal = PreviousGoalSnapshot::from(existing_goal);
                state_db
                    .thread_goals()
                    .update_thread_goal(
                        thread_id,
                        codex_state::GoalUpdate {
                            objective: Some(objective.to_string()),
                            status,
                            token_budget,
                            expected_goal_id: Some(existing_goal.goal_id.clone()),
                        },
                    )
                    .await
                    .map_err(|err| {
                        GoalServiceError::Internal(format!("failed to update thread goal: {err}"))
                    })?
                    .ok_or_else(|| {
                        GoalServiceError::InvalidRequest(format!(
                            "cannot update goal for thread {thread_id}: no goal exists"
                        ))
                    })
                    .map(|goal| (goal, Some(previous_goal)))?
            } else {
                state_db
                    .thread_goals()
                    .replace_thread_goal(
                        thread_id,
                        objective,
                        status.unwrap_or(codex_state::ThreadGoalStatus::Active),
                        token_budget.flatten().or(max_goal_token_budget),
                    )
                    .await
                    .map_err(|err| {
                        GoalServiceError::Internal(format!("failed to replace thread goal: {err}"))
                    })
                    .map(|goal| (goal, None))?
            }
        } else {
            let existing_goal = existing_goal.ok_or_else(|| {
                GoalServiceError::InvalidRequest(format!(
                    "cannot update goal for thread {thread_id}: no goal exists"
                ))
            })?;
            let previous_goal = PreviousGoalSnapshot::from(&existing_goal);
            let expected_goal_id = existing_goal.goal_id.clone();
            state_db
                .thread_goals()
                .update_thread_goal(
                    thread_id,
                    codex_state::GoalUpdate {
                        objective: None,
                        status,
                        token_budget,
                        expected_goal_id: Some(expected_goal_id),
                    },
                )
                .await
                .map_err(|err| {
                    GoalServiceError::Internal(format!("failed to update thread goal: {err}"))
                })?
                .ok_or_else(|| {
                    GoalServiceError::InvalidRequest(format!(
                        "cannot update goal for thread {thread_id}: no goal exists"
                    ))
                })
                .map(|goal| (goal, Some(previous_goal)))?
        };

        if let Some(runtime) = runtime.as_ref() {
            runtime.clear_pending_turn_start_options().await;
        }

        if objective.is_some() {
            fill_empty_thread_preview_if_possible(state_db, thread_id, &goal).await;
        }
        Ok(GoalSetOutcome {
            goal: protocol_goal_from_state(goal.clone()),
            state_goal: goal,
            previous_goal,
            revision: None,
            activation_context: None,
            activation_permit: None,
            activation_runtime: None,
        })
    }

    /// Records a clear instruction even when no goal exists, then deletes goal state under the mutation permit.
    pub async fn clear_thread_goal(
        &self,
        state_db: &codex_state::StateRuntime,
        thread_id: ThreadId,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<bool, GoalServiceError> {
        self.clear_thread_goal_inner(state_db, thread_id, None, record_user_instruction)
            .await
            .map(|(cleared, _)| cleared)
    }

    pub async fn clear_thread_goal_conditionally(
        &self,
        state_db: &codex_state::StateRuntime,
        thread_id: ThreadId,
        expected_revision: &str,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<(bool, String), GoalServiceError> {
        let (cleared, revision) = self
            .clear_thread_goal_inner(
                state_db,
                thread_id,
                Some(expected_revision),
                record_user_instruction,
            )
            .await?;
        Ok((
            cleared,
            revision.ok_or_else(|| {
                GoalServiceError::Internal("conditional clear revision missing".into())
            })?,
        ))
    }

    async fn clear_thread_goal_inner(
        &self,
        state_db: &codex_state::StateRuntime,
        thread_id: ThreadId,
        expected_revision: Option<&str>,
        record_user_instruction: impl Future<Output = Result<(), GoalServiceError>> + Send,
    ) -> Result<(bool, Option<String>), GoalServiceError> {
        let runtime = self.runtime_for_thread(thread_id);
        // Hold this through the prepare/write window so idle continuation cannot
        // launch from goal state that this external mutation is about to change.
        let goal_state_permit = match runtime.as_ref() {
            Some(runtime) => Some(
                runtime
                    .goal_state_permit()
                    .await
                    .map_err(GoalServiceError::Internal)?,
            ),
            None => None,
        };
        prepare_goal_mutation(state_db, thread_id, runtime.as_deref(), expected_revision).await?;

        let (cleared_goal, revision) = if let Some(expected) = expected_revision {
            let outcome = state_db
                .thread_goals()
                .mutate_thread_goal_conditionally(
                    thread_id,
                    expected,
                    codex_state::ConditionalGoalMutation::Clear,
                    record_user_instruction,
                )
                .await
                .map_err(|err| {
                    GoalServiceError::Internal(format!("conditional goal clear failed: {err}"))
                })?
                .ok_or_else(|| {
                    GoalServiceError::InvalidRequest(
                        "goal changed; inspect a fresh snapshot before making another selection"
                            .into(),
                    )
                })?;
            (outcome.before, Some(outcome.snapshot.revision))
        } else {
            // A failed set can leave authorization in history without a goal row.
            record_user_instruction.await?;
            let cleared = state_db
                .thread_goals()
                .delete_thread_goal(thread_id)
                .await
                .map_err(|err| {
                    GoalServiceError::Internal(format!("failed to clear thread goal: {err}"))
                })?;
            (cleared, None)
        };
        let cleared = cleared_goal.is_some();
        if cleared && let Some(runtime) = runtime.as_ref() {
            runtime.clear_pending_turn_start_options().await;
        }
        drop(goal_state_permit);
        drop(runtime);

        if let (Some(runtime), Some(goal)) = (self.runtime_for_thread(thread_id), cleared_goal)
            && let Err(err) = runtime.apply_external_goal_clear(goal).await
        {
            tracing::warn!("failed to apply external goal clear runtime effects: {err}");
        }

        Ok((cleared, revision))
    }

    pub(crate) fn register_runtime(&self, runtime: &Arc<GoalRuntimeHandle>) {
        self.runtimes()
            .insert(runtime.thread_id().to_string(), Arc::downgrade(runtime));
    }

    pub(crate) fn unregister_runtime(&self, runtime: &Arc<GoalRuntimeHandle>) {
        let key = runtime.thread_id().to_string();
        let runtime = Arc::downgrade(runtime);
        let mut runtimes = self.runtimes();
        if runtimes
            .get(&key)
            .is_some_and(|registered| registered.ptr_eq(&runtime))
        {
            runtimes.remove(&key);
        }
    }

    pub(crate) fn runtime_for_thread(&self, thread_id: ThreadId) -> Option<Arc<GoalRuntimeHandle>> {
        let key = thread_id.to_string();
        let mut runtimes = self.runtimes();
        let runtime = runtimes.get(&key).and_then(Weak::upgrade);
        if runtime.is_none() {
            runtimes.remove(&key);
        }
        runtime
    }

    fn runtimes(&self) -> std::sync::MutexGuard<'_, HashMap<String, Weak<GoalRuntimeHandle>>> {
        self.runtimes.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A snapshot precheck avoids known-stale runtime side effects. The database
/// conditional mutation below remains the authoritative race protection.
async fn prepare_goal_mutation(
    state_db: &codex_state::StateRuntime,
    thread_id: ThreadId,
    runtime: Option<&GoalRuntimeHandle>,
    expected_revision: Option<&str>,
) -> Result<(), GoalServiceError> {
    if let Some(expected) = expected_revision {
        let snapshot = state_db
            .thread_goals()
            .snapshot_thread_goal(thread_id)
            .await
            .map_err(|err| {
                GoalServiceError::Internal(format!("failed to inspect goal snapshot: {err}"))
            })?;
        if snapshot.revision != expected {
            return Err(GoalServiceError::InvalidRequest(
                "goal changed; inspect a fresh snapshot before making another selection".into(),
            ));
        }
        if let Some(runtime) = runtime {
            runtime
                .flush_external_goal_progress()
                .await
                .map_err(GoalServiceError::Internal)?;
        }
    } else if let Some(runtime) = runtime
        && let Err(err) = runtime.prepare_external_goal_mutation().await
    {
        tracing::warn!("failed to prepare external goal mutation: {err}");
    }
    Ok(())
}
