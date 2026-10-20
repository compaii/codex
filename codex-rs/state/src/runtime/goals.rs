use super::*;
use crate::model::ThreadGoalRow;
use uuid::Uuid;

#[derive(Clone)]
pub struct GoalStore {
    pool: Arc<SqlitePool>,
}

impl GoalStore {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }
}

pub struct GoalUpdate {
    pub objective: Option<String>,
    pub status: Option<crate::ThreadGoalStatus>,
    pub token_budget: Option<Option<i64>>,
    pub expected_goal_id: Option<String>,
}

/// A definition revision excludes accounting but survives clear/recreate and ABA.
#[derive(Debug, Clone)]
pub struct GoalSnapshot {
    pub goal: Option<crate::ThreadGoal>,
    pub revision: String,
}

pub enum ConditionalGoalMutation {
    Set(GoalUpdate),
    Clear,
}

pub struct ConditionalGoalOutcome {
    pub before: Option<crate::ThreadGoal>,
    pub snapshot: GoalSnapshot,
}

pub enum GoalAccountingOutcome {
    Unchanged(Option<crate::ThreadGoal>),
    Updated(crate::ThreadGoal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalAccountingMode {
    ActiveStatusOnly,
    ActiveOnly,
    ActiveOrComplete,
    ActiveOrStopped,
}

impl GoalStore {
    pub async fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let row = sqlx::query(
            r#"
SELECT
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
FROM thread_goals
WHERE thread_id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| thread_goal_from_row(&row)).transpose()
    }

    /// Read goal and revision in one SQLite snapshot, including an absent goal.
    pub async fn snapshot_thread_goal(&self, thread_id: ThreadId) -> anyhow::Result<GoalSnapshot> {
        let mut tx = self.pool.begin().await?;
        let snapshot = snapshot_in(&mut tx, thread_id).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Compare and mutate under the database writer lock, including legacy/model
    /// writers. A stale selection never records user intent or changes goal state.
    /// `record_instruction` must not write this goals database; rollout/state
    /// persistence uses its independent database and runs before the mutation.
    pub async fn mutate_thread_goal_conditionally<E: Into<anyhow::Error>>(
        &self,
        thread_id: ThreadId,
        expected_revision: &str,
        mutation: ConditionalGoalMutation,
        record_instruction: impl Future<Output = Result<(), E>> + Send,
    ) -> anyhow::Result<Option<ConditionalGoalOutcome>> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let before = snapshot_in(&mut tx, thread_id).await?;
        if before.revision != expected_revision {
            tx.rollback().await?;
            return Ok(None);
        }
        // Validate before publishing an instruction to the separate history.
        if let ConditionalGoalMutation::Set(update) = &mutation {
            if before.goal.is_none() && update.objective.is_none() {
                anyhow::bail!("cannot update goal: no goal exists");
            }
            if update
                .expected_goal_id
                .as_ref()
                .is_some_and(|id| before.goal.as_ref().is_none_or(|goal| &goal.goal_id != id))
            {
                tx.rollback().await?;
                return Ok(None);
            }
        }
        record_instruction.await.map_err(Into::into)?;
        match mutation {
            ConditionalGoalMutation::Clear => {
                sqlx::query("DELETE FROM thread_goals WHERE thread_id = ?")
                    .bind(thread_id.to_string())
                    .execute(&mut *tx)
                    .await?;
            }
            ConditionalGoalMutation::Set(update) => {
                let now_ms = datetime_to_epoch_millis(Utc::now());
                if let Some(goal) = &before.goal {
                    let objective = update.objective.as_deref().unwrap_or(&goal.objective);
                    let budget = update.token_budget.unwrap_or(goal.token_budget);
                    let requested_status = update.status.unwrap_or(goal.status);
                    let status = if goal.status == crate::ThreadGoalStatus::BudgetLimited
                        && matches!(
                            requested_status,
                            crate::ThreadGoalStatus::Paused | crate::ThreadGoalStatus::Blocked
                        ) {
                        crate::ThreadGoalStatus::BudgetLimited
                    } else {
                        status_after_budget_limit(requested_status, goal.tokens_used, budget)
                    };
                    sqlx::query("UPDATE thread_goals SET objective=?, status=?, token_budget=?, updated_at_ms=? WHERE thread_id=?")
                        .bind(objective).bind(status.as_str()).bind(budget).bind(now_ms).bind(thread_id.to_string())
                        .execute(&mut *tx).await?;
                } else {
                    let status = status_after_budget_limit(
                        update.status.unwrap_or(crate::ThreadGoalStatus::Active),
                        0,
                        update.token_budget.flatten(),
                    );
                    sqlx::query("INSERT INTO thread_goals(thread_id,goal_id,objective,status,token_budget,tokens_used,time_used_seconds,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,0,0,?,?)")
                        .bind(thread_id.to_string()).bind(Uuid::new_v4().to_string())
                        .bind(update.objective.as_deref().ok_or_else(|| anyhow::anyhow!("new objective required"))?)
                        .bind(status.as_str()).bind(update.token_budget.flatten()).bind(now_ms).bind(now_ms)
                        .execute(&mut *tx).await?;
                }
            }
        }
        let snapshot = snapshot_in(&mut tx, thread_id).await?;
        tx.commit().await?;
        Ok(Some(ConditionalGoalOutcome {
            before: before.goal,
            snapshot,
        }))
    }

    pub async fn replace_thread_goal_snapshot(
        &self,
        goal: &crate::ThreadGoal,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = excluded.tokens_used,
    time_used_seconds = excluded.time_used_seconds,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
            "#,
        )
        .bind(goal.thread_id.to_string())
        .bind(&goal.goal_id)
        .bind(&goal.objective)
        .bind(goal.status.as_str())
        .bind(goal.token_budget)
        .bind(goal.tokens_used)
        .bind(goal.time_used_seconds)
        .bind(datetime_to_epoch_millis(goal.created_at))
        .bind(datetime_to_epoch_millis(goal.updated_at))
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
INSERT INTO thread_goal_continuation_deferrals (thread_id)
VALUES (?)
ON CONFLICT(thread_id) DO NOTHING
            "#,
        )
        .bind(goal.thread_id.to_string())
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<bool> {
        sqlx::query_scalar(
            r#"
SELECT EXISTS(
    SELECT 1
    FROM thread_goal_continuation_deferrals
    WHERE thread_id = ?
)
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(Into::into)
    }

    pub async fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM thread_goal_continuation_deferrals WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .execute(self.pool.as_ref())
            .await?;

        Ok(())
    }

    pub async fn replace_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: &str,
        status: crate::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> anyhow::Result<crate::ThreadGoal> {
        let goal_id = Uuid::new_v4().to_string();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
        let row = sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .bind(goal_id)
        .bind(objective)
        .bind(status.as_str())
        .bind(token_budget)
        .bind(now_ms)
        .bind(now_ms)
        .fetch_one(self.pool.as_ref())
        .await?;

        thread_goal_from_row(&row)
    }

    pub async fn insert_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: &str,
        status: crate::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let goal_id = Uuid::new_v4().to_string();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
        let row = sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
WHERE thread_goals.status = 'complete'
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .bind(goal_id)
        .bind(objective)
        .bind(status.as_str())
        .bind(token_budget)
        .bind(now_ms)
        .bind(now_ms)
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| thread_goal_from_row(&row)).transpose()
    }

    pub async fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let GoalUpdate {
            objective,
            status,
            token_budget,
            expected_goal_id,
        } = update;
        let objective = objective.as_deref();
        let expected_goal_id = expected_goal_id.as_deref();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let result = match (status, token_budget) {
            (Some(status), Some(token_budget)) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    status = CASE
        WHEN status = ? AND ? IN (?, ?) THEN status
        WHEN ? = 'active' AND ? IS NOT NULL AND tokens_used >= ? THEN ?
        ELSE ?
    END,
    token_budget = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::Paused.as_str())
                .bind(crate::ThreadGoalStatus::Blocked.as_str())
                .bind(status.as_str())
                .bind(token_budget)
                .bind(token_budget)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(token_budget)
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (Some(status), None) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    status = CASE
        WHEN status = ? AND ? IN (?, ?) THEN status
        WHEN ? = 'active' AND token_budget IS NOT NULL AND tokens_used >= token_budget THEN ?
        ELSE ?
    END,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::Paused.as_str())
                .bind(crate::ThreadGoalStatus::Blocked.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (None, Some(token_budget)) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    token_budget = ?,
    status = CASE
        WHEN status = 'active' AND ? IS NOT NULL AND tokens_used >= ? THEN ?
        ELSE status
    END,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(token_budget)
                .bind(token_budget)
                .bind(token_budget)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (None, None) => {
                if let Some(objective) = objective {
                    sqlx::query(
                        r#"
UPDATE thread_goals
SET
    objective = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                    )
                    .bind(objective)
                    .bind(now_ms)
                    .bind(thread_id.to_string())
                    .bind(expected_goal_id)
                    .bind(expected_goal_id)
                    .execute(self.pool.as_ref())
                    .await?
                } else {
                    let goal = self.get_thread_goal(thread_id).await?;
                    return Ok(match (goal, expected_goal_id) {
                        (Some(goal), Some(expected_goal_id))
                            if goal.goal_id != expected_goal_id =>
                        {
                            None
                        }
                        (goal, _) => goal,
                    });
                }
            }
        };

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_thread_goal(thread_id).await
    }

    pub async fn pause_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        self.update_active_thread_goal_status(thread_id, crate::ThreadGoalStatus::Paused)
            .await
    }

    pub async fn usage_limit_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        self.update_active_thread_goal_status(thread_id, crate::ThreadGoalStatus::UsageLimited)
            .await
    }

    async fn update_active_thread_goal_status(
        &self,
        thread_id: ThreadId,
        status: crate::ThreadGoalStatus,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let result = sqlx::query(
            r#"
UPDATE thread_goals
SET
    status = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (
      status = 'active'
      OR (
          ? = 'usage_limited'
          AND status = 'budget_limited'
      )
  )
            "#,
        )
        .bind(status.as_str())
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(status.as_str())
        .execute(self.pool.as_ref())
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_thread_goal(thread_id).await
    }

    pub async fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let row = sqlx::query(
            r#"
DELETE FROM thread_goals
WHERE thread_id = ?
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| thread_goal_from_row(&row)).transpose()
    }

    pub async fn account_thread_goal_usage(
        &self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&str>,
    ) -> anyhow::Result<GoalAccountingOutcome> {
        let time_delta_seconds = time_delta_seconds.max(0);
        let token_delta = token_delta.max(0);
        if time_delta_seconds == 0 && token_delta == 0 {
            return Ok(GoalAccountingOutcome::Unchanged(
                self.get_thread_goal(thread_id).await?,
            ));
        }

        let now_ms = datetime_to_epoch_millis(Utc::now());
        let active_or_stopped_status_filter =
            "status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited')";
        let status_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly => "status = 'active'",
            GoalAccountingMode::ActiveOnly => "status IN ('active', 'budget_limited')",
            GoalAccountingMode::ActiveOrComplete => {
                "status IN ('active', 'budget_limited', 'complete')"
            }
            GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
        };
        let budget_limit_status_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly
            | GoalAccountingMode::ActiveOnly
            | GoalAccountingMode::ActiveOrComplete => "status = 'active'",
            GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
        };
        let mut builder = QueryBuilder::<Sqlite>::new(
            r#"
UPDATE thread_goals
SET
    time_used_seconds = time_used_seconds +
            "#,
        );
        builder.push_bind(time_delta_seconds);
        builder.push(
            r#",
    tokens_used = tokens_used +
            "#,
        );
        builder.push_bind(token_delta);
        builder.push(
            r#",
    status = CASE
        WHEN
            "#,
        );
        builder.push(budget_limit_status_filter);
        builder.push(
            r#"
            AND token_budget IS NOT NULL
            AND tokens_used +
            "#,
        );
        builder.push_bind(token_delta);
        builder.push(
            r#"
                >= token_budget
            THEN
            "#,
        );
        builder.push_bind(crate::ThreadGoalStatus::BudgetLimited.as_str());
        builder.push(
            r#"
        ELSE status
    END,
    updated_at_ms =
            "#,
        );
        builder.push_bind(now_ms);
        builder.push(
            r#"
WHERE thread_id =
            "#,
        );
        builder.push_bind(thread_id.to_string());
        builder.push(" AND ");
        builder.push(status_filter);
        if let Some(expected_goal_id) = expected_goal_id {
            builder.push(" AND goal_id = ").push_bind(expected_goal_id);
        }
        builder.push(
            r#"
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        );

        let row = builder.build().fetch_optional(self.pool.as_ref()).await?;

        let Some(row) = row else {
            return Ok(GoalAccountingOutcome::Unchanged(
                self.get_thread_goal(thread_id).await?,
            ));
        };

        let updated = thread_goal_from_row(&row)?;
        Ok(GoalAccountingOutcome::Updated(updated))
    }
}

async fn snapshot_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    thread_id: ThreadId,
) -> anyhow::Result<GoalSnapshot> {
    let identity: String =
        sqlx::query_scalar("SELECT identity FROM thread_goal_revision_identity WHERE id=1")
            .fetch_one(&mut **tx)
            .await?;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM thread_goal_revisions WHERE thread_id=?")
            .bind(thread_id.to_string())
            .fetch_optional(&mut **tx)
            .await?
            .unwrap_or(0);
    let goal = sqlx::query("SELECT thread_id,goal_id,objective,status,token_budget,tokens_used,time_used_seconds,created_at_ms,updated_at_ms FROM thread_goals WHERE thread_id=?")
        .bind(thread_id.to_string()).fetch_optional(&mut **tx).await?
        .map(|row| thread_goal_from_row(&row)).transpose()?;
    Ok(GoalSnapshot {
        goal,
        revision: format!("goal-v1:{identity}:{thread_id}:{revision}"),
    })
}

fn thread_goal_from_row(row: &sqlx::sqlite::SqliteRow) -> anyhow::Result<crate::ThreadGoal> {
    ThreadGoalRow::try_from_row(row).and_then(crate::ThreadGoal::try_from)
}

fn status_after_budget_limit(
    status: crate::ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> crate::ThreadGoalStatus {
    if status == crate::ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        crate::ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::test_thread_metadata;
    use crate::runtime::test_support::unique_temp_dir;
    use codex_utils_absolute_path::test_support::PathExt;
    use pretty_assertions::assert_eq;

    async fn test_runtime() -> std::sync::Arc<StateRuntime> {
        StateRuntime::init(
            crate::SqliteConfig::new_for_testing(unique_temp_dir().as_path().abs()),
            "test-provider".to_string(),
        )
        .await
        .expect("state db should initialize")
    }

    fn test_thread_id() -> ThreadId {
        ThreadId::from_string("00000000-0000-0000-0000-000000000123").expect("valid thread id")
    }

    async fn upsert_test_thread(runtime: &StateRuntime, thread_id: ThreadId) {
        let sqlite_home = runtime.sqlite().home();
        let metadata = test_thread_metadata(sqlite_home, thread_id, sqlite_home.join("workspace"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("test thread should be upserted");
    }

    fn conditional_status(status: crate::ThreadGoalStatus) -> ConditionalGoalMutation {
        ConditionalGoalMutation::Set(GoalUpdate {
            objective: None,
            status: Some(status),
            token_budget: None,
            expected_goal_id: None,
        })
    }

    #[tokio::test]
    async fn conditional_goal_revision_preserves_accounting_and_creation() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        let original = store
            .replace_thread_goal(id, "original", crate::ThreadGoalStatus::Paused, Some(100))
            .await
            .unwrap();
        let snapshot = store.snapshot_thread_goal(id).await.unwrap();
        store
            .account_thread_goal_usage(
                id,
                3,
                7,
                GoalAccountingMode::ActiveOrStopped,
                Some(&original.goal_id),
            )
            .await
            .unwrap();
        assert_eq!(
            store.snapshot_thread_goal(id).await.unwrap().revision,
            snapshot.revision
        );
        let changed = store
            .mutate_thread_goal_conditionally(
                id,
                &snapshot.revision,
                conditional_status(crate::ThreadGoalStatus::Active),
                async { Ok::<(), anyhow::Error>(()) },
            )
            .await
            .unwrap()
            .unwrap();
        let goal = changed.snapshot.goal.unwrap();
        assert_eq!(goal.tokens_used, 7);
        assert_eq!(goal.time_used_seconds, 3);
        assert_eq!(goal.created_at, original.created_at);
        assert_eq!(goal.goal_id, original.goal_id);
        assert_ne!(changed.snapshot.revision, snapshot.revision);
    }

    #[tokio::test]
    async fn conditional_goal_rejects_replaced_and_aba_definitions_before_recording() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        store
            .replace_thread_goal(id, "same", crate::ThreadGoalStatus::Paused, Some(100))
            .await
            .unwrap();
        let original = store.snapshot_thread_goal(id).await.unwrap();
        store
            .update_thread_goal(
                id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Active),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .unwrap();
        store
            .update_thread_goal(
                id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .unwrap();
        let called = std::sync::atomic::AtomicBool::new(false);
        let result = store
            .mutate_thread_goal_conditionally(
                id,
                &original.revision,
                ConditionalGoalMutation::Clear,
                async {
                    called.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok::<(), anyhow::Error>(())
                },
            )
            .await
            .unwrap();
        assert!(result.is_none());
        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            store.get_thread_goal(id).await.unwrap().unwrap().objective,
            "same"
        );
        let selected = store.snapshot_thread_goal(id).await.unwrap();
        store.delete_thread_goal(id).await.unwrap();
        store
            .replace_thread_goal(id, "same", crate::ThreadGoalStatus::Paused, Some(100))
            .await
            .unwrap();
        assert!(
            store
                .mutate_thread_goal_conditionally(
                    id,
                    &selected.revision,
                    conditional_status(crate::ThreadGoalStatus::Complete),
                    async { Ok::<(), anyhow::Error>(()) }
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get_thread_goal(id).await.unwrap().unwrap().status,
            crate::ThreadGoalStatus::Paused
        );
        assert!(
            store
                .mutate_thread_goal_conditionally(
                    id,
                    &selected.revision,
                    ConditionalGoalMutation::Clear,
                    async { Ok::<(), anyhow::Error>(()) }
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn conditional_goal_absence_tombstone_survives_create_clear_and_restart() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        let absent = store.snapshot_thread_goal(id).await.unwrap();
        store
            .replace_thread_goal(id, "temporary", crate::ThreadGoalStatus::Paused, None)
            .await
            .unwrap();
        store.delete_thread_goal(id).await.unwrap();
        let now = store.snapshot_thread_goal(id).await.unwrap();
        assert!(now.goal.is_none());
        assert_ne!(now.revision, absent.revision);
        let update = ConditionalGoalMutation::Set(GoalUpdate {
            objective: Some("new".into()),
            status: Some(crate::ThreadGoalStatus::Paused),
            token_budget: None,
            expected_goal_id: None,
        });
        assert!(
            store
                .mutate_thread_goal_conditionally(id, &absent.revision, update, async {
                    Ok::<(), anyhow::Error>(())
                })
                .await
                .unwrap()
                .is_none()
        );
        let reopened = StateRuntime::init(
            crate::SqliteConfig::new_for_testing(runtime.sqlite().home().abs()),
            "test-provider".into(),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .thread_goals()
                .snapshot_thread_goal(id)
                .await
                .unwrap()
                .revision,
            now.revision
        );
        let cleared = store
            .mutate_thread_goal_conditionally(
                id,
                &now.revision,
                ConditionalGoalMutation::Clear,
                async { Ok::<(), anyhow::Error>(()) },
            )
            .await
            .unwrap()
            .unwrap();
        assert!(cleared.before.is_none());
        assert_eq!(cleared.snapshot.revision, now.revision);
    }

    #[tokio::test]
    async fn conditional_goal_same_snapshot_has_one_winner() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        store
            .replace_thread_goal(id, "original", crate::ThreadGoalStatus::Paused, None)
            .await
            .unwrap();
        let snapshot = store.snapshot_thread_goal(id).await.unwrap();
        let (a, b) = tokio::join!(
            store.mutate_thread_goal_conditionally(
                id,
                &snapshot.revision,
                conditional_status(crate::ThreadGoalStatus::Active),
                async { Ok::<(), anyhow::Error>(()) }
            ),
            store.mutate_thread_goal_conditionally(
                id,
                &snapshot.revision,
                ConditionalGoalMutation::Clear,
                async { Ok::<(), anyhow::Error>(()) }
            )
        );
        assert_eq!(
            usize::from(a.unwrap().is_some()) + usize::from(b.unwrap().is_some()),
            1
        );
    }

    #[tokio::test]
    async fn conditional_goal_recording_failure_leaves_goal_and_revision_unchanged() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        store
            .replace_thread_goal(id, "original", crate::ThreadGoalStatus::Paused, None)
            .await
            .unwrap();
        let snapshot = store.snapshot_thread_goal(id).await.unwrap();
        assert!(
            store
                .mutate_thread_goal_conditionally(
                    id,
                    &snapshot.revision,
                    ConditionalGoalMutation::Clear,
                    async { Err::<(), _>(anyhow::anyhow!("history unavailable")) }
                )
                .await
                .is_err()
        );
        let after = store.snapshot_thread_goal(id).await.unwrap();
        assert_eq!(after.goal, snapshot.goal);
        assert_eq!(after.revision, snapshot.revision);
    }

    #[tokio::test]
    async fn conditional_goal_preserves_exhausted_budget() {
        let runtime = test_runtime().await;
        let id = test_thread_id();
        upsert_test_thread(&runtime, id).await;
        let store = runtime.thread_goals();
        let original = store
            .replace_thread_goal(id, "limited", crate::ThreadGoalStatus::Paused, Some(10))
            .await
            .unwrap();
        store
            .account_thread_goal_usage(
                id,
                4,
                10,
                GoalAccountingMode::ActiveOrStopped,
                Some(&original.goal_id),
            )
            .await
            .unwrap();
        let snapshot = store.snapshot_thread_goal(id).await.unwrap();
        let after = store
            .mutate_thread_goal_conditionally(
                id,
                &snapshot.revision,
                conditional_status(crate::ThreadGoalStatus::Active),
                async { Ok::<(), anyhow::Error>(()) },
            )
            .await
            .unwrap()
            .unwrap();
        let goal = after.snapshot.goal.unwrap();
        assert_eq!(goal.status, crate::ThreadGoalStatus::BudgetLimited);
        assert_eq!(goal.tokens_used, 10);
    }

    #[tokio::test]
    async fn replace_update_and_get_thread_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!(
            Some(goal.clone()),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("thread metadata should load")
            .expect("thread should exist");
        assert_eq!(metadata.preview.as_deref(), Some("hello"));

        let updated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: Some(Some(200_000)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::Paused,
            token_budget: Some(200_000),
            updated_at: updated.updated_at,
            ..goal.clone()
        };
        assert_eq!(expected, updated);

        let replaced = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "ship the new result",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!("ship the new result", replaced.objective);
        assert_eq!(crate::ThreadGoalStatus::Active, replaced.status);
        assert_eq!(None, replaced.token_budget);
        assert_eq!(0, replaced.tokens_used);
        assert_eq!(0, replaced.time_used_seconds);

        assert_eq!(
            Some(replaced),
            runtime
                .thread_goals()
                .delete_thread_goal(thread_id)
                .await
                .unwrap()
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .delete_thread_goal(thread_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn replace_thread_goal_applies_budget_limit_immediately() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let replaced = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(0),
            )
            .await
            .expect("goal replacement should succeed");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, replaced.status);
        assert_eq!(Some(0), replaced.token_budget);
        assert_eq!(0, replaced.tokens_used);
        assert_eq!(0, replaced.time_used_seconds);
    }

    #[tokio::test]
    async fn insert_thread_goal_does_not_replace_existing_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let inserted = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal insertion should succeed")
            .expect("goal should be inserted");

        let duplicate = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "replace the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(200_000),
            )
            .await
            .expect("duplicate insert should not fail");

        assert_eq!(None, duplicate);
        assert_eq!(
            Some(inserted),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn insert_thread_goal_applies_budget_limit_immediately() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let inserted = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(0),
            )
            .await
            .expect("goal insertion should succeed")
            .expect("goal should be inserted");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, inserted.status);
        assert_eq!(Some(0), inserted.token_budget);
        assert_eq!(0, inserted.tokens_used);
        assert_eq!(0, inserted.time_used_seconds);
    }

    #[tokio::test]
    async fn update_thread_goal_ignores_replaced_goal_version() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let original = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "old objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let replacement = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "new objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(10),
            )
            .await
            .expect("goal replacement should succeed");

        let stale_update = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(original.goal_id),
                },
            )
            .await
            .expect("goal update should succeed");

        assert_eq!(None, stale_update);
        assert_eq!(
            Some(replacement.clone()),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );

        let fresh_update = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(replacement.goal_id),
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("fresh update should match the replacement goal");
        assert_eq!(crate::ThreadGoalStatus::Complete, fresh_update.status);
    }

    #[tokio::test]
    async fn usage_accounting_ignores_replaced_goal_version() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let original = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "old objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let replacement = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "new objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(10),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                Some(original.goal_id.as_str()),
            )
            .await
            .expect("usage accounting should succeed");

        let GoalAccountingOutcome::Unchanged(Some(goal)) = outcome else {
            panic!("stale goal version should not be updated");
        };
        assert_ne!(replacement.goal_id, original.goal_id);
        assert_eq!(replacement.created_at, goal.created_at);
        assert_eq!("new objective", goal.objective);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn update_thread_goal_objective_preserves_usage_and_created_at() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "draft the report",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 12,
                /*token_delta*/ 30,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(accounted) = outcome else {
            panic!("active goal should account usage");
        };

        let updated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: Some("draft the report clearly".to_string()),
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: Some(Some(200)),
                    expected_goal_id: Some(accounted.goal_id.clone()),
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = crate::ThreadGoal {
            objective: "draft the report clearly".to_string(),
            status: crate::ThreadGoalStatus::Paused,
            token_budget: Some(200),
            updated_at: updated.updated_at,
            ..accounted
        };
        assert_eq!(expected, updated);
    }

    #[tokio::test]
    async fn concurrent_partial_updates_preserve_independent_fields() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");

        let status_update = runtime.thread_goals().update_thread_goal(
            thread_id,
            GoalUpdate {
                objective: None,
                status: Some(crate::ThreadGoalStatus::Paused),
                token_budget: None,
                expected_goal_id: None,
            },
        );
        let budget_update = runtime.thread_goals().update_thread_goal(
            thread_id,
            GoalUpdate {
                objective: None,
                status: None,
                token_budget: Some(Some(200_000)),
                expected_goal_id: None,
            },
        );
        let (status_update, budget_update) = tokio::join!(status_update, budget_update);
        status_update.expect("status update should succeed");
        budget_update.expect("budget update should succeed");

        let goal = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .expect("goal read should succeed")
            .expect("goal should exist");
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(Some(200_000), goal.token_budget);
    }

    #[tokio::test]
    async fn pause_active_thread_goal_does_not_clobber_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");

        let paused = runtime
            .thread_goals()
            .pause_active_thread_goal(thread_id)
            .await
            .expect("active pause should succeed")
            .expect("active goal should be paused");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::Paused,
            updated_at: paused.updated_at,
            ..goal
        };
        assert_eq!(expected, paused);

        let complete = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let pause_result = runtime
            .thread_goals()
            .pause_active_thread_goal(thread_id)
            .await
            .expect("terminal pause attempt should succeed");
        assert_eq!(None, pause_result);
        assert_eq!(
            Some(complete),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );
    }

    #[tokio::test]
    async fn usage_limit_active_thread_goal_updates_active_or_budget_limited_goals() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");

        let usage_limited = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("active goal should become usage limited");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..goal
        };
        assert_eq!(expected, usage_limited);

        let second_update = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("repeated usage limiting should succeed");
        assert_eq!(None, second_update);

        let budget_limited = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "keep the usage failure visible",
                crate::ThreadGoalStatus::BudgetLimited,
                /*token_budget*/ Some(1),
            )
            .await
            .expect("goal replacement should succeed");
        let usage_limited = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("budget-limited goal should become usage limited");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, usage_limited);
    }

    #[tokio::test]
    async fn usage_accounting_updates_active_goals_and_accounts_budget_limited_in_flight_usage() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 7,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("active goal should be updated");
        };
        assert_eq!(crate::ThreadGoalStatus::Active, goal.status);
        assert_eq!(5, goal.tokens_used);
        assert_eq!(7, goal.time_used_seconds);

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 3,
                /*token_delta*/ 15,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget crossing should update the goal");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(20, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget-limited goal should still account in-flight active usage");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(15, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn active_status_only_usage_accounting_does_not_update_budget_limited_goals() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay stopped",
                crate::ThreadGoalStatus::BudgetLimited,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveStatusOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = outcome else {
            panic!("budget-limited goal should not be updated");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn stopped_usage_accounting_promotes_paused_goal_over_budget() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stop before overrun",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                crate::GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 3,
                /*token_delta*/ 25,
                GoalAccountingMode::ActiveOrStopped,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("stopped goal should account final usage");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(3, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn budget_updates_immediately_stop_active_goals_already_over_budget() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let lowered = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: None,
                    token_budget: Some(Some(40)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, lowered.status);
        assert_eq!(Some(40), lowered.token_budget);
        assert_eq!(50, lowered.tokens_used);
    }

    #[tokio::test]
    async fn activating_goal_already_over_budget_keeps_it_budget_limited() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let reactivated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: Some("stay within budget, with clearer wording".to_string()),
                    status: Some(crate::ThreadGoalStatus::Active),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, reactivated.status);
        assert_eq!(
            "stay within budget, with clearer wording",
            reactivated.objective
        );
        assert_eq!(Some(40), reactivated.token_budget);
        assert_eq!(50, reactivated.tokens_used);
    }

    #[tokio::test]
    async fn pausing_budget_limited_goal_preserves_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let paused = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, paused.status);
        assert_eq!(Some(40), paused.token_budget);
        assert_eq!(50, paused.tokens_used);
    }

    #[tokio::test]
    async fn blocking_budget_limited_goal_preserves_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(budget_limited) = outcome else {
            panic!("budget crossing should update the goal");
        };

        let blocked = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Blocked),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        let expected = crate::ThreadGoal {
            updated_at: blocked.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, blocked);
    }

    #[tokio::test]
    async fn usage_accounting_can_finalize_completed_goal_for_completing_turn() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "finish the report",
                crate::ThreadGoalStatus::Complete,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let active_only = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = active_only else {
            panic!("completed goal should not be updated by active-only accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Complete, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);

        let completing_turn = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOrComplete,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = completing_turn else {
            panic!("completed goal should be updated for final accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Complete, goal.status);
        assert_eq!(200, goal.tokens_used);
        assert_eq!(30, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn usage_accounting_can_finalize_stopped_goal_for_in_flight_turn() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "finish the report",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        let active_only = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = active_only else {
            panic!("paused goal should not be updated by active-only accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);

        let in_flight_turn = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOrStopped,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = in_flight_turn else {
            panic!("stopped goal should be updated for in-flight accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(200, goal.tokens_used);
        assert_eq!(30, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn usage_accounting_adds_concurrent_token_deltas() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "count every token",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let first = runtime.thread_goals().account_thread_goal_usage(
            thread_id,
            /*time_delta_seconds*/ 4,
            /*token_delta*/ 40,
            GoalAccountingMode::ActiveOnly,
            /*expected_goal_id*/ None,
        );
        let second = runtime.thread_goals().account_thread_goal_usage(
            thread_id,
            /*time_delta_seconds*/ 6,
            /*token_delta*/ 60,
            GoalAccountingMode::ActiveOnly,
            /*expected_goal_id*/ None,
        );
        let (first, second) = tokio::join!(first, second);
        first.expect("first usage accounting should succeed");
        second.expect("second usage accounting should succeed");

        let goal = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .expect("goal read should succeed")
            .expect("goal should exist");
        assert_eq!(100, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn deleting_thread_deletes_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "clean up with the thread",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");

        runtime
            .delete_thread(thread_id)
            .await
            .expect("thread deletion should succeed");

        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );
    }
}
