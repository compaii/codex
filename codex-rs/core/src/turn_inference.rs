//! Native turn-local allocation. Never saved as a reusable thread setting.
use crate::client::ModelClientSession;
use crate::session::turn_context::TurnContext;
use codex_api::InferenceAllowance;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use codex_protocol::turn_input::TurnInferenceAllowance;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) fn prepare(spec: &TurnInferenceAllowance) -> Result<Arc<InferenceAllowance>> {
    let remaining_ms = spec
        .deadline_unix_ms
        .checked_sub(chrono::Utc::now().timestamp_millis())
        .filter(|remaining| (1..=600_000).contains(remaining))
        .ok_or_else(|| {
            CodexErr::InvalidRequest(
                "inference deadline must be future and within600seconds".into(),
            )
        })?;
    if spec.max_provider_attempts == 0 || !(1..=64 * 1024 * 1024).contains(&spec.max_request_bytes)
    {
        return Err(CodexErr::InvalidRequest(
            "inference limits must be positive and request bytes within64MiB".into(),
        ));
    }
    let duration = Duration::from_millis(
        u64::try_from(remaining_ms)
            .map_err(|_| CodexErr::InvalidRequest("invalid inference deadline".into()))?,
    );
    InferenceAllowance::new(
        spec.max_provider_attempts,
        spec.max_request_bytes as usize,
        Instant::now() + duration,
    )
    .map(Arc::new)
    .map_err(|err| CodexErr::InvalidRequest(err.to_string()))
}

pub(crate) fn allowance(ctx: &TurnContext) -> Option<Arc<InferenceAllowance>> {
    ctx.extension_data
        .get::<Arc<InferenceAllowance>>()
        .map(|value| Arc::clone(value.as_ref()))
}

/// Agent starts need an owner-reserved child admission, not a copied root cap.
/// Until that admission exists, refuse before loading or sending to a recipient.
pub(crate) fn require_agent_admission(
    ctx: &TurnContext,
) -> std::result::Result<(), codex_tools::FunctionCallError> {
    if allowance(ctx).is_some() {
        return Err(codex_tools::FunctionCallError::RespondToModel(
            "Bounded attention requires separately reserved agent admission; no agent work was submitted."
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn bind(client: ModelClientSession, ctx: &TurnContext) -> ModelClientSession {
    match allowance(ctx) {
        Some(allowance) => client.with_inference_allowance(allowance),
        None => client,
    }
}

/// Ordinary task cancellation drops this guard too: no later retry may send.
pub(crate) struct CloseOnExit(pub(crate) Arc<InferenceAllowance>);
impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn refuses_expired_zero_or_unbounded_admission_before_any_attempt() {
        let mut spec = TurnInferenceAllowance {
            max_provider_attempts: 2,
            max_request_bytes: 128,
            deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 30_000,
        };
        assert_eq!(prepare(&spec).unwrap().attempted().unwrap(), 0);
        spec.max_provider_attempts = 0;
        assert!(prepare(&spec).is_err());
        spec.max_provider_attempts = 2;
        spec.max_request_bytes = 64 * 1024 * 1024 + 1;
        assert!(prepare(&spec).is_err());
        spec.max_request_bytes = 128;
        spec.deadline_unix_ms = chrono::Utc::now().timestamp_millis() - 1;
        assert!(prepare(&spec).is_err());
        spec.deadline_unix_ms = chrono::Utc::now().timestamp_millis() + 601_000;
        assert!(prepare(&spec).is_err());
    }
}
