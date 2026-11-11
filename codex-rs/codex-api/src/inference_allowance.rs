//! A shared, finite allowance for one native execution, not an account grant.
//!
//! The owner must durably reserve the complete allocation before constructing
//! this value. Sharing an Arc conserves attempts across retries/transports; a
//! fresh value is NOT recovery of an accepted or uncertain execution.
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InferenceAllowanceError {
    #[error("native inference allowance requires positive finite limits")]
    InvalidLimits,
    #[error("native inference allowance is closed")]
    Closed,
    #[error("native inference allowance deadline expired")]
    Deadline,
    #[error("native inference allowance provider attempts exhausted")]
    Attempts,
    #[error("complete model request exceeds native inference byte allowance")]
    InputBytes,
    #[error("native provider attempt reservation unavailable")]
    Reservation,
}

use codex_protocol::provider_attempt::ProviderAttemptGateSelection as SelectedProviderAttemptGate;
pub use codex_protocol::provider_attempt::{
    ProviderAttempt, ProviderAttemptGate, ProviderAttemptObservation, ProviderAttemptReservation,
    ProviderAttemptUsage,
};

/// Witness returned by the same boundary that authorizes the physical send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedProviderAttempt {
    pub attempt: ProviderAttempt,
    pub remaining: Duration,
}

pub(crate) struct AttemptObserver {
    pub allowance: Arc<InferenceAllowance>,
    pub attempt: ProviderAttempt,
    pub initial_model: Option<String>,
}
impl AttemptObserver {
    pub fn new(allowance: Arc<InferenceAllowance>, attempt: ProviderAttempt) -> Option<Self> {
        allowance.provider_attempt_gate.as_ref()?;
        Some(Self {
            allowance,
            attempt,
            initial_model: None,
        })
    }
    pub async fn observe_incomplete(
        &self,
        error: &codex_protocol::error::ResponseIncompleteError,
        model: Option<String>,
        usage_metadata: Option<codex_protocol::ResponseUsageMetadata>,
    ) {
        // A failed response may still consume tokens. Keep its existing error
        // semantics; this owner-only observation is never a completion/refund.
        if let Some(id) = &error.response_id
            && !id.is_empty()
            && id.len() <= 200
            && let Some(gate) = &self.allowance.provider_attempt_gate
        {
            let usage = ProviderAttemptUsage {
                attempt_id: self.attempt.id,
                ordinal: self.attempt.ordinal,
                response_id: id.clone(),
                model: model.or_else(|| self.initial_model.clone()),
                token_usage: error.token_usage.clone(),
                usage_metadata,
            };
            let _ =
                tokio::time::timeout_at(self.attempt.deadline, gate.0.observe_usage(usage)).await;
        }
    }
    pub async fn observe(&self, event: &crate::common::ResponseEvent, model: Option<String>) {
        if let crate::common::ResponseEvent::Completed {
            response_id,
            token_usage,
            usage_metadata,
            ..
        } = event
            && let Some(gate) = &self.allowance.provider_attempt_gate
        {
            let usage = ProviderAttemptUsage {
                attempt_id: self.attempt.id,
                ordinal: self.attempt.ordinal,
                response_id: response_id.clone(),
                model: model.or_else(|| self.initial_model.clone()),
                token_usage: token_usage.clone(),
                usage_metadata: usage_metadata.clone(),
            };
            // Reporting cannot renew the original physical-attempt horizon or
            // hold the provider stream indefinitely. Loss conserves exposure.
            let _ =
                tokio::time::timeout_at(self.attempt.deadline, gate.0.observe_usage(usage)).await;
        }
    }
}

/// Preserve typed/raw nested payloads while adding only the owner field.
#[derive(serde::Serialize)]
pub(crate) struct OutputBoundRequest<'a, T: ?Sized> {
    #[serde(flatten)]
    request: &'a T,
    max_output_tokens: u32,
}

#[derive(Debug, Default)]
struct State {
    attempted: u32,
    closed: bool,
}

#[derive(Debug)]
pub struct InferenceAllowance {
    max_attempts: u32,
    max_request_bytes: usize,
    deadline: Instant,
    state: Mutex<State>,
    closed: tokio_util::sync::CancellationToken,
    provider_attempt_gate: Option<SelectedProviderAttemptGate>,
}

impl InferenceAllowance {
    pub fn new(
        max_attempts: u32,
        max_request_bytes: usize,
        deadline: Instant,
    ) -> Result<Self, InferenceAllowanceError> {
        if max_attempts == 0 || max_request_bytes == 0 {
            return Err(InferenceAllowanceError::InvalidLimits);
        }
        let value = Self {
            max_attempts,
            max_request_bytes,
            deadline,
            state: Mutex::new(State::default()),
            closed: tokio_util::sync::CancellationToken::new(),
            provider_attempt_gate: None,
        };
        value.remaining_time()?;
        Ok(value)
    }

    /// Select an owner-held gate before sharing this new execution allowance.
    /// This neither creates funding nor rehydrates a spent/uncertain allowance.
    pub fn with_provider_attempt_gate(mut self, gate: Arc<dyn ProviderAttemptGate>) -> Self {
        self.provider_attempt_gate = Some(SelectedProviderAttemptGate(gate));
        self
    }

    /// Reserve at the actual physical-send boundary under the original absolute
    /// deadline. Refusal/unknown closes this allowance before any provider bytes;
    /// auth retries, WS writes and HTTP fallback cannot bypass the refusal.
    pub async fn claim_authorized_attempt(
        &self,
    ) -> Result<AuthorizedProviderAttempt, InferenceAllowanceError> {
        let (ordinal, _) = self.claim_attempt_number()?;
        let attempt = ProviderAttempt {
            id: uuid::Uuid::new_v4(),
            ordinal,
            max_request_bytes: self.max_request_bytes,
            deadline: self.deadline,
        };
        if let Some(gate) = &self.provider_attempt_gate
            && !matches!(
                tokio::time::timeout_at(self.deadline, gate.0.reserve(attempt.clone())).await,
                Ok(true)
            )
        {
            self.close();
            return Err(InferenceAllowanceError::Reservation);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| InferenceAllowanceError::Closed)?;
        if state.closed {
            return Err(InferenceAllowanceError::Closed);
        }
        Ok(AuthorizedProviderAttempt {
            attempt,
            remaining: self.remaining_time()?,
        })
    }

    /// Check the execution lifetime without consuming or requiring another provider
    /// attempt. Tools may finish after the final permitted response.
    pub fn check_execution_open(&self) -> Result<(), InferenceAllowanceError> {
        if self.closed.is_cancelled() {
            return Err(InferenceAllowanceError::Closed);
        }
        self.remaining_time().map(|_| ())
    }

    /// Wait for the same execution's explicit close or absolute deadline. This
    /// creates no renewal, retry or independent background attention.
    pub async fn execution_finished(&self) {
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => {},
            _ = tokio::time::sleep_until(self.deadline) => {},
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn max_output_tokens(&self) -> Option<u32> {
        self.provider_attempt_gate
            .as_ref()
            .and_then(|gate| gate.0.max_output_tokens())
    }

    pub(crate) fn output_bound_request<'a, T: ?Sized>(
        &self,
        request: &'a T,
    ) -> Result<Option<OutputBoundRequest<'a, T>>, InferenceAllowanceError> {
        match self.max_output_tokens() {
            None => Ok(None),
            Some(0) => Err(InferenceAllowanceError::InvalidLimits),
            Some(max_output_tokens) => Ok(Some(OutputBoundRequest {
                request,
                max_output_tokens,
            })),
        }
    }

    /// Apply only the selected owner's ceiling, preserving a tighter caller
    /// ceiling. Must run before compression, byte validation and physical sends.
    pub(crate) fn bound_output(
        &self,
        body: &mut serde_json::Value,
    ) -> Result<(), InferenceAllowanceError> {
        let Some(limit) = self.max_output_tokens() else {
            return Ok(());
        };
        if limit == 0 {
            return Err(InferenceAllowanceError::InvalidLimits);
        }
        let object = body
            .as_object_mut()
            .ok_or(InferenceAllowanceError::InvalidLimits)?;
        let selected = match object.get("max_output_tokens") {
            None | Some(serde_json::Value::Null) => u64::from(limit),
            Some(value) => value
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or(InferenceAllowanceError::InvalidLimits)?
                .min(u64::from(limit)),
        };
        object.insert("max_output_tokens".into(), serde_json::json!(selected));
        Ok(())
    }

    pub fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    /// Validate the complete logical request BEFORE compression or incremental
    /// WebSocket encoding. This does not consume an attempt.
    pub fn validate_request(&self, bytes: usize) -> Result<(), InferenceAllowanceError> {
        let state = self
            .state
            .lock()
            .map_err(|_| InferenceAllowanceError::Closed)?;
        if state.closed {
            return Err(InferenceAllowanceError::Closed);
        }
        self.remaining_time()?;
        if bytes > self.max_request_bytes {
            return Err(InferenceAllowanceError::InputBytes);
        }
        if state.attempted >= self.max_attempts {
            return Err(InferenceAllowanceError::Attempts);
        }
        Ok(())
    }

    /// Call immediately before each physical provider attempt. Even a lost
    /// acknowledgment consumes its attempt; there is deliberately no refund.
    pub fn claim_attempt(&self) -> Result<Duration, InferenceAllowanceError> {
        self.claim_attempt_number().map(|(_, remaining)| remaining)
    }

    fn claim_attempt_number(&self) -> Result<(u32, Duration), InferenceAllowanceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| InferenceAllowanceError::Closed)?;
        if state.closed {
            return Err(InferenceAllowanceError::Closed);
        }
        let remaining = self.remaining_time()?;
        if state.attempted >= self.max_attempts {
            return Err(InferenceAllowanceError::Attempts);
        }
        state.attempted += 1;
        Ok((state.attempted, remaining))
    }

    /// Fence future attempts. This does not cancel an already-active provider
    /// response; the native execution owner must use its existing cancellation path.
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
        self.closed.cancel();
    }

    pub fn attempted(&self) -> Result<u32, InferenceAllowanceError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| InferenceAllowanceError::Closed)?
            .attempted)
    }

    fn remaining_time(&self) -> Result<Duration, InferenceAllowanceError> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(InferenceAllowanceError::Deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    #[allow(clippy::needless_collect)] // Spawn every contender before joining any thread.
    fn concurrent_claims_share_one_allowance_and_close_never_refills() {
        let allowance = Arc::new(
            InferenceAllowance::new(2, 128, Instant::now() + Duration::from_secs(60)).unwrap(),
        );
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let allowance = Arc::clone(&allowance);
                std::thread::spawn(move || allowance.claim_attempt().is_ok())
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| u32::from(worker.join().unwrap()))
                .sum::<u32>(),
            2
        );
        assert_eq!(allowance.attempted().unwrap(), 2);
        allowance.close();
        assert_eq!(
            allowance.claim_attempt(),
            Err(InferenceAllowanceError::Closed)
        );
        assert_eq!(allowance.attempted().unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn execution_lifetime_observes_close_deadline_and_final_attempt_without_refill() {
        let allowance =
            InferenceAllowance::new(1, 128, Instant::now() + Duration::from_secs(5)).unwrap();
        allowance.claim_attempt().unwrap();
        assert!(allowance.check_execution_open().is_ok());
        let finished = allowance.execution_finished();
        tokio::pin!(finished);
        assert!(futures::poll!(&mut finished).is_pending());
        allowance.close();
        finished.await;
        allowance.execution_finished().await; // A later subscriber cannot miss close.
        assert_eq!(
            allowance.check_execution_open(),
            Err(InferenceAllowanceError::Closed)
        );
        assert_eq!(allowance.attempted().unwrap(), 1);

        let deadline =
            InferenceAllowance::new(1, 128, Instant::now() + Duration::from_secs(5)).unwrap();
        deadline.execution_finished().await;
        assert_eq!(
            deadline.check_execution_open(),
            Err(InferenceAllowanceError::Deadline)
        );
        assert_eq!(deadline.attempted().unwrap(), 0);
    }

    #[derive(Debug)]
    struct StalledGate;
    impl ProviderAttemptGate for StalledGate {
        fn reserve(&self, _: ProviderAttempt) -> ProviderAttemptReservation<'_> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Debug)]
    struct PrivateGate(&'static str);
    impl ProviderAttemptGate for PrivateGate {
        fn reserve(&self, _: ProviderAttempt) -> ProviderAttemptReservation<'_> {
            Box::pin(async move { !self.0.is_empty() })
        }
    }

    #[test]
    fn owner_gate_context_is_not_exposed_by_allowance_debug() {
        let allowance = InferenceAllowance::new(2, 128, Instant::now() + Duration::from_secs(5))
            .unwrap()
            .with_provider_attempt_gate(Arc::new(PrivateGate("owner-context-canary")));
        let debug = format!("{allowance:?}");
        assert!(!debug.contains("owner-context-canary"));
        assert!(debug.contains("ProviderAttemptGate { .. }"));
    }

    #[tokio::test(start_paused = true)]
    async fn lost_reservation_response_closes_without_refill() {
        let allowance = InferenceAllowance::new(8, 128, Instant::now() + Duration::from_secs(5))
            .unwrap()
            .with_provider_attempt_gate(Arc::new(StalledGate));
        assert_eq!(
            allowance.claim_authorized_attempt().await,
            Err(InferenceAllowanceError::Reservation)
        );
        assert_eq!(allowance.attempted().unwrap(), 1);
        assert_eq!(
            allowance.claim_authorized_attempt().await,
            Err(InferenceAllowanceError::Closed)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn input_and_absolute_deadline_refuse_without_spending() {
        let allowance =
            InferenceAllowance::new(2, 128, Instant::now() + Duration::from_secs(5)).unwrap();
        assert_eq!(
            allowance.validate_request(129),
            Err(InferenceAllowanceError::InputBytes)
        );
        assert_eq!(allowance.attempted().unwrap(), 0);
        allowance.claim_attempt().unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            allowance.validate_request(1),
            Err(InferenceAllowanceError::Deadline)
        );
        assert_eq!(
            allowance.claim_attempt(),
            Err(InferenceAllowanceError::Deadline)
        );
        assert_eq!(allowance.attempted().unwrap(), 1);
    }
}
