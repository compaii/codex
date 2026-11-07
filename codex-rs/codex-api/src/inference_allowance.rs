//! A shared, finite allowance for one native execution, not an account grant.
//!
//! The owner must durably reserve the complete allocation before constructing
//! this value. Sharing an Arc conserves attempts across retries/transports; a
//! fresh value is NOT recovery of an accepted or uncertain execution.
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
        };
        value.remaining_time()?;
        Ok(value)
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
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
        Ok(remaining)
    }

    /// Fence future attempts. This does not cancel an already-active provider
    /// response; the native execution owner must use its existing cancellation path.
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
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
