//! Host-only, nonserialized provider reservation callbacks for a single turn.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::Instant;

/// Host-only claim for one physical send. No provider credentials, prompt or
/// model-visible metadata are carried here. The gate must already bind the
/// exact causal account, route/model and qualified conservative cost contract.
/// The complete allowance byte ceiling is the upper bound, never a WS delta.
#[derive(Debug)]
pub struct ProviderAttempt {
    pub id: uuid::Uuid,
    pub ordinal: u32,
    pub max_request_bytes: usize,
    pub deadline: Instant,
}

pub type ProviderAttemptReservation<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Trusted owner adapter, not a model callback or another budget journal.
/// Return true only after durably reserving this exact attempt. A lost response
/// or dropped future retains the reservation; it never authorizes replay or
/// refund. The host owns authenticated usage settlement and real route isolation.
pub trait ProviderAttemptGate: Send + Sync {
    fn reserve(&self, attempt: ProviderAttempt) -> ProviderAttemptReservation<'_>;
}

#[derive(Clone)]
pub struct ProviderAttemptGateSelection(pub Arc<dyn ProviderAttemptGate>);

impl std::fmt::Debug for ProviderAttemptGateSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Owner adapters may contain private accounting/custody references.
        formatter.write_str("ProviderAttemptGate { .. }")
    }
}
