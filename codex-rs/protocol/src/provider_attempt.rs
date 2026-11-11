//! Host-only, nonserialized provider reservation callbacks for a single turn.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::Instant;

/// Host-only claim for one physical send. No provider credentials, prompt or
/// model-visible metadata are carried here. The gate must already bind the
/// exact causal account, route/model and qualified conservative cost contract.
/// The complete allowance byte ceiling is the upper bound, never a WS delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAttempt {
    pub id: uuid::Uuid,
    pub ordinal: u32,
    pub max_request_bytes: usize,
    pub deadline: Instant,
}

pub type ProviderAttemptReservation<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Provider completion observed on the exact physical attempt, before model
/// interceptors. This is usage evidence, not a signed financial settlement.
#[derive(Debug)]
pub struct ProviderAttemptUsage {
    pub attempt_id: uuid::Uuid,
    pub ordinal: u32,
    pub response_id: String,
    pub model: Option<String>,
    pub token_usage: Option<crate::protocol::TokenUsage>,
    pub usage_metadata: Option<crate::ResponseUsageMetadata>,
}

pub type ProviderAttemptObservation<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Trusted owner adapter, not a model callback or another budget journal.
/// Return true only after durably reserving this exact attempt. A lost response
/// or dropped future retains the reservation; it never authorizes replay or
/// refund. The host owns authenticated usage settlement and real route isolation.
pub trait ProviderAttemptGate: Send + Sync {
    fn reserve(&self, attempt: ProviderAttempt) -> ProviderAttemptReservation<'_>;

    /// Optional owner-selected output ceiling for every physical request.
    /// It is not a billing receipt or a provider/account selection.
    fn max_output_tokens(&self) -> Option<u32> {
        None
    }

    /// Existing owners without an accounting observer retain maximum exposure.
    /// Missing usage or an unobserved callback never implies zero consumption.
    fn observe_usage(&self, _usage: ProviderAttemptUsage) -> ProviderAttemptObservation<'_> {
        Box::pin(async {})
    }
}

#[derive(Clone)]
pub struct ProviderAttemptGateSelection(pub Arc<dyn ProviderAttemptGate>);

impl std::fmt::Debug for ProviderAttemptGateSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Owner adapters may contain private accounting/custody references.
        formatter.write_str("ProviderAttemptGate { .. }")
    }
}
