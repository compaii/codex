//! One owner connection's durable accounting gate on an existing native turn.
//! No account balances, signing keys, model tools or provider credentials live here.

use std::sync::Arc;

use codex_api::ProviderAttempt;
use codex_api::ProviderAttemptGate;
use codex_api::ProviderAttemptReservation;
use codex_app_server_protocol::ProviderAttemptReservationParams;
use codex_app_server_protocol::ProviderAttemptReservationResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerRequestPayload;
use codex_protocol::ThreadId;

use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;

pub(crate) struct OwnerProviderReservation {
    pub(crate) outgoing: Arc<OutgoingMessageSender>,
    pub(crate) owner: ConnectionId,
    pub(crate) thread_id: ThreadId,
    pub(crate) cycle_id: String,
    pub(crate) deadline_unix_ms: i64,
}

// Allocate the RPC identity before registering/sending. Dropping while the
// outgoing channel is backpressured must also retire the exact callback.
struct PendingReservation {
    outgoing: Arc<OutgoingMessageSender>,
    request_id: RequestId,
}

impl Drop for PendingReservation {
    fn drop(&mut self) {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let outgoing = self.outgoing.clone();
            let request_id = self.request_id.clone();
            runtime.spawn(async move {
                outgoing.cancel_request(&request_id).await;
            });
        }
    }
}

impl ProviderAttemptGate for OwnerProviderReservation {
    fn reserve(&self, attempt: ProviderAttempt) -> ProviderAttemptReservation<'_> {
        Box::pin(async move {
            let Ok(max_request_bytes) = u32::try_from(attempt.max_request_bytes) else {
                return false;
            };
            let request_id = self.outgoing.next_request_id();
            let _pending = PendingReservation {
                outgoing: self.outgoing.clone(),
                request_id: request_id.clone(),
            };
            let attempt_id = attempt.id.to_string();
            let request = ServerRequestPayload::ProviderAttemptReservation(
                ProviderAttemptReservationParams {
                    thread_id: self.thread_id.to_string(),
                    cycle_id: self.cycle_id.clone(),
                    attempt_id: attempt_id.clone(),
                    ordinal: attempt.ordinal,
                    max_request_bytes,
                    deadline_unix_ms: self.deadline_unix_ms,
                },
            );
            let reservation = async {
                let (_, response) = self
                    .outgoing
                    .send_request_with_id(
                        request_id,
                        Some(&[self.owner]),
                        request,
                        Some(self.thread_id),
                    )
                    .await;
                let Ok(Ok(value)) = response.await else {
                    return false;
                };
                let Ok(response) =
                    serde_json::from_value::<ProviderAttemptReservationResponse>(value)
                else {
                    return false;
                };
                response.authorized
                    && response.cycle_id == self.cycle_id
                    && response.attempt_id == attempt_id
            };
            matches!(
                tokio::time::timeout_at(attempt.deadline, reservation).await,
                Ok(true)
            )
        })
    }
}
