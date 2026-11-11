use super::*;
use crate::provider_reservation::OwnerProviderReservation;
use codex_api::ProviderAttempt;
use codex_api::ProviderAttemptGate;
use codex_app_server_protocol::ProviderAttemptReservationParams;
use serde_json::json;
use std::time::Duration;
use tokio::time::Instant;

fn gate(outgoing: Arc<OutgoingMessageSender>, thread_id: ThreadId) -> OwnerProviderReservation {
    OwnerProviderReservation {
        outgoing,
        owner: ConnectionId(1),
        thread_id,
        cycle_id: uuid::Uuid::new_v4().to_string(),
        deadline_unix_ms: 123456789,
    }
}

fn attempt() -> ProviderAttempt {
    ProviderAttempt {
        id: uuid::Uuid::new_v4(),
        ordinal: 2,
        max_request_bytes: 8192,
        deadline: Instant::now() + Duration::from_secs(3),
    }
}

#[tokio::test]
async fn reservation_only_accepts_exact_owner_and_exact_journal_operation() {
    for wrong_identity in [false, true] {
        let (sender, mut messages) = mpsc::channel(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            sender,
            AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let gate = gate(outgoing.clone(), thread_id);
        let attempt = attempt();
        let attempt_id = attempt.id.to_string();
        let cycle_id = gate.cycle_id.clone();
        let task = tokio::spawn(async move { gate.reserve(attempt).await });
        let Some(OutgoingEnvelope::ToConnection {
            connection_id: ConnectionId(1),
            message:
                OutgoingMessage::Request(ServerRequest::ProviderAttemptReservation {
                    request_id,
                    params,
                }),
            ..
        }) = messages.recv().await
        else {
            panic!("reservation not routed to owner");
        };
        assert_eq!(params.thread_id, thread_id.to_string());
        assert_eq!(params.cycle_id, cycle_id);
        assert_eq!(params.attempt_id, attempt_id);
        assert_eq!(params.ordinal, 2);
        assert_eq!(params.max_request_bytes, 8192);
        let ack = json!({"cycleId": if wrong_identity { "other" } else { &cycle_id }, "attemptId": attempt_id, "authorized": true});
        outgoing
            .notify_client_response(ConnectionId(2), request_id.clone(), ack.clone())
            .await;
        outgoing
            .notify_client_error(
                ConnectionId(2),
                request_id.clone(),
                internal_error("not owner"),
            )
            .await;
        assert!(!task.is_finished());
        assert!(
            outgoing
                .pending_requests_for_thread(thread_id)
                .await
                .is_empty()
        );
        outgoing
            .replay_requests_to_connection_for_thread(ConnectionId(2), thread_id)
            .await;
        assert!(messages.try_recv().is_err());
        outgoing
            .notify_client_response(ConnectionId(1), request_id, ack)
            .await;
        assert_eq!(task.await.unwrap(), !wrong_identity);
        assert!(outgoing.request_id_to_callback.lock().await.is_empty());
    }
}

#[tokio::test]
async fn owner_disconnect_and_reservation_deadline_refuse_without_replay() {
    for disconnect in [false, true] {
        let (sender, mut messages) = mpsc::channel(1);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            sender,
            AnalyticsEventsClient::disabled(),
        ));
        let gate = gate(outgoing.clone(), ThreadId::new());
        let mut attempt = attempt();
        attempt.deadline = Instant::now() + Duration::from_millis(50);
        let task = tokio::spawn(async move { gate.reserve(attempt).await });
        messages.recv().await.unwrap();
        if disconnect {
            outgoing.connection_closed(ConnectionId(1)).await;
        }
        assert!(!task.await.unwrap());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !outgoing.request_id_to_callback.lock().await.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(messages.try_recv().is_err());
    }
}

#[tokio::test]
async fn cancelled_backpressured_send_retires_its_callback() {
    let (sender, _messages) = mpsc::channel(1);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        sender,
        AnalyticsEventsClient::disabled(),
    ));
    let payload =
        ServerRequestPayload::ProviderAttemptReservation(ProviderAttemptReservationParams {
            thread_id: ThreadId::new().to_string(),
            cycle_id: "retained".into(),
            attempt_id: "retained".into(),
            ordinal: 1,
            max_request_bytes: 1,
            deadline_unix_ms: 1,
        });
    let (original, _response) = outgoing
        .send_request_to_connections(Some(&[ConnectionId(1)]), payload.clone(), None)
        .await;
    let gate = gate(outgoing.clone(), ThreadId::new());
    let task = tokio::spawn(async move { gate.reserve(attempt()).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while outgoing.request_id_to_callback.lock().await.len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while outgoing.request_id_to_callback.lock().await.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    outgoing.cancel_request(&original).await;
    for recipients in [None, Some(&[ConnectionId(1), ConnectionId(2)][..])] {
        let (_, response) = outgoing
            .send_request_to_connections(recipients, payload.clone(), None)
            .await;
        assert!(response.await.is_err());
    }
    assert!(outgoing.request_id_to_callback.lock().await.is_empty());
}
