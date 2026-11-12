#![allow(clippy::expect_used)]
//! Actual loopback HTTP/retry boundaries; no real provider, model or credentials.
use codex_api::ApiError;
use codex_api::AuthProvider;
use codex_api::Compression;
use codex_api::InferenceAllowance;
use codex_api::Provider;
use codex_api::ProviderAttempt;
use codex_api::ProviderAttemptGate;
use codex_api::ProviderAttemptObservation;
use codex_api::ProviderAttemptReservation;
use codex_api::ProviderAttemptUsage;
use codex_api::ReqwestTransport;
use codex_api::ResponseCreateWsRequest;
use codex_api::ResponseEvent;
use codex_api::ResponsesClient;
use codex_api::ResponsesWebsocketClient;
use codex_api::ResponsesWebsocketConnection;
use codex_api::ResponsesWsRequest;
use codex_api::RetryConfig;
use codex_http_client::HttpClientBuilder;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderMap;
use serde_json::json;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::extensions::ExtensionsConfig;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

struct NoAuth;
impl AuthProvider for NoAuth {
    fn add_auth_headers(&self, _: &mut HeaderMap) -> Result<(), codex_api::AuthError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ReservationGate {
    permitted: u32,
    claims: Mutex<Vec<(uuid::Uuid, u32, usize)>>,
    usage: Mutex<Vec<ProviderAttemptUsage>>,
    hang_observer: bool,
    output_limit: Option<u32>,
    observed: tokio::sync::Notify,
}

impl ProviderAttemptGate for ReservationGate {
    fn max_output_tokens(&self) -> Option<u32> {
        self.output_limit
    }
    fn reserve(&self, attempt: ProviderAttempt) -> ProviderAttemptReservation<'_> {
        self.claims.lock().expect("gate fixture").push((
            attempt.id,
            attempt.ordinal,
            attempt.max_request_bytes,
        ));
        Box::pin(async move { attempt.ordinal <= self.permitted })
    }
    fn observe_usage(&self, usage: ProviderAttemptUsage) -> ProviderAttemptObservation<'_> {
        self.usage.lock().expect("gate usage fixture").push(usage);
        self.observed.notify_one();
        Box::pin(async move {
            if self.hang_observer {
                std::future::pending::<()>().await;
            }
        })
    }
}

fn client(
    server: &MockServer,
    allowance: Arc<InferenceAllowance>,
) -> ResponsesClient<ReqwestTransport> {
    let transport = ReqwestTransport::from_http_client(
        HttpClientBuilder::new()
            .without_replays()
            .build_direct()
            .expect("loopback allowance fixture"),
    );
    ResponsesClient::new(transport, provider(server.uri()), Arc::new(NoAuth))
        .with_inference_allowance(allowance)
}

fn provider(base_url: String) -> Provider {
    Provider {
        name: "allowance-loopback".into(),
        base_url,
        query_params: None,
        headers: HeaderMap::new(),
        retry: RetryConfig {
            max_attempts: 9,
            base_delay: Duration::from_millis(1),
            retry_429: true,
            retry_5xx: true,
            retry_transport: true,
        },
        stream_idle_timeout: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn inference_allowance_counts_physical_http_retries_and_does_not_refill() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(500))
        .expect(2)
        .mount(&server)
        .await;
    let allowance = Arc::new(
        InferenceAllowance::new(2, 1024, Instant::now() + Duration::from_secs(30))
            .expect("loopback allowance fixture"),
    );
    let client = client(&server, Arc::clone(&allowance));
    for _ in 0..2 {
        let error = client
            .stream(
                json!({"model":"loopback"}),
                HeaderMap::new(),
                Compression::None,
                None,
            )
            .await
            .err()
            .expect("loopback allowance fixture");
        assert!(matches!(error, ApiError::InvalidRequest { .. }));
    }
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        2
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("loopback allowance fixture")
            .len(),
        2
    );
    server.verify().await;
}

#[tokio::test]
async fn inference_allowance_bounds_complete_json_before_compression() {
    let server = MockServer::start().await;
    let allowance = Arc::new(
        InferenceAllowance::new(2, 256, Instant::now() + Duration::from_secs(30))
            .expect("loopback allowance fixture"),
    );
    let client = client(&server, Arc::clone(&allowance));
    let error = client
        .stream(
            json!({"model":"loopback","input":"x".repeat(8192)}),
            HeaderMap::new(),
            Compression::Zstd,
            None,
        )
        .await
        .err()
        .expect("loopback allowance fixture");
    assert!(matches!(error, ApiError::InvalidRequest { .. }));
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        0
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("loopback allowance fixture")
            .is_empty()
    );
}

#[tokio::test]
async fn inference_allowance_deadline_bounds_a_stalled_real_transport() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let allowance = Arc::new(
        InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(2))
            .expect("loopback allowance fixture"),
    );
    let client = client(&server, Arc::clone(&allowance));
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        client.stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        ),
    )
    .await;
    assert!(
        result.is_ok(),
        "The provider attempt must end at its absolute allowance deadline"
    );
    assert!(result.expect("loopback allowance fixture").is_err());
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        1
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("loopback allowance fixture")
            .len(),
        1
    );
}

fn ws_request() -> ResponsesWsRequest<'static> {
    ResponsesWsRequest::ResponseCreate(ResponseCreateWsRequest {
        model: "loopback",
        stream: true,
        previous_response_id: None,
        input: &[],
        tools: None,
        tool_choice: "auto",
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream_options: None,
        include: &[],
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        generate: None,
        client_metadata: None,
        access_programs: None,
    })
}

async fn websocket_peer(
    complete: bool,
) -> (
    ResponsesWebsocketConnection,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    websocket_peer_mode(complete, "legacy").await
}

async fn websocket_peer_mode(
    complete: bool,
    mode: &'static str,
) -> (
    ResponsesWebsocketConnection,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback allowance fixture");
    let addr = listener.local_addr().expect("loopback allowance fixture");
    let received = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&received);
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("loopback allowance fixture");
        // Match the maintained Responses fixture's deflate negotiation.
        let mut extensions = ExtensionsConfig::default();
        extensions.permessage_deflate = Some(DeflateConfig::default());
        let mut config = WebSocketConfig::default();
        config.extensions = extensions;
        let mut ws = accept_hdr_async_with_config(
            tcp,
            |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
             response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                Ok(response)
            },
            Some(config),
        )
        .await
        .expect("loopback allowance fixture");
        while let Some(Ok(message)) = ws.next().await {
            if let Message::Text(text) = message {
                let value: serde_json::Value =
                    serde_json::from_str(&text).expect("loopback allowance fixture");
                if value["type"] == "response.create" {
                    if mode == "output-bound" {
                        assert_eq!(value["max_output_tokens"], 13);
                        assert!(
                            text.contains("[              {"),
                            "RawValue tool spacing changed"
                        );
                    }
                    let ordinal = observed.fetch_add(1, Ordering::SeqCst) + 1;
                    if complete {
                        let id = if mode == "legacy" {
                            "allowance-response".into()
                        } else {
                            format!("verified-response-{ordinal}")
                        };
                        if matches!(mode, "verified" | "stale" | "output-bound" | "incomplete") {
                            let created=Message::Text(json!({"type":"response.created","response":{"id":id,"status":"in_progress"}}).to_string().into());
                            if ws.send(created).await.is_err() {
                                break;
                            }
                        }
                        let terminal = if mode.starts_with("incomplete") {
                            Message::Text(
                                json!({"type":"response.incomplete","response":{
                                    "id":id,"status":"incomplete","output":[],
                                    "incomplete_details":{"reason":"max_output_tokens"},
                                    "usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10},
                                "usage_metadata":{"amount":"0.0123"}
                                }})
                                .to_string()
                                .into(),
                            )
                        } else {
                            Message::Text(json!({"type":"response.completed","response":{"id":id,"status":"completed","output":[]}}).to_string().into())
                        };
                        if ws.send(terminal.clone()).await.is_err() {
                            break;
                        }
                        if mode == "stale" && ordinal == 1 {
                            let _ = ws.send(terminal).await;
                        }
                    }
                }
            }
        }
    });
    let connection =
        ResponsesWebsocketClient::new(provider(format!("http://{addr}")), Arc::new(NoAuth))
            .connect(
                &HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                HeaderMap::new(),
                HeaderMap::new(),
                None,
                None,
            )
            .await
            .expect("loopback allowance fixture");
    (connection, received, task)
}

#[tokio::test]
async fn inference_allowance_is_shared_across_websocket_then_http_fallback() {
    let (connection, received, server_task) = websocket_peer(true).await;
    let allowance = Arc::new(
        InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(30))
            .expect("loopback allowance fixture"),
    );
    // A small incremental frame cannot conceal an oversized retained context.
    let rejected = connection
        .stream_request_with_allowance(ws_request(), false, None, 4096, Arc::clone(&allowance))
        .await;
    assert!(matches!(rejected, Err(ApiError::InvalidRequest { .. })));
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        0
    );
    let mut events = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, Arc::clone(&allowance))
        .await
        .expect("loopback allowance fixture");
    let event = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("loopback allowance fixture")
        .expect("loopback allowance fixture")
        .expect("loopback allowance fixture");
    assert!(matches!(event, ResponseEvent::Completed { .. }));
    assert_eq!(received.load(Ordering::SeqCst), 1);
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        1
    );
    let http = MockServer::start().await;
    let fallback = client(&http, Arc::clone(&allowance))
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await;
    assert!(matches!(fallback, Err(ApiError::InvalidRequest { .. })));
    assert!(
        http.received_requests()
            .await
            .expect("loopback allowance fixture")
            .is_empty()
    );
    drop(events);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("loopback allowance fixture")
        .expect("loopback allowance fixture");
}

#[tokio::test]
async fn inference_allowance_websocket_absolute_deadline_retires_stalled_stream() {
    let (connection, received, server_task) = websocket_peer(false).await;
    let allowance = Arc::new(
        InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(2))
            .expect("loopback allowance fixture"),
    );
    let mut events = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, Arc::clone(&allowance))
        .await
        .expect("loopback allowance fixture");
    let event = tokio::time::timeout(Duration::from_secs(8), events.next())
        .await
        .expect("loopback allowance fixture")
        .expect("loopback allowance fixture");
    assert!(matches!(event, Err(ApiError::InvalidRequest { .. })));
    assert_eq!(received.load(Ordering::SeqCst), 1);
    assert_eq!(
        allowance.attempted().expect("loopback allowance fixture"),
        1
    );
    assert!(connection.is_closed().await);
    drop(events);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("loopback allowance fixture")
        .expect("loopback allowance fixture");
}

#[tokio::test]
async fn inference_allowance_deadline_covers_retry_after_wait() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "30"))
        .expect(1)
        .mount(&server)
        .await;
    let allowance = Arc::new(
        InferenceAllowance::new(5, 1024, Instant::now() + Duration::from_secs(2))
            .expect("allowance"),
    );
    let client = client(&server, allowance);
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        client.stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        ),
    )
    .await
    .expect("absolute allowance must end before Retry-After");
    assert!(matches!(result, Err(ApiError::InvalidRequest { .. })));
}

struct EnlargingAuth;
impl AuthProvider for EnlargingAuth {
    fn add_auth_headers(&self, _: &mut HeaderMap) -> Result<(), codex_api::AuthError> {
        Ok(())
    }
    fn apply_auth(&self, request: codex_client::Request) -> codex_api::AuthProviderFuture<'_> {
        Box::pin(async move { Ok(request.with_json(&json!({"input":"x".repeat(4096)}))) })
    }
}

#[tokio::test]
async fn inference_allowance_rejects_auth_body_replacement_before_send() {
    let server = MockServer::start().await;
    let allowance = Arc::new(
        InferenceAllowance::new(2, 1024, Instant::now() + Duration::from_secs(10))
            .expect("allowance"),
    );
    let transport = ReqwestTransport::from_http_client(
        HttpClientBuilder::new()
            .without_replays()
            .build_direct()
            .expect("client"),
    );
    let client = ResponsesClient::new(transport, provider(server.uri()), Arc::new(EnlargingAuth))
        .with_inference_allowance(Arc::clone(&allowance));
    let result = client
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await;
    assert!(matches!(result, Err(ApiError::InvalidRequest { .. })));
    assert_eq!(allowance.attempted().expect("attempts"), 0);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test]
async fn inference_allowance_never_follows_307_in_direct_or_routed_clients() {
    for routed in [false, true] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/redirect", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redirect"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let transport = if routed {
            ReqwestTransport::from_route_aware_client_pool(
                codex_http_client::RouteAwareClientPool::with_builder(
                    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                    codex_http_client::ClientRouteClass::Api,
                    HttpClientBuilder::new().without_replays(),
                )
                .with_tls_backend_fallback(),
            )
        } else {
            ReqwestTransport::from_http_client(
                HttpClientBuilder::new()
                    .without_replays()
                    .build_direct()
                    .expect("client"),
            )
        };
        let allowance = Arc::new(
            InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(10))
                .expect("allowance"),
        );
        let client = ResponsesClient::new(transport, provider(server.uri()), Arc::new(NoAuth))
            .with_inference_allowance(Arc::clone(&allowance));
        let result = client
            .stream(
                json!({"model":"loopback"}),
                HeaderMap::new(),
                Compression::None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(allowance.attempted().expect("attempts"), 1);
        assert_eq!(server.received_requests().await.expect("requests").len(), 1);
    }
}

#[tokio::test]
async fn inference_allowance_refuses_an_unqualified_transport_without_spending() {
    let server = MockServer::start().await;
    let transport = ReqwestTransport::from_http_client(
        HttpClientBuilder::new().build_direct().expect("client"),
    );
    let allowance = Arc::new(
        InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(10))
            .expect("allowance"),
    );
    let client = ResponsesClient::new(transport, provider(server.uri()), Arc::new(NoAuth))
        .with_inference_allowance(Arc::clone(&allowance));
    let result = client
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await;
    assert!(matches!(result, Err(ApiError::InvalidRequest { .. })));
    assert_eq!(allowance.attempted().expect("attempts"), 0);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test]
async fn inference_allowance_deadline_also_closes_a_stalled_http_event_body() {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut request = Vec::new();
        let mut chunk = [0; 1024];
        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let n = socket.read(&mut chunk).await.expect("request");
            assert!(n > 0);
            request.extend_from_slice(&chunk[..n]);
            assert!(request.len() < 8192);
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.expect("headers");
        // Headers succeed immediately; the SSE body never completes.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 1024, Instant::now() + Duration::from_secs(2))
            .expect("allowance"),
    );
    let transport = ReqwestTransport::from_http_client(
        HttpClientBuilder::new()
            .without_replays()
            .build_direct()
            .expect("client"),
    );
    let client = ResponsesClient::new(
        transport,
        provider(format!("http://{address}")),
        Arc::new(NoAuth),
    )
    .with_inference_allowance(Arc::clone(&allowance));
    let mut response = client
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await
        .expect("HTTP headers");
    let ended_with_error = tokio::time::timeout(Duration::from_secs(4), async {
        while let Some(event) = response.next().await {
            if event.is_err() {
                return true;
            }
        }
        false
    })
    .await
    .expect("event body deadline");
    assert!(ended_with_error);
    assert_eq!(allowance.attempted().expect("attempts"), 1);
    server.abort();
    assert!(
        server
            .await
            .expect_err("owned server retired")
            .is_cancelled()
    );
}

#[tokio::test]
async fn provider_gate_refuses_before_http_and_closes_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        claims: Mutex::default(),
        usage: Mutex::default(),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(8, 4096, Instant::now() + Duration::from_secs(30))
            .expect("gate fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let client = client(&server, Arc::clone(&allowance));
    for _ in 0..2 {
        assert!(matches!(
            client
                .stream(
                    json!({"model":"loopback"}),
                    HeaderMap::new(),
                    Compression::None,
                    None,
                )
                .await,
            Err(ApiError::InvalidRequest { .. })
        ));
    }
    let claims = gate.claims.lock().expect("gate fixture");
    assert_eq!(claims.len(), 2);
    assert_ne!(claims[0].0, claims[1].0);
    assert_eq!((claims[0].1, claims[1].1), (1, 2));
    assert!(claims.iter().all(|claim| claim.2 == 4096));
    assert_eq!(allowance.attempted().expect("gate fixture"), 2);
}

#[tokio::test]
async fn provider_gate_refuses_websocket_write_and_http_fallback() {
    let (connection, received, server_task) = websocket_peer(true).await;
    let gate = Arc::new(ReservationGate {
        permitted: 0,
        claims: Mutex::default(),
        usage: Mutex::default(),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(8, 4096, Instant::now() + Duration::from_secs(30))
            .expect("gate fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut events = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, Arc::clone(&allowance))
        .await
        .expect("gate fixture");
    let event = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("gate fixture")
        .expect("gate fixture");
    assert!(event.is_err());
    assert_eq!(received.load(Ordering::SeqCst), 0);
    let http = MockServer::start().await;
    assert!(matches!(
        client(&http, allowance)
            .stream(
                json!({"model":"loopback"}),
                HeaderMap::new(),
                Compression::None,
                None,
            )
            .await,
        Err(ApiError::InvalidRequest { .. })
    ));
    assert!(
        http.received_requests()
            .await
            .expect("gate fixture")
            .is_empty()
    );
    assert_eq!(gate.claims.lock().expect("gate fixture").len(), 1);
    drop(events);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("gate fixture")
        .expect("gate fixture");
}

#[tokio::test]
async fn successful_http_retry_reports_usage_for_only_its_exact_physical_claim() {
    let server = MockServer::start().await;
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    Mock::given(method("POST")).and(path("/responses"))
        .respond_with(move |request: &wiremock::Request| {
            assert_eq!(request.body_json::<serde_json::Value>().unwrap()["max_output_tokens"],11);
            if observed.fetch_add(1,Ordering::SeqCst)==0 {ResponseTemplate::new(500)} else {
                ResponseTemplate::new(200).insert_header("content-type","text/event-stream")
                    .insert_header("openai-model","effective-loopback")
                    .set_body_string("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"success-after-retry\",\"usage\":{\"input_tokens\":7,\"output_tokens\":5,\"total_tokens\":12}}}\n\n")
            }
        }).expect(2).mount(&server).await;
    let gate = Arc::new(ReservationGate {
        permitted: 2,
        output_limit: Some(11),
        claims: Mutex::default(),
        usage: Mutex::default(),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(2, 4096, Instant::now() + Duration::from_secs(30))
            .expect("usage fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut stream = client(&server, allowance)
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await
        .expect("usage fixture");
    while let Some(event) = stream.next().await {
        event.expect("usage fixture");
    }
    let claims = gate.claims.lock().expect("claims");
    let usage = gate.usage.lock().expect("usage");
    assert_eq!(claims.len(), 2);
    assert_ne!(claims[0].0, claims[1].0);
    assert_eq!(usage.len(), 1);
    assert_eq!((usage[0].attempt_id, usage[0].ordinal), (claims[1].0, 2));
    assert_eq!(usage[0].response_id, "success-after-retry");
    assert_eq!(usage[0].model.as_deref(), Some("effective-loopback"));
    assert_eq!(
        usage[0]
            .token_usage
            .as_ref()
            .expect("observed usage")
            .total_tokens,
        12
    );
}

#[tokio::test]
async fn reused_websocket_reports_each_completion_with_its_own_claim_and_unknown_usage() {
    let (connection, received, server) = websocket_peer_mode(true, "verified").await;
    let gate = Arc::new(ReservationGate {
        permitted: 2,
        claims: Mutex::default(),
        usage: Mutex::default(),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(2, 4096, Instant::now() + Duration::from_secs(30))
            .expect("usage fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    for _ in 0..2 {
        let mut stream = connection
            .stream_request_with_allowance(ws_request(), false, None, 512, allowance.clone())
            .await
            .expect("usage fixture");
        while let Some(event) = stream.next().await {
            event.expect("usage fixture");
        }
    }
    assert_eq!(received.load(Ordering::SeqCst), 2);
    {
        let claims = gate.claims.lock().expect("claims");
        let usage = gate.usage.lock().expect("usage");
        assert_eq!(usage.len(), 2);
        for (index, usage) in usage.iter().enumerate() {
            assert_eq!(usage.attempt_id, claims[index].0);
            assert_eq!(usage.ordinal, index as u32 + 1);
            assert!(usage.token_usage.is_none());
            assert!(usage.model.is_none());
        }
    }
    drop(connection);
    server.await.expect("peer closed");
}

#[tokio::test]
async fn late_duplicate_completion_never_becomes_usage_of_next_websocket_attempt() {
    let (connection, _, server) = websocket_peer_mode(true, "stale").await;
    let gate = Arc::new(ReservationGate {
        permitted: 2,
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(2, 4096, Instant::now() + Duration::from_secs(5))
            .expect("stale fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut first = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, allowance.clone())
        .await
        .expect("first");
    while let Some(event) = first.next().await {
        event.expect("first completion");
    }
    let mut next = connection
        .stream_request_with_allowance(ws_request(), true, None, 512, allowance)
        .await
        .expect("next");
    let mut refused = false;
    while let Some(event) = next.next().await {
        if event.is_err() {
            refused = true;
        }
    }
    assert!(refused);
    assert_eq!(gate.claims.lock().expect("claims").len(), 2);
    assert_eq!(gate.usage.lock().expect("usage").len(), 1);
    drop(connection);
    server.await.expect("closed stale peer");
}

#[tokio::test]
async fn websocket_without_creation_proof_preserves_completion_but_never_invents_usage_binding() {
    let (connection, _, server) = websocket_peer_mode(true, "no-created").await;
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 4096, Instant::now() + Duration::from_secs(5))
            .expect("source fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut stream = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, allowance)
        .await
        .expect("stream");
    let mut completed = false;
    while let Some(event) = stream.next().await {
        completed |= matches!(event.expect("event"), ResponseEvent::Completed { .. });
    }
    assert!(completed);
    assert_eq!(gate.claims.lock().expect("claims").len(), 1);
    assert!(gate.usage.lock().expect("usage").is_empty());
    drop(connection);
    server.await.expect("closed source peer");
}

#[tokio::test]
async fn pending_usage_observer_cannot_erase_completion_at_websocket_cancel_deadline() {
    let (connection, _, server) = websocket_peer_mode(true, "verified").await;
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        hang_observer: true,
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 4096, Instant::now() + Duration::from_secs(5))
            .expect("cancel fixture")
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut stream = connection
        .stream_request_with_allowance(ws_request(), false, None, 512, allowance)
        .await
        .expect("stream");
    tokio::time::timeout(Duration::from_secs(1), gate.observed.notified())
        .await
        .expect("usage parsed");
    stream
        .interrupt
        .take()
        .expect("interrupt")
        .send(codex_api::ResponseInterrupt::Cancel {
            deadline: Instant::now() + Duration::from_millis(100),
        })
        .unwrap_or_else(|_| panic!("interrupt admitted"));
    let response = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(event) = stream.next().await {
            if let ResponseEvent::Completed { response_id, .. } = event.expect("retained terminal")
            {
                return response_id;
            }
        }
        panic!("Parsed completion disappeared");
    })
    .await
    .expect("must not await original five-second bound");
    assert_eq!(response, "verified-response-1");
    drop(connection);
    server.await.expect("closed cancelled peer");
}

#[tokio::test]
async fn owner_output_bound_preserves_tighter_http_cap_and_rejects_invalid_before_send() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        output_limit: Some(11),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 4096, Instant::now() + Duration::from_secs(5))
            .unwrap()
            .with_provider_attempt_gate(gate),
    );
    let client = client(&server, allowance.clone());
    for invalid in [json!(0), json!("4"), json!(-1)] {
        assert!(
            client
                .stream(
                    json!({"model":"loopback","max_output_tokens":invalid}),
                    HeaderMap::new(),
                    Compression::None,
                    None
                )
                .await
                .is_err()
        );
    }
    assert_eq!(allowance.attempted().unwrap(), 0);
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        client
            .stream(
                json!({"model":"loopback","max_output_tokens":3}),
                HeaderMap::new(),
                Compression::None,
                None
            )
            .await
            .is_err()
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].body_json::<serde_json::Value>().unwrap()["max_output_tokens"],
        3
    );
}

#[tokio::test]
async fn websocket_output_bound_counts_new_field_in_complete_request_and_real_write() {
    let (connection, received, server) = websocket_peer_mode(true, "output-bound").await;
    let tools = serde_json::value::RawValue::from_string(
        "[              {\"type\":\"function\",\"name\":\"raw-tool\"}              ]".into(),
    )
    .unwrap();
    let request = || {
        let ResponsesWsRequest::ResponseCreate(base) = ws_request();
        ResponsesWsRequest::ResponseCreate(ResponseCreateWsRequest {
            tools: Some(&tools),
            ..base
        })
    };
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        output_limit: Some(13),
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 512, Instant::now() + Duration::from_secs(5))
            .unwrap()
            .with_provider_attempt_gate(gate),
    );
    assert!(
        connection
            .stream_request_with_allowance(request(), false, None, 512, allowance.clone())
            .await
            .is_err()
    );
    assert_eq!(allowance.attempted().unwrap(), 0);
    assert_eq!(received.load(Ordering::SeqCst), 0);
    let mut stream = connection
        .stream_request_with_allowance(request(), false, None, 400, allowance)
        .await
        .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    assert_eq!(received.load(Ordering::SeqCst), 1);
    drop(stream);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn incomplete_http_response_reports_consumption_but_remains_failed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!(
                    "data: {}\n\n",
                    json!({
                        "type":"response.incomplete","response":{
                            "id":"limited-response","status":"incomplete","output":[],
                            "incomplete_details":{"reason":"max_output_tokens"},
                            "usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10},
                                "usage_metadata":{"amount":"0.0123"}
                        }
                    })
                )),
        )
        .expect(1)
        .mount(&server)
        .await;
    let gate = Arc::new(ReservationGate {
        permitted: 1,
        ..Default::default()
    });
    let allowance = Arc::new(
        InferenceAllowance::new(1, 4096, Instant::now() + Duration::from_secs(5))
            .unwrap()
            .with_provider_attempt_gate(gate.clone()),
    );
    let mut stream = client(&server, allowance)
        .stream(
            json!({"model":"loopback"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await
        .unwrap();
    let mut failed = false;
    while let Some(event) = stream.next().await {
        assert!(!matches!(event, Ok(ResponseEvent::Completed { .. })));
        failed |= matches!(event, Err(ApiError::ResponseIncomplete(_)));
    }
    assert!(failed);
    let claims = gate.claims.lock().unwrap();
    let usage = gate.usage.lock().unwrap();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].attempt_id, claims[0].0);
    assert_eq!(usage[0].response_id, "limited-response");
    assert_eq!(usage[0].token_usage.as_ref().unwrap().total_tokens, 10);
    assert_eq!(
        usage[0].usage_metadata.as_ref().unwrap().amount.as_deref(),
        Some("0.0123")
    );
    assert_eq!(
        usage[0]
            .usage_metadata
            .as_ref()
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()["total_tokens"],
        10
    );
}

#[tokio::test]
async fn incomplete_websocket_usage_requires_matching_creation_and_remains_failed() {
    for mode in ["incomplete", "incomplete-no-created"] {
        let (connection, _, server) = websocket_peer_mode(true, mode).await;
        let gate = Arc::new(ReservationGate {
            permitted: 1,
            ..Default::default()
        });
        let allowance = Arc::new(
            InferenceAllowance::new(1, 4096, Instant::now() + Duration::from_secs(5))
                .unwrap()
                .with_provider_attempt_gate(gate.clone()),
        );
        let mut stream = connection
            .stream_request_with_allowance(ws_request(), false, None, 512, allowance)
            .await
            .unwrap();
        let mut failed = false;
        while let Some(event) = stream.next().await {
            assert!(!matches!(event, Ok(ResponseEvent::Completed { .. })));
            failed |= matches!(event, Err(ApiError::ResponseIncomplete(_)));
        }
        assert!(failed);
        {
            let usage = gate.usage.lock().unwrap();
            if mode == "incomplete" {
                assert_eq!(usage.len(), 1);
                assert_eq!(usage[0].attempt_id, gate.claims.lock().unwrap()[0].0);
                assert_eq!(usage[0].token_usage.as_ref().unwrap().total_tokens, 10);
                assert_eq!(
                    usage[0].usage_metadata.as_ref().unwrap().amount.as_deref(),
                    Some("0.0123")
                );
                assert_eq!(
                    usage[0]
                        .usage_metadata
                        .as_ref()
                        .unwrap()
                        .metadata
                        .as_ref()
                        .unwrap()["total_tokens"],
                    10
                );
            } else {
                assert!(usage.is_empty());
            }
        }
        drop(connection);
        server.await.unwrap();
    }
}
