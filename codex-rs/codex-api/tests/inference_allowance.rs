#![allow(clippy::expect_used)]
//! Actual loopback HTTP/retry boundaries; no real provider, model or credentials.
use codex_api::ApiError;
use codex_api::AuthProvider;
use codex_api::Compression;
use codex_api::InferenceAllowance;
use codex_api::Provider;
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
                    observed.fetch_add(1, Ordering::SeqCst);
                    if complete {
                        ws.send(Message::Text(json!({"type":"response.completed","response":{"id":"allowance-response","status":"completed","output":[]}}).to_string().into())).await.expect("loopback allowance fixture");
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
