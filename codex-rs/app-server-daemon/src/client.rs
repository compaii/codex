use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::RequestId;
use codex_uds::UnixStream;
use futures::SinkExt;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::Message;

pub(crate) const CONTROL_SOCKET_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_NAME: &str = "codex_app_server_daemon";
const INITIALIZE_REQUEST_ID: RequestId = RequestId::Integer(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeInfo {
    pub(crate) app_server_version: String,
}

pub(crate) async fn probe(socket_path: &Path) -> Result<ProbeInfo> {
    timeout(CONTROL_SOCKET_RESPONSE_TIMEOUT, probe_inner(socket_path))
        .await
        .with_context(|| {
            format!(
                "timed out probing app-server control socket {}",
                socket_path.display()
            )
        })?
}

async fn probe_inner(socket_path: &Path) -> Result<ProbeInfo> {
    let mut websocket = connect(socket_path).await?;

    let initialize_response = initialize(&mut websocket, /*experimental_api*/ false).await?;
    let initialized = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "initialized".to_string(),
        params: None,
    });
    send_message(&mut websocket, &initialized)
        .await
        .context("failed to send initialized notification")?;
    websocket.close(None).await.ok();

    Ok(ProbeInfo {
        app_server_version: parse_version_from_user_agent(&initialize_response.user_agent)?,
    })
}

pub(crate) async fn connect(socket_path: &Path) -> Result<WebSocketStream<UnixStream>> {
    connect_at(socket_path, "ws://localhost/").await
}

async fn connect_at(socket_path: &Path, url: &str) -> Result<WebSocketStream<UnixStream>> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    let (websocket, _response) = client_async(url, stream)
        .await
        .with_context(|| format!("failed to upgrade {}", socket_path.display()))?;
    Ok(websocket)
}

#[cfg(windows)]
pub(crate) async fn request_shutdown(socket_path: &Path, pid: u32) -> Result<()> {
    timeout(CONTROL_SOCKET_RESPONSE_TIMEOUT, async {
        let mut websocket = connect_at(socket_path, "ws://localhost/daemon/shutdown").await?;
        websocket
            .send(Message::Text(pid.to_string().into()))
            .await?;
        let reply = websocket
            .next()
            .await
            .context("shutdown socket closed without acknowledgment")??;
        anyhow::ensure!(
            matches!(reply, Message::Text(ack) if ack == pid.to_string()),
            "shutdown acknowledgment did not match the managed process {pid}"
        );
        websocket.close(None).await?;
        Ok(())
    })
    .await
    .context("timed out waiting for managed app-server shutdown acknowledgment")?
}

pub(crate) async fn initialize<S>(
    websocket: &mut WebSocketStream<S>,
    experimental_api: bool,
) -> Result<InitializeResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initialize = JSONRPCMessage::Request(JSONRPCRequest {
        id: INITIALIZE_REQUEST_ID,
        method: "initialize".to_string(),
        params: Some(serde_json::to_value(InitializeParams {
            client_info: ClientInfo {
                name: CLIENT_NAME.to_string(),
                title: Some("Codex App Server Daemon".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            capabilities: if experimental_api {
                Some(InitializeCapabilities {
                    experimental_api: true,
                    ..Default::default()
                })
            } else {
                None
            },
        })?),
        trace: None,
    });
    send_message(websocket, &initialize)
        .await
        .context("failed to send initialize request")?;

    let response = loop {
        let message = timeout(CONTROL_SOCKET_RESPONSE_TIMEOUT, read_message(websocket))
            .await
            .context("timed out waiting for initialize response")??;
        if let JSONRPCMessage::Response(response) = message
            && response.id == INITIALIZE_REQUEST_ID
        {
            break response;
        }
    };
    serde_json::from_value::<InitializeResponse>(response.result)
        .context("failed to parse initialize response")
}

pub(crate) async fn send_message<S>(
    websocket: &mut WebSocketStream<S>,
    message: &JSONRPCMessage,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Text(serde_json::to_string(message)?.into()))
        .await?;
    Ok(())
}

pub(crate) async fn read_message<S>(websocket: &mut WebSocketStream<S>) -> Result<JSONRPCMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let frame = websocket
            .next()
            .await
            .ok_or_else(|| anyhow!("app-server closed the control socket"))??;
        let Message::Text(payload) = frame else {
            continue;
        };
        return serde_json::from_str::<JSONRPCMessage>(&payload)
            .context("failed to parse app-server JSON-RPC message");
    }
}

fn parse_version_from_user_agent(user_agent: &str) -> Result<String> {
    let (_originator, rest) = user_agent
        .split_once('/')
        .ok_or_else(|| anyhow!("app-server user-agent omitted version separator"))?;
    let version = rest
        .split_whitespace()
        .next()
        .filter(|version| !version.is_empty())
        .ok_or_else(|| anyhow!("app-server user-agent omitted version"))?;
    Ok(version.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use pretty_assertions::assert_eq;

    use super::parse_version_from_user_agent;

    #[test]
    fn parses_version_from_codex_user_agent() {
        assert_eq!(
            parse_version_from_user_agent(
                "codex_app_server_daemon/1.2.3 (Linux 6.8.0; x86_64) codex_cli_rs/1.2.3",
            )
            .expect("version"),
            "1.2.3"
        );
    }

    #[test]
    fn rejects_user_agent_without_version() {
        assert!(parse_version_from_user_agent("codex_app_server_daemon").is_err());
    }
}

/// Never falls back to signals or holds the daemon operation lock. Older
/// receivers lack the capability header, so refusal has no lifecycle effect.
pub(crate) async fn request_maintenance(socket_path: &Path) -> Result<crate::MaintenanceOutput> {
    request_maintenance_with_timeout(socket_path, Duration::from_secs(133)).await
}

async fn request_maintenance_with_timeout(
    socket_path: &Path,
    limit: Duration,
) -> Result<crate::MaintenanceOutput> {
    timeout(limit, async {
        let stream = UnixStream::connect(socket_path).await?;
        let (mut websocket, response) = timeout(
            CONTROL_SOCKET_RESPONSE_TIMEOUT,
            client_async("ws://localhost/daemon/maintenance", stream),
        )
        .await??;
        let capability = response
            .headers()
            .get(codex_app_server_transport::DAEMON_MAINTENANCE_CAPABILITY_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("v1:"))
            .context("receiver does not support nonblocking maintenance; no request was sent")?;
        let pid: u32 = capability
            .parse()
            .context("invalid maintenance receiver identity")?;
        websocket
            .send(Message::Text(pid.to_string().into()))
            .await?;
        let reply = websocket
            .next()
            .await
            .context("maintenance outcome unconfirmed; do not retry or force shutdown")??;
        let Message::Text(reply) = reply else {
            anyhow::bail!("maintenance outcome unconfirmed; do not retry")
        };
        let outcome: crate::MaintenanceOutput = serde_json::from_str(&reply)?;
        anyhow::ensure!(
            outcome.pid == pid,
            "maintenance receiver identity changed; outcome unconfirmed"
        );
        anyhow::ensure!(
            matches!(
                outcome.outcome.as_str(),
                "handoffReady" | "deferred" | "alreadyPending" | "supersededByExplicitStop"
            ),
            "unknown maintenance outcome"
        );
        Ok(outcome)
    })
    .await
    .context("maintenance observation expired; outcome unconfirmed; never force or replay")?
}

#[cfg(all(test, unix))]
mod maintenance_tests {
    use super::*;

    #[tokio::test]
    async fn maintenance_refuses_old_receiver_before_sending_any_request() -> Result<()> {
        let home = tempfile::TempDir::new()?;
        let path = home.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&path)?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        });
        let error = request_maintenance(&path).await.unwrap_err();
        assert!(error.to_string().contains("no request was sent"));
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn maintenance_reports_deferred_and_rejects_mismatched_or_missing_outcomes() -> Result<()>
    {
        for mode in ["deferred", "mismatch", "missing"] {
            let home = tempfile::TempDir::new()?;
            let path = home.path().join("server.sock");
            let listener = tokio::net::UnixListener::bind(&path)?;
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(stream,
                    |_request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        response.headers_mut().insert(codex_app_server_transport::DAEMON_MAINTENANCE_CAPABILITY_HEADER, "v1:42".parse().unwrap());
                        Ok(response)
                    }).await.unwrap();
                assert_eq!(
                    ws.next().await.unwrap().unwrap(),
                    Message::Text("42".into())
                );
                if mode == "missing" {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                } else {
                    let pid = if mode == "mismatch" { 43 } else { 42 };
                    ws.send(Message::Text(
                        serde_json::json!({"pid":pid,"outcome":"deferred"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
            });
            let result = request_maintenance_with_timeout(&path, Duration::from_millis(300)).await;
            match mode {
                "deferred" => assert_eq!(result?.outcome, "deferred"),
                "mismatch" => assert!(result.unwrap_err().to_string().contains("identity changed")),
                _ => assert!(result.unwrap_err().to_string().contains("unconfirmed")),
            }
            if mode == "missing" {
                server.abort();
            } else {
                server.await?;
            }
        }
        Ok(())
    }
}
