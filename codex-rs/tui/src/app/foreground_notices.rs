//! Optional owner-local presentation of retained conversation control outcomes.
//! This reader never submits native input, invokes a tool, or starts an agent.

use super::App;
use crate::app_event::AppEvent;
use crate::tui::FrameRequester;
use codex_protocol::ThreadId;
use ratatui::text::Line;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinHandle;
use uuid::Uuid;

const REFRESH: Duration = Duration::from_secs(2);
const BACKOFF: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct ForegroundNotices {
    socket: Option<PathBuf>,
    thread: Option<ThreadId>,
    cursor: Option<i64>,
    request: Option<Uuid>,
    task: Option<JoinHandle<()>>,
    next: Option<Instant>,
    failed: bool,
}

impl Drop for ForegroundNotices {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl ForegroundNotices {
    pub(super) fn from_environment() -> Self {
        Self {
            socket: std::env::var_os("CODEX_FOREGROUND_NOTICES_SOCKET")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            thread: None,
            cursor: None,
            request: None,
            task: None,
            next: None,
            failed: false,
        }
    }

    fn select(&mut self, thread: Option<ThreadId>) {
        if self.thread != thread {
            if let Some(task) = self.task.take() {
                task.abort();
            }
            self.thread = thread;
            self.cursor = None;
            self.request = None;
            self.next = None;
            self.failed = false;
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct NoticeBatch {
    schema: String,
    native_thread: String,
    available: bool,
    history: bool,
    history_truncated: bool,
    cursor: i64,
    has_more: bool,
    items: Vec<Notice>,
}

#[derive(Debug, Deserialize)]
struct Notice {
    id: String,
    sequence: i64,
    source: String,
    text: String,
}

impl NoticeBatch {
    fn validate(&self, thread: ThreadId, after: Option<i64>) -> Result<(), ()> {
        if self.schema != "telecodex.thread-notices/v1"
            || self.native_thread != thread.to_string()
            || self.history != after.is_none()
            || self.cursor < after.unwrap_or(0)
            || self.items.len() > 32
            || (!self.available && !self.items.is_empty())
            || (self.history_truncated && !self.history)
            || (self.has_more && (self.history || self.items.len() != 32))
        {
            return Err(());
        }
        let mut previous = after.unwrap_or(0);
        for notice in &self.items {
            if notice.sequence <= previous
                || notice.id != format!("publication:{}", notice.sequence)
                || notice.source != "interaction"
                || notice.text.is_empty()
                || notice.text.chars().count() > 2000
                || notice.text.chars().any(|c| c.is_control() && c != '\n')
                || notice.text.lines().count() > 8
            {
                return Err(());
            }
            previous = notice.sequence;
        }
        if self.cursor != previous {
            return Err(());
        }
        Ok(())
    }
}

#[cfg(unix)]
async fn read(socket: PathBuf, thread: ThreadId, after: Option<i64>) -> Result<NoticeBatch, ()> {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::MetadataExt;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::BufReader;
    use tokio::net::UnixStream;

    tokio::time::timeout(Duration::from_millis(750), async move {
        if !socket.is_absolute() {
            return Err(());
        }
        let metadata = tokio::fs::symlink_metadata(&socket).await.map_err(|_| ())?;
        let uid = unsafe { libc::geteuid() };
        if !metadata.file_type().is_socket()
            || metadata.uid() != uid
            || metadata.mode() & 0o077 != 0
        {
            return Err(());
        }
        let mut connection = UnixStream::connect(socket).await.map_err(|_| ())?;
        if connection.peer_cred().map_err(|_| ())?.uid() != uid {
            return Err(());
        }
        let command = match after {
            Some(cursor) => format!("topics notices {thread} {cursor}"),
            None => format!("topics notices {thread}"),
        };
        let mut request =
            serde_json::to_vec(&serde_json::json!({"command":command})).map_err(|_| ())?;
        request.push(b'\n');
        connection.write_all(&request).await.map_err(|_| ())?;
        let mut bytes = Vec::new();
        BufReader::new(connection.take(262145))
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|_| ())?;
        if bytes.len() > 262144 {
            return Err(());
        }
        let response: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| ())?;
        if response["ok"] != true {
            return Err(());
        }
        let batch: NoticeBatch =
            serde_json::from_value(response["result"].clone()).map_err(|_| ())?;
        batch.validate(thread, after)?;
        Ok(batch)
    })
    .await
    .map_err(|_| ())?
}

#[cfg(not(unix))]
async fn read(_socket: PathBuf, _thread: ThreadId, _after: Option<i64>) -> Result<NoticeBatch, ()> {
    Err(())
}

impl App {
    /// Uses the existing foreground draw scheduler; no daemon, agent attention,
    /// shell command, file watcher or independently running polling loop.
    pub(super) fn refresh_foreground_notices(&mut self, frames: FrameRequester) {
        if self.app_server_target.uses_remote_workspace() {
            self.foreground_notices.select(None);
            return;
        }
        let thread = self.current_displayed_thread_id();
        let state = &mut self.foreground_notices;
        state.select(thread);
        let (Some(thread), Some(socket)) = (thread, state.socket.clone()) else {
            return;
        };
        if state.request.is_some() {
            return;
        }
        let now = Instant::now();
        if let Some(next) = state.next.filter(|next| *next > now) {
            frames.schedule_frame_in(next.saturating_duration_since(now));
            return;
        }
        let request = Uuid::new_v4();
        state.request = Some(request);
        let after = state.cursor;
        let sender = self.app_event_tx.clone();
        state.task = Some(tokio::spawn(async move {
            let result = read(socket, thread, after).await;
            sender.send(AppEvent::ForegroundNoticesLoaded {
                thread,
                request,
                result,
            });
        }));
    }

    pub(super) fn foreground_notices_loaded(
        &mut self,
        thread: ThreadId,
        request: Uuid,
        result: Result<NoticeBatch, ()>,
        frames: FrameRequester,
    ) {
        if self.app_server_target.uses_remote_workspace() {
            return;
        }
        let current = self.current_displayed_thread_id();
        let state = &mut self.foreground_notices;
        if current != Some(thread) || state.thread != Some(thread) || state.request != Some(request)
        {
            return;
        }
        state.request = None;
        state.task = None;
        let batch = match result {
            Ok(batch) => batch,
            Err(()) => {
                if !state.failed {
                    self.chat_widget.add_info_message(
                        "Interaction notices unavailable; conversation input is unaffected.".into(),
                        None,
                    );
                }
                state.failed = true;
                state.next = Some(Instant::now() + BACKOFF);
                frames.schedule_frame_in(BACKOFF);
                return;
            }
        };
        state.failed = false;
        state.cursor = batch.available.then_some(batch.cursor);
        let delay = if batch.has_more {
            Duration::ZERO
        } else {
            REFRESH
        };
        state.next = Some(Instant::now() + delay);
        // Startup is a snapshot, not newly routed work. Show only its latest
        // retained outcome; the bounded history remains in the owner journal.
        let skip = if batch.history {
            batch.items.len().saturating_sub(1)
        } else {
            0
        };
        for notice in batch.items.into_iter().skip(skip) {
            let mut lines = Vec::new();
            if batch.history {
                lines.push(Line::from("Last retained interaction action"));
            }
            lines.extend(notice.text.lines().map(|line| Line::from(line.to_owned())));
            self.chat_widget.add_plain_history_lines(lines);
        }
        frames.schedule_frame_in(delay);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chatwidget::tests::make_chatwidget_manual_with_sender;
    use crate::test_support::boxed_future;
    use serde_json::json;

    fn thread() -> ThreadId {
        ThreadId::from_string("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn value(after: Option<i64>) -> serde_json::Value {
        json!({"schema":"telecodex.thread-notices/v1","native_thread":thread().to_string(),
            "available":true,"history":after.is_none(),"history_truncated":false,"cursor":4,"has_more":false,
            "items":[{"id":"publication:4","sequence":4,"source":"interaction","text":"Integrator action · goal pause\nNative session acknowledged the goal change."}]})
    }

    #[test]
    fn foreground_notices_require_exact_scope_order_and_safe_plain_text() {
        serde_json::from_value::<NoticeBatch>(value(None))
            .unwrap()
            .validate(thread(), None)
            .unwrap();
        for changed in [
            json!({"schema":"other"}),
            json!({"native_thread":"another-thread"}),
            json!({"cursor":3}),
            json!({"available":false}),
            json!({"history":false}),
            json!({"has_more":true}),
        ] {
            let mut v = value(None);
            v.as_object_mut()
                .unwrap()
                .extend(changed.as_object().unwrap().clone());
            assert!(
                serde_json::from_value::<NoticeBatch>(v)
                    .unwrap()
                    .validate(thread(), None)
                    .is_err()
            );
        }
        let oversized = "x".repeat(2001);
        for text in ["\u{1b}[2J", "bad\rcontrol", "", oversized.as_str()] {
            let mut v = value(None);
            v["items"][0]["text"] = json!(text);
            assert!(
                serde_json::from_value::<NoticeBatch>(v)
                    .unwrap()
                    .validate(thread(), None)
                    .is_err()
            );
        }
        assert!(
            serde_json::from_value::<NoticeBatch>(value(Some(4)))
                .unwrap()
                .validate(thread(), Some(4))
                .is_err()
        );
    }

    #[tokio::test]
    async fn foreground_notices_drop_stale_and_duplicate_replies_without_agent_input() {
        let mut app = boxed_future!(super::super::test_support::make_test_app()).await;
        let (widget, sender, mut events, mut agent_ops) =
            boxed_future!(make_chatwidget_manual_with_sender()).await;
        app.chat_widget = widget;
        app.app_event_tx = sender;
        app.active_thread_id = Some(thread());
        while events.try_recv().is_ok() {}
        while agent_ops.try_recv().is_ok() {}
        app.foreground_notices.select(Some(thread()));
        let request = Uuid::new_v4();
        app.foreground_notices.request = Some(request);
        app.foreground_notices.cursor = Some(3);
        let frames = FrameRequester::test_dummy();
        let stale = Uuid::new_v4();
        app.foreground_notices_loaded(
            thread(),
            stale,
            Ok(serde_json::from_value(value(Some(3))).unwrap()),
            frames.clone(),
        );
        assert_eq!(app.foreground_notices.cursor, Some(3));
        assert!(events.try_recv().is_err());
        app.active_thread_id = Some(ThreadId::new());
        app.foreground_notices_loaded(
            thread(),
            request,
            Ok(serde_json::from_value(value(Some(3))).unwrap()),
            frames.clone(),
        );
        assert_eq!(app.foreground_notices.cursor, Some(3));
        assert!(events.try_recv().is_err());
        app.active_thread_id = Some(thread());
        app.foreground_notices_loaded(
            thread(),
            request,
            Ok(serde_json::from_value(value(Some(3))).unwrap()),
            frames.clone(),
        );
        assert_eq!(app.foreground_notices.cursor, Some(4));
        let event = events.try_recv().unwrap();
        let AppEvent::InsertHistoryCell(cell) = event else {
            panic!("Expected ordinary information cell")
        };
        let text = cell
            .display_lines(80)
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("goal pause"));
        assert!(text.contains("acknowledged"));
        app.foreground_notices_loaded(
            thread(),
            request,
            Ok(serde_json::from_value(value(Some(3))).unwrap()),
            frames,
        );
        assert!(events.try_recv().is_err());
        assert!(agent_ops.try_recv().is_err());
        // A local source never supplies observations to a remote workspace,
        // even if that remote later reuses the same native thread identifier.
        app.app_server_target = crate::AppServerTarget::Remote {
            endpoint: crate::RemoteAppServerEndpoint::WebSocket {
                websocket_url: "ws://example.invalid".into(),
                auth_token: None,
            },
        };
        app.foreground_notices.request = Some(request);
        app.foreground_notices_loaded(
            thread(),
            request,
            Ok(serde_json::from_value(value(Some(3))).unwrap()),
            FrameRequester::test_dummy(),
        );
        assert!(events.try_recv().is_err());
        app.refresh_foreground_notices(FrameRequester::test_dummy());
        assert!(app.foreground_notices.thread.is_none());
    }

    #[tokio::test]
    async fn foreground_notices_are_disabled_without_an_owner_selected_socket() {
        let mut app = boxed_future!(super::super::test_support::make_test_app()).await;
        app.active_thread_id = Some(thread());
        app.refresh_foreground_notices(FrameRequester::test_dummy());
        assert!(app.foreground_notices.task.is_none());
        assert!(app.foreground_notices.request.is_none());
        app.foreground_notices.cursor = Some(20);
        app.foreground_notices.request = Some(Uuid::new_v4());
        app.foreground_notices.select(None);
        assert!(app.foreground_notices.cursor.is_none());
        assert!(app.foreground_notices.request.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn foreground_notices_read_only_existing_owner_socket_and_reject_unsafe_modes() {
        use std::os::unix::fs::PermissionsExt;
        use tokio::io::AsyncBufReadExt;
        use tokio::io::AsyncWriteExt;
        use tokio::io::BufReader;
        use tokio::net::UnixListener;
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("owner.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = tokio::spawn(async move {
            let (connection, _) = listener.accept().await.unwrap();
            let mut connection = BufReader::new(connection);
            let mut command = String::new();
            connection.read_line(&mut command).await.unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&command).unwrap(),
                json!({"command":format!("topics notices {} 3",thread())})
            );
            let response = format!("{}\n", json!({"ok":true,"result":value(Some(3))}));
            connection
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
            listener
        });
        let batch = read(socket.clone(), thread(), Some(3)).await.unwrap();
        assert_eq!(batch.cursor, 4);
        let listener = server.await.unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(read(socket.clone(), thread(), None).await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        assert!(
            read(PathBuf::from("relative.sock"), thread(), None)
                .await
                .is_err()
        );
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let oversized = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let response = format!(
                "{}\n",
                json!({"ok":true,"result":value(None),"padding":"x".repeat(262145)})
            );
            let _ = connection.write_all(response.as_bytes()).await;
        });
        assert!(read(socket, thread(), None).await.is_err());
        oversized.await.unwrap();
    }
}
