//! WebSocket transport: handshake checks (origin, username, admission), framing, per-connection
//! session task. Domain decisions are delegated to [`crate::service::ChatService`].

use crate::{
    error::ChatError,
    protocol::{validate_username, ClientMsg, ServerMsg},
    ratelimit::TokenBucket,
    registry::ConnGuard,
    routes::RequestId,
    AppState,
};
use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, Extension, Query, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tracing::Instrument;

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);
/// A socket that cannot accept a frame for this long is considered dead.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Outgoing queue depth per connection; when full, producers wait (backpressure).
const OUT_QUEUE: usize = 512;

#[derive(Deserialize)]
pub struct WsQuery {
    username: Option<String>,
}

/// Frames queued for the writer task.
#[derive(Debug)]
pub(crate) enum Out {
    Msg(ServerMsg),
    Ping,
}

/// Abort the wrapped task when dropped, so no forwarder outlives its session on any exit path.
pub(crate) struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Same-origin requests are always allowed, as are non-browser clients (no `Origin` header).
/// Anything else must be listed in `allowed` (or `allowed` contains `*`).
pub fn origin_allowed(origin: Option<&str>, host: Option<&str>, allowed: &[String]) -> bool {
    let Some(origin) = origin else { return true };
    let origin = origin.trim().trim_end_matches('/').to_lowercase();
    if allowed.iter().any(|a| a == "*" || *a == origin) {
        return true;
    }
    let authority = origin.strip_prefix("https://").or_else(|| origin.strip_prefix("http://"));
    matches!((authority, host), (Some(a), Some(h)) if a == h.trim().to_lowercase())
}

fn header_str(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
    peer: Option<ConnectInfo<SocketAddr>>,
    req_id: Option<Extension<RequestId>>,
) -> Response {
    if !origin_allowed(header_str(&headers, header::ORIGIN), header_str(&headers, header::HOST), &state.config.allowed_origins) {
        tracing::warn!(origin = ?header_str(&headers, header::ORIGIN), "websocket origin rejected");
        return (StatusCode::FORBIDDEN, ChatError::ForbiddenOrigin.to_string()).into_response();
    }
    let username = match validate_username(q.username.as_deref().unwrap_or("")) {
        Ok(u) => u,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let ip = peer.map(|ConnectInfo(a)| a.ip());
    // Claim the name before upgrading; the guard is released whenever the connection ends,
    // including when the upgrade itself fails (the closure is dropped).
    let admission = state.chat.registry().admit(&username, ip);
    let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let request_id = req_id.map(|Extension(r)| r.0).unwrap_or_default();
    let span = tracing::info_span!("conn", id, user = %username, ip = ?ip, request_id = %request_id);
    let max = state.config.max_frame_bytes;
    ws.max_message_size(max)
        .max_frame_size(max)
        .on_upgrade(move |socket| handle_socket(socket, state, username, id, admission).instrument(span))
}

/// Forward a broadcast receiver into a connection's outgoing queue until either side closes.
/// On `Lagged` (this consumer fell behind the channel) the client is told to resync `lag_room`.
pub(crate) async fn forward(mut rx: broadcast::Receiver<ServerMsg>, out: mpsc::Sender<Out>, lag_room: Option<String>) {
    loop {
        match rx.recv().await {
            Ok(m) => {
                if out.send(Out::Msg(m)).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, room = ?lag_room, "slow consumer lagged");
                if let Some(room) = &lag_room
                    && out.send(Out::Msg(ServerMsg::Lagged { room: room.clone(), skipped: n })).await.is_err()
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn spawn_forwarder(rx: broadcast::Receiver<ServerMsg>, out: mpsc::Sender<Out>, lag_room: Option<String>) -> AbortOnDrop {
    AbortOnDrop(tokio::spawn(forward(rx, out, lag_room)))
}

struct Session {
    id: u64,
    username: String,
    state: AppState,
    out: mpsc::Sender<Out>,
    joined: HashMap<String, AbortOnDrop>,
    bucket: TokenBucket,
}

/// Politely reject a connection that failed admission: error frame, close frame, wait briefly
/// for the peer so the frames are not lost to a TCP reset.
async fn reject(mut socket: WebSocket, e: ChatError) {
    tracing::info!(reason = %e, "connection rejected");
    let err = ServerMsg::Error { message: e.to_string(), code: e.code().into() };
    if let Ok(text) = serde_json::to_string(&err) {
        let _ = tokio::time::timeout(WRITE_TIMEOUT, socket.send(Message::Text(text))).await;
    }
    let close = Message::Close(Some(CloseFrame { code: e.close_code(), reason: e.to_string().into() }));
    let _ = tokio::time::timeout(WRITE_TIMEOUT, socket.send(close)).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), async { while let Some(Ok(_)) = socket.recv().await {} }).await;
}

async fn handle_socket(socket: WebSocket, state: AppState, username: String, id: u64, admission: Result<ConnGuard, ChatError>) {
    let _guard = match admission {
        Ok(g) => g,
        Err(e) => return reject(socket, e).await,
    };
    tracing::info!("connected");

    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Out>(OUT_QUEUE);
    let mut writer = tokio::spawn(async move {
        while let Some(o) = out_rx.recv().await {
            let frame = match o {
                Out::Ping => Message::Ping(Vec::new()),
                Out::Msg(m) => match serde_json::to_string(&m) {
                    Ok(t) => Message::Text(t),
                    Err(e) => {
                        tracing::error!(error = %e, "cannot serialise server message");
                        continue;
                    }
                },
            };
            if !matches!(tokio::time::timeout(WRITE_TIMEOUT, sink.send(frame)).await, Ok(Ok(()))) {
                return; // dead or stalled socket; dropping out_rx makes every producer fail fast
            }
        }
        let _ = tokio::time::timeout(WRITE_TIMEOUT, sink.send(Message::Close(None))).await;
    });

    let mut shutdown = state.shutdown.subscribe();
    let global = spawn_forwarder(state.chat.hub().subscribe_global(), out_tx.clone(), None);
    let mut session = Session {
        id,
        username: username.clone(),
        bucket: TokenBucket::new(state.config.rate_capacity, state.config.rate_refill_per_sec),
        state: state.clone(),
        out: out_tx,
        joined: HashMap::new(),
    };

    match session.state.chat.room_list().await {
        Ok(rooms) => session.send(ServerMsg::Welcome { username, rooms }).await,
        Err(e) => session.error(&e).await,
    }

    let idle = Duration::from_secs(state.config.idle_timeout_secs);
    let mut beat = tokio::time::interval_at(Instant::now() + idle / 3, idle / 3);
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_seen = Instant::now();
    let mut writer_done = false;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("server shutting down, closing connection");
                break;
            }
            _ = &mut writer => {
                writer_done = true;
                tracing::debug!("writer finished, closing session");
                break;
            }
            _ = beat.tick() => {
                if last_seen.elapsed() > idle {
                    tracing::info!("idle timeout, dropping connection");
                    break;
                }
                let _ = session.out.try_send(Out::Ping);
            }
            frame = stream.next() => {
                last_seen = Instant::now();
                match frame {
                    Some(Ok(Message::Text(t))) => session.on_text(&t).await,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {} // ping/pong answered by axum; binary ignored
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "socket error");
                        break;
                    }
                }
            }
        }
    }

    // Leave rooms (aborts their forwarders), then drop every sender so the writer can
    // flush a Close frame and finish; bound the wait and kill it if it is stuck.
    drop(global);
    session.cleanup().await;
    drop(session);
    if !writer_done && tokio::time::timeout(Duration::from_secs(2), &mut writer).await.is_err() {
        writer.abort();
    }
    tracing::info!("disconnected");
}

impl Session {
    async fn send(&self, m: ServerMsg) {
        let _ = self.out.send(Out::Msg(m)).await;
    }

    async fn error(&self, e: &ChatError) {
        if let ChatError::Store(source) = e {
            tracing::error!(error = %source, "storage error while handling client request");
        }
        self.send(ServerMsg::Error { message: e.to_string(), code: e.code().into() }).await;
    }

    async fn on_text(&mut self, text: &str) {
        let msg: ClientMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return self.error(&ChatError::Invalid("invalid message format".into())).await,
        };
        if !self.bucket.try_take() {
            // Typing indicators are best-effort; drop them silently.
            if !matches!(msg, ClientMsg::Typing { .. }) {
                tracing::debug!("rate limited");
                self.error(&ChatError::RateLimited).await;
            }
            return;
        }
        if let Err(e) = self.dispatch(msg).await {
            self.error(&e).await;
        }
    }

    async fn dispatch(&mut self, msg: ClientMsg) -> Result<(), ChatError> {
        let chat = self.state.chat.clone();
        match msg {
            ClientMsg::ListRooms => {
                let rooms = chat.room_list().await?;
                self.send(ServerMsg::Rooms { rooms }).await;
            }
            ClientMsg::CreateRoom { room } => {
                chat.create_room(&room).await?;
            }
            ClientMsg::Join { room } => self.join(&room).await?,
            ClientMsg::Leave { room } => {
                let room = crate::protocol::normalize_room(&room);
                if !self.leave_room(&room) {
                    return Err(ChatError::NotInRoom(room));
                }
                self.send(ServerMsg::Left { room }).await;
                chat.announce_rooms().await;
            }
            ClientMsg::Message { room, text } => {
                chat.post(self.id, &self.username, &room, &text).await?;
            }
            ClientMsg::Typing { room, typing } => chat.typing(self.id, &self.username, &room, typing),
        }
        Ok(())
    }

    async fn join(&mut self, raw: &str) -> Result<(), ChatError> {
        let chat = self.state.chat.clone();
        let j = chat.join(self.id, &self.username, raw, self.joined.len()).await?;
        // Queue `Joined` before the forwarder starts so it always arrives first.
        self.send(ServerMsg::Joined { room: j.room.clone(), history: j.history, users: j.users.clone(), has_more: j.has_more }).await;
        if let Some(rx) = j.rx {
            self.joined.insert(j.room.clone(), spawn_forwarder(rx, self.out.clone(), Some(j.room.clone())));
            tracing::info!(room = %j.room, "joined");
            chat.announce_joined(&j.room, &self.username, j.users).await;
        }
        Ok(())
    }

    /// Stop forwarding and leave the room. Returns false if not joined.
    fn leave_room(&mut self, room: &str) -> bool {
        let Some(forwarder) = self.joined.remove(room) else { return false };
        drop(forwarder);
        if self.state.chat.leave(self.id, &self.username, room) {
            tracing::info!(room, "left");
        }
        true
    }

    async fn cleanup(&mut self) {
        let rooms: Vec<String> = self.joined.keys().cloned().collect();
        for r in rooms {
            self.leave_room(&r);
        }
        self.state.chat.announce_rooms().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn origin_rules() {
        // non-browser clients send no Origin
        assert!(origin_allowed(None, Some("chat.example"), &[]));
        // same origin
        assert!(origin_allowed(Some("https://chat.example"), Some("chat.example"), &[]));
        assert!(origin_allowed(Some("http://127.0.0.1:3000"), Some("127.0.0.1:3000"), &[]));
        assert!(origin_allowed(Some("HTTP://Chat.Example/"), Some("chat.example"), &[]));
        // cross origin is refused by default
        assert!(!origin_allowed(Some("https://evil.example"), Some("chat.example"), &[]));
        assert!(!origin_allowed(Some("http://chat.example:81"), Some("chat.example"), &[]));
        assert!(!origin_allowed(Some("null"), Some("chat.example"), &[]));
        assert!(!origin_allowed(Some("https://chat.example"), None, &[]));
        // allow-list and wildcard
        assert!(origin_allowed(Some("https://app.example"), Some("chat.example"), &s(&["https://app.example"])));
        assert!(!origin_allowed(Some("https://evil.example"), Some("chat.example"), &s(&["https://app.example"])));
        assert!(origin_allowed(Some("https://anything"), Some("chat.example"), &s(&["*"])));
    }

    #[tokio::test]
    async fn lagged_consumer_is_told_to_resync() {
        // A 2-slot channel overrun by 10 messages before the consumer runs: it must be
        // notified (not silently skipped, not disconnected) and then keep receiving.
        let (tx, rx) = broadcast::channel::<ServerMsg>(2);
        for i in 0..10 {
            tx.send(ServerMsg::System { room: "r".into(), text: format!("m{i}"), ts: i }).unwrap();
        }
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let task = tokio::spawn(forward(rx, out_tx, Some("r".into())));

        match out_rx.recv().await.unwrap() {
            Out::Msg(ServerMsg::Lagged { room, skipped }) => {
                assert_eq!(room, "r");
                assert_eq!(skipped, 8);
            }
            other => panic!("expected Lagged first, got {other:?}"),
        }
        // the two newest messages survive
        assert!(matches!(out_rx.recv().await.unwrap(), Out::Msg(ServerMsg::System { text, .. }) if text == "m8"));
        assert!(matches!(out_rx.recv().await.unwrap(), Out::Msg(ServerMsg::System { text, .. }) if text == "m9"));
        drop(tx);
        task.await.unwrap(); // channel closed -> forwarder ends
    }

    #[tokio::test]
    async fn forwarder_stops_when_connection_queue_is_gone() {
        let (tx, rx) = broadcast::channel::<ServerMsg>(4);
        let (out_tx, out_rx) = mpsc::channel(1);
        drop(out_rx);
        let task = tokio::spawn(forward(rx, out_tx, None));
        tx.send(ServerMsg::Left { room: "r".into() }).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task).await.expect("forwarder must exit").unwrap();
    }

    #[tokio::test]
    async fn abort_on_drop_cancels_task() {
        let (tx, _rx) = broadcast::channel::<ServerMsg>(2);
        let rx = tx.subscribe();
        let (out_tx, _out_rx) = mpsc::channel(1);
        let handle = tokio::spawn(forward(rx, out_tx, None));
        let probe = handle.abort_handle();
        drop(AbortOnDrop(handle));
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(probe.is_finished());
    }
}
