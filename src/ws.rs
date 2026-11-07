//! WebSocket connection handling.

use crate::{
    db::now_ms,
    protocol::{validate_room, validate_text, validate_username, ClientMsg, ServerMsg},
    ratelimit::TokenBucket,
    AppState,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);
const MAX_FRAME_BYTES: usize = 16 * 1024;

#[derive(Deserialize)]
pub struct WsQuery {
    username: Option<String>,
}

pub async fn ws_handler(ws: WebSocketUpgrade, Query(q): Query<WsQuery>, State(state): State<AppState>) -> Response {
    let username = match validate_username(q.username.as_deref().unwrap_or("")) {
        Ok(u) => u,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state, username))
}

fn spawn_forwarder(mut rx: broadcast::Receiver<ServerMsg>, out: mpsc::Sender<ServerMsg>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(m) => {
                    if out.send(m).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => tracing::warn!(skipped = n, "receiver lagged"),
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

struct Session {
    id: u64,
    username: String,
    state: AppState,
    out: mpsc::Sender<ServerMsg>,
    joined: HashMap<String, JoinHandle<()>>,
    bucket: TokenBucket,
}

async fn handle_socket(socket: WebSocket, state: AppState, username: String) {
    let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    tracing::info!(id, user = %username, "connected");
    let inner = async move {
        let (mut sink, mut stream) = socket.split();
        let (out_tx, mut out_rx) = mpsc::channel::<ServerMsg>(512);
        let writer = tokio::spawn(async move {
            while let Some(m) = out_rx.recv().await {
                let Ok(text) = serde_json::to_string(&m) else { continue };
                if sink.send(Message::Text(text)).await.is_err() {
                    return;
                }
            }
            let _ = sink.send(Message::Close(None)).await;
        });

        let mut shutdown = state.shutdown.subscribe();
        let global = spawn_forwarder(state.hub.subscribe_global(), out_tx.clone());
        let mut session = Session {
            id,
            username: username.clone(),
            bucket: TokenBucket::new(state.config.rate_capacity, state.config.rate_refill_per_sec),
            state: state.clone(),
            out: out_tx,
            joined: HashMap::new(),
        };

        let rooms = session.room_list().await;
        session.send(ServerMsg::Welcome { username, rooms }).await;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::info!("server shutting down, closing connection");
                    break;
                }
                frame = stream.next() => {
                    match frame {
                        Some(Ok(Message::Text(t))) => session.on_text(&t).await,
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {} // ping/pong handled by axum; binary ignored
                        Some(Err(e)) => {
                            tracing::debug!(error = %e, "socket error");
                            break;
                        }
                    }
                }
            }
        }

        global.abort();
        session.cleanup().await;
        drop(session);
        // The writer finishes once every sender is dropped; don't wait forever.
        let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    };
    inner.await;
    tracing::info!(id, "disconnected");
}

impl Session {
    async fn send(&self, m: ServerMsg) {
        let _ = self.out.send(m).await;
    }

    async fn error(&self, message: impl Into<String>) {
        self.send(ServerMsg::Error { message: message.into() }).await;
    }

    async fn room_list(&self) -> Vec<crate::protocol::RoomInfo> {
        let names = self.state.db.list_rooms().await.unwrap_or_default();
        self.state.hub.room_infos(&names)
    }

    async fn announce_rooms(&self) {
        let rooms = self.room_list().await;
        self.state.hub.announce(ServerMsg::Rooms { rooms });
    }

    async fn on_text(&mut self, text: &str) {
        let msg: ClientMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return self.error("invalid message format").await,
        };
        if !self.bucket.try_take() {
            // Typing indicators are best-effort; drop them silently.
            if !matches!(msg, ClientMsg::Typing { .. }) {
                tracing::debug!(user = %self.username, "rate limited");
                self.error("rate limit exceeded, slow down").await;
            }
            return;
        }
        match msg {
            ClientMsg::ListRooms => {
                let rooms = self.room_list().await;
                self.send(ServerMsg::Rooms { rooms }).await;
            }
            ClientMsg::CreateRoom { room } => self.create_room(&room).await,
            ClientMsg::Join { room } => self.join(&room).await,
            ClientMsg::Leave { room } => self.leave(&room).await,
            ClientMsg::Message { room, text } => self.message(&room, &text).await,
            ClientMsg::Typing { room, typing } => {
                if self.joined.contains_key(&room) {
                    self.state.hub.broadcast(&room, ServerMsg::Typing { room: room.clone(), user: self.username.clone(), typing });
                }
            }
        }
    }

    async fn create_room(&mut self, raw: &str) {
        let room = match validate_room(raw) {
            Ok(r) => r,
            Err(e) => return self.error(e).await,
        };
        match self.state.db.create_room(&room).await {
            Ok(created) => {
                if created {
                    tracing::info!(room, "room created");
                }
                self.announce_rooms().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "create_room failed");
                self.error("could not create room").await;
            }
        }
    }

    async fn join(&mut self, raw: &str) {
        let room = match validate_room(raw) {
            Ok(r) => r,
            Err(e) => return self.error(e).await,
        };
        match self.state.db.room_exists(&room).await {
            Ok(true) => {}
            Ok(false) => return self.error(format!("room '{room}' does not exist")).await,
            Err(e) => {
                tracing::error!(error = %e, "room_exists failed");
                return self.error("internal error").await;
            }
        }
        let already = self.joined.contains_key(&room);
        let (rx, users) = if already {
            // Re-send a fresh snapshot without touching membership.
            (None, self.state.hub.users(&room))
        } else {
            let (rx, users) = self.state.hub.join(&room, self.id, &self.username);
            (Some(rx), users)
        };
        let history = self.state.db.recent_messages(&room, self.state.config.history_limit).await.unwrap_or_default();
        // Queue `Joined` before the forwarder starts so it always arrives first.
        self.send(ServerMsg::Joined { room: room.clone(), history, users: users.clone() }).await;
        if let Some(rx) = rx {
            self.joined.insert(room.clone(), spawn_forwarder(rx, self.out.clone()));
            tracing::info!(room, "joined");
            self.state.hub.broadcast(&room, ServerMsg::System { room: room.clone(), text: format!("{} joined the room", self.username), ts: now_ms() });
            self.state.hub.broadcast(&room, ServerMsg::Presence { room: room.clone(), users });
            self.announce_rooms().await;
        }
    }

    async fn leave(&mut self, raw: &str) {
        let room = raw.trim().to_lowercase();
        if !self.leave_room(&room).await {
            return self.error(format!("you are not in room '{room}'")).await;
        }
        self.send(ServerMsg::Left { room }).await;
        self.announce_rooms().await;
    }

    /// Remove from hub + broadcast departure. Returns false if not joined.
    async fn leave_room(&mut self, room: &str) -> bool {
        let Some(handle) = self.joined.remove(room) else { return false };
        handle.abort();
        if let Some(users) = self.state.hub.leave(room, self.id) {
            tracing::info!(room, "left");
            let h = &self.state.hub;
            h.broadcast(room, ServerMsg::Typing { room: room.into(), user: self.username.clone(), typing: false });
            h.broadcast(room, ServerMsg::System { room: room.into(), text: format!("{} left the room", self.username), ts: now_ms() });
            h.broadcast(room, ServerMsg::Presence { room: room.into(), users });
        }
        true
    }

    async fn message(&mut self, room: &str, text: &str) {
        if !self.joined.contains_key(room) {
            return self.error(format!("join '{room}' before sending messages")).await;
        }
        let text = match validate_text(text, self.state.config.max_message_len) {
            Ok(t) => t,
            Err(e) => return self.error(e).await,
        };
        match self.state.db.insert_message(room, &self.username, &text).await {
            Ok(m) => {
                self.state.hub.broadcast(room, ServerMsg::Typing { room: room.into(), user: self.username.clone(), typing: false });
                self.state.hub.broadcast(room, ServerMsg::Message(m));
            }
            Err(e) => {
                tracing::error!(error = %e, "insert_message failed");
                self.error("could not save message").await;
            }
        }
    }

    async fn cleanup(&mut self) {
        let rooms: Vec<String> = self.joined.keys().cloned().collect();
        for r in rooms {
            self.leave_room(&r).await;
        }
        self.announce_rooms().await;
    }
}
