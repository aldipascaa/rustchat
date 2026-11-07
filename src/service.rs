//! Chat domain logic. Knows nothing about WebSockets or HTTP: transport code (ws.rs, routes.rs)
//! calls in, and persistence is reached only through the [`Repo`]/[`Store`] abstraction.

use crate::{
    config::Config,
    error::ChatError,
    hub::Hub,
    now_ms,
    protocol::{normalize_room, validate_room, validate_text, ChatMessage, RoomInfo, ServerMsg},
    registry::Registry,
    store::{CreateOutcome, PrunePolicy, PruneReport, Repo},
};
use std::sync::Arc;
use tokio::sync::broadcast;

const DAY_MS: i64 = 86_400_000;

/// Result of joining a room.
pub struct JoinOutcome {
    pub room: String,
    pub history: Vec<ChatMessage>,
    pub users: Vec<String>,
    /// `Some` when the connection was newly added (`None` for a repeated join = snapshot refresh).
    pub rx: Option<broadcast::Receiver<ServerMsg>>,
}

pub struct ChatService {
    repo: Repo,
    hub: Hub,
    registry: Arc<Registry>,
    config: Arc<Config>,
}

impl ChatService {
    pub fn new(repo: Repo, config: Arc<Config>) -> ChatService {
        ChatService {
            hub: Hub::new(config.broadcast_capacity),
            registry: Registry::new(&config.reserved_usernames, config.max_connections, config.max_connections_per_ip),
            repo,
            config,
        }
    }

    pub fn hub(&self) -> &Hub {
        &self.hub
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn repo(&self) -> &Repo {
        &self.repo
    }

    pub async fn room_list(&self) -> Result<Vec<RoomInfo>, ChatError> {
        let names = self.repo.list_rooms().await?;
        Ok(self.hub.room_infos(&names))
    }

    /// Tell every connection about the current room list. Best effort: failure is logged.
    pub async fn announce_rooms(&self) {
        match self.room_list().await {
            Ok(rooms) => self.hub.announce(ServerMsg::Rooms { rooms }),
            Err(e) => tracing::error!(error = %e, "could not announce room list"),
        }
    }

    /// Create a room (idempotent). Returns the normalised name.
    pub async fn create_room(&self, raw: &str) -> Result<String, ChatError> {
        let room = validate_room(raw)?;
        match self.repo.create_room(&room, self.config.max_rooms).await? {
            CreateOutcome::Created => tracing::info!(room, "room created"),
            CreateOutcome::AlreadyExists => {}
            CreateOutcome::LimitReached => return Err(ChatError::RoomLimit(self.config.max_rooms)),
        }
        self.announce_rooms().await;
        Ok(room)
    }

    /// Add `conn` to a room and fetch its history. `joined_count` is how many rooms the
    /// connection is already in. The caller must deliver `Joined` before reading `rx`.
    pub async fn join(&self, conn: u64, user: &str, raw: &str, joined_count: usize) -> Result<JoinOutcome, ChatError> {
        let room = validate_room(raw)?;
        if !self.repo.room_exists(&room).await? {
            return Err(ChatError::RoomNotFound(room));
        }
        let already = self.hub.is_member(&room, conn);
        if !already && joined_count >= self.config.max_joined_rooms {
            return Err(ChatError::JoinLimit(self.config.max_joined_rooms));
        }
        let (rx, users) = if already {
            (None, self.hub.users(&room))
        } else {
            let (rx, users) = self.hub.join(&room, conn, user);
            (Some(rx), users)
        };
        // Subscribe first, then read history: a message racing the join shows up in both
        // (clients de-duplicate by id) rather than being lost.
        match self.repo.recent_messages(&room, self.config.history_limit).await {
            Ok(history) => Ok(JoinOutcome { room, history, users, rx }),
            Err(e) => {
                if rx.is_some() {
                    self.hub.leave(&room, conn);
                }
                Err(e.into())
            }
        }
    }

    /// Announce a completed join to the room and refresh everyone's room list.
    pub async fn announce_joined(&self, room: &str, user: &str, users: Vec<String>) {
        self.hub.broadcast(room, ServerMsg::System { room: room.into(), text: format!("{user} joined the room"), ts: now_ms() });
        self.hub.broadcast(room, ServerMsg::Presence { room: room.into(), users });
        self.announce_rooms().await;
    }

    /// Remove `conn` from `room` and tell the others. Returns false if it was not a member.
    pub fn leave(&self, conn: u64, user: &str, room: &str) -> bool {
        let Some(users) = self.hub.leave(room, conn) else { return false };
        self.hub.broadcast(room, ServerMsg::Typing { room: room.into(), user: user.into(), typing: false });
        self.hub.broadcast(room, ServerMsg::System { room: room.into(), text: format!("{user} left the room"), ts: now_ms() });
        self.hub.broadcast(room, ServerMsg::Presence { room: room.into(), users });
        true
    }

    /// Validate, persist and broadcast a message.
    pub async fn post(&self, conn: u64, user: &str, raw_room: &str, raw_text: &str) -> Result<ChatMessage, ChatError> {
        let room = normalize_room(raw_room);
        if !self.hub.is_member(&room, conn) {
            return Err(ChatError::NotInRoom(room));
        }
        let text = validate_text(raw_text, self.config.max_message_len)?;
        let Some(msg) = self.repo.insert_message(&room, user, &text).await? else {
            return Err(ChatError::RoomNotFound(room));
        };
        self.hub.broadcast(&room, ServerMsg::Typing { room: room.clone(), user: user.into(), typing: false });
        self.hub.broadcast(&room, ServerMsg::Message(msg.clone()));
        Ok(msg)
    }

    /// Relay a typing indicator (best effort; ignored unless `conn` is in the room).
    pub fn typing(&self, conn: u64, user: &str, raw_room: &str, typing: bool) {
        let room = normalize_room(raw_room);
        if self.hub.is_member(&room, conn) {
            self.hub.broadcast(&room, ServerMsg::Typing { room: room.clone(), user: user.into(), typing });
        }
    }

    /// History for the REST API. Unknown or malformed room names are `RoomNotFound`.
    pub async fn history(&self, raw_room: &str, limit: Option<usize>) -> Result<(String, Vec<ChatMessage>), ChatError> {
        let not_found = || ChatError::RoomNotFound(raw_room.trim().to_string());
        let room = validate_room(raw_room).map_err(|_| not_found())?;
        if !self.repo.room_exists(&room).await? {
            return Err(not_found());
        }
        let limit = limit.unwrap_or(self.config.history_limit).clamp(1, 200);
        let messages = self.repo.recent_messages(&room, limit).await?;
        Ok((room, messages))
    }

    /// The retention policy derived from configuration, evaluated at `now`.
    pub fn prune_policy(&self, now: i64) -> PrunePolicy {
        let c = &self.config;
        PrunePolicy {
            messages_before_ms: (c.retention_days > 0).then(|| now - c.retention_days as i64 * DAY_MS),
            max_per_room: (c.max_messages_per_room > 0).then_some(c.max_messages_per_room),
            idle_rooms_before_ms: (c.room_ttl_days > 0).then(|| now - c.room_ttl_days as i64 * DAY_MS),
            protected_rooms: self.hub.active_rooms(),
        }
    }

    /// Apply the retention policy once; announces the room list if rooms were removed.
    pub async fn prune_once(&self) -> Result<PruneReport, ChatError> {
        let report = self.repo.prune(self.prune_policy(now_ms())).await?;
        if report != PruneReport::default() {
            tracing::info!(messages = report.messages_deleted, rooms = report.rooms_deleted, "pruned old data");
        }
        if report.rooms_deleted > 0 {
            self.announce_rooms().await;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{error::StoreError, sqlite::SqliteStore, store::Store};

    fn svc_with(cfg: Config) -> ChatService {
        ChatService::new(Repo::new(Arc::new(SqliteStore::open(":memory:").unwrap())), Arc::new(cfg))
    }

    /// A store whose every call fails, to prove storage errors surface as typed errors.
    struct BrokenStore;
    fn boom<T>() -> Result<T, StoreError> {
        Err(StoreError::Poisoned)
    }
    impl Store for BrokenStore {
        fn list_rooms(&self) -> Result<Vec<String>, StoreError> { boom() }
        fn room_exists(&self, _: &str) -> Result<bool, StoreError> { boom() }
        fn create_room(&self, _: &str, _: usize) -> Result<CreateOutcome, StoreError> { boom() }
        fn insert_message(&self, _: &str, _: &str, _: &str) -> Result<Option<ChatMessage>, StoreError> { boom() }
        fn recent_messages(&self, _: &str, _: usize) -> Result<Vec<ChatMessage>, StoreError> { boom() }
        fn prune(&self, _: &PrunePolicy) -> Result<PruneReport, StoreError> { boom() }
        fn ping(&self) -> Result<(), StoreError> { boom() }
    }

    #[tokio::test]
    async fn storage_failure_is_a_typed_error_not_a_panic() {
        let svc = ChatService::new(Repo::new(Arc::new(BrokenStore)), Arc::new(Config::default()));
        assert!(matches!(svc.room_list().await, Err(ChatError::Store(_))));
        assert!(matches!(svc.create_room("x").await, Err(ChatError::Store(_))));
        assert!(matches!(svc.join(1, "a", "general", 0).await, Err(ChatError::Store(_))));
        assert!(matches!(svc.history("general", None).await, Err(ChatError::Store(_))));
        assert!(matches!(svc.prune_once().await, Err(ChatError::Store(_))));
        assert_eq!(ChatError::Store(StoreError::Poisoned).to_string(), "internal error");
    }

    #[tokio::test]
    async fn join_post_leave_flow() {
        let svc = svc_with(Config::default());
        let j = svc.join(1, "alice", "General", 0).await.unwrap();
        assert_eq!(j.room, "general");
        let mut rx = j.rx.unwrap();
        svc.post(1, "alice", "general", " hi ").await.unwrap();
        assert!(matches!(rx.recv().await.unwrap(), ServerMsg::Typing { .. }));
        assert!(matches!(rx.recv().await.unwrap(), ServerMsg::Message(m) if m.text == "hi"));
        assert!(matches!(svc.post(2, "bob", "general", "x").await, Err(ChatError::NotInRoom(_))));
        assert!(svc.leave(1, "alice", "general"));
        assert!(!svc.leave(1, "alice", "general"));
    }

    #[tokio::test]
    async fn room_cap_and_join_cap() {
        let svc = svc_with(Config { max_rooms: 4, max_joined_rooms: 1, ..Config::default() });
        svc.create_room("fourth").await.unwrap();
        assert!(matches!(svc.create_room("fifth").await, Err(ChatError::RoomLimit(4))));
        svc.create_room("fourth").await.unwrap(); // idempotent, not counted again
        let _first = svc.join(1, "a", "general", 0).await.unwrap();
        assert!(matches!(svc.join(1, "a", "random", 1).await, Err(ChatError::JoinLimit(1))));
        // re-joining the same room is a refresh, not a new join
        assert!(svc.join(1, "a", "general", 1).await.unwrap().rx.is_none());
    }

    #[tokio::test]
    async fn prune_once_applies_config_and_protects_occupied_rooms() {
        let svc = svc_with(Config { max_messages_per_room: 50, room_ttl_days: 7, ..Config::default() });
        for i in 0..60 {
            svc.repo().insert_message("general", "a", &format!("m{i}")).await.unwrap();
        }
        let rep = svc.prune_once().await.unwrap();
        assert_eq!(rep.messages_deleted, 10);
        assert_eq!(svc.repo().recent_messages("general", 200).await.unwrap().len(), 50);
        // occupied rooms are in the protected list handed to the store
        let _j = svc.join(1, "a", "random", 0).await.unwrap();
        assert!(svc.prune_policy(now_ms()).protected_rooms.contains(&"random".to_string()));
    }

    #[test]
    fn policy_reflects_config() {
        let svc = svc_with(Config { retention_days: 2, max_messages_per_room: 0, room_ttl_days: 0, ..Config::default() });
        let p = svc.prune_policy(10 * DAY_MS);
        assert_eq!(p.messages_before_ms, Some(8 * DAY_MS));
        assert_eq!(p.max_per_room, None);
        assert_eq!(p.idle_rooms_before_ms, None);
    }
}
