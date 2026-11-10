//! Persistence abstraction.
//!
//! [`Store`] is a small synchronous repository trait: the domain depends on it, never on
//! SQLite. [`Repo`] wraps any `Store` and runs each call on tokio's blocking pool, so the
//! async server never blocks a worker thread. Swap in another backend (or a fake in tests)
//! by implementing `Store`.

use crate::{error::StoreError, protocol::ChatMessage};
use std::sync::Arc;

pub const DEFAULT_ROOMS: [&str; 3] = ["general", "random", "rust"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
    LimitReached,
}

/// What a prune pass should delete. `None` disables that rule.
#[derive(Debug, Clone, Default)]
pub struct PrunePolicy {
    /// Delete messages with `ts` before this (unix ms).
    pub messages_before_ms: Option<i64>,
    /// Keep only the newest N messages in every room.
    pub max_per_room: Option<usize>,
    /// Delete non-default rooms created before this with no newer messages.
    pub idle_rooms_before_ms: Option<i64>,
    /// Rooms that must never be deleted (in addition to [`DEFAULT_ROOMS`]), e.g. rooms with people in them.
    pub protected_rooms: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneReport {
    pub messages_deleted: usize,
    pub rooms_deleted: usize,
}

pub trait Store: Send + Sync + 'static {
    fn list_rooms(&self) -> Result<Vec<String>, StoreError>;
    fn room_exists(&self, name: &str) -> Result<bool, StoreError>;
    /// Create a room unless it exists or `max_rooms` would be exceeded (atomic).
    fn create_room(&self, name: &str, max_rooms: usize) -> Result<CreateOutcome, StoreError>;
    /// Persist a message. `Ok(None)` means the room no longer exists.
    fn insert_message(&self, room: &str, user: &str, text: &str) -> Result<Option<ChatMessage>, StoreError>;
    /// The most recent `limit` messages of a room, oldest first.
    /// With `before`, only messages with a smaller id (the page older than that message).
    fn recent_messages(&self, room: &str, before: Option<i64>, limit: usize) -> Result<Vec<ChatMessage>, StoreError>;
    fn prune(&self, policy: &PrunePolicy) -> Result<PruneReport, StoreError>;
    /// Cheap liveness probe used by the health endpoint.
    fn ping(&self) -> Result<(), StoreError>;
}

/// Async facade over a [`Store`]; cheap to clone.
#[derive(Clone)]
pub struct Repo {
    store: Arc<dyn Store>,
}

impl Repo {
    pub fn new(store: Arc<dyn Store>) -> Repo {
        Repo { store }
    }

    async fn run<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&dyn Store) -> Result<T, StoreError> + Send + 'static,
        T: Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(store.as_ref())).await?
    }

    pub async fn list_rooms(&self) -> Result<Vec<String>, StoreError> {
        self.run(|s| s.list_rooms()).await
    }

    pub async fn room_exists(&self, name: &str) -> Result<bool, StoreError> {
        let name = name.to_string();
        self.run(move |s| s.room_exists(&name)).await
    }

    pub async fn create_room(&self, name: &str, max_rooms: usize) -> Result<CreateOutcome, StoreError> {
        let name = name.to_string();
        self.run(move |s| s.create_room(&name, max_rooms)).await
    }

    pub async fn insert_message(&self, room: &str, user: &str, text: &str) -> Result<Option<ChatMessage>, StoreError> {
        let (room, user, text) = (room.to_string(), user.to_string(), text.to_string());
        self.run(move |s| s.insert_message(&room, &user, &text)).await
    }

    pub async fn recent_messages(&self, room: &str, before: Option<i64>, limit: usize) -> Result<Vec<ChatMessage>, StoreError> {
        let room = room.to_string();
        self.run(move |s| s.recent_messages(&room, before, limit)).await
    }

    pub async fn prune(&self, policy: PrunePolicy) -> Result<PruneReport, StoreError> {
        self.run(move |s| s.prune(&policy)).await
    }

    pub async fn ping(&self) -> Result<(), StoreError> {
        self.run(|s| s.ping()).await
    }
}
