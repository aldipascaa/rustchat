//! Typed errors for each layer. Nothing on a runtime path panics; every failure
//! ends up as one of these and is mapped to a wire/HTTP representation at the edge.

use thiserror::Error;

/// Failures reported by a [`crate::store::Store`] implementation.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("could not open database at {path}: {source}")]
    Open { path: String, source: rusqlite::Error },
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("database lock poisoned")]
    Poisoned,
    #[error("storage task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// Invalid configuration, reported once at startup.
#[derive(Debug, Error, PartialEq)]
pub enum ConfigError {
    #[error("{key}: cannot parse {value:?} ({expected})")]
    Parse { key: &'static str, value: String, expected: &'static str },
    #[error("{key}: {reason}")]
    Invalid { key: &'static str, reason: String },
}

/// Failure to construct the application.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid configuration: {0}")]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Domain/service-level errors. `Display` is safe to show to clients.
#[derive(Debug, Error)]
pub enum ChatError {
    #[error("{0}")]
    Invalid(String),
    #[error("room '{0}' does not exist")]
    RoomNotFound(String),
    #[error("you are not in room '{0}'")]
    NotInRoom(String),
    #[error("username '{0}' is already in use")]
    UsernameTaken(String),
    #[error("username '{0}' is reserved")]
    UsernameReserved(String),
    #[error("room limit reached (max {0})")]
    RoomLimit(usize),
    #[error("you can be in at most {0} rooms at once")]
    JoinLimit(usize),
    #[error("server is full, try again later")]
    ServerFull,
    #[error("too many connections from your address")]
    TooManyConnections,
    #[error("rate limit exceeded, slow down")]
    RateLimited,
    #[error("origin not allowed")]
    ForbiddenOrigin,
    /// Details are logged server-side and never sent to the client.
    #[error("internal error")]
    Store(#[from] StoreError),
}

impl ChatError {
    /// Stable machine-readable code sent in the `error` frame.
    pub fn code(&self) -> &'static str {
        match self {
            ChatError::Invalid(_) => "invalid",
            ChatError::RoomNotFound(_) => "room_not_found",
            ChatError::NotInRoom(_) => "not_in_room",
            ChatError::UsernameTaken(_) => "username_taken",
            ChatError::UsernameReserved(_) => "username_reserved",
            ChatError::RoomLimit(_) => "room_limit",
            ChatError::JoinLimit(_) => "join_limit",
            ChatError::ServerFull => "server_full",
            ChatError::TooManyConnections => "too_many_connections",
            ChatError::RateLimited => "rate_limited",
            ChatError::ForbiddenOrigin => "forbidden_origin",
            ChatError::Store(_) => "internal",
        }
    }

    /// WebSocket close code used when this error rejects a connection.
    pub fn close_code(&self) -> u16 {
        match self {
            ChatError::UsernameTaken(_) | ChatError::UsernameReserved(_) => 4009,
            ChatError::ServerFull | ChatError::TooManyConnections => 1013,
            _ => 1008,
        }
    }
}
