//! Wire protocol (JSON over WebSocket) and input validation.

use crate::error::ChatError;
use serde::{Deserialize, Serialize};

pub const MAX_USERNAME_LEN: usize = 24;
pub const MAX_ROOM_LEN: usize = 32;

/// A persisted chat message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub id: i64,
    pub room: String,
    pub user: String,
    pub text: String,
    /// Unix time in milliseconds.
    pub ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoomInfo {
    pub name: String,
    pub users: usize,
}

/// Messages sent by clients.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Join { room: String },
    Leave { room: String },
    Message { room: String, text: String },
    Typing { room: String, typing: bool },
    CreateRoom { room: String },
    ListRooms,
}

/// Messages sent by the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Welcome { username: String, rooms: Vec<RoomInfo> },
    Rooms { rooms: Vec<RoomInfo> },
    Joined { room: String, history: Vec<ChatMessage>, users: Vec<String> },
    Left { room: String },
    Message(ChatMessage),
    System { room: String, text: String, ts: i64 },
    Presence { room: String, users: Vec<String> },
    Typing { room: String, user: String, typing: bool },
    /// `code` is a stable machine-readable identifier (see `ChatError::code`).
    Error {
        message: String,
        #[serde(default)]
        code: String,
    },
    /// This connection fell behind on `room`; `skipped` messages were dropped for it.
    /// Re-send `join` for the room to receive a fresh snapshot.
    Lagged { room: String, skipped: u64 },
}

/// Rooms are matched case-insensitively; used where strict validation is unnecessary.
pub fn normalize_room(raw: &str) -> String {
    raw.trim().to_lowercase()
}

pub fn validate_username(raw: &str) -> Result<String, ChatError> {
    let name = raw.trim();
    if name.is_empty() || name.chars().count() > MAX_USERNAME_LEN {
        return Err(ChatError::Invalid(format!("username must be 1-{MAX_USERNAME_LEN} characters")));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
        return Err(ChatError::Invalid("username may only contain letters, digits, '_', '-' and '.'".into()));
    }
    Ok(name.to_string())
}

/// Normalise a room name to lowercase `[a-z0-9_-]{1,32}`.
pub fn validate_room(raw: &str) -> Result<String, ChatError> {
    let name = raw.trim().to_lowercase();
    if name.is_empty() || name.len() > MAX_ROOM_LEN {
        return Err(ChatError::Invalid(format!("room name must be 1-{MAX_ROOM_LEN} characters")));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(ChatError::Invalid("room name may only contain a-z, 0-9, '_' and '-'".into()));
    }
    Ok(name)
}

/// Trim and validate message text against the configured maximum length.
pub fn validate_text(raw: &str, max: usize) -> Result<String, ChatError> {
    let t = raw.trim();
    if t.is_empty() {
        return Err(ChatError::Invalid("message is empty".into()));
    }
    if t.chars().count() > max {
        return Err(ChatError::Invalid(format!("message too long (max {max} characters)")));
    }
    Ok(t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn username_rules() {
        assert_eq!(validate_username("  alice ").unwrap(), "alice");
        assert!(validate_username("").is_err());
        assert!(validate_username("a b").is_err());
        assert!(validate_username(&"x".repeat(25)).is_err());
        assert!(validate_username("<script>").is_err());
    }

    #[test]
    fn room_rules() {
        assert_eq!(validate_room(" General ").unwrap(), "general");
        assert!(validate_room("bad room").is_err());
        assert!(validate_room("").is_err());
        assert!(validate_room(&"a".repeat(33)).is_err());
    }

    #[test]
    fn text_rules() {
        assert_eq!(validate_text(" hi ", 10).unwrap(), "hi");
        assert!(validate_text("   ", 10).is_err());
        assert!(validate_text("12345678901", 10).is_err());
        assert!(validate_text("ééééé", 5).is_ok());
    }

    #[test]
    fn parses_client_messages() {
        let m: ClientMsg = serde_json::from_str(r#"{"type":"join","room":"general"}"#).unwrap();
        assert_eq!(m, ClientMsg::Join { room: "general".into() });
        let m: ClientMsg = serde_json::from_str(r#"{"type":"list_rooms"}"#).unwrap();
        assert_eq!(m, ClientMsg::ListRooms);
        assert!(serde_json::from_str::<ClientMsg>(r#"{"type":"nope"}"#).is_err());
    }

    #[test]
    fn serialises_server_messages() {
        let m = ServerMsg::Message(ChatMessage { id: 1, room: "r".into(), user: "u".into(), text: "t".into(), ts: 5 });
        let v: serde_json::Value = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["user"], "u");
    }
}
