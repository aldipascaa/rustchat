//! SQLite persistence (rusqlite behind a mutex, calls run on the blocking pool).

use crate::protocol::ChatMessage;
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

pub const DEFAULT_ROOMS: [&str; 3] = ["general", "random", "rust"];

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Db {
    pub fn open(path: &str) -> Result<Db> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path).with_context(|| format!("opening database at {path}"))?
        };
        if path != ":memory:" {
            // journal_mode returns a row, so use query_row.
            let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS rooms (
                name TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room TEXT NOT NULL,
                user TEXT NOT NULL,
                text TEXT NOT NULL,
                ts INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages(room, id);",
        )?;
        for r in DEFAULT_ROOMS {
            conn.execute("INSERT OR IGNORE INTO rooms (name, created_at) VALUES (?1, ?2)", params![r, now_ms()])?;
        }
        Ok(Db { conn: Arc::new(Mutex::new(conn)) })
    }

    async fn call<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;
            f(&guard).map_err(anyhow::Error::from)
        })
        .await?
    }

    pub async fn list_rooms(&self) -> Result<Vec<String>> {
        self.call(|c| {
            let mut st = c.prepare("SELECT name FROM rooms ORDER BY name")?;
            let rows = st.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect()
        })
        .await
    }

    pub async fn room_exists(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.call(move |c| c.query_row("SELECT COUNT(*) FROM rooms WHERE name=?1", [name], |r| r.get::<_, i64>(0)).map(|n| n > 0))
            .await
    }

    /// Returns true when the room was newly created.
    pub async fn create_room(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.call(move |c| c.execute("INSERT OR IGNORE INTO rooms (name, created_at) VALUES (?1, ?2)", params![name, now_ms()]).map(|n| n > 0))
            .await
    }

    pub async fn insert_message(&self, room: &str, user: &str, text: &str) -> Result<ChatMessage> {
        let (room, user, text) = (room.to_string(), user.to_string(), text.to_string());
        self.call(move |c| {
            let ts = now_ms();
            c.execute("INSERT INTO messages (room, user, text, ts) VALUES (?1, ?2, ?3, ?4)", params![room, user, text, ts])?;
            Ok(ChatMessage { id: c.last_insert_rowid(), room, user, text, ts })
        })
        .await
    }

    /// The most recent `limit` messages of a room, oldest first.
    pub async fn recent_messages(&self, room: &str, limit: usize) -> Result<Vec<ChatMessage>> {
        let room = room.to_string();
        self.call(move |c| {
            let mut st = c.prepare("SELECT id, room, user, text, ts FROM messages WHERE room=?1 ORDER BY id DESC LIMIT ?2")?;
            let rows = st.query_map(params![room, limit as i64], |r| {
                Ok(ChatMessage { id: r.get(0)?, room: r.get(1)?, user: r.get(2)?, text: r.get(3)?, ts: r.get(4)? })
            })?;
            let mut v: Vec<ChatMessage> = rows.collect::<rusqlite::Result<_>>()?;
            v.reverse();
            Ok(v)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn seeds_default_rooms() {
        let db = Db::open(":memory:").unwrap();
        assert_eq!(db.list_rooms().await.unwrap(), vec!["general", "random", "rust"]);
    }

    #[tokio::test]
    async fn create_room_is_idempotent() {
        let db = Db::open(":memory:").unwrap();
        assert!(db.create_room("new").await.unwrap());
        assert!(!db.create_room("new").await.unwrap());
        assert!(db.room_exists("new").await.unwrap());
        assert!(!db.room_exists("ghost").await.unwrap());
    }

    #[tokio::test]
    async fn history_is_last_n_oldest_first() {
        let db = Db::open(":memory:").unwrap();
        for i in 0..60 {
            db.insert_message("general", "bob", &format!("m{i}")).await.unwrap();
        }
        db.insert_message("random", "bob", "other").await.unwrap();
        let h = db.recent_messages("general", 50).await.unwrap();
        assert_eq!(h.len(), 50);
        assert_eq!(h.first().unwrap().text, "m10");
        assert_eq!(h.last().unwrap().text, "m59");
        assert!(h.windows(2).all(|w| w[0].id < w[1].id));
    }
}
