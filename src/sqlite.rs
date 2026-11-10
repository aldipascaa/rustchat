//! SQLite implementation of [`Store`] (one rusqlite connection behind a mutex).

use crate::{
    error::StoreError,
    now_ms,
    protocol::ChatMessage,
    store::{CreateOutcome, PrunePolicy, PruneReport, Store, DEFAULT_ROOMS},
};
use rusqlite::{params, Connection};
use std::sync::{Mutex, MutexGuard};

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (and migrate) a database file, or `":memory:"` for an ephemeral one.
    pub fn open(path: &str) -> Result<SqliteStore, StoreError> {
        let memory = path == ":memory:";
        let conn = if memory { Connection::open_in_memory() } else { Connection::open(path) }
            .map_err(|source| StoreError::Open { path: path.to_string(), source })?;
        if !memory {
            // journal_mode returns a row, so use query_row.
            let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
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
             CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages(room, id);
             CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);",
        )?;
        for r in DEFAULT_ROOMS {
            conn.execute("INSERT OR IGNORE INTO rooms (name, created_at) VALUES (?1, ?2)", params![r, now_ms()])?;
        }
        Ok(SqliteStore { conn: Mutex::new(conn) })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.conn.lock().map_err(|_| StoreError::Poisoned)
    }
}

impl Store for SqliteStore {
    fn list_rooms(&self) -> Result<Vec<String>, StoreError> {
        let c = self.lock()?;
        let mut st = c.prepare("SELECT name FROM rooms ORDER BY name")?;
        let rows = st.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn room_exists(&self, name: &str) -> Result<bool, StoreError> {
        let c = self.lock()?;
        Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM rooms WHERE name=?1)", [name], |r| r.get(0))?)
    }

    fn create_room(&self, name: &str, max_rooms: usize) -> Result<CreateOutcome, StoreError> {
        // The mutex is held for check + insert, so the cap cannot be raced past.
        let c = self.lock()?;
        let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM rooms WHERE name=?1)", [name], |r| r.get(0))?;
        if exists {
            return Ok(CreateOutcome::AlreadyExists);
        }
        let count: i64 = c.query_row("SELECT COUNT(*) FROM rooms", [], |r| r.get(0))?;
        if count as usize >= max_rooms {
            return Ok(CreateOutcome::LimitReached);
        }
        c.execute("INSERT INTO rooms (name, created_at) VALUES (?1, ?2)", params![name, now_ms()])?;
        Ok(CreateOutcome::Created)
    }

    fn insert_message(&self, room: &str, user: &str, text: &str) -> Result<Option<ChatMessage>, StoreError> {
        let c = self.lock()?;
        let ts = now_ms();
        let n = c.execute(
            "INSERT INTO messages (room, user, text, ts) SELECT ?1, ?2, ?3, ?4 WHERE EXISTS (SELECT 1 FROM rooms WHERE name=?1)",
            params![room, user, text, ts],
        )?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(ChatMessage { id: c.last_insert_rowid(), room: room.into(), user: user.into(), text: text.into(), ts }))
    }

    fn recent_messages(&self, room: &str, before: Option<i64>, limit: usize) -> Result<Vec<ChatMessage>, StoreError> {
        let c = self.lock()?;
        let mut st = c.prepare("SELECT id, room, user, text, ts FROM messages WHERE room=?1 AND id < ?2 ORDER BY id DESC LIMIT ?3")?;
        let rows = st.query_map(params![room, before.unwrap_or(i64::MAX), limit as i64], |r| {
            Ok(ChatMessage { id: r.get(0)?, room: r.get(1)?, user: r.get(2)?, text: r.get(3)?, ts: r.get(4)? })
        })?;
        let mut v: Vec<ChatMessage> = rows.collect::<rusqlite::Result<_>>()?;
        v.reverse();
        Ok(v)
    }

    fn prune(&self, policy: &PrunePolicy) -> Result<PruneReport, StoreError> {
        let mut c = self.lock()?;
        let tx = c.transaction()?;
        let mut report = PruneReport::default();

        if let Some(before) = policy.messages_before_ms {
            report.messages_deleted += tx.execute("DELETE FROM messages WHERE ts < ?1", [before])?;
        }
        if let Some(max) = policy.max_per_room {
            report.messages_deleted += tx.execute(
                "DELETE FROM messages WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY room ORDER BY id DESC) AS rn FROM messages
                    ) WHERE rn > ?1
                 )",
                [max as i64],
            )?;
        }
        if let Some(before) = policy.idle_rooms_before_ms {
            let idle: Vec<String> = {
                let mut st = tx.prepare(
                    "SELECT name FROM rooms r WHERE r.created_at < ?1
                     AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.room = r.name AND m.ts >= ?1)",
                )?;
                let rows = st.query_map([before], |r| r.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for name in idle {
                if DEFAULT_ROOMS.contains(&name.as_str()) || policy.protected_rooms.contains(&name) {
                    continue;
                }
                report.messages_deleted += tx.execute("DELETE FROM messages WHERE room=?1", [&name])?;
                report.rooms_deleted += tx.execute("DELETE FROM rooms WHERE name=?1", [&name])?;
            }
        }
        tx.commit()?;
        Ok(report)
    }

    fn ping(&self) -> Result<(), StoreError> {
        let c = self.lock()?;
        c.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Repo;
    use std::sync::Arc;

    fn repo() -> Repo {
        Repo::new(Arc::new(SqliteStore::open(":memory:").unwrap()))
    }

    #[tokio::test]
    async fn seeds_default_rooms() {
        assert_eq!(repo().list_rooms().await.unwrap(), vec!["general", "random", "rust"]);
    }

    #[tokio::test]
    async fn create_room_is_idempotent_and_capped() {
        let db = repo();
        assert_eq!(db.create_room("new", 10).await.unwrap(), CreateOutcome::Created);
        assert_eq!(db.create_room("new", 10).await.unwrap(), CreateOutcome::AlreadyExists);
        assert!(db.room_exists("new").await.unwrap());
        assert!(!db.room_exists("ghost").await.unwrap());
        // 4 rooms exist now; a cap of 4 refuses the fifth.
        assert_eq!(db.create_room("fifth", 4).await.unwrap(), CreateOutcome::LimitReached);
        assert!(!db.room_exists("fifth").await.unwrap());
    }

    #[tokio::test]
    async fn history_is_last_n_oldest_first() {
        let db = repo();
        for i in 0..60 {
            db.insert_message("general", "bob", &format!("m{i}")).await.unwrap().unwrap();
        }
        db.insert_message("random", "bob", "other").await.unwrap().unwrap();
        let h = db.recent_messages("general", None, 50).await.unwrap();
        assert_eq!(h.len(), 50);
        assert_eq!(h.first().unwrap().text, "m10");
        assert_eq!(h.last().unwrap().text, "m59");
        assert!(h.windows(2).all(|w| w[0].id < w[1].id));
    }

    #[tokio::test]
    async fn insert_into_missing_room_is_none() {
        assert!(repo().insert_message("ghost", "bob", "x").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn prune_caps_messages_per_room() {
        let db = repo();
        for i in 0..10 {
            db.insert_message("general", "a", &format!("g{i}")).await.unwrap();
        }
        for i in 0..2 {
            db.insert_message("random", "a", &format!("r{i}")).await.unwrap();
        }
        let rep = db.prune(PrunePolicy { max_per_room: Some(3), ..Default::default() }).await.unwrap();
        assert_eq!(rep, PruneReport { messages_deleted: 7, rooms_deleted: 0 });
        let g = db.recent_messages("general", None, 50).await.unwrap();
        assert_eq!(g.iter().map(|m| m.text.as_str()).collect::<Vec<_>>(), ["g7", "g8", "g9"]);
        assert_eq!(db.recent_messages("random", None, 50).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn prune_by_age() {
        let db = repo();
        db.insert_message("general", "a", "old").await.unwrap();
        let none = db.prune(PrunePolicy { messages_before_ms: Some(now_ms() - 60_000), ..Default::default() }).await.unwrap();
        assert_eq!(none.messages_deleted, 0);
        let all = db.prune(PrunePolicy { messages_before_ms: Some(now_ms() + 60_000), ..Default::default() }).await.unwrap();
        assert_eq!(all.messages_deleted, 1);
        assert!(db.recent_messages("general", None, 50).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn prune_deletes_idle_custom_rooms_only() {
        let db = repo();
        db.create_room("idle", 50).await.unwrap();
        db.create_room("busy", 50).await.unwrap();
        db.create_room("occupied", 50).await.unwrap();
        db.insert_message("busy", "a", "hi").await.unwrap();
        let future = now_ms() + 60_000;
        // "busy" has a message newer than the cutoff only if the cutoff is in the past.
        let rep = db
            .prune(PrunePolicy { idle_rooms_before_ms: Some(future), protected_rooms: vec!["occupied".into()], ..Default::default() })
            .await
            .unwrap();
        assert_eq!(rep.rooms_deleted, 2); // idle + busy (its message is older than the future cutoff)
        let rooms = db.list_rooms().await.unwrap();
        assert!(rooms.contains(&"general".to_string()), "default rooms survive");
        assert!(rooms.contains(&"occupied".to_string()), "protected rooms survive");
        assert!(!rooms.contains(&"idle".to_string()));
        assert!(db.insert_message("idle", "a", "x").await.unwrap().is_none());

        // A recent message keeps a room alive against a past cutoff.
        db.create_room("fresh", 50).await.unwrap();
        db.insert_message("fresh", "a", "hello").await.unwrap();
        let rep = db.prune(PrunePolicy { idle_rooms_before_ms: Some(now_ms() - 60_000), ..Default::default() }).await.unwrap();
        assert_eq!(rep.rooms_deleted, 0);
    }
}
