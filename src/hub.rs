//! In-memory room registry: membership/presence and tokio broadcast channels.

use crate::protocol::{RoomInfo, ServerMsg};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

struct Room {
    tx: broadcast::Sender<ServerMsg>,
    members: HashMap<u64, String>,
}

pub struct Hub {
    rooms: Mutex<HashMap<String, Room>>,
    /// Global channel used to announce room-list changes to every connection.
    global: broadcast::Sender<ServerMsg>,
}

fn unique_sorted(members: &HashMap<u64, String>) -> Vec<String> {
    let mut v: Vec<String> = members.values().cloned().collect();
    v.sort_by_key(|s| s.to_lowercase());
    v.dedup();
    v
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

impl Hub {
    pub fn new() -> Hub {
        Hub { rooms: Mutex::new(HashMap::new()), global: broadcast::channel(64).0 }
    }

    pub fn subscribe_global(&self) -> broadcast::Receiver<ServerMsg> {
        self.global.subscribe()
    }

    pub fn announce(&self, msg: ServerMsg) {
        let _ = self.global.send(msg);
    }

    /// Add a connection to a room. Returns a receiver and the new presence list.
    pub fn join(&self, room: &str, conn: u64, user: &str) -> (broadcast::Receiver<ServerMsg>, Vec<String>) {
        let mut rooms = self.rooms.lock().unwrap();
        let r = rooms
            .entry(room.to_string())
            .or_insert_with(|| Room { tx: broadcast::channel(256).0, members: HashMap::new() });
        r.members.insert(conn, user.to_string());
        (r.tx.subscribe(), unique_sorted(&r.members))
    }

    /// Remove a connection. Returns the new presence list if it was a member.
    pub fn leave(&self, room: &str, conn: u64) -> Option<Vec<String>> {
        let mut rooms = self.rooms.lock().unwrap();
        let r = rooms.get_mut(room)?;
        r.members.remove(&conn)?;
        Some(unique_sorted(&r.members))
    }

    pub fn broadcast(&self, room: &str, msg: ServerMsg) {
        if let Some(r) = self.rooms.lock().unwrap().get(room) {
            let _ = r.tx.send(msg);
        }
    }

    pub fn users(&self, room: &str) -> Vec<String> {
        self.rooms.lock().unwrap().get(room).map(|r| unique_sorted(&r.members)).unwrap_or_default()
    }

    /// Room list with online counts, given the persisted room names.
    pub fn room_infos(&self, names: &[String]) -> Vec<RoomInfo> {
        let rooms = self.rooms.lock().unwrap();
        names
            .iter()
            .map(|n| RoomInfo { name: n.clone(), users: rooms.get(n).map(|r| unique_sorted(&r.members).len()).unwrap_or(0) })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn join_broadcast_leave() {
        let hub = Hub::new();
        let (mut rx1, u) = hub.join("r", 1, "alice");
        assert_eq!(u, vec!["alice"]);
        let (_rx2, u) = hub.join("r", 2, "bob");
        assert_eq!(u, vec!["alice", "bob"]);
        hub.broadcast("r", ServerMsg::Left { room: "r".into() });
        assert_eq!(rx1.recv().await.unwrap(), ServerMsg::Left { room: "r".into() });
        assert_eq!(hub.leave("r", 2).unwrap(), vec!["alice"]);
        assert!(hub.leave("r", 2).is_none());
    }

    #[test]
    fn same_user_two_connections_listed_once() {
        let hub = Hub::new();
        hub.join("r", 1, "alice");
        hub.join("r", 2, "alice");
        assert_eq!(hub.users("r"), vec!["alice"]);
        assert_eq!(
            hub.room_infos(&["r".into(), "x".into()]),
            vec![RoomInfo { name: "r".into(), users: 1 }, RoomInfo { name: "x".into(), users: 0 }]
        );
    }
}
