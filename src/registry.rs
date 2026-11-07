//! Connection admission: unique usernames, reserved names and connection limits.
//!
//! [`Registry::admit`] claims a username atomically and returns a [`ConnGuard`]; dropping the
//! guard (when the connection task ends, however it ends) releases the name and the slot.

use crate::error::ChatError;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Default)]
struct Inner {
    /// lowercase username -> display name currently holding it
    names: HashMap<String, String>,
    per_ip: HashMap<IpAddr, usize>,
}

pub struct Registry {
    inner: Mutex<Inner>,
    reserved: HashSet<String>,
    max_connections: usize,
    max_per_ip: usize,
}

impl Registry {
    pub fn new(reserved: &[String], max_connections: usize, max_per_ip: usize) -> Arc<Registry> {
        Arc::new(Registry {
            inner: Mutex::new(Inner::default()),
            reserved: reserved.iter().map(|s| s.to_lowercase()).collect(),
            max_connections,
            max_per_ip,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Number of live, admitted connections.
    pub fn connections(&self) -> usize {
        self.lock().names.len()
    }

    /// Claim `username` (case-insensitively unique across the server).
    pub fn admit(self: &Arc<Self>, username: &str, ip: Option<IpAddr>) -> Result<ConnGuard, ChatError> {
        let key = username.to_lowercase();
        if self.reserved.contains(&key) {
            return Err(ChatError::UsernameReserved(username.to_string()));
        }
        let mut g = self.lock();
        if g.names.len() >= self.max_connections {
            return Err(ChatError::ServerFull);
        }
        if let Some(ip) = ip
            && g.per_ip.get(&ip).copied().unwrap_or(0) >= self.max_per_ip
        {
            return Err(ChatError::TooManyConnections);
        }
        if g.names.contains_key(&key) {
            return Err(ChatError::UsernameTaken(username.to_string()));
        }
        g.names.insert(key.clone(), username.to_string());
        if let Some(ip) = ip {
            *g.per_ip.entry(ip).or_insert(0) += 1;
        }
        Ok(ConnGuard { registry: self.clone(), key, ip })
    }
}

/// Holds a username and connection slot until dropped.
pub struct ConnGuard {
    registry: Arc<Registry>,
    key: String,
    ip: Option<IpAddr>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut g = self.registry.lock();
        g.names.remove(&self.key);
        if let Some(ip) = self.ip
            && let Some(n) = g.per_ip.get_mut(&ip)
        {
            *n -= 1;
            if *n == 0 {
                g.per_ip.remove(&ip);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(max: usize, per_ip: usize) -> Arc<Registry> {
        Registry::new(&["admin".to_string()], max, per_ip)
    }

    #[test]
    fn usernames_are_unique_case_insensitively_and_released_on_drop() {
        let r = reg(10, 10);
        let a = r.admit("Alice", None).unwrap();
        assert!(matches!(r.admit("alice", None), Err(ChatError::UsernameTaken(_))));
        assert!(matches!(r.admit("ALICE", None), Err(ChatError::UsernameTaken(_))));
        assert!(r.admit("bob", None).is_ok());
        assert_eq!(r.connections(), 1); // bob's guard was dropped immediately
        drop(a);
        assert!(r.admit("alice", None).is_ok());
    }

    #[test]
    fn reserved_names_rejected() {
        let r = reg(10, 10);
        assert!(matches!(r.admit("Admin", None), Err(ChatError::UsernameReserved(_))));
    }

    #[test]
    fn global_and_per_ip_limits() {
        let r = reg(2, 1);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let _a = r.admit("a", Some(ip)).unwrap();
        assert!(matches!(r.admit("b", Some(ip)), Err(ChatError::TooManyConnections)));
        let _c = r.admit("c", Some(other)).unwrap();
        assert!(matches!(r.admit("d", Some("10.0.0.3".parse().unwrap())), Err(ChatError::ServerFull)));
    }

    #[test]
    fn per_ip_slot_is_released() {
        let r = reg(10, 1);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        drop(r.admit("a", Some(ip)).unwrap());
        assert!(r.admit("b", Some(ip)).is_ok());
    }
}
